package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"multiple-protocol-controller/internal/collector"
	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/conn"
	"multiple-protocol-controller/internal/protocol"
	bacnetProtocol "multiple-protocol-controller/internal/protocol/bacnet"
	opcuaProtocol "multiple-protocol-controller/internal/protocol/opcua"
	"multiple-protocol-controller/internal/store"
	"multiple-protocol-controller/pkg/logger"
	"multiple-protocol-controller/pkg/utils"

	"github.com/gopcua/opcua/ua"
	"go.uber.org/zap"
)

// ProcessCommand parses raw command payload and dispatches protocol-specific write operations.
// redisClient is optional: when provided, command result will be published to config.SendCmdResCh.
func ProcessCommand(redisClient *store.RedisClient, cmd config.Command4MPC) (processErr error) {
	started := time.Now()
	log := commandLogger(cmd.Uid)
	defer func() {
		if processErr != nil {
			// A batch may have written earlier items before processing stopped.
			log.Warn("device control", zap.String("stage", "execution_finished"), zap.String("result", "unknown"),
				zap.String("reason", "processing_error"), zap.Int64("elapsed_ms", time.Since(started).Milliseconds()))
		}
	}()
	cfgVal := config.IotCfgStore.Load()
	if cfgVal == nil {
		return errors.New("iot config not loaded")
	}
	cfg, ok := cfgVal.(config.IotCfgType)
	if !ok {
		return errors.New("iot config store contains unexpected data")
	}

	dispatchResults := make([]attrDispatchResult, 0)
	var manager *conn.Manager

	for _, item := range cmd.Data {
		itemStarted := time.Now()
		serial, _ := item["device_id"].(string)
		if serial == "" {
			log.Warn("device control", zap.String("stage", "device_execution_finished"), zap.String("result", "skipped"),
				zap.String("reason", "missing_device_id"), zap.Int64("elapsed_ms", time.Since(itemStarted).Milliseconds()))
			continue
		}
		device, exists := cfg.DeviceBySerial[serial]
		if !exists {
			log.Warn("device control", zap.String("stage", "device_execution_finished"), zap.String("result", "failed"),
				zap.String("device_serial", serial), zap.String("reason", "device_not_found"),
				zap.Int64("elapsed_ms", time.Since(itemStarted).Milliseconds()))
			dispatchResults = append(dispatchResults, attrDispatchResult{DeviceSerial: serial, Status: commandStatusFailed, Err: errors.New("device not found")})
			continue
		}

		resultOffset := len(dispatchResults)
		switch {
		case isBacnetProtocol(device.Config.Protocol):
			dispatchResults = append(dispatchResults, dispatchBacnet(device, item, cfg.DeviceBacnetCmdBySerial[serial])...)
		case isMqttProtocol(device.Config.Protocol):
			dispatchResults = append(dispatchResults, dispatchMqtt(device, item, cfg.DeviceMqttCmdBySerial[serial])...)
		case isOpcuaProtocol(device.Config.Protocol):
			dispatchResults = append(dispatchResults, dispatchOpcua(device, item, cfg.DeviceOpcuaCmdBySerial[serial])...)
		default:
			if manager == nil {
				var managerReady bool
				manager, managerReady = conn.Default()
				if !managerReady {
					return errors.New("TCP manager not initialised")
				}
			}
			dispatchResults = append(dispatchResults, dispatchModbus(manager, device, item, cfg.DeviceCmdBySerial[serial])...)
		}
		log.Info("device control", zap.String("stage", "device_execution_finished"),
			zap.String("result", commandLogResult(dispatchResults[resultOffset:])),
			zap.String("device_serial", serial), zap.String("device_id", device.Config.ID),
			zap.Int64("elapsed_ms", time.Since(itemStarted).Milliseconds()))
	}

	log.Info("device control", zap.String("stage", "execution_finished"),
		zap.String("result", commandLogResult(dispatchResults)), zap.Int("attribute_count", len(dispatchResults)),
		zap.Int64("elapsed_ms", time.Since(started).Milliseconds()))
	publishCommandResults(redisClient, cfg, cmd.Uid, dispatchResults)

	return nil
}

func commandLogger(uid string) *zap.Logger {
	log := logger.Log
	if log == nil {
		log = zap.NewNop()
	}
	return log.With(zap.String("command_id", uid))
}

func commandLogResult(results []attrDispatchResult) string {
	if len(results) == 0 {
		return "no_result"
	}
	status := commandStatusVerified
	for _, result := range results {
		status = mergeCommandStatus(status, normalizedCommandStatus(result))
	}
	return string(status)
}

func commandKeys(item map[string]interface{}) []string {
	keys := make([]string, 0, len(item))
	for key := range item {
		if key != "device_id" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func failedDispatch(serial string, identify string, err error) attrDispatchResult {
	return attrDispatchResult{DeviceSerial: serial, Identify: identify, Status: commandStatusFailed, Err: err}
}

func completedDispatch(serial string, identify string, result commandExecutionResult) attrDispatchResult {
	return attrDispatchResult{DeviceSerial: serial, Identify: identify, Status: result.Status, Err: result.Err}
}

func controlTimeout(device config.DeviceRuntime) time.Duration {
	timeout := time.Duration(device.ResponseTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		return 5 * time.Second
	}
	return timeout
}

func dispatchBacnet(device config.DeviceRuntime, item map[string]interface{}, commands []config.BacnetCommand) []attrDispatchResult {
	serial := device.Config.SerialNumber
	results := make([]attrDispatchResult, 0, len(item))
	manager := conn.DefaultBacnetManager()
	connectionConfig := conn.BacnetConfig{IP: device.GatewayIP, Port: device.GatewayPort, DeviceInstance: device.BacnetDeviceInstance}

	for _, key := range commandKeys(item) {
		command, found := findBacnetCommand(commands, key)
		if !found {
			results = append(results, failedDispatch(serial, key, errors.New("command mapping not found")))
			continue
		}
		target, err := normalizeBacnetCommandValue(item[key], command.DataType, command.DataTypeExplicit)
		if err != nil {
			results = append(results, failedDispatch(serial, key, err))
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), controlTimeout(device))
		if err := manager.WritePresentValue(ctx, connectionConfig, command.ObjectType, command.ObjectInstance, target); err != nil {
			cancel()
			results = append(results, failedDispatch(serial, key, err))
			continue
		}
		readParam, readable := findBacnetReadParam(device.BacnetReadPoints, key)
		if !readable {
			cancel()
			results = append(results, completedDispatch(serial, key, commandExecutionResult{Status: commandStatusUnverified}))
			continue
		}
		verification := verifyControlReadback(ctx, target, command.DataType, command.DataTypeExplicit, readParam.DataType, readParam.DataTypeExplicit, 100*time.Millisecond, func(readCtx context.Context) (any, error) {
			return manager.ReadPresentValue(readCtx, connectionConfig, readParam.ObjectType, readParam.ObjectInstance)
		})
		cancel()
		results = append(results, completedDispatch(serial, key, verification))
	}
	return results
}

func dispatchMqtt(device config.DeviceRuntime, item map[string]interface{}, commands []config.MqttCommand) []attrDispatchResult {
	serial := device.Config.SerialNumber
	results := make([]attrDispatchResult, 0, len(item))
	manager := conn.DefaultMqttManager()
	connectionConfig := conn.MqttConfig{
		Broker: device.MqttBroker, ClientID: device.MqttClientID, Username: device.MqttUsername,
		Password: device.MqttPassword, Qos: device.MqttQos, KeepAlive: device.MqttKeepAlive, CleanSession: device.MqttCleanSession,
	}

	for _, key := range commandKeys(item) {
		command, found := findMqttCommand(commands, key)
		if !found {
			results = append(results, failedDispatch(serial, key, errors.New("command mapping not found")))
			continue
		}
		payload, err := buildMqttPayload(command.Path, command.DataType, command.DataTypeExplicit, item[key])
		if err != nil {
			results = append(results, failedDispatch(serial, key, err))
			continue
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			results = append(results, failedDispatch(serial, key, err))
			continue
		}
		if err := manager.Publish(connectionConfig, command.PublishTopic, encoded, command.Qos); err != nil {
			results = append(results, failedDispatch(serial, key, err))
			continue
		}
		results = append(results, completedDispatch(serial, key, commandExecutionResult{Status: commandStatusUnverified}))
	}
	return results
}

func dispatchOpcua(device config.DeviceRuntime, item map[string]interface{}, commands []config.OpcuaCommand) []attrDispatchResult {
	serial := device.Config.SerialNumber
	results := make([]attrDispatchResult, 0, len(item))
	manager := conn.DefaultOpcuaManager()
	connectionConfig := conn.OpcuaConfig{
		Endpoint: device.OpcuaEndpoint, SecurityPolicy: device.OpcuaSecurityPolicy, SecurityMode: device.OpcuaSecurityMode,
		Username: device.Config.Username, Password: device.Config.Password,
	}

	for _, key := range commandKeys(item) {
		command, found := findOpcuaCommand(commands, key)
		if !found {
			results = append(results, failedDispatch(serial, key, errors.New("command mapping not found")))
			continue
		}
		target, err := normalizeOpcuaCommandValue(item[key], command.DataType, command.DataTypeExplicit)
		if err != nil {
			results = append(results, failedDispatch(serial, key, err))
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), controlTimeout(device))
		if err := manager.WriteNode(ctx, connectionConfig, command.NodeID, target); err != nil {
			cancel()
			results = append(results, failedDispatch(serial, key, err))
			continue
		}
		readParam, readable := findOpcuaReadParam(device.OpcuaReadPoints, key)
		if !readable {
			cancel()
			results = append(results, completedDispatch(serial, key, commandExecutionResult{Status: commandStatusUnverified}))
			continue
		}
		verification := verifyControlReadback(ctx, target, command.DataType, command.DataTypeExplicit, readParam.DataType, readParam.DataTypeExplicit, 100*time.Millisecond, func(readCtx context.Context) (any, error) {
			values, err := manager.ReadNodes(readCtx, connectionConfig, []string{readParam.NodeID})
			if err != nil {
				return nil, err
			}
			if len(values) == 0 || values[0] == nil || values[0].Status != ua.StatusOK || values[0].Value == nil {
				return nil, errors.New("opcua readback returned no valid value")
			}
			return normalizeOpcuaCommandValue(values[0].Value.Value(), readParam.DataType, readParam.DataTypeExplicit)
		})
		cancel()
		results = append(results, completedDispatch(serial, key, verification))
	}
	return results
}

func dispatchModbus(manager *conn.Manager, device config.DeviceRuntime, item map[string]interface{}, commands []config.ModbusCommand) []attrDispatchResult {
	serial := device.Config.SerialNumber
	results := make([]attrDispatchResult, 0, len(item))
	protoImpl, err := protocol.GetProtocol(device.Config.Protocol)
	if err != nil {
		for _, key := range commandKeys(item) {
			results = append(results, failedDispatch(serial, key, err))
		}
		return results
	}

	for _, key := range commandKeys(item) {
		command, found := findCommand(commands, key)
		if !found {
			results = append(results, failedDispatch(serial, key, errors.New("command mapping not found")))
			continue
		}
		frame, err := protoImpl.EncodeCommand(&protocol.CommandMessage{
			DeviceAddress: device.SlaveID, FunctionCode: command.FunctionCode, Address: command.Address,
			DataType: command.DataType, DataTypeExplicit: command.DataTypeExplicit, Endian: command.Endian, Value: item[key],
		})
		if err != nil {
			results = append(results, failedDispatch(serial, key, err))
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), controlTimeout(device))
		netConn, release, err := acquireControlConnection(ctx, manager, serial)
		if err != nil {
			cancel()
			results = append(results, failedDispatch(serial, key, err))
			continue
		}
		gatewaySerial := collector.GatewaySerial(device)
		if gatewaySerial != "" {
			collector.PauseGateway(gatewaySerial)
		}
		readParam, readable := findModbusReadParam(device.ReadPoints, key)
		var readParamPointer *config.ModbusParam
		if readable {
			readParamPointer = &readParam
		}
		result := executeModbusWrite(ctx, netConn, frame, device, command, readParamPointer, item[key])
		if gatewaySerial != "" {
			collector.ResumeGateway(gatewaySerial)
		}
		release()
		if result.Status == commandStatusFailed {
			manager.ResetConnection(serial)
		}
		cancel()
		results = append(results, completedDispatch(serial, key, result))
	}
	return results
}

func acquireControlConnection(ctx context.Context, manager *conn.Manager, serial string) (net.Conn, func(), error) {
	for {
		netConn, release, err := manager.ExclusiveConnection(ctx, serial)
		if err == nil {
			return netConn, release, nil
		}
		if !errors.Is(err, conn.ErrGatewayBusy) {
			return nil, nil, err
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func findModbusReadParam(params []config.ModbusParam, identify string) (config.ModbusParam, bool) {
	for _, param := range params {
		if param.Identify == identify && !param.ReadDisabled {
			return param, true
		}
	}
	return config.ModbusParam{}, false
}

func findBacnetReadParam(params []config.BacnetParam, identify string) (config.BacnetParam, bool) {
	for _, param := range params {
		if param.Identify == identify && !param.ReadDisabled {
			return param, true
		}
	}
	return config.BacnetParam{}, false
}

func findOpcuaReadParam(params []config.OpcuaParam, identify string) (config.OpcuaParam, bool) {
	for _, param := range params {
		if param.Identify == identify && !param.ReadDisabled {
			return param, true
		}
	}
	return config.OpcuaParam{}, false
}

func findCommand(cmds []config.ModbusCommand, identify string) (config.ModbusCommand, bool) {
	for _, cmd := range cmds {
		if cmd.Identify == identify {
			return cmd, true
		}
	}
	return config.ModbusCommand{}, false
}

func findOpcuaCommand(cmds []config.OpcuaCommand, identify string) (config.OpcuaCommand, bool) {
	for _, cmd := range cmds {
		if cmd.Identify == identify {
			return cmd, true
		}
	}
	return config.OpcuaCommand{}, false
}

func findBacnetCommand(cmds []config.BacnetCommand, identify string) (config.BacnetCommand, bool) {
	for _, cmd := range cmds {
		if cmd.Identify == identify {
			return cmd, true
		}
	}
	return config.BacnetCommand{}, false
}

func isOpcuaProtocol(proto string) bool {
	return protocol.NormalizeName(proto) == "OPCUA"
}

func isBacnetProtocol(proto string) bool {
	return protocol.NormalizeName(proto) == "BACNET"
}

type attrDispatchResult struct {
	DeviceSerial string
	Identify     string
	Status       commandStatus
	Err          error
}

type commandAttributeResult struct {
	Identify string        `json:"identify"`
	Status   commandStatus `json:"status"`
	Message  string        `json:"message,omitempty"`
}

type commandResultEntry struct {
	DeviceID           string                   `json:"device_id"`
	DeviceType         string                   `json:"device_type"`
	DeviceName         string                   `json:"device_name"`
	AddressID          string                   `json:"address_id"`
	Result             string                   `json:"control_command_result"`
	VerificationStatus commandStatus            `json:"verification_status"`
	Attributes         []commandAttributeResult `json:"attributes"`
}

type commandResultBody struct {
	Code      string               `json:"code"`
	Msg       string               `json:"msg"`
	Data      []commandResultEntry `json:"data"`
	Timestamp int64                `json:"timestampe"`
	Status    int                  `json:"status"`
	UID       string               `json:"uid"`
}

type commandResultEnvelope struct {
	Result commandResultBody `json:"result"`
}

func publishCommandResults(redisClient *store.RedisClient, cfg config.IotCfgType, uid string, results []attrDispatchResult) {
	if redisClient == nil || len(results) == 0 {
		return
	}
	publishCommandResultsWith(redisClient.Publish, cfg, uid, results)
}

func publishCommandResultsWith(publish func(context.Context, string, interface{}) error, cfg config.IotCfgType, uid string, results []attrDispatchResult) {
	started := time.Now()
	log := commandLogger(uid)
	envelope := buildCommandResultEnvelope(cfg, uid, results, time.Now())
	if len(envelope.Result.Data) == 0 {
		return
	}

	payload, err := json.Marshal(envelope)
	if err != nil {
		log.Warn("device control", zap.String("stage", "receipt_publish"), zap.String("result", "not_sent"),
			zap.String("reason", "receipt_encode_error"), zap.Int64("elapsed_ms", time.Since(started).Milliseconds()))
		return
	}
	if err := publish(context.Background(), config.SendCmdResCh, string(payload)); err != nil {
		log.Warn("device control", zap.String("stage", "receipt_publish"), zap.String("result", "unknown"),
			zap.String("reason", "receipt_publish_error"), zap.Int64("elapsed_ms", time.Since(started).Milliseconds()))
		return
	}
	log.Info("device control", zap.String("stage", "receipt_publish"), zap.String("result", "published"),
		zap.Int64("elapsed_ms", time.Since(started).Milliseconds()))
}

func buildCommandResultEnvelope(cfg config.IotCfgType, uid string, results []attrDispatchResult, now time.Time) commandResultEnvelope {
	grouped := make(map[string][]attrDispatchResult)
	order := make([]string, 0, len(results))
	for _, res := range results {
		if res.DeviceSerial == "" {
			continue
		}
		if _, exists := grouped[res.DeviceSerial]; !exists {
			order = append(order, res.DeviceSerial)
		}
		grouped[res.DeviceSerial] = append(grouped[res.DeviceSerial], res)
	}
	if len(grouped) == 0 {
		return commandResultEnvelope{}
	}

	entries := make([]commandResultEntry, 0, len(order))
	overallStatus := commandStatusVerified
	for _, serial := range order {
		entry := commandResultEntry{
			DeviceID:           serial,
			Result:             "command_executed_success",
			VerificationStatus: commandStatusVerified,
			Attributes:         make([]commandAttributeResult, 0, len(grouped[serial])),
		}
		if dev, exists := cfg.DeviceBySerial[serial]; exists {
			entry.DeviceType = strconv.Itoa(dev.Config.DeviceType)
			entry.DeviceName = dev.Config.DeviceName
			entry.AddressID = dev.Config.Position
		}
		for _, result := range grouped[serial] {
			attribute := commandAttributeResult{Identify: result.Identify, Status: normalizedCommandStatus(result)}
			if result.Err != nil {
				attribute.Message = result.Err.Error()
			}
			entry.Attributes = append(entry.Attributes, attribute)
			entry.VerificationStatus = mergeCommandStatus(entry.VerificationStatus, attribute.Status)
		}
		overallStatus = mergeCommandStatus(overallStatus, entry.VerificationStatus)

		failures := collectAttrFailures(grouped[serial])
		if failures != "" {
			entry.Result = "command_executed_fail: return " + failures
		} else if entry.VerificationStatus == commandStatusUnverified {
			entry.Result = "command_dispatched_unverified"
		}
		entries = append(entries, entry)
	}

	code := "0"
	msg := "success"
	if overallStatus == commandStatusFailed {
		code = "1"
		msg = "fail"
	} else if overallStatus == commandStatusUnverified {
		msg = "dispatched_unverified"
	}

	return commandResultEnvelope{
		Result: commandResultBody{
			Code:      code,
			Msg:       msg,
			Data:      entries,
			Timestamp: now.Unix(),
			Status:    200,
			UID:       uid,
		},
	}
}

func normalizedCommandStatus(result attrDispatchResult) commandStatus {
	if result.Err != nil {
		return commandStatusFailed
	}
	if result.Status == "" {
		return commandStatusVerified
	}
	return result.Status
}

func mergeCommandStatus(current commandStatus, next commandStatus) commandStatus {
	if current == commandStatusFailed || next == commandStatusFailed {
		return commandStatusFailed
	}
	if current == commandStatusUnverified || next == commandStatusUnverified {
		return commandStatusUnverified
	}
	return commandStatusVerified
}

func collectAttrFailures(items []attrDispatchResult) string {
	if len(items) == 0 {
		return ""
	}
	failures := make([]string, 0, len(items))
	for _, item := range items {
		if item.Err != nil {
			if item.Identify != "" {
				failures = append(failures, item.Identify+": "+item.Err.Error())
			} else {
				failures = append(failures, item.Err.Error())
			}
		}
	}
	return strings.Join(failures, "; ")
}

func isMqttProtocol(proto string) bool {
	return protocol.NormalizeName(proto) == "MQTT"
}

func findMqttCommand(cmds []config.MqttCommand, identify string) (config.MqttCommand, bool) {
	for _, cmd := range cmds {
		if cmd.Identify == identify {
			return cmd, true
		}
	}
	return config.MqttCommand{}, false
}

func buildMqttPayload(path string, dataType string, explicit bool, value interface{}) (map[string]interface{}, error) {
	target, err := normalizeCommandValue(dataType, explicit, value)
	if err != nil {
		return nil, err
	}
	// 如果有路径，构建嵌套结构
	if path != "" {
		result := make(map[string]interface{})
		parts := strings.Split(path, ".")
		current := result
		for i := 0; i < len(parts)-1; i++ {
			current[parts[i]] = make(map[string]interface{})
			current = current[parts[i]].(map[string]interface{})
		}
		current[parts[len(parts)-1]] = target
		return result, nil
	}
	// 无路径，直接返回值
	return map[string]interface{}{
		"value": target,
	}, nil
}

func normalizeCommandValue(dataType string, explicit bool, value interface{}) (interface{}, error) {
	if !explicit {
		return normalizeCommandValueLegacy(dataType, value), nil
	}
	dt := strings.ToLower(strings.TrimSpace(dataType))
	switch dt {
	case "bool":
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			return strings.ToLower(v) == "true" || v == "1", nil
		case float64:
			return v != 0, nil
		case json.Number:
			f, err := v.Float64()
			if err != nil {
				return nil, err
			}
			return f != 0, nil
		default:
			return value, nil
		}
	case "int8":
		return narrowCommandSigned(value, math.MinInt8, math.MaxInt8, func(n int64) interface{} { return int8(n) })
	case "int16":
		return narrowCommandSigned(value, math.MinInt16, math.MaxInt16, func(n int64) interface{} { return int16(n) })
	case "int32":
		return narrowCommandSigned(value, math.MinInt32, math.MaxInt32, func(n int64) interface{} { return int32(n) })
	case "int64":
		n, err := utils.CoerceExactInt64(value)
		if err != nil {
			return nil, err
		}
		return n, nil
	case "uint8":
		return narrowCommandUnsigned(value, math.MaxUint8, func(n uint64) interface{} { return uint8(n) })
	case "uint16":
		return narrowCommandUnsigned(value, math.MaxUint16, func(n uint64) interface{} { return uint16(n) })
	case "uint32":
		return narrowCommandUnsigned(value, math.MaxUint32, func(n uint64) interface{} { return uint32(n) })
	case "uint64":
		n, err := utils.CoerceExactUint64(value)
		if err != nil {
			return nil, err
		}
		return n, nil
	case "float32":
		f, err := utils.CoerceFloat64(value)
		if err != nil {
			return nil, err
		}
		if math.IsNaN(f) || math.IsInf(f, 0) || math.IsInf(float64(float32(f)), 0) {
			return nil, fmt.Errorf("value %v does not fit float32", f)
		}
		return float32(f), nil
	case "float64":
		f, err := utils.CoerceFloat64(value)
		if err != nil {
			return nil, err
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, fmt.Errorf("value %v is not a finite float64", f)
		}
		return f, nil
	case "int":
		switch v := value.(type) {
		case int64:
			return v, nil
		case float64:
			return int64(v), nil
		case json.Number:
			// Legacy behaviour for the old "int" name: the value used to arrive
			// as float64 and was truncated.
			f, err := v.Float64()
			if err != nil {
				return value, nil
			}
			return int64(f), nil
		case string:
			var i int64
			if _, err := fmt.Sscanf(v, "%d", &i); err == nil {
				return i, nil
			}
			return value, nil
		default:
			return value, nil
		}
	case "float", "double":
		switch v := value.(type) {
		case float64:
			return v, nil
		case int64:
			return float64(v), nil
		case json.Number:
			f, err := v.Float64()
			if err != nil {
				return value, nil
			}
			return f, nil
		case string:
			var f float64
			if _, err := fmt.Sscanf(v, "%f", &f); err == nil {
				return f, nil
			}
			return value, nil
		default:
			return value, nil
		}
	default:
		return value, nil
	}
}

func narrowCommandSigned(value interface{}, min int64, max int64, convert func(int64) interface{}) (interface{}, error) {
	n, err := utils.CoerceExactInt64(value)
	if err != nil {
		return nil, err
	}
	if n < min || n > max {
		return nil, fmt.Errorf("%d exceeds %T range", n, convert(0))
	}
	return convert(n), nil
}

func narrowCommandUnsigned(value interface{}, max uint64, convert func(uint64) interface{}) (interface{}, error) {
	n, err := utils.CoerceExactUint64(value)
	if err != nil {
		return nil, err
	}
	if n > max {
		return nil, fmt.Errorf("%d exceeds %T range", n, convert(0))
	}
	return convert(n), nil
}

func normalizeBacnetCommandValue(value interface{}, dataType string, explicit bool) (interface{}, error) {
	if explicit {
		return bacnetProtocol.NormalizeValueExplicit(value, dataType)
	}
	return bacnetProtocol.NormalizeValue(value, dataType)
}

func normalizeOpcuaCommandValue(value interface{}, dataType string, explicit bool) (interface{}, error) {
	if explicit {
		return opcuaProtocol.NormalizeValueExplicit(value, dataType)
	}
	return opcuaProtocol.NormalizeValue(value, dataType)
}

// normalizeCommandValueLegacy reproduces the original MQTT command conversion
// for properties without an explicit point data type.
func normalizeCommandValueLegacy(dataType string, value interface{}) interface{} {
	value = utils.LegacyJSONNumberValue(value)
	dt := strings.ToLower(strings.TrimSpace(dataType))
	switch dt {
	case "bool":
		switch v := value.(type) {
		case bool:
			return v
		case string:
			return strings.ToLower(v) == "true" || v == "1"
		case float64:
			return v != 0
		default:
			return value
		}
	case "int", "int32", "int64":
		switch v := value.(type) {
		case int64:
			return v
		case float64:
			return int64(v)
		case string:
			var i int64
			if _, err := fmt.Sscanf(v, "%d", &i); err == nil {
				return i
			}
			return value
		default:
			return value
		}
	case "float", "double":
		switch v := value.(type) {
		case float64:
			return v
		case int64:
			return float64(v)
		case string:
			var f float64
			if _, err := fmt.Sscanf(v, "%f", &f); err == nil {
				return f
			}
			return value
		default:
			return value
		}
	default:
		return value
	}
}
