package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	"github.com/gopcua/opcua/ua"
	"go.uber.org/zap"
)

// ProcessCommand parses raw command payload and dispatches protocol-specific write operations.
// redisClient is optional: when provided, command result will be published to config.SendCmdResCh.
func ProcessCommand(redisClient *store.RedisClient, cmd config.Command4MPC) error {
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
		serial, _ := item["device_id"].(string)
		if serial == "" {
			logger.Log.Warn("command item missing device_id", zap.Any("data", item))
			continue
		}
		device, exists := cfg.DeviceBySerial[serial]
		if !exists {
			logger.Log.Warn("device not found for command", zap.String("serial", serial))
			dispatchResults = append(dispatchResults, attrDispatchResult{DeviceSerial: serial, Status: commandStatusFailed, Err: errors.New("device not found")})
			continue
		}

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
	}

	publishCommandResults(redisClient, cfg, cmd.Uid, dispatchResults)

	return nil
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
		target, err := bacnetProtocol.NormalizeValue(item[key], command.DataType)
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
		verification := verifyControlReadback(ctx, target, command.DataType, 100*time.Millisecond, func(readCtx context.Context) (any, error) {
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
		payload, err := json.Marshal(buildMqttPayload(command.Path, command.DataType, item[key]))
		if err != nil {
			results = append(results, failedDispatch(serial, key, err))
			continue
		}
		if err := manager.Publish(connectionConfig, command.PublishTopic, payload, command.Qos); err != nil {
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
		target, err := opcuaProtocol.NormalizeValue(item[key], command.DataType)
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
		verification := verifyControlReadback(ctx, target, command.DataType, 100*time.Millisecond, func(readCtx context.Context) (any, error) {
			values, err := manager.ReadNodes(readCtx, connectionConfig, []string{readParam.NodeID})
			if err != nil {
				return nil, err
			}
			if len(values) == 0 || values[0] == nil || values[0].Status != ua.StatusOK || values[0].Value == nil {
				return nil, errors.New("opcua readback returned no valid value")
			}
			return opcuaProtocol.NormalizeValue(values[0].Value.Value(), command.DataType)
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
			DataType: command.DataType, Endian: command.Endian, Value: item[key],
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

	envelope := buildCommandResultEnvelope(cfg, uid, results, time.Now())
	if len(envelope.Result.Data) == 0 {
		return
	}

	payload, err := json.Marshal(envelope)
	if err != nil {
		logger.Log.Warn("marshal command result failed", zap.Error(err))
		return
	}
	if err := redisClient.Publish(context.Background(), config.SendCmdResCh, string(payload)); err != nil {
		logger.Log.Warn("publish command result failed", zap.Error(err))
	}
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

func buildMqttPayload(path string, dataType string, value interface{}) map[string]interface{} {
	// 如果有路径，构建嵌套结构
	if path != "" {
		result := make(map[string]interface{})
		parts := strings.Split(path, ".")
		current := result
		for i := 0; i < len(parts)-1; i++ {
			current[parts[i]] = make(map[string]interface{})
			current = current[parts[i]].(map[string]interface{})
		}
		current[parts[len(parts)-1]] = normalizeCommandValue(dataType, value)
		return result
	}
	// 无路径，直接返回值
	return map[string]interface{}{
		"value": normalizeCommandValue(dataType, value),
	}
}

func normalizeCommandValue(dataType string, value interface{}) interface{} {
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
