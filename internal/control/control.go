package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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

	"go.uber.org/zap"
)

// ProcessCommand parses raw command payload and dispatches write operations over modbus.
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

	var dispatchResults []attrDispatchResult
	var manager *conn.Manager

	for _, item := range cmd.Data {
		serial, _ := item["device_id"].(string)
		success := false
		errMsg := ""
		if serial == "" {
			logger.Log.Warn("command item missing device_id", zap.Any("data", item))
			errMsg = "missing device_id"
			dispatchResults = append(dispatchResults, attrDispatchResult{DeviceSerial: serial, Identify: "", Err: errors.New(errMsg)})
			continue
		}
		device, exists := cfg.DeviceBySerial[serial]
		if !exists {
			logger.Log.Warn("device not found for command", zap.String("serial", serial))
			errMsg = "device not found"
			dispatchResults = append(dispatchResults, attrDispatchResult{DeviceSerial: serial, Identify: "", Err: errors.New(errMsg)})
			continue
		}

		if isBacnetProtocol(device.Config.Protocol) {
			commands := cfg.DeviceBacnetCmdBySerial[serial]
			for key, rawVal := range item {
				if key == "device_id" {
					continue
				}
				commandMeta, found := findBacnetCommand(commands, key)
				if !found {
					logger.Log.Warn("bacnet command mapping not found", zap.String("serial", serial), zap.String("identify", key))
					errMsg = "command mapping not found"
					continue
				}

				timeout := time.Duration(device.ResponseTimeoutMs)
				if timeout == 0 {
					timeout = 5000
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout*time.Millisecond)
				value, err := bacnetProtocol.NormalizeValue(rawVal, commandMeta.DataType)
				if err != nil {
					logger.Log.Warn("bacnet normalize command value failed", zap.String("serial", serial), zap.String("identify", key), zap.Error(err))
					errMsg = err.Error()
					cancel()
					continue
				}

				bacnetCfg := conn.BacnetConfig{
					IP:             device.GatewayIP,
					Port:           device.GatewayPort,
					DeviceInstance: device.BacnetDeviceInstance,
				}
				if err := conn.DefaultBacnetManager().WritePresentValue(ctx, bacnetCfg, commandMeta.ObjectType, commandMeta.ObjectInstance, value); err != nil {
					logger.Log.Warn("bacnet send command failed", zap.String("serial", serial), zap.String("identify", key), zap.Error(err))
					errMsg = err.Error()
				} else {
					success = true
					errMsg = ""
				}
				cancel()
			}

			dispatchResults = append(dispatchResults, attrDispatchResult{
				DeviceSerial: serial,
				Identify:     "",
				Err: func() error {
					if success {
						return nil
					}
					if errMsg == "" {
						return nil
					}
					return errors.New(errMsg)
				}(),
			})
			continue
		}

		// 处理 MQTT 协议
		if isMqttProtocol(device.Config.Protocol) {
			commands := cfg.DeviceMqttCmdBySerial[serial]
			for key, rawVal := range item {
				if key == "device_id" {
					continue
				}
				commandMeta, found := findMqttCommand(commands, key)
				if !found {
					logger.Log.Warn("mqtt command mapping not found", zap.String("serial", serial), zap.String("identify", key))
					errMsg = "command mapping not found"
					continue
				}

				// 构建 MQTT 配置
				mqttCfg := conn.MqttConfig{
					Broker:       device.MqttBroker,
					ClientID:     device.MqttClientID,
					Username:     device.MqttUsername,
					Password:     device.MqttPassword,
					Qos:          device.MqttQos,
					KeepAlive:    device.MqttKeepAlive,
					CleanSession: device.MqttCleanSession,
				}

				// 构建消息载荷
				payload := buildMqttPayload(commandMeta.Path, commandMeta.DataType, rawVal)
				payloadBytes, err := json.Marshal(payload)
				if err != nil {
					logger.Log.Warn("mqtt build payload failed", zap.String("serial", serial), zap.String("identify", key), zap.Error(err))
					errMsg = err.Error()
					continue
				}

				// 发布消息
				if err := conn.DefaultMqttManager().Publish(mqttCfg, commandMeta.PublishTopic, payloadBytes, commandMeta.Qos); err != nil {
					logger.Log.Warn("mqtt publish command failed", zap.String("serial", serial), zap.String("identify", key), zap.Error(err))
					errMsg = err.Error()
				} else {
					success = true
					errMsg = ""
				}
			}

			dispatchResults = append(dispatchResults, attrDispatchResult{
				DeviceSerial: serial,
				Identify:     "",
				Err: func() error {
					if success {
						return nil
					}
					if errMsg == "" {
						return nil
					}
					return errors.New(errMsg)
				}(),
			})
			continue
		}

		if isOpcuaProtocol(device.Config.Protocol) {
			commands := cfg.DeviceOpcuaCmdBySerial[serial]
			for key, rawVal := range item {
				if key == "device_id" {
					continue
				}
				commandMeta, found := findOpcuaCommand(commands, key)
				if !found {
					logger.Log.Warn("command mapping not found", zap.String("serial", serial), zap.String("identify", key))
					errMsg = "command mapping not found"
					continue
				}

				timeout := time.Duration(device.ResponseTimeoutMs)
				if timeout == 0 {
					timeout = 5000
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout*time.Millisecond)
				value, err := opcuaProtocol.NormalizeValue(rawVal, commandMeta.DataType)
				if err != nil {
					logger.Log.Warn("opcua normalize command value failed", zap.String("serial", serial), zap.String("identify", key), zap.Error(err))
					errMsg = err.Error()
					cancel()
					continue
				}

				cfg := conn.OpcuaConfig{
					Endpoint:       device.OpcuaEndpoint,
					SecurityPolicy: device.OpcuaSecurityPolicy,
					SecurityMode:   device.OpcuaSecurityMode,
					Username:       device.Config.Username,
					Password:       device.Config.Password,
				}
				if err := conn.DefaultOpcuaManager().WriteNode(ctx, cfg, commandMeta.NodeID, value); err != nil {
					logger.Log.Warn("opcua send command failed", zap.String("serial", serial), zap.String("identify", key), zap.Error(err))
					errMsg = err.Error()
				} else {
					success = true
					errMsg = ""
				}
				cancel()
			}

			dispatchResults = append(dispatchResults, attrDispatchResult{
				DeviceSerial: serial,
				Identify:     "",
				Err: func() error {
					if success {
						return nil
					}
					if errMsg == "" {
						return nil
					}
					return errors.New(errMsg)
				}(),
			})
			continue
		}

		if manager == nil {
			var ok bool
			manager, ok = conn.Default()
			if !ok {
				return errors.New("TCP manager not initialised")
			}
		}
		commands := cfg.DeviceCmdBySerial[serial]

		for key, rawVal := range item {
			if key == "device_id" {
				continue
			}
			commandMeta, found := findCommand(commands, key)
			if !found {
				logger.Log.Warn("command mapping not found", zap.String("serial", serial), zap.String("identify", key))
				errMsg = "command mapping not found"
				continue
			}

			protoImpl, err := protocol.GetProtocol(device.Config.Protocol)
			if err != nil {
				logger.Log.Warn("protocol not registered", zap.String("protocol", device.Config.Protocol), zap.Error(err))
				continue
			}

			gatewaySerial := collector.GatewaySerial(device)
			if gatewaySerial != "" {
				collector.PauseGateway(gatewaySerial)
				defer collector.ResumeGateway(gatewaySerial)
			}

			payload := &protocol.CommandMessage{
				DeviceAddress: device.SlaveID,
				FunctionCode:  commandMeta.FunctionCode,
				Address:       commandMeta.Address,
				DataType:      commandMeta.DataType,
				Endian:        commandMeta.Endian,
				Value:         rawVal,
			}
			frame, err := protoImpl.EncodeCommand(payload)
			if err != nil {
				logger.Log.Warn("encode command failed", zap.String("serial", serial), zap.String("identify", key), zap.Error(err))
				errMsg = err.Error()
				continue
			}

			timeout := time.Duration(device.ResponseTimeoutMs)
			if timeout == 0 {
				timeout = 5000
			}
			timeoutDur := timeout * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), timeoutDur)
			var (
				netConn net.Conn
				release func()
			)
			for {
				netConn, release, err = manager.ExclusiveConnection(ctx, serial)
				if err == nil {
					break
				}
				if errors.Is(err, conn.ErrGatewayBusy) {
					select {
					case <-ctx.Done():
						break
					case <-time.After(100 * time.Millisecond):
					}
					if ctx.Err() == nil {
						continue
					}
				}
				logger.Log.Warn("acquire connection failed", zap.String("serial", serial), zap.Error(err))
				cancel()
				netConn = nil
				break
			}
			if netConn == nil {
				errMsg = "acquire connection failed: " + err.Error()
				continue
			}

			if _, err := netConn.Write(frame); err != nil {
				logger.Log.Warn("send command failed", zap.String("serial", serial), zap.String("identify", key), zap.Error(err))
				errMsg = err.Error()
			} else {
				_ = collector.WaitCommandAck(ctx, netConn, device, commandMeta)
				success = true
				errMsg = ""
			}
			release()
			cancel()
		}
		dispatchResults = append(dispatchResults, attrDispatchResult{
			DeviceSerial: serial,
			Identify:     "",
			Err: func() error {
				if success {
					return nil
				}
				if errMsg == "" {
					return nil
				}
				return errors.New(errMsg)
			}(),
		})
	}

	publishCommandResults(redisClient, cfg, cmd.Uid, dispatchResults)

	return nil
}

// PublishCommandResult publishes the control result (succ/fail) to redis if SendCmdResCh is configured.
func PublishCommandResult(redisClient *store.RedisClient, channel string, payload interface{}) {
	if redisClient == nil || channel == "" {
		return
	}
	data, err := json.Marshal(payload)
	if err != nil {
		logger.Log.Warn("marshal command result failed", zap.Error(err))
		return
	}
	if err := redisClient.Publish(context.Background(), channel, string(data)); err != nil {
		logger.Log.Warn("publish command result failed", zap.Error(err))
	}
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

func isOpcuaProtocol(protocol string) bool {
	return strings.EqualFold(strings.TrimSpace(protocol), "opcua")
}

func isBacnetProtocol(protocol string) bool {
	return strings.EqualFold(strings.TrimSpace(protocol), "bacnet")
}

type attrDispatchResult struct {
	DeviceSerial string
	Identify     string
	Err          error
}

type commandResultEntry struct {
	DeviceID   string `json:"device_id"`
	DeviceType string `json:"device_type"`
	DeviceName string `json:"device_name"`
	AddressID  string `json:"address_id"`
	Result     string `json:"control_command_result"`
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
		return
	}

	entries := make([]commandResultEntry, 0, len(order))
	for _, serial := range order {
		entry := commandResultEntry{
			DeviceID: serial,
			Result:   "command_executed_success",
		}
		if dev, exists := cfg.DeviceBySerial[serial]; exists {
			entry.DeviceType = strconv.Itoa(dev.Config.DeviceType)
			entry.DeviceName = dev.Config.DeviceName
			entry.AddressID = dev.Config.Position
		}
		failures := collectAttrFailures(grouped[serial])
		if failures != "" {
			entry.Result = "command_executed_fail: return " + failures
		}
		entries = append(entries, entry)
	}

	if len(entries) == 0 {
		return
	}

	success := true
	for _, e := range entries {
		if e.Result != "command_executed_success" {
			success = false
			break
		}
	}
	code := "0"
	msg := "success"
	if !success {
		code = "1"
		msg = "fail"
	}

	envelope := commandResultEnvelope{
		Result: commandResultBody{
			Code:      code,
			Msg:       msg,
			Data:      entries,
			Timestamp: time.Now().Unix(),
			Status:    200,
			UID:       uid,
		},
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

func isMqttProtocol(protocol string) bool {
	return strings.EqualFold(strings.TrimSpace(protocol), "mqtt")
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
