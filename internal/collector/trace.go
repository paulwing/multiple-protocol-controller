package collector

import (
	"encoding/hex"
	"net"
	"os"
	"strconv"
	"strings"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/pkg/logger"

	"go.uber.org/zap"
)

func shouldTraceModbus(device config.DeviceRuntime, param config.ModbusParam) bool {
	if logger.Log == nil {
		return false
	}
	// Allow enabling trace via device.SerialNumber or param.Identify hooks later.
	if device.Config.SerialNumber == os.Getenv("MODBUS_TRACE_DEVICES") && param.Identify == os.Getenv("MODBUS_TRACE_PARAM") {
		return true
	}
	return false
}

func traceModbusFrame(direction string, device config.DeviceRuntime, param config.ModbusParam, payload []byte, extra map[string]interface{}) {
	if !shouldTraceModbus(device, param) {
		return
	}
	frame := map[string]interface{}{
		"tag":           "MODBUS_TRACE",
		"direction":     direction,
		"deviceSerial":  device.Config.SerialNumber,
		"functionCode":  param.FunctionCode,
		"param":         param.Identify,
		"count":         len(payload),
		"address":       param.Address,
		"gatewaySerial": gatewaySerial(device),
		"gatewayAddress": func() string {
			ip, port := gatewayAddress(device)
			if ip == "" || port == 0 {
				return ""
			}
			return net.JoinHostPort(ip, strconv.Itoa(int(port)))
		}(),
	}
	if payload != nil {
		frame["payload"] = strings.ToUpper(hex.EncodeToString(payload))
		frame["length"] = len(payload)
	}
	for k, v := range extra {
		frame[k] = v
	}
	logger.Log.Info("modbus trace", zap.Any("frame", frame))
}

func gatewaySerial(device config.DeviceRuntime) string {
	if device.GatewaySerial != "" {
		return device.GatewaySerial
	}
	return device.Config.SerialNumber
}

func gatewayAddress(device config.DeviceRuntime) (string, uint16) {
	if device.GatewayIP != "" && device.GatewayPort != 0 {
		return device.GatewayIP, device.GatewayPort
	}
	return device.Config.GatewayInfo.IP, uint16(device.Config.GatewayInfo.Port)
}
