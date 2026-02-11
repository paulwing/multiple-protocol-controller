package config

import (
	"encoding/json"
	"fmt"
	"multiple-protocol-controller/internal/protocol"
	"multiple-protocol-controller/internal/protocol/modbusRtu"
	"multiple-protocol-controller/internal/protocol/opcua"
	"multiple-protocol-controller/pkg/logger"
	"multiple-protocol-controller/pkg/utils"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

// IotCfgType is the runtime configuration snapshot produced from the raw DeviceConfig list.
// It keeps devices as-is but augments them with parsed modbus point information.
type IotCfgType struct {
	Devices                []DeviceRuntime
	DeviceBySerial         map[string]DeviceRuntime
	DeviceCmdBySerial      map[string][]ModbusCommand
	DeviceOpcuaCmdBySerial map[string][]OpcuaCommand
}

// DeviceRuntime wraps DeviceConfig with resolved gateway info and parsed modbus points.
type DeviceRuntime struct {
	Config            DeviceConfig
	SlaveID           uint64
	GatewaySerial     string
	GatewayID         string
	GatewayIP         string
	GatewayPort       uint16
	ResponseTimeoutMs int

	ReadPoints  []ModbusParam
	WritePoints []ModbusCommand

	OpcuaEndpoint       string
	OpcuaSecurityPolicy string
	OpcuaSecurityMode   string
	OpcuaReadPoints     []OpcuaParam
	OpcuaWritePoints    []OpcuaCommand
}

// ModbusParam 描述单个采集点。
type ModbusParam struct {
	PropertyID   string
	DeviceID     string
	SerialNumber string
	Identify     string
	Name         string
	DataType     string
	FunctionCode int
	Address      uint64
	DeviceAddr   uint64
	Quantity     int
	Bit          int
	Passive      bool
	Unit         string
	Endian       string
}

// ModbusCommand 描述单个控制点。
type ModbusCommand struct {
	PropertyID   string
	DeviceID     string
	SerialNumber string
	Identify     string
	Name         string
	DataType     string
	FunctionCode int
	Address      uint64
	DeviceAddr   uint64
	Quantity     int
	Unit         string
	Endian       string
}

// OpcuaParam 描述单个 OPC UA 采集点。
type OpcuaParam struct {
	PropertyID   string
	DeviceID     string
	SerialNumber string
	Identify     string
	Name         string
	DataType     string
	NodeID       string
	Passive      bool
	Unit         string
}

// OpcuaCommand 描述单个 OPC UA 控制点。
type OpcuaCommand struct {
	PropertyID   string
	DeviceID     string
	SerialNumber string
	Identify     string
	Name         string
	DataType     string
	NodeID       string
	Unit         string
}

// BuildRuntimeConfig converts raw device configs into DeviceRuntime slices.
func BuildRuntimeConfig(devices []DeviceConfig) (IotCfgType, error) {
	runtime := IotCfgType{
		DeviceBySerial:         make(map[string]DeviceRuntime),
		DeviceCmdBySerial:      make(map[string][]ModbusCommand),
		DeviceOpcuaCmdBySerial: make(map[string][]OpcuaCommand),
	}

	for _, dev := range devices {
		rt, err := buildDeviceRuntime(dev)
		if err != nil {
			logger.Log.Warn("skip device with invalid config", zap.Error(err), zap.String("deviceId", dev.ID), zap.String("serial", dev.SerialNumber))
			continue
		}
		runtime.Devices = append(runtime.Devices, rt)
		runtime.DeviceBySerial[dev.SerialNumber] = rt
		if len(rt.WritePoints) > 0 {
			runtime.DeviceCmdBySerial[dev.SerialNumber] = append(runtime.DeviceCmdBySerial[dev.SerialNumber], rt.WritePoints...)
		}
		if len(rt.OpcuaWritePoints) > 0 {
			runtime.DeviceOpcuaCmdBySerial[dev.SerialNumber] = append(runtime.DeviceOpcuaCmdBySerial[dev.SerialNumber], rt.OpcuaWritePoints...)
		}
	}
	return runtime, nil
}

func buildDeviceRuntime(dev DeviceConfig) (DeviceRuntime, error) {
	port := uint16(dev.GatewayInfo.Port)
	ip := strings.TrimSpace(dev.GatewayInfo.IP)
	if ip == "" || port == 0 {
		return DeviceRuntime{}, fmt.Errorf("gateway address missing")
	}

	gatewaySerial := dev.GatewayInfo.ID
	if gatewaySerial == "" {
		gatewaySerial = dev.SerialNumber
	}

	slaveID := uint64(0)
	var readPts []ModbusParam
	var writePts []ModbusCommand
	var opcuaRead []OpcuaParam
	var opcuaWrite []OpcuaCommand
	var opcuaEndpoint string
	var opcuaPolicy string
	var opcuaMode string

	if isModbusProtocol(dev.Protocol) {
		slaveID = parseSlaveID(dev)
		readPts, writePts = parseModbusPoints(dev, slaveID)
	}
	if isOpcuaProtocol(dev.Protocol) {
		opcuaRead, opcuaWrite = parseOpcuaPoints(dev)
		opcuaEndpoint = buildOpcuaEndpoint(dev)
		opcuaPolicy, opcuaMode = parseOpcuaSecurity(dev.Params)
	}

	timeout := dev.AcqFreq
	if timeout <= 0 {
		timeout = 5000
	}

	return DeviceRuntime{
		Config:              dev,
		SlaveID:             slaveID,
		GatewaySerial:       gatewaySerial,
		GatewayID:           dev.GatewayInfo.ID,
		GatewayIP:           ip,
		GatewayPort:         port,
		ResponseTimeoutMs:   timeout,
		ReadPoints:          readPts,
		WritePoints:         writePts,
		OpcuaEndpoint:       opcuaEndpoint,
		OpcuaSecurityPolicy: opcuaPolicy,
		OpcuaSecurityMode:   opcuaMode,
		OpcuaReadPoints:     opcuaRead,
		OpcuaWritePoints:    opcuaWrite,
	}, nil
}

func parseModbusPoints(dev DeviceConfig, slaveID uint64) ([]ModbusParam, []ModbusCommand) {
	var (
		params   []ModbusParam
		commands []ModbusCommand
	)

	if strings.TrimSpace(dev.Protocol) == "" {
		return params, commands
	}
	protoImpl, err := protocol.GetProtocol(dev.Protocol)
	if err != nil {
		logger.Log.Warn("unsupported protocol for device", zap.String("protocol", dev.Protocol), zap.String("serial", dev.SerialNumber), zap.Error(err))
		return params, commands
	}

	for _, prop := range dev.Properties {
		parsed, err := protoImpl.ParsePropProtocol(prop.Protocol)
		if err != nil {
			logger.Log.Warn("parse property protocol failed", zap.String("deviceSerial", dev.SerialNumber), zap.String("property", prop.Key), zap.Error(err))
			continue
		}

		switch cfg := parsed.(type) {
		case *modbusRtu.ModbusProtocol:
			for _, pt := range cfg.Points.Read {
				fc, fcErr := strconv.Atoi(pt.FunctionCode)
				if fcErr != nil {
					logger.Log.Warn("invalid function code", zap.String("deviceSerial", dev.SerialNumber), zap.String("property", prop.Key), zap.Error(fcErr))
					continue
				}
				addr, addrErr := utils.ParseUintDecHex(pt.Address)
				if addrErr != nil {
					logger.Log.Warn("invalid modbus address", zap.String("deviceSerial", dev.SerialNumber), zap.String("property", prop.Key), zap.Error(addrErr))
					continue
				}
				params = append(params, ModbusParam{
					PropertyID:   prop.ID,
					DeviceID:     dev.ID,
					SerialNumber: dev.SerialNumber,
					Identify:     prop.Key,
					Name:         prop.Name,
					DataType:     prop.Type,
					FunctionCode: fc,
					Address:      addr,
					DeviceAddr:   slaveID,
					Quantity:     pt.Quantity,
					Bit:          pt.Bit,
					Passive:      len(prop.Access) > 0 && prop.Access[0] != '1',
					Unit:         prop.Unit,
					Endian:       strings.ToUpper(strings.TrimSpace(pt.Endian)),
				})
			}
			for _, pt := range cfg.Points.Write {
				fc, fcErr := strconv.Atoi(pt.FunctionCode)
				if fcErr != nil {
					logger.Log.Warn("invalid function code", zap.String("deviceSerial", dev.SerialNumber), zap.String("property", prop.Key), zap.Error(fcErr))
					continue
				}
				addr, addrErr := utils.ParseUintDecHex(pt.Address)
				if addrErr != nil {
					logger.Log.Warn("invalid modbus address", zap.String("deviceSerial", dev.SerialNumber), zap.String("property", prop.Key), zap.Error(addrErr))
					continue
				}
				commands = append(commands, ModbusCommand{
					PropertyID:   prop.ID,
					DeviceID:     dev.ID,
					SerialNumber: dev.SerialNumber,
					Identify:     prop.Key,
					Name:         prop.Name,
					DataType:     prop.Type,
					FunctionCode: fc,
					Address:      addr,
					DeviceAddr:   slaveID,
					Quantity:     pt.Quantity,
					Unit:         prop.Unit,
					Endian:       strings.ToUpper(strings.TrimSpace(pt.Endian)),
				})
			}
		default:
			logger.Log.Warn("protocol converter not implemented", zap.String("protocol", dev.Protocol))
		}
	}

	return params, commands
}

func parseOpcuaPoints(dev DeviceConfig) ([]OpcuaParam, []OpcuaCommand) {
	var (
		params   []OpcuaParam
		commands []OpcuaCommand
	)

	if strings.TrimSpace(dev.Protocol) == "" {
		return params, commands
	}
	protoImpl, err := protocol.GetProtocol(dev.Protocol)
	if err != nil {
		logger.Log.Warn("unsupported protocol for device", zap.String("protocol", dev.Protocol), zap.String("serial", dev.SerialNumber), zap.Error(err))
		return params, commands
	}

	for _, prop := range dev.Properties {
		parsed, err := protoImpl.ParsePropProtocol(prop.Protocol)
		if err != nil {
			logger.Log.Warn("parse property protocol failed", zap.String("deviceSerial", dev.SerialNumber), zap.String("property", prop.Key), zap.Error(err))
			continue
		}

		switch cfg := parsed.(type) {
		case *opcua.OpcuaProtocol:
			for _, node := range cfg.Nodes.Read {
				nodeID := strings.TrimSpace(node.NodeID)
				if nodeID == "" {
					logger.Log.Warn("opcua nodeId missing", zap.String("deviceSerial", dev.SerialNumber), zap.String("property", prop.Key))
					continue
				}
				params = append(params, OpcuaParam{
					PropertyID:   prop.ID,
					DeviceID:     dev.ID,
					SerialNumber: dev.SerialNumber,
					Identify:     prop.Key,
					Name:         prop.Name,
					DataType:     prop.Type,
					NodeID:       nodeID,
					Passive:      len(prop.Access) > 0 && prop.Access[0] != '1',
					Unit:         prop.Unit,
				})
			}
			for _, node := range cfg.Nodes.Write {
				nodeID := strings.TrimSpace(node.NodeID)
				if nodeID == "" {
					logger.Log.Warn("opcua nodeId missing", zap.String("deviceSerial", dev.SerialNumber), zap.String("property", prop.Key))
					continue
				}
				commands = append(commands, OpcuaCommand{
					PropertyID:   prop.ID,
					DeviceID:     dev.ID,
					SerialNumber: dev.SerialNumber,
					Identify:     prop.Key,
					Name:         prop.Name,
					DataType:     prop.Type,
					NodeID:       nodeID,
					Unit:         prop.Unit,
				})
			}
		default:
			logger.Log.Warn("protocol converter not implemented", zap.String("protocol", dev.Protocol))
		}
	}

	return params, commands
}

func parseSlaveID(dev DeviceConfig) uint64 {
	if len(strings.TrimSpace(dev.Protocol)) == 0 {
		return 1
	}
	if strings.EqualFold(dev.Protocol, "modbusrtu") {
		var temp struct {
			SlaveID string `json:"slaveID"`
		}
		if err := json.Unmarshal(dev.Params, &temp); err == nil && strings.TrimSpace(temp.SlaveID) != "" {
			if v, err := utils.ParseUintDecHex(temp.SlaveID); err == nil {
				return v
			}
		}
	}
	return 1
}

func parseOpcuaSecurity(raw json.RawMessage) (string, string) {
	policy := "None"
	mode := "None"
	if len(raw) == 0 {
		return policy, mode
	}
	var temp struct {
		SecurityPolicy string `json:"securityPolicy"`
		SecurityMode   string `json:"securityMode"`
	}
	if err := json.Unmarshal(raw, &temp); err != nil {
		return policy, mode
	}
	if strings.TrimSpace(temp.SecurityPolicy) != "" {
		policy = strings.TrimSpace(temp.SecurityPolicy)
	}
	if strings.TrimSpace(temp.SecurityMode) != "" {
		mode = strings.TrimSpace(temp.SecurityMode)
	}
	return policy, mode
}

func buildOpcuaEndpoint(dev DeviceConfig) string {
	if strings.TrimSpace(dev.GatewayInfo.IP) == "" || dev.GatewayInfo.Port == 0 {
		return ""
	}
	return fmt.Sprintf("opc.tcp://%s:%d", strings.TrimSpace(dev.GatewayInfo.IP), dev.GatewayInfo.Port)
}

func isModbusProtocol(proto string) bool {
	p := strings.ToLower(strings.TrimSpace(proto))
	return p == "modbusrtu" || p == "modbus" || p == "modbustcp"
}

func isOpcuaProtocol(proto string) bool {
	return strings.EqualFold(strings.TrimSpace(proto), "opcua")
}
