package config

import (
	"encoding/json"
	"fmt"
	"multiple-protocol-controller/internal/protocol"
	"multiple-protocol-controller/internal/protocol/bacnet"
	"multiple-protocol-controller/internal/protocol/modbusRtu"
	"multiple-protocol-controller/internal/protocol/mqtt"
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
	Devices                 []DeviceRuntime
	DeviceBySerial          map[string]DeviceRuntime
	DeviceCmdBySerial       map[string][]ModbusCommand
	DeviceBacnetBySerial    map[string][]BacnetParam
	DeviceBacnetCmdBySerial map[string][]BacnetCommand
	DeviceOpcuaCmdBySerial  map[string][]OpcuaCommand
	DeviceMqttCmdBySerial   map[string][]MqttCommand
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

	BacnetDeviceInstance uint32
	BacnetReadPoints     []BacnetParam
	BacnetWritePoints    []BacnetCommand

	OpcuaEndpoint       string
	OpcuaSecurityPolicy string
	OpcuaSecurityMode   string
	OpcuaReadPoints     []OpcuaParam
	OpcuaWritePoints    []OpcuaCommand

	// MQTT 相关配置
	MqttBroker       string
	MqttClientID     string
	MqttUsername     string
	MqttPassword     string
	MqttQos          byte
	MqttKeepAlive    int
	MqttCleanSession bool
	MqttReadPoints   []MqttParam
	MqttWritePoints  []MqttCommand
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
	ReadDisabled bool
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

// BacnetParam 描述单个 BACnet 采集点。
type BacnetParam struct {
	PropertyID         string
	DeviceID           string
	SerialNumber       string
	Identify           string
	Name               string
	DataType           string
	ObjectType         string
	ObjectInstance     uint32
	PropertyIdentifier string
	ReadDisabled       bool
	Unit               string
}

// BacnetCommand 描述单个 BACnet 控制点。
type BacnetCommand struct {
	PropertyID         string
	DeviceID           string
	SerialNumber       string
	Identify           string
	Name               string
	DataType           string
	ObjectType         string
	ObjectInstance     uint32
	PropertyIdentifier string
	Unit               string
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
	ReadDisabled bool
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

// MqttParam 描述单个 MQTT 采集点。
type MqttParam struct {
	PropertyID     string
	DeviceID       string
	SerialNumber   string
	Identify       string
	Name           string
	DataType       string
	SubscribeTopic string
	PublishTopic   string
	Path           string
	Qos            byte
	ReadDisabled   bool
	Unit           string
}

// MqttCommand 描述单个 MQTT 控制点。
type MqttCommand struct {
	PropertyID     string
	DeviceID       string
	SerialNumber   string
	Identify       string
	Name           string
	DataType       string
	SubscribeTopic string
	PublishTopic   string
	Path           string
	Qos            byte
	Unit           string
}

// BuildRuntimeConfig converts raw device configs into DeviceRuntime slices.
func BuildRuntimeConfig(devices []DeviceConfig) (IotCfgType, error) {
	runtime := IotCfgType{
		DeviceBySerial:          make(map[string]DeviceRuntime),
		DeviceCmdBySerial:       make(map[string][]ModbusCommand),
		DeviceBacnetBySerial:    make(map[string][]BacnetParam),
		DeviceBacnetCmdBySerial: make(map[string][]BacnetCommand),
		DeviceOpcuaCmdBySerial:  make(map[string][]OpcuaCommand),
		DeviceMqttCmdBySerial:   make(map[string][]MqttCommand),
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
		if len(rt.BacnetReadPoints) > 0 {
			runtime.DeviceBacnetBySerial[dev.SerialNumber] = append(runtime.DeviceBacnetBySerial[dev.SerialNumber], rt.BacnetReadPoints...)
		}
		if len(rt.BacnetWritePoints) > 0 {
			runtime.DeviceBacnetCmdBySerial[dev.SerialNumber] = append(runtime.DeviceBacnetCmdBySerial[dev.SerialNumber], rt.BacnetWritePoints...)
		}
		if len(rt.OpcuaWritePoints) > 0 {
			runtime.DeviceOpcuaCmdBySerial[dev.SerialNumber] = append(runtime.DeviceOpcuaCmdBySerial[dev.SerialNumber], rt.OpcuaWritePoints...)
		}
		if len(rt.MqttWritePoints) > 0 {
			runtime.DeviceMqttCmdBySerial[dev.SerialNumber] = append(runtime.DeviceMqttCmdBySerial[dev.SerialNumber], rt.MqttWritePoints...)
		}
	}
	return runtime, nil
}

func buildDeviceRuntime(dev DeviceConfig) (DeviceRuntime, error) {
	// MQTT 协议使用 gatewayInfo 存储 Broker 地址，不需要设备地址验证
	isMqtt := isMqttProtocol(dev.Protocol)

	port := uint16(dev.GatewayInfo.Port)
	ip := strings.TrimSpace(dev.GatewayInfo.IP)

	// MQTT 协议允许 gatewayInfo 为空（使用默认配置），其他协议需要验证
	if !isMqtt && (ip == "" || port == 0) {
		return DeviceRuntime{}, fmt.Errorf("gateway address missing")
	}

	gatewaySerial := dev.GatewayInfo.ID
	if gatewaySerial == "" {
		gatewaySerial = dev.SerialNumber
	}

	slaveID := uint64(0)
	var readPts []ModbusParam
	var writePts []ModbusCommand
	var bacnetDeviceInstance uint32
	var bacnetRead []BacnetParam
	var bacnetWrite []BacnetCommand
	var opcuaRead []OpcuaParam
	var opcuaWrite []OpcuaCommand
	var opcuaEndpoint string
	var opcuaPolicy string
	var opcuaMode string

	// MQTT 配置
	var mqttBroker string
	var mqttClientID string
	var mqttUsername string
	var mqttPassword string
	var mqttQos byte
	var mqttKeepAlive int
	var mqttCleanSession bool
	var mqttRead []MqttParam
	var mqttWrite []MqttCommand

	if isModbusProtocol(dev.Protocol) {
		slaveID = parseSlaveID(dev)
		readPts, writePts = parseModbusPoints(dev, slaveID)
	}
	if isBacnetProtocol(dev.Protocol) {
		bacnetDeviceInstance = parseBacnetDeviceInstance(dev)
		bacnetRead, bacnetWrite = parseBacnetPoints(dev)
	}
	if isOpcuaProtocol(dev.Protocol) {
		opcuaRead, opcuaWrite = parseOpcuaPoints(dev)
		opcuaEndpoint = buildOpcuaEndpoint(dev)
		opcuaPolicy, opcuaMode = parseOpcuaSecurity(dev.Params)
	}
	if isMqtt {
		mqttRead, mqttWrite = parseMqttPoints(dev)
		mqttBroker = buildMqttBroker(dev)
		mqttClientID, mqttUsername, mqttPassword, mqttQos, mqttKeepAlive, mqttCleanSession = parseMqttParams(dev)
	}

	timeout := dev.AcqFreq
	if timeout <= 0 {
		timeout = 5000
	}

	// 对于 MQTT 协议，使用空字符串作为 GatewayIP/Port（不需要 TCP 连接）
	resultIP := ip
	resultPort := port
	if isMqtt {
		resultIP = ""
		resultPort = 0
	}

	return DeviceRuntime{
		Config:               dev,
		SlaveID:              slaveID,
		GatewaySerial:        gatewaySerial,
		GatewayID:            dev.GatewayInfo.ID,
		GatewayIP:            resultIP,
		GatewayPort:          resultPort,
		ResponseTimeoutMs:    timeout,
		ReadPoints:           readPts,
		WritePoints:          writePts,
		BacnetDeviceInstance: bacnetDeviceInstance,
		BacnetReadPoints:     bacnetRead,
		BacnetWritePoints:    bacnetWrite,
		OpcuaEndpoint:        opcuaEndpoint,
		OpcuaSecurityPolicy:  opcuaPolicy,
		OpcuaSecurityMode:    opcuaMode,
		OpcuaReadPoints:      opcuaRead,
		OpcuaWritePoints:     opcuaWrite,
		MqttBroker:           mqttBroker,
		MqttClientID:         mqttClientID,
		MqttUsername:         mqttUsername,
		MqttPassword:         mqttPassword,
		MqttQos:              mqttQos,
		MqttKeepAlive:        mqttKeepAlive,
		MqttCleanSession:     mqttCleanSession,
		MqttReadPoints:       mqttRead,
		MqttWritePoints:      mqttWrite,
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
					ReadDisabled: isReadDisabled(prop.Access),
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
					ReadDisabled: isReadDisabled(prop.Access),
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

func parseBacnetPoints(dev DeviceConfig) ([]BacnetParam, []BacnetCommand) {
	var (
		params   []BacnetParam
		commands []BacnetCommand
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

		cfg, ok := parsed.(*bacnet.BacnetProtocol)
		if !ok {
			logger.Log.Warn("protocol converter not implemented", zap.String("protocol", dev.Protocol))
			continue
		}

		for _, obj := range cfg.Objects.Read {
			propertyID, valid := normalizeBacnetPropertyIdentifier(obj.PropertyID)
			if !valid {
				logger.Log.Warn("unsupported BACnet property identifier",
					zap.String("deviceSerial", dev.SerialNumber),
					zap.String("property", prop.Key),
					zap.String("propertyIdentifier", obj.PropertyID))
				continue
			}
			objectType, valid := normalizeBacnetObjectType(obj.ObjectType)
			if !valid {
				logger.Log.Warn("unsupported BACnet object type",
					zap.String("deviceSerial", dev.SerialNumber),
					zap.String("property", prop.Key),
					zap.String("objectType", obj.ObjectType))
				continue
			}
			params = append(params, BacnetParam{
				PropertyID:         prop.ID,
				DeviceID:           dev.ID,
				SerialNumber:       dev.SerialNumber,
				Identify:           prop.Key,
				Name:               prop.Name,
				DataType:           prop.Type,
				ObjectType:         objectType,
				ObjectInstance:     obj.Instance,
				PropertyIdentifier: propertyID,
				ReadDisabled:       isReadDisabled(prop.Access),
				Unit:               prop.Unit,
			})
		}
		for _, obj := range cfg.Objects.Write {
			propertyID, valid := normalizeBacnetPropertyIdentifier(obj.PropertyID)
			if !valid {
				logger.Log.Warn("unsupported BACnet property identifier",
					zap.String("deviceSerial", dev.SerialNumber),
					zap.String("property", prop.Key),
					zap.String("propertyIdentifier", obj.PropertyID))
				continue
			}
			objectType, valid := normalizeBacnetObjectType(obj.ObjectType)
			if !valid {
				logger.Log.Warn("unsupported BACnet object type",
					zap.String("deviceSerial", dev.SerialNumber),
					zap.String("property", prop.Key),
					zap.String("objectType", obj.ObjectType))
				continue
			}
			commands = append(commands, BacnetCommand{
				PropertyID:         prop.ID,
				DeviceID:           dev.ID,
				SerialNumber:       dev.SerialNumber,
				Identify:           prop.Key,
				Name:               prop.Name,
				DataType:           prop.Type,
				ObjectType:         objectType,
				ObjectInstance:     obj.Instance,
				PropertyIdentifier: propertyID,
				Unit:               prop.Unit,
			})
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

func parseBacnetDeviceInstance(dev DeviceConfig) uint32 {
	if len(dev.Params) == 0 {
		return 0
	}
	var temp struct {
		DeviceInstance interface{} `json:"deviceInstance"`
	}
	if err := json.Unmarshal(dev.Params, &temp); err != nil {
		return 0
	}
	val, err := utils.CoerceUint64(temp.DeviceInstance)
	if err != nil {
		return 0
	}
	return uint32(val)
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

func isBacnetProtocol(proto string) bool {
	return strings.EqualFold(strings.TrimSpace(proto), "bacnet")
}

func isOpcuaProtocol(proto string) bool {
	return strings.EqualFold(strings.TrimSpace(proto), "opcua")
}

func isMqttProtocol(proto string) bool {
	return strings.EqualFold(strings.TrimSpace(proto), "mqtt")
}

// parseMqttPoints 解析 MQTT 采集点和控制点
func parseMqttPoints(dev DeviceConfig) ([]MqttParam, []MqttCommand) {
	var (
		params   []MqttParam
		commands []MqttCommand
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
		case *mqtt.MqttProtocol:
			for _, topic := range cfg.Topics.Subscribe {
				subTopic := replaceTopicVars(topic.Topic, dev.SerialNumber, dev.ID)
				path := strings.TrimSpace(topic.Path)
				if path == "" {
					path = prop.Key
				}
				params = append(params, MqttParam{
					PropertyID:     prop.ID,
					DeviceID:       dev.ID,
					SerialNumber:   dev.SerialNumber,
					Identify:       prop.Key,
					Name:           prop.Name,
					DataType:       prop.Type,
					SubscribeTopic: subTopic,
					Path:           path,
					Qos:            topic.Qos,
					ReadDisabled:   isReadDisabled(prop.Access),
					Unit:           prop.Unit,
				})
			}
			for _, topic := range cfg.Topics.Publish {
				pubTopic := replaceTopicVars(topic.Topic, dev.SerialNumber, dev.ID)
				path := strings.TrimSpace(topic.Path)
				if path == "" {
					path = prop.Key
				}
				commands = append(commands, MqttCommand{
					PropertyID:   prop.ID,
					DeviceID:     dev.ID,
					SerialNumber: dev.SerialNumber,
					Identify:     prop.Key,
					Name:         prop.Name,
					DataType:     prop.Type,
					PublishTopic: pubTopic,
					Path:         path,
					Qos:          topic.Qos,
					Unit:         prop.Unit,
				})
			}
		default:
			logger.Log.Warn("protocol converter not implemented", zap.String("protocol", dev.Protocol))
		}
	}

	return params, commands
}

// replaceTopicVars 替换 Topic 中的变量占位符
func replaceTopicVars(topic string, serial string, deviceID string) string {
	topic = strings.ReplaceAll(topic, "{serial}", serial)
	topic = strings.ReplaceAll(topic, "{deviceId}", deviceID)
	return topic
}

func normalizeBacnetPropertyIdentifier(raw string) (string, bool) {
	switch strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(raw), "_", ""), "-", "")) {
	case "presentvalue":
		return "presentValue", true
	default:
		return "", false
	}
}

func normalizeBacnetObjectType(raw string) (string, bool) {
	switch strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(raw), "_", ""), "-", "")) {
	case "analoginput":
		return "analogInput", true
	case "analogoutput":
		return "analogOutput", true
	case "analogvalue":
		return "analogValue", true
	case "binaryinput":
		return "binaryInput", true
	case "binaryoutput":
		return "binaryOutput", true
	case "binaryvalue":
		return "binaryValue", true
	case "multistateinput":
		return "multiStateInput", true
	case "multistateoutput":
		return "multiStateOutput", true
	case "multistatevalue":
		return "multiStateValue", true
	default:
		return "", false
	}
}

func isReadDisabled(access string) bool {
	if len(access) == 0 {
		return true
	}
	return access[0] != '1'
}

// buildMqttBroker 构建 MQTT Broker 地址
func buildMqttBroker(dev DeviceConfig) string {
	ip := strings.TrimSpace(dev.GatewayInfo.IP)
	port := dev.GatewayInfo.Port
	if ip == "" {
		ip = "localhost"
	}
	if port == 0 {
		port = 1883
	}
	return fmt.Sprintf("tcp://%s:%d", ip, port)
}

// parseMqttParams 解析 MQTT 连接参数
func parseMqttParams(dev DeviceConfig) (clientID, username, password string, qos byte, keepAlive int, cleanSession bool) {
	clientID = dev.SerialNumber
	username = dev.Username
	password = dev.Password
	qos = 1
	keepAlive = 30
	cleanSession = true

	if len(dev.Params) == 0 {
		return
	}

	var params struct {
		ClientID     string `json:"clientId"`
		Username     string `json:"username"`
		Password     string `json:"password"`
		Qos          int    `json:"qos"`
		KeepAlive    int    `json:"keepAlive"`
		CleanSession bool   `json:"cleanSession"`
	}

	if err := json.Unmarshal(dev.Params, &params); err != nil {
		logger.Log.Warn("parse mqtt params failed", zap.String("serial", dev.SerialNumber), zap.Error(err))
		return
	}

	if strings.TrimSpace(params.ClientID) != "" {
		clientID = params.ClientID
	}
	if strings.TrimSpace(params.Username) != "" {
		username = params.Username
	}
	if strings.TrimSpace(params.Password) != "" {
		password = params.Password
	}
	if params.Qos >= 0 && params.Qos <= 2 {
		qos = byte(params.Qos)
	}
	if params.KeepAlive > 0 {
		keepAlive = params.KeepAlive
	}
	cleanSession = params.CleanSession

	return
}
