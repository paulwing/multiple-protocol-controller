package modbusRtu

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"multiple-protocol-controller/internal/protocol"
	"multiple-protocol-controller/internal/protocol/datatypes"
)

type ModbusRtu struct{}

type ProtocolBase struct {
	Type string `json:"type"`
}

type ModbusProtocol struct {
	Type   string `json:"type"` // 示例：MODBUSRTU、MODBUSTCP
	Points struct {
		Read  []ModbusPoint `json:"read"`  // 属性读点位
		Write []ModbusPoint `json:"write"` // 属性写点位
	} `json:"points"`
}
type ModbusPoint struct {
	Endian       string `json:"endian"`       // 字节序， 取值示例：ABCD/BADC/CDAB
	FunctionCode string `json:"functionCode"` // 功能码
	Address      string `json:"address"`      // 线圈/寄存器地址
	Quantity     int    `json:"quantity"`     // 寄存器/线圈数量，可选，缺省按数据类型推导
	Bit          int    `json:"bit"`          // 位偏移，可选，缺省从第 0 位
	DataType     string `json:"dataType"`     // 线缆数据类型，可选；缺省沿用属性 type
}

type FunCodeType struct {
	ReadCoil           int
	ReadDisperse       int
	ReadHoldRegister   int
	ReadInputRegister  int
	WriteCoil          int
	WriteHoldRegister  int
	WriteMultiCoil     int
	WriteMultiRegister int
}

var ModbusRtuFunCode = FunCodeType{
	ReadCoil:           1,  // ReadCoil
	ReadDisperse:       2,  // ReadDisperse
	ReadHoldRegister:   3,  // ReadHoldRegister
	ReadInputRegister:  4,  // ReadInputRegister
	WriteCoil:          5,  // WriteCoil
	WriteHoldRegister:  6,  // WriteHoldRegister
	WriteMultiCoil:     15, // WriteMultiCoil
	WriteMultiRegister: 16, // WriteMultiRegister
}

func NewModbusRtu() *ModbusRtu { return &ModbusRtu{} }

func (m *ModbusRtu) ParsePropProtocol(raw json.RawMessage) (any, error) {
	var base ProtocolBase
	if err := json.Unmarshal(raw, &base); err != nil {
		return nil, err
	}
	switch strings.ToUpper(strings.TrimSpace(base.Type)) {
	case "MODBUS_RTU":
		var cfg ModbusProtocol
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
		if err := validatePointDataTypes(&cfg); err != nil {
			return nil, err
		}
		return &cfg, nil
	default:
		return nil, fmt.Errorf("protocol is not MODBUS_RTU type %q", base.Type)
	}
}

// validatePointDataTypes normalises and validates only explicitly configured data
// types. Missing values stay empty so runtime falls back to the property type and
// old configurations keep their previous behaviour.
func validatePointDataTypes(cfg *ModbusProtocol) error {
	for index := range cfg.Points.Read {
		point := &cfg.Points.Read[index]
		if err := normalizeModbusDataType(point, true); err != nil {
			return fmt.Errorf("modbusRtu: read point %d: %w", index, err)
		}
	}
	for index := range cfg.Points.Write {
		point := &cfg.Points.Write[index]
		if err := normalizeModbusDataType(point, false); err != nil {
			return fmt.Errorf("modbusRtu: write point %d: %w", index, err)
		}
	}
	return nil
}

func normalizeModbusDataType(point *ModbusPoint, read bool) error {
	if strings.TrimSpace(point.DataType) == "" {
		point.DataType = ""
		return nil
	}
	canonical, err := datatypes.Canonical(point.DataType)
	if err != nil {
		return err
	}
	functionCode, err := strconv.Atoi(strings.TrimSpace(point.FunctionCode))
	if err != nil {
		return fmt.Errorf("invalid function code %q for explicit data type %q", point.FunctionCode, point.DataType)
	}
	supported := datatypes.SupportsModbusRead(canonical, functionCode)
	if !read {
		supported = datatypes.SupportsModbusWrite(canonical, functionCode)
	}
	if !supported {
		return fmt.Errorf("unsupported data type %q for function code %02d", canonical, functionCode)
	}
	if required := requiredRegisterCount(canonical); point.Quantity > 0 && point.Quantity < required {
		return fmt.Errorf("data type %q needs %d registers but quantity is %d", canonical, required, point.Quantity)
	}
	point.DataType = canonical
	return nil
}

func requiredRegisterCount(dataType string) int {
	switch dataType {
	case datatypes.Int32, datatypes.Uint32, datatypes.Float32:
		return 2
	case datatypes.Int64, datatypes.Uint64, datatypes.Float64:
		return 4
	default:
		return 1
	}
}

// init 自动注册到 protocol 工厂
func init() {
	protocol.RegisterProtocol("MODBUS_RTU", NewModbusRtu())
}
