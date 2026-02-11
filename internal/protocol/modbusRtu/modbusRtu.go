package modbusRtu

import (
	"multiple-protocol-controller/internal/protocol"
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
}

func NewModbusRtu() *ModbusRtu { return &ModbusRtu{} }

// init 自动注册到 protocol 工厂
func init() {
	protocol.RegisterProtocol("ModbusRtu", NewModbusRtu())
}
