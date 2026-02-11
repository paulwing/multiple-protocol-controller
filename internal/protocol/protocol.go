package protocol

import (
	"encoding/json"
	"fmt"
)

type Protocol interface {
	// ParsePropProtocol 解析属性下发的 protocol 字段
	ParsePropProtocol(raw json.RawMessage) (any, error)
	// EncodeCommand 根据 CommandMessage 生成下行报文
	EncodeCommand(cmd any) ([]byte, error)
}

// CommandMessage 承载控制指令下发所需的上下文信息.
type CommandMessage struct {
	DeviceAddress uint64
	FunctionCode  int
	Address       uint64
	DataType      string
	Endian        string
	Value         interface{}
}

var registry = map[string]Protocol{}

// RegisterProtocol 注册协议实例（在 init 中调用）
func RegisterProtocol(name string, p Protocol) {
	registry[name] = p
}

// 工厂方法：根据协议名选择实现
func GetProtocol(name string) (Protocol, error) {
	if p, ok := registry[name]; ok {
		return p, nil
	}
	return nil, fmt.Errorf("unsupported protocol: %s", name)
}
