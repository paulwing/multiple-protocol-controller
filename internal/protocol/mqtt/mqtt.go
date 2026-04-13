package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"

	"multiple-protocol-controller/internal/protocol"
)

type Mqtt struct{}

type ProtocolBase struct {
	Type string `json:"type"`
}

// MqttProtocol MQTT 协议配置结构
type MqttProtocol struct {
	Type   string       `json:"type"`
	Topics MqttTopics  `json:"topics"`
}

type MqttTopics struct {
	Subscribe []MqttTopicConfig `json:"subscribe"`
	Publish   []MqttTopicConfig `json:"publish"`
}

type MqttTopicConfig struct {
	Topic string `json:"topic"`
	Path  string `json:"path"`
	Qos   byte   `json:"qos"`
}

func NewMqtt() *Mqtt { return &Mqtt{} }

func init() {
	protocol.RegisterProtocol("MQTT", NewMqtt())
	protocol.RegisterProtocol("mqtt", NewMqtt())
}

func (m *Mqtt) ParsePropProtocol(raw json.RawMessage) (any, error) {
	var base ProtocolBase
	if err := json.Unmarshal(raw, &base); err != nil {
		return nil, err
	}
	if !strings.EqualFold(strings.TrimSpace(base.Type), "MQTT") {
		return nil, fmt.Errorf("protocol is not MQTT type %q", base.Type)
	}
	var cfg MqttProtocol
	return &cfg, json.Unmarshal(raw, &cfg)
}

func (m *Mqtt) EncodeCommand(cmd any) ([]byte, error) {
	// MQTT 命令编码由 mqtt_manager 处理，这里返回 nil
	// 控制命令通过 MQTT 客户端发布，不需要生成二进制帧
	return nil, fmt.Errorf("mqtt: EncodeCommand not supported, use MQTT client to publish")
}
