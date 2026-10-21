package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"

	"multiple-protocol-controller/internal/protocol"
	"multiple-protocol-controller/internal/protocol/datatypes"
)

type Mqtt struct{}

type ProtocolBase struct {
	Type string `json:"type"`
}

// MqttProtocol MQTT 协议配置结构
type MqttProtocol struct {
	Type   string     `json:"type"`
	Topics MqttTopics `json:"topics"`
}

type MqttTopics struct {
	Subscribe []MqttTopicConfig `json:"subscribe"`
	Publish   []MqttTopicConfig `json:"publish"`
}

type MqttTopicConfig struct {
	Topic    string `json:"topic"`
	Path     string `json:"path"`
	Qos      byte   `json:"qos"`
	DataType string `json:"dataType"` // 线缆数据类型，可选；缺省沿用属性 type
}

func NewMqtt() *Mqtt { return &Mqtt{} }

func init() {
	protocol.RegisterProtocol("MQTT", NewMqtt())
}

func (m *Mqtt) ParsePropProtocol(raw json.RawMessage) (any, error) {
	var base ProtocolBase
	if err := json.Unmarshal(raw, &base); err != nil {
		return nil, err
	}
	switch strings.ToUpper(strings.TrimSpace(base.Type)) {
	case "MQTT":
		var cfg MqttProtocol
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
		if err := validatePointDataTypes(&cfg); err != nil {
			return nil, err
		}
		return &cfg, nil
	default:
		return nil, fmt.Errorf("protocol is not MQTT type %q", base.Type)
	}
}

func validatePointDataTypes(cfg *MqttProtocol) error {
	for index := range cfg.Topics.Subscribe {
		if err := normalizeTopicDataType(&cfg.Topics.Subscribe[index]); err != nil {
			return fmt.Errorf("mqtt: subscribe topic %d: %w", index, err)
		}
	}
	for index := range cfg.Topics.Publish {
		if err := normalizeTopicDataType(&cfg.Topics.Publish[index]); err != nil {
			return fmt.Errorf("mqtt: publish topic %d: %w", index, err)
		}
	}
	return nil
}

func normalizeTopicDataType(topic *MqttTopicConfig) error {
	if strings.TrimSpace(topic.DataType) == "" {
		topic.DataType = ""
		return nil
	}
	canonical, err := datatypes.Canonical(topic.DataType)
	if err != nil {
		return err
	}
	if !datatypes.SupportsMqtt(canonical) {
		return fmt.Errorf("unsupported data type %q", canonical)
	}
	topic.DataType = canonical
	return nil
}

func (m *Mqtt) EncodeCommand(cmd any) ([]byte, error) {
	// MQTT 命令编码由 mqtt_manager 处理，这里返回 nil
	// 控制命令通过 MQTT 客户端发布，不需要生成二进制帧
	return nil, fmt.Errorf("mqtt: EncodeCommand not supported, use MQTT client to publish")
}
