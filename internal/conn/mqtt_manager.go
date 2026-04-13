package conn

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"multiple-protocol-controller/pkg/logger"

	"go.uber.org/zap"
)

// MqttConfig MQTT 连接配置
type MqttConfig struct {
	Broker       string
	ClientID     string
	Username     string
	Password     string
	Qos          byte
	KeepAlive    int
	CleanSession bool
}

// MqttMessage MQTT 消息
type MqttMessage struct {
	Topic   string
	Payload []byte
	Qos     byte
}

// MessageHandler MQTT 消息处理函数
type MessageHandler func(topic string, payload []byte)

// MqttManager MQTT 连接管理器
type MqttManager struct {
	mu          sync.RWMutex
	clients     map[string]*MqttClient
	messageCh   chan MqttMessage
	handlers    map[string]MessageHandler
	subChannels map[string]map[string]chan MqttMessage
}

type MqttClient struct {
	cfg      MqttConfig
	client   mqtt.Client
	handlers map[string]MessageHandler
}

var mqttDefaultHolder struct {
	mu  sync.RWMutex
	mgr *MqttManager
}

// DefaultMqttManager 获取默认 MQTT 管理器
func DefaultMqttManager() *MqttManager {
	mqttDefaultHolder.mu.Lock()
	defer mqttDefaultHolder.mu.Unlock()
	if mqttDefaultHolder.mgr == nil {
		mqttDefaultHolder.mgr = NewMqttManager(100)
	}
	return mqttDefaultHolder.mgr
}

// NewMqttManager 创建新的 MQTT 管理器
func NewMqttManager(bufferSize int) *MqttManager {
	mgr := &MqttManager{
		clients:     make(map[string]*MqttClient),
		messageCh:   make(chan MqttMessage, bufferSize),
		handlers:    make(map[string]MessageHandler),
		subChannels: make(map[string]map[string]chan MqttMessage),
	}
	// 启动消息分发协程
	go mgr.dispatchMessages()
	return mgr
}

// dispatchMessages 分发消息到订阅者
func (m *MqttManager) dispatchMessages() {
	for msg := range m.messageCh {
		m.mu.RLock()
		// 查找匹配的订阅者
		for topic, chans := range m.subChannels {
			if matchTopic(msg.Topic, topic) {
				for _, ch := range chans {
					select {
					case ch <- msg:
					default:
						// 通道满，跳过
					}
				}
			}
		}
		m.mu.RUnlock()
	}
}

// matchTopic 检查主题是否匹配 (支持 + 和 # 通配符)
func matchTopic(topic string, pattern string) bool {
	// 简单实现：支持 + 单层通配符
	patternParts := strings.Split(pattern, "/")
	topicParts := strings.Split(topic, "/")

	if len(patternParts) != len(topicParts) {
		// 检查是否有 # 通配符
		for _, part := range patternParts {
			if part == "#" {
				return true
			}
		}
		return false
	}

	for i, part := range patternParts {
		if part == "+" || part == "#" {
			continue
		}
		if part != topicParts[i] {
			return false
		}
	}
	return true
}

// Connect 连接到 MQTT Broker
func (m *MqttManager) Connect(cfg MqttConfig) error {
	if strings.TrimSpace(cfg.Broker) == "" {
		return fmt.Errorf("mqtt: broker address missing")
	}

	key := mqttClientKey(cfg)
	m.mu.Lock()
	defer m.mu.Unlock()

	if client, ok := m.clients[key]; ok && client.client.IsConnected() {
		return nil
	}

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.Broker).
		SetClientID(cfg.ClientID).
		SetUsername(cfg.Username).
		SetPassword(cfg.Password).
		SetCleanSession(cfg.CleanSession).
		SetKeepAlive(time.Duration(cfg.KeepAlive) * time.Second).
		SetAutoReconnect(true).
		SetOnConnectHandler(func(c mqtt.Client) {
			logger.Log.Info("mqtt connected", zap.String("broker", cfg.Broker), zap.String("clientId", cfg.ClientID))
		}).
		SetConnectionLostHandler(func(c mqtt.Client, err error) {
			logger.Log.Warn("mqtt connection lost", zap.String("broker", cfg.Broker), zap.Error(err))
		})

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("mqtt connect failed: %w", token.Error())
	}

	m.clients[key] = &MqttClient{
		cfg:      cfg,
		client:   client,
		handlers: make(map[string]MessageHandler),
	}

	return nil
}

// Subscribe 订阅主题
func (m *MqttManager) Subscribe(cfg MqttConfig, topic string, qos byte, handler MessageHandler) error {
	key := mqttClientKey(cfg)

	m.mu.Lock()
	defer m.mu.Unlock()

	client, ok := m.clients[key]
	if !ok || !client.client.IsConnected() {
		return fmt.Errorf("mqtt: client not connected")
	}

	// 注册消息处理函数
	client.handlers[topic] = handler

	// 创建专用通道
	ch := make(chan MqttMessage, 10)
	if _, ok := m.subChannels[key]; !ok {
		m.subChannels[key] = make(map[string]chan MqttMessage)
	}
	m.subChannels[key][topic] = ch

	// 订阅主题
	token := client.client.Subscribe(topic, qos, func(c mqtt.Client, msg mqtt.Message) {
		m.messageCh <- MqttMessage{
			Topic:   msg.Topic(),
			Payload: msg.Payload(),
			Qos:     msg.Qos(),
		}
	})
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("mqtt subscribe failed: %w", token.Error())
	}

	return nil
}

// Publish 发布消息
func (m *MqttManager) Publish(cfg MqttConfig, topic string, payload []byte, qos byte) error {
	key := mqttClientKey(cfg)

	m.mu.RLock()
	client, ok := m.clients[key]
	m.mu.RUnlock()

	if !ok || !client.client.IsConnected() {
		return fmt.Errorf("mqtt: client not connected")
	}

	token := client.client.Publish(topic, qos, false, payload)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("mqtt publish failed: %w", token.Error())
	}

	return nil
}

// SubscribeAndWait 订阅主题并等待消息（用于同步采集）
func (m *MqttManager) SubscribeAndWait(cfg MqttConfig, topic string, qos byte, timeout time.Duration) ([]byte, error) {
	key := mqttClientKey(cfg)

	// 确保连接
	m.mu.RLock()
	client, ok := m.clients[key]
	m.mu.RUnlock()

	if !ok || !client.client.IsConnected() {
		return nil, fmt.Errorf("mqtt: client not connected")
	}

	// 创建专用通道
	ch := make(chan MqttMessage, 1)
	defer func() {
		m.mu.Lock()
		if channels, exists := m.subChannels[key]; exists {
			delete(channels, topic)
		}
		m.mu.Unlock()
		close(ch)
	}()

	// 临时订阅
	token := client.client.Subscribe(topic, qos, func(c mqtt.Client, msg mqtt.Message) {
		select {
		case ch <- MqttMessage{
			Topic:   msg.Topic(),
			Payload: msg.Payload(),
			Qos:     msg.Qos(),
		}:
		default:
		}
	})
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("mqtt subscribe failed: %w", token.Error())
	}

	// 等待消息或超时
	select {
	case msg := <-ch:
		return msg.Payload, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("mqtt: timeout waiting for message")
	}
}

// Unsubscribe 取消订阅
func (m *MqttManager) Unsubscribe(cfg MqttConfig, topic string) error {
	key := mqttClientKey(cfg)

	m.mu.Lock()
	defer m.mu.Unlock()

	client, ok := m.clients[key]
	if !ok || !client.client.IsConnected() {
		return nil
	}

	token := client.client.Unsubscribe(topic)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("mqtt unsubscribe failed: %w", token.Error())
	}

	// 清理
	delete(client.handlers, topic)
	if channels, exists := m.subChannels[key]; exists {
		if ch, ok := channels[topic]; ok {
			close(ch)
			delete(channels, topic)
		}
	}

	return nil
}

// Disconnect 断开连接
func (m *MqttManager) Disconnect(cfg MqttConfig) error {
	key := mqttClientKey(cfg)

	m.mu.Lock()
	defer m.mu.Unlock()

	client, ok := m.clients[key]
	if !ok {
		return nil
	}

	if client.client.IsConnected() {
		client.client.Disconnect(250)
	}

	delete(m.clients, key)
	if channels, exists := m.subChannels[key]; exists {
		for _, ch := range channels {
			close(ch)
		}
		delete(m.subChannels, key)
	}

	return nil
}

// Close 关闭管理器
func (m *MqttManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, client := range m.clients {
		if client.client.IsConnected() {
			client.client.Disconnect(250)
		}
		delete(m.clients, key)
	}

	close(m.messageCh)
	return nil
}

// mqttClientKey 生成客户端唯一标识
func mqttClientKey(cfg MqttConfig) string {
	return fmt.Sprintf("%s|%s", cfg.Broker, cfg.ClientID)
}

// ParseMqttPayload 解析 MQTT 消息载荷，支持 JSON 和纯文本
func ParseMqttPayload(payload []byte, path string) (interface{}, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("empty payload")
	}

	// 尝试 JSON 解析
	if path == "" {
		// 没有路径，直接返回原始值
		var result interface{}
		if err := json.Unmarshal(payload, &result); err != nil {
			// 返回原始字符串
			return string(payload), nil
		}
		return result, nil
	}

	var data interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return string(payload), nil
	}

	// 解析路径 (例如 "payload.temperature")
	parts := strings.Split(path, ".")
	current := data
	for _, part := range parts {
		if current == nil {
			return nil, fmt.Errorf("path not found: %s", path)
		}
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid path: %s", path)
		}
		current, ok = m[part]
		if !ok {
			return nil, fmt.Errorf("path not found: %s", path)
		}
	}

	return current, nil
}
