package conn

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
	"multiple-protocol-controller/pkg/logger"
)

type MqttConfig struct {
	Broker       string
	ClientID     string
	Username     string
	Password     string
	Qos          byte
	KeepAlive    int
	CleanSession bool
}

type MqttMessage struct {
	Topic   string
	Payload []byte
	Qos     byte
}

type MessageHandler func(topic string, payload []byte)

// ErrMqttConnectionConfigConflict can be temporary while collectors hand a
// shared connection over to a new configuration. A failed Watch owns nothing.
var ErrMqttConnectionConfigConflict = errors.New("mqtt: conflicting connection options for broker/client ID")

// MqttManager shares a connection, but never shares ownership of a subscription.
// Paho matches topic filters; each route fans out only to its current owners.
type MqttManager struct {
	mu               sync.Mutex
	clients          map[string]*MqttClient
	closed           bool
	newClient        func(*mqtt.ClientOptions) mqtt.Client
	retryInterval    time.Duration
	operationTimeout time.Duration
	cleanupTimeout   time.Duration
}

type MqttClient struct {
	cfg           MqttConfig
	client        mqtt.Client
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	wake          chan struct{}
	filters       map[string]map[*mqttSubscription]byte
	applied       map[string]byte
	generation    uint64
	publishes     int
	restarting    bool
	operationDone <-chan struct{}
}

type mqttSubscription struct {
	mu          sync.Mutex
	active      bool
	handler     MessageHandler
	stopContext func() bool
}

var mqttDefaultHolder struct {
	mu  sync.RWMutex
	mgr *MqttManager
}

func DefaultMqttManager() *MqttManager {
	mqttDefaultHolder.mu.Lock()
	defer mqttDefaultHolder.mu.Unlock()
	if mqttDefaultHolder.mgr == nil {
		mqttDefaultHolder.mgr = NewMqttManager(100)
	}
	return mqttDefaultHolder.mgr
}

// bufferSize is retained for constructor compatibility. Workers own their bounded
// queues; there is no second, unconsumed manager message queue.
func NewMqttManager(bufferSize int) *MqttManager {
	return &MqttManager{clients: make(map[string]*MqttClient), newClient: mqtt.NewClient, retryInterval: time.Second, operationTimeout: 5 * time.Second, cleanupTimeout: time.Second}
}

// Watch registers one independent owner and returns an idempotent release.
// Registration does not wait for the network. Connection and subscription
// failures are retried until the owner releases or ctx is cancelled.
// Handlers must return promptly (the collector only enqueues a message).
func (m *MqttManager) Watch(ctx context.Context, cfg MqttConfig, topic string, qos byte, handler MessageHandler) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(cfg.Broker) == "" {
		return nil, fmt.Errorf("mqtt: broker address missing")
	}
	if topic == "" || qos > 2 || handler == nil {
		return nil, fmt.Errorf("mqtt: invalid subscription")
	}
	key := mqttClientKey(cfg)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, fmt.Errorf("mqtt: manager closed")
		}
		entry := m.clients[key]
		if entry != nil && entry.ctx.Err() != nil {
			done := entry.done
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
			}
			continue
		}
		start := entry == nil
		if start {
			entry = m.newEntry(cfg)
			m.clients[key] = entry
		} else if entry.cfg.Username != cfg.Username || entry.cfg.Password != cfg.Password || entry.cfg.KeepAlive != cfg.KeepAlive || entry.cfg.CleanSession != cfg.CleanSession {
			m.mu.Unlock()
			return nil, ErrMqttConnectionConfigConflict
		}
		subscription := &mqttSubscription{active: true, handler: handler}
		if entry.filters[topic] == nil {
			entry.filters[topic] = make(map[*mqttSubscription]byte)
			// Install before Connect: persistent sessions can deliver before SUBACK.
			entry.client.AddRoute(topic, m.route(entry, topic))
		}
		entry.filters[topic][subscription] = qos
		m.mu.Unlock()
		if start {
			go m.run(entry)
		}
		m.signal(entry)

		var releaseOnce sync.Once
		release := func() {
			releaseOnce.Do(func() {
				subscription.deactivate()
				m.mu.Lock()
				delete(entry.filters[topic], subscription)
				m.releaseUnused(entry)
				m.mu.Unlock()
				m.signal(entry)
			})
		}
		stop := context.AfterFunc(ctx, release)
		subscription.mu.Lock()
		if subscription.active {
			subscription.stopContext = stop
		} else {
			stop()
		}
		subscription.mu.Unlock()
		return release, nil
	}
}

func (s *mqttSubscription) deactivate() {
	s.mu.Lock()
	s.active = false
	s.handler = nil
	stop := s.stopContext
	s.stopContext = nil
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
}

func (m *MqttManager) newEntry(cfg MqttConfig) *MqttClient {
	ctx, cancel := context.WithCancel(context.Background())
	entry := &MqttClient{cfg: cfg, ctx: ctx, cancel: cancel, done: make(chan struct{}), wake: make(chan struct{}, 1), filters: make(map[string]map[*mqttSubscription]byte), applied: make(map[string]byte)}
	entry.client = m.makeClient(entry)
	return entry
}

func (m *MqttManager) makeClient(entry *MqttClient) mqtt.Client {
	cfg := entry.cfg
	opts := mqtt.NewClientOptions().AddBroker(cfg.Broker).SetClientID(cfg.ClientID).
		SetUsername(cfg.Username).SetPassword(cfg.Password).SetCleanSession(cfg.CleanSession).
		SetKeepAlive(time.Duration(cfg.KeepAlive) * time.Second).
		SetAutoReconnect(true).SetConnectRetry(true).SetConnectRetryInterval(m.retryInterval).
		SetConnectTimeout(5 * time.Second).
		SetDefaultPublishHandler(func(mqtt.Client, mqtt.Message) {}).
		SetOnConnectHandler(func(c mqtt.Client) {
			m.mu.Lock()
			if entry.ctx.Err() == nil && entry.client == c {
				entry.restarting = false
				entry.generation++
				entry.applied = make(map[string]byte)
			}
			m.mu.Unlock()
			m.signal(entry)
		}).SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		m.logFailure(entry, "mqtt connection lost", err)
		m.signal(entry)
	})
	return m.newClient(opts)
}

func (m *MqttManager) signal(entry *MqttClient) {
	select {
	case entry.wake <- struct{}{}:
	default:
	}
}

// The single reconciliation loop serializes broker subscription changes. Its
// waits are cancellable and never hold the manager lock. An unacknowledged
// operation stays in flight instead of accumulating a token on every retry tick.
func (m *MqttManager) run(entry *MqttClient) {
	defer func() {
		m.cleanupSubscriptions(entry)
		entry.client.Disconnect(250)
		m.mu.Lock()
		if m.clients[mqttClientKey(entry.cfg)] == entry {
			delete(m.clients, mqttClientKey(entry.cfg))
		}
		close(entry.done)
		m.mu.Unlock()
	}()
	if entry.ctx.Err() != nil {
		return
	}
	if err := m.connect(entry); err != nil {
		return
	}
	ticker := time.NewTicker(m.retryInterval)
	defer ticker.Stop()
	restart := false
	for {
		if entry.ctx.Err() != nil {
			return
		}
		if restart {
			m.mu.Lock()
			canRestart := entry.publishes == 0
			var oldClient mqtt.Client
			if canRestart {
				entry.restarting = true
				oldClient = entry.client
				entry.client = m.makeClient(entry)
				entry.applied = make(map[string]byte)
				for topic, owners := range entry.filters {
					if len(owners) != 0 {
						entry.client.AddRoute(topic, m.route(entry, topic))
					}
				}
			}
			m.mu.Unlock()
			if canRestart {
				// Install the new identity before disconnecting. A late OnConnect
				// from the old client must not admit a publish into this window.
				oldClient.Disconnect(250)
				if err := m.connect(entry); err != nil {
					return
				}
				restart = false
			}
		} else if entry.client.IsConnectionOpen() {
			if err := m.reconcile(entry); err != nil && entry.ctx.Err() == nil {
				m.logFailure(entry, "mqtt subscription retry pending", err)
				restart = errors.Is(err, context.DeadlineExceeded)
			}
		}
		select {
		case <-entry.ctx.Done():
			return
		case <-entry.wake:
		case <-ticker.C:
		}
	}
}

func (m *MqttManager) connect(entry *MqttClient) error {
	if err := entry.ctx.Err(); err != nil {
		return err
	}
	token := entry.client.Connect()
	select {
	case <-entry.ctx.Done():
		return entry.ctx.Err()
	case <-token.Done():
		return token.Error()
	}
}

// Paho's Subscribe/Unsubscribe can themselves wait for an outgoing slot. Keep
// that call outside the reconciliation goroutine so cancellation remains prompt.
// There is only one operation in flight per connection; timeout forces a fresh
// client before another operation is issued.
func mqttToken(ctx context.Context, start func() mqtt.Token) (mqtt.Token, <-chan struct{}, error) {
	done := make(chan struct{})
	if err := ctx.Err(); err != nil {
		close(done)
		return nil, done, err
	}
	result := make(chan mqtt.Token, 1)
	go func() { defer close(done); result <- start() }()
	select {
	case <-ctx.Done():
		return nil, done, ctx.Err()
	case token := <-result:
		return token, done, nil
	}
}

func (m *MqttManager) cleanupSubscriptions(entry *MqttClient) {
	if !entry.client.IsConnectionOpen() {
		return
	}
	m.mu.Lock()
	topics := make([]string, 0, len(entry.filters))
	for topic := range entry.filters {
		topics = append(topics, topic)
	}
	m.mu.Unlock()
	if len(topics) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), m.cleanupTimeout)
	defer cancel()
	// A cancelled caller can leave Subscribe waiting for Paho's outgoing slot.
	// Establish its send order before final Unsubscribe, within one cleanup budget.
	if entry.operationDone != nil {
		select {
		case <-entry.operationDone:
		case <-ctx.Done():
			m.logFailure(entry, "mqtt final unsubscribe incomplete", ctx.Err())
			return
		}
	}
	token, _, err := mqttToken(ctx, func() mqtt.Token { return entry.client.Unsubscribe(topics...) })
	if err == nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-token.Done():
			err = token.Error()
		}
	}
	if err != nil {
		m.logFailure(entry, "mqtt final unsubscribe incomplete", err)
	}
}

func (m *MqttManager) reconcile(entry *MqttClient) error {
	m.mu.Lock()
	generation := entry.generation
	topics := make([]string, 0, len(entry.filters))
	for topic := range entry.filters {
		topics = append(topics, topic)
	}
	m.mu.Unlock()
	if generation == 0 {
		return nil
	} // OnConnect has not published its state yet.
	for _, topic := range topics {
		if entry.ctx.Err() != nil {
			return entry.ctx.Err()
		}
		m.mu.Lock()
		owners, exists := entry.filters[topic]
		var qos byte
		for _, requested := range owners {
			if requested > qos {
				qos = requested
			}
		}
		remove := len(owners) == 0
		applied, subscribed := entry.applied[topic]
		unchanged := !remove && subscribed && applied == qos
		m.mu.Unlock()
		if !exists || unchanged {
			continue
		}
		opCtx, cancel := context.WithTimeout(entry.ctx, m.operationTimeout)
		client := entry.client
		token, methodDone, err := mqttToken(opCtx, func() mqtt.Token {
			if remove {
				return client.Unsubscribe(topic)
			}
			return client.Subscribe(topic, qos, m.route(entry, topic))
		})
		entry.operationDone = methodDone
		if err == nil {
			err = m.waitSubscription(opCtx, entry, token, generation)
		}
		cancel()
		if err != nil {
			return err
		}
		if !remove {
			if result, ok := token.(*mqtt.SubscribeToken); ok {
				for _, code := range result.Result() {
					if code == 0x80 {
						return fmt.Errorf("broker rejected subscription %q", topic)
					}
				}
			}
		}
		m.mu.Lock()
		if entry.generation == generation {
			if remove {
				delete(entry.applied, topic)
				if len(entry.filters[topic]) == 0 {
					delete(entry.filters, topic)
				}
			} else {
				entry.applied[topic] = qos
			}
		}
		changed := entry.generation != generation
		m.mu.Unlock()
		if changed {
			return nil
		}
	}
	return nil
}

func (m *MqttManager) waitSubscription(ctx context.Context, entry *MqttClient, token mqtt.Token, generation uint64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-token.Done():
			return token.Error()
		case <-entry.wake:
			m.mu.Lock()
			changed := entry.generation != generation
			m.mu.Unlock()
			if changed {
				return fmt.Errorf("mqtt: connection changed during subscription")
			}
		}
	}
}

func (m *MqttManager) route(entry *MqttClient, topic string) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		m.mu.Lock()
		owners := make([]*mqttSubscription, 0, len(entry.filters[topic]))
		if entry.ctx.Err() == nil && entry.client == client {
			for owner := range entry.filters[topic] {
				owners = append(owners, owner)
			}
		}
		m.mu.Unlock()
		for _, owner := range owners {
			owner.mu.Lock()
			if owner.active {
				owner.handler(msg.Topic(), append([]byte(nil), msg.Payload()...))
			}
			owner.mu.Unlock()
		}
	}
}

// Called with m.mu held. A running control publish pins the same connection;
// releasing the last collector must not interrupt its existing token.Wait.
func (m *MqttManager) releaseUnused(entry *MqttClient) {
	if entry.publishes != 0 {
		return
	}
	for _, owners := range entry.filters {
		if len(owners) != 0 {
			return
		}
	}
	entry.cancel()
}

func (m *MqttManager) Publish(cfg MqttConfig, topic string, payload []byte, qos byte) error {
	m.mu.Lock()
	entry := m.clients[mqttClientKey(cfg)]
	if entry == nil || entry.ctx.Err() != nil || entry.generation == 0 || entry.restarting || !entry.client.IsConnected() {
		m.mu.Unlock()
		return fmt.Errorf("mqtt: client not connected")
	}
	entry.publishes++
	m.mu.Unlock()
	defer func() { m.mu.Lock(); entry.publishes--; m.releaseUnused(entry); m.mu.Unlock() }()
	token := entry.client.Publish(topic, qos, false, payload)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("mqtt publish failed: %w", token.Error())
	}
	return nil
}

func (m *MqttManager) SubscribeAndWait(cfg MqttConfig, topic string, qos byte, timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ch := make(chan []byte, 1)
	release, err := m.Watch(ctx, cfg, topic, qos, func(_ string, payload []byte) {
		select {
		case ch <- payload:
		default:
		}
	})
	if err != nil {
		return nil, err
	}
	defer release()
	select {
	case payload := <-ch:
		return payload, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("mqtt: timeout waiting for message")
	}
}

func (m *MqttManager) Close() error {
	m.mu.Lock()
	m.closed = true
	entries := make([]*MqttClient, 0, len(m.clients))
	var subscriptions []*mqttSubscription
	for _, entry := range m.clients {
		entries = append(entries, entry)
		entry.cancel()
		for _, owners := range entry.filters {
			for owner := range owners {
				subscriptions = append(subscriptions, owner)
			}
		}
		for topic := range entry.filters {
			entry.filters[topic] = make(map[*mqttSubscription]byte)
		}
	}
	m.mu.Unlock()
	for _, subscription := range subscriptions {
		subscription.deactivate()
	}
	for _, entry := range entries {
		<-entry.done
	}
	return nil
}

func (m *MqttManager) logFailure(entry *MqttClient, message string, err error) {
	if logger.Log != nil {
		logger.Log.Warn(message, zap.String("broker", entry.cfg.Broker), zap.String("clientId", entry.cfg.ClientID), zap.Error(err))
	}
}

func mqttClientKey(cfg MqttConfig) string { return fmt.Sprintf("%s|%s", cfg.Broker, cfg.ClientID) }

// ParseMqttPayload 解析 MQTT 消息载荷，支持 JSON 和纯文本
func ParseMqttPayload(payload []byte, path string) (interface{}, error) {
	return parseMqttPayload(payload, path, false)
}

// ParseMqttPayloadExact keeps JSON numbers as json.Number so an explicitly typed
// integer point is not rounded through float64. Old configurations keep using
// ParseMqttPayload and its float64 behaviour.
func ParseMqttPayloadExact(payload []byte, path string) (interface{}, error) {
	return parseMqttPayload(payload, path, true)
}

func parseMqttPayload(payload []byte, path string, useNumber bool) (interface{}, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("empty payload")
	}

	decode := func(target interface{}) error {
		decoder := json.NewDecoder(bytes.NewReader(payload))
		if useNumber {
			decoder.UseNumber()
		}
		if err := decoder.Decode(target); err != nil {
			return err
		}
		// json.Unmarshal rejected trailing content; keep that behaviour.
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			return fmt.Errorf("unexpected trailing content")
		}
		return nil
	}

	// 尝试 JSON 解析
	if path == "" {
		// 没有路径，直接返回原始值
		var result interface{}
		if err := decode(&result); err != nil {
			// 返回原始字符串
			return string(payload), nil
		}
		return result, nil
	}

	var data interface{}
	if err := decode(&data); err != nil {
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
