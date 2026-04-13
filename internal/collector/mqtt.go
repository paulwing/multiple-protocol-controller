package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/conn"

	"go.uber.org/zap"
)

type mqttSpec struct {
	Device config.DeviceRuntime
	Params []config.MqttParam
}

// buildMqttDeviceSpecs 构建 MQTT 设备规格
func buildMqttDeviceSpecs(cfg config.IotCfgType) map[string]mqttSpec {
	specs := make(map[string]mqttSpec)
	for _, dev := range cfg.Devices {
		if !isMqttDevice(dev.Config.Protocol) {
			continue
		}
		if dev.Config.SerialNumber == "" {
			continue
		}
		params := dev.MqttReadPoints
		if len(params) == 0 {
			continue
		}
		specs[dev.Config.SerialNumber] = mqttSpec{
			Device: dev,
			Params: params,
		}
	}
	return specs
}

func isMqttDevice(protocol string) bool {
	return strings.EqualFold(strings.TrimSpace(protocol), "mqtt")
}

// mqttWorker MQTT 采集 worker
type mqttWorker struct {
	spec   mqttSpec
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	msgCh  chan conn.MqttMessage
}

func newMqttWorker(parent context.Context, spec mqttSpec) *mqttWorker {
	ctx, cancel := context.WithCancel(parent)
	return &mqttWorker{
		spec:   spec,
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
		msgCh:  make(chan conn.MqttMessage, 10),
	}
}

func (w *mqttWorker) start() {
	go w.run()
}

func (w *mqttWorker) stop() {
	w.cancel()
	<-w.done
}

func (w *mqttWorker) run() {
	defer close(w.done)

	// 构建 MQTT 配置
	mqttCfg := conn.MqttConfig{
		Broker:       w.spec.Device.MqttBroker,
		ClientID:     w.spec.Device.MqttClientID,
		Username:     w.spec.Device.MqttUsername,
		Password:     w.spec.Device.MqttPassword,
		Qos:          w.spec.Device.MqttQos,
		KeepAlive:    w.spec.Device.MqttKeepAlive,
		CleanSession: w.spec.Device.MqttCleanSession,
	}

	// 连接到 MQTT Broker
	mgr := conn.DefaultMqttManager()
	if err := mgr.Connect(mqttCfg); err != nil {
		logInfo("mqtt connect failed",
			zap.String("deviceSerial", w.spec.Device.Config.SerialNumber),
			zap.Error(err))
		return
	}

	// 收集所有需要订阅的主题
	topicParams := make(map[string][]config.MqttParam)
	for _, param := range w.spec.Params {
		if param.ReadDisabled {
			continue
		}
		topic := param.SubscribeTopic
		if topic == "" {
			continue
		}
		topicParams[topic] = append(topicParams[topic], param)
	}

	// 创建消息处理函数
	handler := func(topic string, payload []byte) {
		select {
		case w.msgCh <- conn.MqttMessage{Topic: topic, Payload: payload}:
		default:
			logInfo("mqtt message channel full",
				zap.String("deviceSerial", w.spec.Device.Config.SerialNumber))
		}
	}

	// 订阅所有主题
	for topic, params := range topicParams {
		qos := params[0].Qos
		if qos == 0 {
			qos = mqttCfg.Qos
		}
		if err := mgr.Subscribe(mqttCfg, topic, qos, handler); err != nil {
			logInfo("mqtt subscribe failed",
				zap.String("deviceSerial", w.spec.Device.Config.SerialNumber),
				zap.String("topic", topic),
				zap.Error(err))
			continue
		}
		logInfo("mqtt subscribed",
			zap.String("deviceSerial", w.spec.Device.Config.SerialNumber),
			zap.String("topic", topic))
	}

	// 启动消息处理循环
	interval := time.Duration(w.spec.Device.Config.AcqFreq) * time.Millisecond
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 定期检查连接状态
			if err := mgr.Connect(mqttCfg); err != nil {
				logInfo("mqtt reconnect failed",
					zap.String("deviceSerial", w.spec.Device.Config.SerialNumber),
					zap.Error(err))
			}
		case msg := <-w.msgCh:
			w.handleMessage(msg, topicParams)
		case <-w.ctx.Done():
			// 取消订阅并断开连接
			for topic := range topicParams {
				_ = mgr.Unsubscribe(mqttCfg, topic)
			}
			_ = mgr.Disconnect(mqttCfg)
			return
		}
	}
}

func (w *mqttWorker) handleMessage(msg conn.MqttMessage, topicParams map[string][]config.MqttParam) {
	// 找到对应的参数
	var params []config.MqttParam
	for topic, ps := range topicParams {
		if matchTopic(msg.Topic, topic) {
			params = ps
			break
		}
	}

	if len(params) == 0 {
		return
	}

	// 解析消息
	for _, param := range params {
		value, err := conn.ParseMqttPayload(msg.Payload, param.Path)
		if err != nil {
			logInfo("mqtt parse payload failed",
				zap.String("deviceSerial", w.spec.Device.Config.SerialNumber),
				zap.String("param", param.Identify),
				zap.Error(err))
			continue
		}

		// 类型转换
		converted, err := normalizeMqttValue(value, param.DataType)
		if err != nil {
			logInfo("mqtt value normalize failed",
				zap.String("deviceSerial", w.spec.Device.Config.SerialNumber),
				zap.String("param", param.Identify),
				zap.Error(err))
			continue
		}

		logDebug("mqtt collect",
			zap.String("deviceSerial", w.spec.Device.Config.SerialNumber),
			zap.String("param", param.Identify),
			zap.Any("value", converted))

		recordCollectedMqttValue(w.spec.Device, param, converted)
	}
}

// normalizeMqttValue 将值转换为目标类型
func normalizeMqttValue(value interface{}, dataType string) (interface{}, error) {
	if value == nil {
		return nil, fmt.Errorf("nil value")
	}

	dt := strings.ToLower(strings.TrimSpace(dataType))
	if dt == "" {
		return value, nil
	}

	// 如果类型匹配，直接返回
	switch dt {
	case "float", "double":
		if _, ok := value.(float64); ok {
			return value, nil
		}
	case "int", "int32", "int64":
		if _, ok := value.(int64); ok {
			return value, nil
		}
	case "uint", "uint32", "uint64":
		if _, ok := value.(uint64); ok {
			return value, nil
		}
	case "bool":
		if _, ok := value.(bool); ok {
			return value, nil
		}
	case "string":
		if _, ok := value.(string); ok {
			return value, nil
		}
	}

	// 尝试类型转换
	switch dt {
	case "float":
		return toFloat64(value)
	case "double":
		return toFloat64(value)
	case "int", "int32", "int64":
		return toInt64(value)
	case "uint", "uint32", "uint64":
		return toUint64(value)
	case "bool":
		return toBool(value)
	case "string":
		return fmt.Sprintf("%v", value), nil
	default:
		// JSON number
		if f, ok := toFloat64(value); ok == nil {
			return f, nil
		}
		if b, ok := toBool(value); ok == nil {
			return b, nil
		}
		return fmt.Sprintf("%v", value), nil
	}
}

func toFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	case uint:
		return float64(val), nil
	case string:
		var f float64
		if err := json.Unmarshal([]byte(val), &f); err == nil {
			return f, nil
		}
		return 0, fmt.Errorf("cannot convert string to float64")
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func toInt64(v interface{}) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case float64:
		return int64(val), nil
	case string:
		var i int64
		if err := json.Unmarshal([]byte(val), &i); err == nil {
			return i, nil
		}
		return 0, fmt.Errorf("cannot convert string to int64")
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toUint64(v interface{}) (uint64, error) {
	switch val := v.(type) {
	case uint64:
		return val, nil
	case uint:
		return uint64(val), nil
	case int64:
		if val >= 0 {
			return uint64(val), nil
		}
		return 0, fmt.Errorf("negative value")
	case int:
		if val >= 0 {
			return uint64(val), nil
		}
		return 0, fmt.Errorf("negative value")
	case float64:
		if val >= 0 {
			return uint64(val), nil
		}
		return 0, fmt.Errorf("negative value")
	case string:
		var u uint64
		if err := json.Unmarshal([]byte(val), &u); err == nil {
			return u, nil
		}
		return 0, fmt.Errorf("cannot convert string to uint64")
	default:
		return 0, fmt.Errorf("cannot convert %T to uint64", v)
	}
}

func toBool(v interface{}) (bool, error) {
	switch val := v.(type) {
	case bool:
		return val, nil
	case int64:
		return val != 0, nil
	case int:
		return val != 0, nil
	case float64:
		return val != 0, nil
	case string:
		lower := strings.ToLower(val)
		if lower == "true" || lower == "1" || lower == "yes" {
			return true, nil
		}
		if lower == "false" || lower == "0" || lower == "no" {
			return false, nil
		}
		return false, fmt.Errorf("cannot convert string to bool")
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v)
	}
}

func matchTopic(topic string, pattern string) bool {
	patternParts := strings.Split(pattern, "/")
	topicParts := strings.Split(topic, "/")

	if len(patternParts) != len(topicParts) {
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

// recordCollectedMqttValue 记录采集的 MQTT 数据
func recordCollectedMqttValue(device config.DeviceRuntime, param config.MqttParam, value interface{}) {
	writer := currentResultWriter()
	if writer == nil {
		return
	}
	if err := writer.recordMqtt(device, param, value); err != nil {
		logInfo("write mqtt realtime data failed",
			zap.String("deviceSerial", device.Config.SerialNumber),
			zap.String("param", param.Identify),
			zap.Error(err))
	}
}
