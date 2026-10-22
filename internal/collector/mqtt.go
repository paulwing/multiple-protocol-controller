package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/conn"
	"multiple-protocol-controller/internal/protocol"
	"multiple-protocol-controller/pkg/utils"

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

func isMqttDevice(proto string) bool {
	return protocol.NormalizeName(proto) == "MQTT"
}

// mqttWorker MQTT 采集 worker
type mqttWorker struct {
	spec      mqttSpec
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	msgCh     chan mqttDelivery
	subscribe func(context.Context, conn.MqttConfig, string, byte, conn.MessageHandler) (func(), error)
}

type mqttDelivery struct {
	message conn.MqttMessage
	params  []config.MqttParam
}

func newMqttWorker(parent context.Context, spec mqttSpec) *mqttWorker {
	ctx, cancel := context.WithCancel(parent)
	return &mqttWorker{
		spec:      spec,
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
		msgCh:     make(chan mqttDelivery, 10),
		subscribe: conn.DefaultMqttManager().Watch,
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

	// Each callback carries its own configured mapping. Paho has already
	// matched the topic filter; matching again loses overlapping point groups.
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	for topic, params := range topicParams {
		qos := params[0].Qos
		if qos == 0 {
			qos = mqttCfg.Qos
		}
		handler := func(actualTopic string, payload []byte) {
			if w.ctx.Err() != nil {
				return
			}
			select {
			case w.msgCh <- mqttDelivery{message: conn.MqttMessage{Topic: actualTopic, Payload: payload}, params: params}:
			default:
				logInfo("mqtt message channel full", zap.String("deviceSerial", w.spec.Device.Config.SerialNumber))
			}
		}
		release, err := w.subscribeAfterConfigHandoff(mqttCfg, topic, qos, handler)
		if err != nil {
			if w.ctx.Err() != nil {
				return
			}
			logInfo("mqtt subscription registration failed", zap.String("deviceSerial", w.spec.Device.Config.SerialNumber), zap.String("topic", topic), zap.Error(err))
			continue
		}
		releases = append(releases, release)
	}
	for {
		select {
		case delivery := <-w.msgCh:
			if w.ctx.Err() != nil {
				return
			}
			w.handleMessage(delivery.message, delivery.params)
		case <-w.ctx.Done():
			return
		}
	}
}

// A reload starts each replacement worker independently. Other old workers or
// an in-flight control publish may still own the previous connection options.
// Wait for that handoff without owning, replacing or publishing on the old
// connection. A genuinely inconsistent configuration remains a conflict.
func (w *mqttWorker) subscribeAfterConfigHandoff(cfg conn.MqttConfig, topic string, qos byte, handler conn.MessageHandler) (func(), error) {
	waiting := false
	for {
		if err := w.ctx.Err(); err != nil {
			return nil, err
		}
		release, err := w.subscribe(w.ctx, cfg, topic, qos, handler)
		if !errors.Is(err, conn.ErrMqttConnectionConfigConflict) {
			return release, err
		}
		if !waiting {
			logInfo("mqtt waiting for shared connection configuration", zap.String("deviceSerial", w.spec.Device.Config.SerialNumber), zap.String("topic", topic))
			waiting = true
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-w.ctx.Done():
			timer.Stop()
			return nil, w.ctx.Err()
		case <-timer.C:
		}
	}
}

func (w *mqttWorker) handleMessage(msg conn.MqttMessage, params []config.MqttParam) {
	// 解析消息
	for _, param := range params {
		parse := conn.ParseMqttPayload
		if param.DataTypeExplicit {
			// Explicit integer points must see the exact JSON number, not a
			// float64 rounded before normalization. Old configs keep the old
			// parser and its behaviour.
			parse = conn.ParseMqttPayloadExact
		}
		value, err := parse(msg.Payload, param.Path)
		if err != nil {
			logInfo("mqtt parse payload failed",
				zap.String("deviceSerial", w.spec.Device.Config.SerialNumber),
				zap.String("param", param.Identify),
				zap.Error(err))
			continue
		}

		// 类型转换
		converted, err := normalizeMqttValue(value, param.DataType, param.DataTypeExplicit)
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
// normalizeMqttValueLegacy is the pre-dataType read conversion, preserved for
// properties that fall back to their old property type.
func normalizeMqttValueLegacy(value interface{}, dataType string) (interface{}, error) {
	value = utils.LegacyJSONNumberValue(value)
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
	case "float", "double":
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

func normalizeMqttValue(value interface{}, dataType string, explicit bool) (interface{}, error) {
	if value == nil {
		return nil, fmt.Errorf("nil value")
	}
	if !explicit {
		return normalizeMqttValueLegacy(value, dataType)
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

	// Canonical point-level types are explicit configuration: convert without
	// rounding and reject values the target width cannot hold. The legacy names
	// int/uint/float/double keep the loose conversion below.
	switch dt {
	case "int8":
		return coerceNarrowSigned(value, math.MinInt8, math.MaxInt8, func(n int64) interface{} { return int8(n) })
	case "int16":
		return coerceNarrowSigned(value, math.MinInt16, math.MaxInt16, func(n int64) interface{} { return int16(n) })
	case "int32":
		return coerceNarrowSigned(value, math.MinInt32, math.MaxInt32, func(n int64) interface{} { return int32(n) })
	case "int64":
		n, err := utils.CoerceExactInt64(value)
		if err != nil {
			return nil, err
		}
		return n, nil
	case "uint8":
		return coerceNarrowUnsigned(value, math.MaxUint8, func(n uint64) interface{} { return uint8(n) })
	case "uint16":
		return coerceNarrowUnsigned(value, math.MaxUint16, func(n uint64) interface{} { return uint16(n) })
	case "uint32":
		return coerceNarrowUnsigned(value, math.MaxUint32, func(n uint64) interface{} { return uint32(n) })
	case "uint64":
		n, err := utils.CoerceExactUint64(value)
		if err != nil {
			return nil, err
		}
		return n, nil
	case "float32":
		f, err := utils.CoerceFloat64(value)
		if err != nil {
			return nil, err
		}
		if math.IsNaN(f) || math.IsInf(f, 0) || math.IsInf(float64(float32(f)), 0) {
			return nil, fmt.Errorf("value %v does not fit float32", f)
		}
		return float32(f), nil
	case "float64":
		f, err := utils.CoerceFloat64(value)
		if err != nil {
			return nil, err
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, fmt.Errorf("value %v is not a finite float64", f)
		}
		return f, nil
	}

	// 尝试类型转换
	switch dt {
	case "float":
		return toFloat64(value)
	case "double":
		return toFloat64(value)
	case "int":
		return toInt64(value)
	case "uint":
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

func coerceNarrowSigned(value interface{}, min int64, max int64, convert func(int64) interface{}) (interface{}, error) {
	n, err := utils.CoerceExactInt64(value)
	if err != nil {
		return nil, err
	}
	if n < min || n > max {
		return nil, fmt.Errorf("%d exceeds %T range", n, convert(0))
	}
	return convert(n), nil
}

func coerceNarrowUnsigned(value interface{}, max uint64, convert func(uint64) interface{}) (interface{}, error) {
	n, err := utils.CoerceExactUint64(value)
	if err != nil {
		return nil, err
	}
	if n > max {
		return nil, fmt.Errorf("%d exceeds %T range", n, convert(0))
	}
	return convert(n), nil
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
	case json.Number:
		f, err := val.Float64()
		if err != nil {
			return 0, err
		}
		return int64(f), nil
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
	case json.Number:
		f, err := val.Float64()
		if err != nil {
			return 0, err
		}
		if f < 0 {
			return 0, fmt.Errorf("negative value")
		}
		return uint64(f), nil
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
	case json.Number:
		f, err := val.Float64()
		if err != nil {
			return false, err
		}
		return f != 0, nil
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
