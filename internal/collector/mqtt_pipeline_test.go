package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/conn"
)

type mqttSnapshotWrite struct {
	key, payload string
}

type mqttSnapshotSink struct {
	writes chan mqttSnapshotWrite
}

func (s *mqttSnapshotSink) Set(_ context.Context, key string, value interface{}, _ time.Duration) error {
	s.writes <- mqttSnapshotWrite{key: key, payload: fmt.Sprint(value)}
	return nil
}

type mqttTestRegistration struct {
	ctx      context.Context
	topic    string
	qos      byte
	handler  conn.MessageHandler
	releases atomic.Int32
}

func startMqttPipeline(t *testing.T, params []config.MqttParam) (*mqttWorker, map[string]*mqttTestRegistration, *mqttSnapshotSink) {
	t.Helper()
	sink := &mqttSnapshotSink{writes: make(chan mqttSnapshotWrite, 32)}
	writer := &deviceResultWriter{ctx: context.Background(), redis: sink, snapshots: make(map[string]*deviceSnapshot)}
	resultWriterMu.Lock()
	previous := resultWriter
	resultWriter = writer
	resultWriterMu.Unlock()
	t.Cleanup(func() {
		resultWriterMu.Lock()
		resultWriter = previous
		resultWriterMu.Unlock()
	})
	worker := newMqttWorker(context.Background(), mqttSpec{
		Device: config.DeviceRuntime{
			Config:     config.DeviceConfig{ID: "mqtt-device", SerialNumber: "mqtt-serial", Protocol: "MQTT"},
			MqttBroker: "tcp://unused.invalid:1883", MqttClientID: "pipeline", MqttQos: 1,
		},
		Params: params,
	})
	registered := make(chan *mqttTestRegistration, len(params))
	worker.subscribe = func(ctx context.Context, _ conn.MqttConfig, topic string, qos byte, handler conn.MessageHandler) (func(), error) {
		r := &mqttTestRegistration{ctx: ctx, topic: topic, qos: qos, handler: handler}
		registered <- r
		return func() { r.releases.Add(1) }, nil
	}
	worker.start()
	t.Cleanup(func() {
		worker.stop()
		select {
		case r := <-registered:
			t.Errorf("unexpected subscription registered for %q", r.topic)
		default:
		}
	})
	expected := make(map[string]bool)
	for _, param := range params {
		if !param.ReadDisabled && param.SubscribeTopic != "" {
			expected[param.SubscribeTopic] = true
		}
	}
	registrations := make(map[string]*mqttTestRegistration)
	for range expected {
		select {
		case r := <-registered:
			registrations[r.topic] = r
		case <-time.After(3 * time.Second):
			t.Fatal("worker did not register its configured subscriptions")
		}
	}
	return worker, registrations, sink
}

func (s *mqttSnapshotSink) next(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	select {
	case write := <-s.writes:
		if write.key != "device:data:mqtt-device" {
			t.Fatalf("snapshot key = %q", write.key)
		}
		var result struct {
			DeviceID string                     `json:"device_id"`
			Values   map[string]json.RawMessage `json:"current_value"`
		}
		if err := json.Unmarshal([]byte(write.payload), &result); err != nil {
			t.Fatal(err)
		}
		if result.DeviceID != "mqtt-device" {
			t.Fatalf("snapshot device = %q", result.DeviceID)
		}
		return result.Values
	case <-time.After(3 * time.Second):
		t.Fatal("subscribed MQTT message did not reach the realtime snapshot writer")
		return nil
	}
}

func TestMqttWorkerRoutesOverlappingSubscriptionsToTheirOwnPoints(t *testing.T) {
	_, registrations, sink := startMqttPipeline(t, []config.MqttParam{
		{Identify: "counter", SubscribeTopic: "sensors/+", Path: "data.counter", DataType: "int64", DataTypeExplicit: true},
		{Identify: "temperature", SubscribeTopic: "sensors/a", Path: "data.temperature", DataType: "float32", DataTypeExplicit: true, Qos: 2},
		{Identify: "disabled", SubscribeTopic: "disabled", ReadDisabled: true},
		{Identify: "empty-topic"},
	})
	if registrations["sensors/+"].qos != 1 || registrations["sensors/a"].qos != 2 {
		t.Fatal("device default QoS or explicit point QoS changed")
	}
	payload := []byte(`{"data":{"counter":9007199254740993,"temperature":21.5}}`)
	registrations["sensors/+"].handler("sensors/a", payload)
	values := sink.next(t)
	if len(values) != 1 || string(values["counter"]) != "9007199254740993" {
		t.Fatalf("wildcard subscription lost the exact integer or used another filter's points: %s", values)
	}
	registrations["sensors/a"].handler("sensors/a", payload)
	values = sink.next(t)
	if len(values) != 2 || string(values["counter"]) != "9007199254740993" || string(values["temperature"]) != "21.5" {
		t.Fatalf("overlapping subscription did not update its own point: %s", values)
	}
}

func TestMqttWorkerKeepsLegacyConversionAndRejectsInvalidExplicitValue(t *testing.T) {
	_, registrations, sink := startMqttPipeline(t, []config.MqttParam{
		{Identify: "strict", SubscribeTopic: "data", Path: "strict", DataType: "int16", DataTypeExplicit: true},
		{Identify: "legacy", SubscribeTopic: "data", Path: "legacy", DataType: "int"},
	})
	handler := registrations["data"].handler
	handler("data", []byte(`{"strict":7,"legacy":1.9}`))
	sink.next(t)
	values := sink.next(t)
	if string(values["strict"]) != "7" || string(values["legacy"]) != "1" {
		t.Fatalf("initial conversions = %s", values)
	}
	// The legacy point's update is a processing barrier for the invalid strict point
	// in the same message: the prior valid strict value must remain untouched.
	handler("data", []byte(`{"strict":7.5,"legacy":2.9}`))
	values = sink.next(t)
	if string(values["strict"]) != "7" || string(values["legacy"]) != "2" {
		t.Fatalf("invalid explicit integer overwrote a valid reading or legacy behavior changed: %s", values)
	}
	select {
	case write := <-sink.writes:
		t.Fatalf("invalid point produced an unexpected snapshot: %s", write.payload)
	default:
	}
}

func TestMqttWorkerStopReleasesSubscriptionsAndIgnoresLateCallbacks(t *testing.T) {
	worker, registrations, sink := startMqttPipeline(t, []config.MqttParam{
		{Identify: "value", SubscribeTopic: "data", Path: "value", DataType: "int16", DataTypeExplicit: true},
	})
	r := registrations["data"]
	r.handler("data", []byte(`{"value":1}`))
	sink.next(t)
	worker.stop()
	worker.stop()
	if r.releases.Load() != 1 || r.ctx.Err() != context.Canceled {
		t.Fatalf("stop did not release its subscription exactly once: releases=%d, context=%v", r.releases.Load(), r.ctx.Err())
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for i := 0; i < 100; i++ {
			r.handler("data", []byte(`{"value":2}`))
		}
	}()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("late callbacks blocked after the worker stopped")
	}
	select {
	case write := <-sink.writes:
		t.Fatalf("stopped worker wrote another snapshot: %s", write.payload)
	default:
	}
}
