package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
)

func TestRecordPublishesNestedFiveFieldJSONAndKeepsSnapshotIndependent(t *testing.T) {
	writer, snapshots, publications := newJudgeEnabledResultWriter(t)
	device := judgeTestDevice("device-1", "serial-1")

	if err := writer.record(device, config.ModbusParam{Identify: "temperature"}, 23.5); err != nil {
		t.Fatalf("record() error = %v", err)
	}
	if got := snapshots.singleKey(t); got != "device:data:device-1" {
		t.Fatalf("snapshot key = %q", got)
	}
	published := publications.next(t)
	if published.channel != defaultJudgeSourceChannel {
		t.Fatalf("channel = %q", published.channel)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(published.payload, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload) != 5 {
		t.Fatalf("top-level field count = %d, payload=%s", len(payload), published.payload)
	}
	var values map[string]any
	if err := json.Unmarshal(payload["values"], &values); err != nil {
		t.Fatalf("values is not a nested JSON object: %v", err)
	}
	if values["temperature"] != 23.5 {
		t.Fatalf("values = %#v", values)
	}
}

func TestRecordDoesNotPublishWhenJudgeSourceDisabled(t *testing.T) {
	snapshots := &resultSnapshotFake{keys: make(chan string, 2)}
	writer := &deviceResultWriter{ctx: context.Background(), redis: snapshots, snapshots: make(map[string]*deviceSnapshot)}
	if err := writer.record(judgeTestDevice("device-1", "serial-1"), config.ModbusParam{Identify: "temperature"}, 23.5); err != nil {
		t.Fatal(err)
	}
	if got := snapshots.singleKey(t); got != "device:data:device-1" {
		t.Fatalf("snapshot key = %q", got)
	}
}

func TestProtocolRecordersPublishTheirUpdatedPointInDeviceOrder(t *testing.T) {
	writer, _, publications := newJudgeEnabledResultWriter(t)
	device := judgeTestDevice("device-1", "serial-1")
	recorders := []struct {
		point string
		call  func() error
	}{
		{"modbus", func() error { return writer.record(device, config.ModbusParam{Identify: "modbus"}, 1) }},
		{"opcua", func() error { return writer.recordOpcua(device, config.OpcuaParam{Identify: "opcua"}, 2) }},
		{"bacnet", func() error { return writer.recordBacnet(device, config.BacnetParam{Identify: "bacnet"}, 3) }},
		{"mqtt", func() error { return writer.recordMqtt(device, config.MqttParam{Identify: "mqtt"}, 4) }},
	}
	for _, recorder := range recorders {
		if err := recorder.call(); err != nil {
			t.Fatalf("record %s: %v", recorder.point, err)
		}
	}
	for _, recorder := range recorders {
		var event judgeSourceEvent
		publication := publications.next(t)
		if err := json.Unmarshal(publication.payload, &event); err != nil || event.UpdatedPoint != recorder.point {
			t.Fatalf("publication=%s point=%q want=%q err=%v", publication.payload, event.UpdatedPoint, recorder.point, err)
		}
	}
}

func TestPublishFailureDoesNotBlockRealtimeSnapshot(t *testing.T) {
	writer, snapshots, publications := newJudgeEnabledResultWriter(t)
	publications.err = errors.New("redis unavailable")
	started := time.Now()
	if err := writer.record(judgeTestDevice("device-1", "serial-1"), config.ModbusParam{Identify: "temperature"}, 23.5); err != nil {
		t.Fatalf("record() error = %v", err)
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("record waited for the asynchronous Judge publication")
	}
	if got := snapshots.singleKey(t); got != "device:data:device-1" {
		t.Fatalf("snapshot key = %q", got)
	}
	_ = publications.next(t)
}

func TestEventIDFailureStillWritesRealtimeSnapshot(t *testing.T) {
	writer, snapshots, publications := newJudgeEnabledResultWriter(t)
	writer.newEventID = func() (string, error) { return "", errors.New("entropy unavailable") }
	if err := writer.record(judgeTestDevice("device-1", "serial-1"), config.ModbusParam{Identify: "temperature"}, 23.5); err != nil {
		t.Fatal(err)
	}
	_ = snapshots.singleKey(t)
	select {
	case publication := <-publications.calls:
		t.Fatalf("unexpected publication: %+v", publication)
	case <-time.After(20 * time.Millisecond):
	}
}

func newJudgeEnabledResultWriter(t *testing.T) (*deviceResultWriter, *resultSnapshotFake, *judgePublishFake) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	snapshots := &resultSnapshotFake{keys: make(chan string, 16)}
	publications := &judgePublishFake{calls: make(chan judgePublishCall, 16), subscribers: 1}
	config := judgeSourceConfig{
		enabled: true, channel: defaultJudgeSourceChannel, writeTimeout: 100 * time.Millisecond,
		workerCount: 1, queueSize: 16, queueMaxBytes: 1 << 20, maximumEventBytes: maximumJudgeEventBytes,
		deviceQueueSize: 16, deviceQueueMaxBytes: 1 << 20,
	}
	publisher, err := newJudgeSourcePublisher(ctx, config, publications)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); publisher.stop() })
	var idMu sync.Mutex
	var nextID uint64
	writer := &deviceResultWriter{
		ctx: ctx, redis: snapshots, snapshots: make(map[string]*deviceSnapshot),
		judgeSource: config, judgePublisher: publisher,
		newEventID: func() (string, error) {
			idMu.Lock()
			defer idMu.Unlock()
			nextID++
			return fmt.Sprintf("550e8400-e29b-41d4-a716-%012x", nextID), nil
		},
	}
	return writer, snapshots, publications
}

func judgeTestDevice(id, serial string) config.DeviceRuntime {
	return config.DeviceRuntime{Config: config.DeviceConfig{ID: id, SerialNumber: serial, DeviceName: "pump"}}
}

type resultSnapshotFake struct {
	keys chan string
}

func (fake *resultSnapshotFake) Set(_ context.Context, key string, _ interface{}, _ time.Duration) error {
	fake.keys <- key
	return nil
}

func (fake *resultSnapshotFake) singleKey(t *testing.T) string {
	t.Helper()
	select {
	case key := <-fake.keys:
		return key
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for snapshot SET")
		return ""
	}
}

type judgePublishCall struct {
	channel string
	payload []byte
}

type judgePublishFake struct {
	mu          sync.Mutex
	calls       chan judgePublishCall
	subscribers int64
	err         error
}

func (fake *judgePublishFake) PublishWithSubscriberCount(_ context.Context, channel string, payload interface{}) (int64, error) {
	encoded, _ := payload.([]byte)
	fake.calls <- judgePublishCall{channel: channel, payload: append([]byte(nil), encoded...)}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.subscribers, fake.err
}

func (fake *judgePublishFake) Close() error { return nil }

func (fake *judgePublishFake) next(t *testing.T) judgePublishCall {
	t.Helper()
	select {
	case call := <-fake.calls:
		return call
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Judge publication")
		return judgePublishCall{}
	}
}
