package collector

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/conn"
	"multiple-protocol-controller/pkg/logger"
)

func TestGatewayStatusPublisherWritesOnlineForEverySharedEndpointID(t *testing.T) {
	manager := newGatewayStatusTestManager(t,
		gatewayStatusDevice("gw-1", "127.0.0.1", 1502),
		gatewayStatusDevice("gw-2", "127.0.0.1", 1502),
	)
	writer := &recordingGatewayWriter{}
	publisher := newTestGatewayStatusPublisher(manager, writer)
	fixed := time.UnixMilli(1700000000123)
	publisher.now = func() time.Time { return fixed }
	publisher.probe = healthyProbe(manager)

	publisher.runOnce(context.Background())

	records := writer.records()
	if len(records) != 2 {
		t.Fatalf("writes = %#v, want one per gateway ID", records)
	}
	for _, record := range records {
		if record.expiration != gatewayStatusTTL {
			t.Fatalf("ttl = %v, want %v", record.expiration, gatewayStatusTTL)
		}
		if strings.HasPrefix(record.key, "device:data:") {
			t.Fatalf("gateway status must not touch device:data, got %s", record.key)
		}
		var payload gatewayStatusPayload
		if err := json.Unmarshal([]byte(record.value), &payload); err != nil {
			t.Fatalf("payload %q: %v", record.value, err)
		}
		if payload.Status != gatewayStatusOnline {
			t.Fatalf("status = %q, want %q", payload.Status, gatewayStatusOnline)
		}
		if payload.Timestamp != fixed.UnixMilli() {
			t.Fatalf("timestamp = %d, want %d", payload.Timestamp, fixed.UnixMilli())
		}
	}
	keys := writer.keys()
	for _, want := range []string{"gateway:status:gw-1", "gateway:status:gw-2"} {
		if _, ok := keys[want]; !ok {
			t.Fatalf("missing write for %s in %v", want, keys)
		}
	}
}

func TestGatewayStatusPublisherWritesOfflineOnUnhealthy(t *testing.T) {
	manager := newGatewayStatusTestManager(t, gatewayStatusDevice("gw-1", "127.0.0.1", 1502))
	writer := &recordingGatewayWriter{}
	publisher := newTestGatewayStatusPublisher(manager, writer)
	publisher.probe = func(context.Context, *conn.Manager, string) conn.GatewayProbeResult {
		return conn.GatewayProbeResult{State: conn.GatewayProbeUnhealthy, Target: manager.GatewayTargets()[0], Generation: manager.ConfigGeneration()}
	}

	publisher.runOnce(context.Background())

	records := writer.records()
	if len(records) != 1 {
		t.Fatalf("writes = %#v, want 1", records)
	}
	var payload gatewayStatusPayload
	if err := json.Unmarshal([]byte(records[0].value), &payload); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if payload.Status != gatewayStatusOffline {
		t.Fatalf("status = %q, want %q", payload.Status, gatewayStatusOffline)
	}
}

func TestGatewayStatusPublisherSkipsInconclusiveProbeStates(t *testing.T) {
	manager := newGatewayStatusTestManager(t, gatewayStatusDevice("gw-1", "127.0.0.1", 1502))
	for _, state := range []conn.GatewayProbeState{
		conn.GatewayProbeUnknown,
		conn.GatewayProbeBusy,
		conn.GatewayProbeStale,
	} {
		t.Run(stateName(state), func(t *testing.T) {
			writer := &recordingGatewayWriter{}
			publisher := newTestGatewayStatusPublisher(manager, writer)
			publisher.probe = func(context.Context, *conn.Manager, string) conn.GatewayProbeResult {
				return conn.GatewayProbeResult{State: state, Generation: manager.ConfigGeneration()}
			}

			publisher.runOnce(context.Background())

			if got := writer.records(); len(got) != 0 {
				t.Fatalf("inconclusive probe must not refresh or overwrite status, got %#v", got)
			}
		})
	}
}

func TestGatewayStatusPublisherDropsResultsFromStaleGeneration(t *testing.T) {
	manager := newGatewayStatusTestManager(t, gatewayStatusDevice("gw-1", "127.0.0.1", 1502))
	writer := &recordingGatewayWriter{}
	publisher := newTestGatewayStatusPublisher(manager, writer)
	stale := manager.ConfigGeneration() - 1
	publisher.probe = func(context.Context, *conn.Manager, string) conn.GatewayProbeResult {
		return conn.GatewayProbeResult{State: conn.GatewayProbeHealthy, Generation: stale}
	}

	publisher.runOnce(context.Background())

	if got := writer.records(); len(got) != 0 {
		t.Fatalf("stale generation result must not be written, got %#v", got)
	}
}

func TestGatewayStatusPublisherRedisFailureDoesNotBlockOtherGateways(t *testing.T) {
	manager := newGatewayStatusTestManager(t,
		gatewayStatusDevice("gw-1", "127.0.0.1", 1502),
		gatewayStatusDevice("gw-2", "127.0.0.1", 1503),
	)
	writer := &recordingGatewayWriter{failOn: map[string]error{
		"gateway:status:gw-1": errors.New("redis unavailable"),
	}}
	publisher := newTestGatewayStatusPublisher(manager, writer)
	publisher.probe = healthyProbe(manager)

	publisher.runOnce(context.Background())

	keys := writer.keys()
	if _, ok := keys["gateway:status:gw-2"]; !ok {
		t.Fatalf("other gateway was not written after one Redis failure: %v", keys)
	}
}

func TestGatewayStatusPublisherStopsWithContext(t *testing.T) {
	manager := newGatewayStatusTestManager(t, gatewayStatusDevice("gw-1", "127.0.0.1", 1502))
	writer := &recordingGatewayWriter{}
	publisher := newTestGatewayStatusPublisher(manager, writer)
	publisher.interval = 10 * time.Millisecond
	publisher.probe = func(context.Context, *conn.Manager, string) conn.GatewayProbeResult {
		return conn.GatewayProbeResult{State: conn.GatewayProbeUnknown}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		publisher.Run(ctx)
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Run did not stop after context cancellation")
	}
}

func TestGatewayStatusPublisherDoesNotOverlapTicks(t *testing.T) {
	manager := newGatewayStatusTestManager(t, gatewayStatusDevice("gw-1", "127.0.0.1", 1502))
	writer := &recordingGatewayWriter{}
	publisher := newTestGatewayStatusPublisher(manager, writer)
	publisher.interval = 10 * time.Millisecond

	started := make(chan struct{}, 8)
	release := make(chan struct{})
	publisher.probe = func(context.Context, *conn.Manager, string) conn.GatewayProbeResult {
		started <- struct{}{}
		<-release
		return conn.GatewayProbeResult{State: conn.GatewayProbeUnknown}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		publisher.Run(ctx)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatalf("first probe did not start")
	}
	select {
	case <-started:
		t.Fatalf("next tick started before the previous one finished")
	case <-time.After(60 * time.Millisecond):
	}
	close(release)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Run did not stop")
	}
}

func TestGatewayStatusPayloadMatchesCrossStackContract(t *testing.T) {
	// 与 iot-platform-backend onlineStatusFromGatewayStatus 的解析契约对齐：
	// status 为字符串 "0"/"1"，timestamp 为 Unix 毫秒，整体为 JSON 对象。
	for _, tc := range []struct {
		status string
		want   string
	}{
		{gatewayStatusOnline, `{"status":"0","timestamp":1770000000000}`},
		{gatewayStatusOffline, `{"status":"1","timestamp":1770000000000}`},
	} {
		payload, err := json.Marshal(gatewayStatusPayload{Status: tc.status, Timestamp: 1770000000000})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if string(payload) != tc.want {
			t.Fatalf("payload = %s, want %s", payload, tc.want)
		}
	}
}

func newTestGatewayStatusPublisher(manager *conn.Manager, writer GatewayStatusWriter) *GatewayStatusPublisher {
	return NewGatewayStatusPublisher(func() (*conn.Manager, bool) { return manager, true }, writer)
}

func healthyProbe(manager *conn.Manager) func(context.Context, *conn.Manager, string) conn.GatewayProbeResult {
	return func(_ context.Context, _ *conn.Manager, key string) conn.GatewayProbeResult {
		for _, target := range manager.GatewayTargets() {
			if target.Key == key {
				return conn.GatewayProbeResult{State: conn.GatewayProbeHealthy, Target: target, Generation: manager.ConfigGeneration()}
			}
		}
		return conn.GatewayProbeResult{State: conn.GatewayProbeStale}
	}
}

func TestGatewayStatusPublisherDropsReboundSnapshotAndRecoversNextRound(t *testing.T) {
	manager := newGatewayStatusTestManager(t, gatewayStatusDevice("old-id", "127.0.0.1", 1502))
	writer := &recordingGatewayWriter{}
	publisher := newTestGatewayStatusPublisher(manager, writer)
	publisher.probe = func(ctx context.Context, m *conn.Manager, key string) conn.GatewayProbeResult {
		cfg, err := config.BuildRuntimeConfig([]config.DeviceConfig{gatewayStatusDevice("new-id", "127.0.0.1", 1502)})
		if err != nil {
			t.Error(err)
			return conn.GatewayProbeResult{State: conn.GatewayProbeUnknown}
		}
		if err := manager.ApplyConfig(cfg); err != nil {
			t.Error(err)
			return conn.GatewayProbeResult{State: conn.GatewayProbeUnknown}
		}
		return healthyProbe(manager)(ctx, m, key)
	}
	publisher.runOnce(context.Background())
	if records := writer.records(); len(records) != 0 {
		t.Fatalf("rebound snapshot must not refresh any ID: %#v", records)
	}
	publisher.probe = healthyProbe(manager)
	publisher.runOnce(context.Background())
	records := writer.records()
	if len(records) != 1 || records[0].key != "gateway:status:new-id" {
		t.Fatalf("next round must refresh only the new ID: %#v", records)
	}
}

func newGatewayStatusTestManager(t *testing.T, devices ...config.DeviceConfig) *conn.Manager {
	t.Helper()
	previous := logger.Log
	logger.Log = zap.NewNop()
	t.Cleanup(func() { logger.Log = previous })

	cfg, err := config.BuildRuntimeConfig(devices)
	if err != nil {
		t.Fatalf("BuildRuntimeConfig: %v", err)
	}
	manager := conn.NewManager(context.Background())
	t.Cleanup(func() { _ = manager.Close() })
	if err := manager.ApplyConfig(cfg); err != nil {
		t.Fatalf("ApplyConfig: %v", err)
	}
	return manager
}

func gatewayStatusDevice(id, ip string, port int) config.DeviceConfig {
	device := config.DeviceConfig{
		ID:           id,
		DeviceType:   1,
		SerialNumber: "SN-" + id,
		DeviceName:   id,
		Protocol:     "modbus_rtu",
		AcqFreq:      1000,
	}
	device.GatewayInfo.ID = id
	device.GatewayInfo.IP = ip
	device.GatewayInfo.Port = port
	device.GatewayInfo.DeviceName = id
	return device
}

type recordedGatewaySet struct {
	key        string
	value      string
	expiration time.Duration
}

type recordingGatewayWriter struct {
	mu     sync.Mutex
	sets   []recordedGatewaySet
	failOn map[string]error
}

func (w *recordingGatewayWriter) Set(_ context.Context, key string, value interface{}, expiration time.Duration) error {
	text, _ := value.(string)
	w.mu.Lock()
	w.sets = append(w.sets, recordedGatewaySet{key: key, value: text, expiration: expiration})
	failErr := w.failOn[key]
	w.mu.Unlock()
	return failErr
}

func (w *recordingGatewayWriter) records() []recordedGatewaySet {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]recordedGatewaySet{}, w.sets...)
}

func (w *recordingGatewayWriter) keys() map[string]struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	keys := make(map[string]struct{}, len(w.sets))
	for _, set := range w.sets {
		keys[set.key] = struct{}{}
	}
	return keys
}

func stateName(state conn.GatewayProbeState) string {
	switch state {
	case conn.GatewayProbeUnknown:
		return "unknown"
	case conn.GatewayProbeBusy:
		return "busy"
	case conn.GatewayProbeStale:
		return "stale"
	default:
		return "other"
	}
}
