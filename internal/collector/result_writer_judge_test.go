package collector

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/store"
)

func TestRecordPublishesFiveFieldJudgeEventWhenEnabled(t *testing.T) {
	redis := newResultRedisFake()
	writer := newJudgeEnabledResultWriter(redis)
	device := judgeTestDevice("device-1", "serial-1")

	if err := writer.record(device, config.ModbusParam{Identify: "temperature"}, 23.5); err != nil {
		t.Fatalf("record() error = %v", err)
	}

	call := redis.singlePipelineCall(t)
	if call.snapshotKey != "device:data:device-1" || call.streamKey != "judge:source" {
		t.Fatalf("pipeline call = %#v", call)
	}
	if len(call.streamValues) != 5 {
		t.Fatalf("stream field count = %d, want 5", len(call.streamValues))
	}
	if call.streamValues["event_id"] != "550e8400-e29b-41d4-a716-000000000001" ||
		call.streamValues["device_id"] != "device-1" ||
		call.streamValues["updated_point"] != "temperature" {
		t.Fatalf("stream values = %#v", call.streamValues)
	}
}

func TestRecordDoesNotPublishJudgeEventWhenDisabled(t *testing.T) {
	redis := newResultRedisFake()
	writer := &deviceResultWriter{
		ctx:       context.Background(),
		redis:     redis,
		snapshots: make(map[string]*deviceSnapshot),
	}

	if err := writer.record(judgeTestDevice("device-1", "serial-1"), config.ModbusParam{Identify: "temperature"}, 23.5); err != nil {
		t.Fatalf("record() error = %v", err)
	}

	redis.mu.Lock()
	defer redis.mu.Unlock()
	if len(redis.setCalls) != 1 {
		t.Fatalf("SET calls = %d, want 1", len(redis.setCalls))
	}
	if len(redis.pipelineCalls) != 0 || len(redis.xaddCalls) != 0 {
		t.Fatalf("Source calls = pipeline:%d xadd:%d, want zero", len(redis.pipelineCalls), len(redis.xaddCalls))
	}
}

func TestRecordGeneratesDistinctUUIDPerLogicalEvent(t *testing.T) {
	redis := newResultRedisFake()
	writer := newJudgeEnabledResultWriter(redis)
	device := judgeTestDevice("device-1", "serial-1")

	if err := writer.record(device, config.ModbusParam{Identify: "temperature"}, 23.5); err != nil {
		t.Fatal(err)
	}
	if err := writer.record(device, config.ModbusParam{Identify: "pressure"}, 18.2); err != nil {
		t.Fatal(err)
	}

	calls := redis.pipelineCallsSnapshot()
	if len(calls) != 2 {
		t.Fatalf("pipeline calls = %d, want 2", len(calls))
	}
	if calls[0].streamValues["event_id"] == calls[1].streamValues["event_id"] ||
		!validJudgeEventID(calls[0].streamValues["event_id"].(string)) || !validJudgeEventID(calls[1].streamValues["event_id"].(string)) {
		t.Fatalf("event identities = %#v %#v", calls[0].streamValues, calls[1].streamValues)
	}
}

func TestProtocolRecordersPublishTheirUpdatedPoint(t *testing.T) {
	redis := newResultRedisFake()
	writer := newJudgeEnabledResultWriter(redis)
	device := judgeTestDevice("device-1", "serial-1")

	recorders := []struct {
		point string
		call  func() error
	}{
		{point: "modbus", call: func() error { return writer.record(device, config.ModbusParam{Identify: "modbus"}, 1) }},
		{point: "opcua", call: func() error { return writer.recordOpcua(device, config.OpcuaParam{Identify: "opcua"}, 2) }},
		{point: "bacnet", call: func() error { return writer.recordBacnet(device, config.BacnetParam{Identify: "bacnet"}, 3) }},
		{point: "mqtt", call: func() error { return writer.recordMqtt(device, config.MqttParam{Identify: "mqtt"}, 4) }},
	}
	for _, recorder := range recorders {
		if err := recorder.call(); err != nil {
			t.Fatalf("record %s error = %v", recorder.point, err)
		}
	}

	calls := redis.pipelineCallsSnapshot()
	if len(calls) != len(recorders) {
		t.Fatalf("pipeline calls = %d, want %d", len(calls), len(recorders))
	}
	for i, recorder := range recorders {
		if calls[i].streamValues["updated_point"] != recorder.point {
			t.Fatalf("call %d updated_point = %#v, want %q", i, calls[i].streamValues["updated_point"], recorder.point)
		}
	}
}

func TestRecordRetriesSameEventIdentityWithoutLocalQueue(t *testing.T) {
	redis := newResultRedisFake()
	redis.pipelineResults = []store.SnapshotStreamWriteResult{{StreamErr: errors.New("temporary failure")}}
	redis.xaddResults = []error{nil}
	writer := newJudgeEnabledResultWriter(redis)

	if err := writer.record(judgeTestDevice("device-1", "serial-1"), config.ModbusParam{Identify: "temperature"}, 23.5); err != nil {
		t.Fatalf("record() error = %v", err)
	}

	pipelineCall := redis.singlePipelineCall(t)
	xaddCall := redis.singleXAddCall(t)
	if !reflect.DeepEqual(pipelineCall.streamValues, xaddCall.streamValues) {
		t.Fatalf("retry values changed:\ninitial=%#v\nretry=%#v", pipelineCall.streamValues, xaddCall.streamValues)
	}
}

func TestRecordSourceFailureDoesNotFailSuccessfulRealtimeWrite(t *testing.T) {
	redis := newResultRedisFake()
	redis.pipelineResults = []store.SnapshotStreamWriteResult{{StreamErr: errors.New("redis unavailable")}}
	redis.xaddResults = []error{errors.New("redis unavailable")}
	writer := newJudgeEnabledResultWriter(redis)

	started := time.Now()
	err := writer.record(judgeTestDevice("device-1", "serial-1"), config.ModbusParam{Identify: "temperature"}, 23.5)
	if err != nil {
		t.Fatalf("record() error = %v, want nil because SET succeeded", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("record() took %v, finite retry budget exceeded", elapsed)
	}
	redis.mu.Lock()
	defer redis.mu.Unlock()
	if len(redis.pipelineCalls) != 1 || len(redis.xaddCalls) != 1 {
		t.Fatalf("attempt counts = pipeline:%d retry:%d, want 1/1", len(redis.pipelineCalls), len(redis.xaddCalls))
	}
}

func TestRecordEventIDFailureStillWritesRealtimeSnapshot(t *testing.T) {
	redis := newResultRedisFake()
	writer := newJudgeEnabledResultWriter(redis)
	writer.newEventID = func() (string, error) { return "", errors.New("entropy unavailable") }
	if err := writer.record(judgeTestDevice("device-1", "serial-1"), config.ModbusParam{Identify: "temperature"}, 23.5); err != nil {
		t.Fatalf("record() error = %v", err)
	}
	redis.mu.Lock()
	defer redis.mu.Unlock()
	if len(redis.setCalls) != 1 || len(redis.pipelineCalls) != 0 || len(redis.xaddCalls) != 0 {
		t.Fatalf("set=%d pipeline=%d xadd=%d", len(redis.setCalls), len(redis.pipelineCalls), len(redis.xaddCalls))
	}
}

func TestRecordSerializesSameDeviceButAllowsDifferentDevices(t *testing.T) {
	redis := newResultRedisFake()
	firstRelease := make(chan struct{})
	redis.pipelineBlocks["device:data:device-1"] = firstRelease
	writer := newJudgeEnabledResultWriter(redis)

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- writer.record(judgeTestDevice("device-1", "serial-1"), config.ModbusParam{Identify: "temperature"}, 1)
	}()
	waitPipelineStart(t, redis.pipelineStarted, "device:data:device-1")

	sameDone := make(chan error, 1)
	go func() {
		sameDone <- writer.record(judgeTestDevice("device-1", "serial-1"), config.ModbusParam{Identify: "pressure"}, 2)
	}()

	differentDone := make(chan error, 1)
	go func() {
		differentDone <- writer.record(judgeTestDevice("device-2", "serial-2"), config.ModbusParam{Identify: "temperature"}, 3)
	}()
	waitPipelineStart(t, redis.pipelineStarted, "device:data:device-2")

	select {
	case key := <-redis.pipelineStarted:
		t.Fatalf("same device reached Redis before first write completed: %s", key)
	case <-time.After(50 * time.Millisecond):
	}
	close(firstRelease)
	waitPipelineStart(t, redis.pipelineStarted, "device:data:device-1")

	for name, done := range map[string]<-chan error{"first": firstDone, "same": sameDone, "different": differentDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s record error = %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s record", name)
		}
	}
}

func TestApplyConfigDoesNotRetainSeparateSourceIdentityState(t *testing.T) {
	redis := newResultRedisFake()
	writer := newJudgeEnabledResultWriter(redis)
	device := judgeTestDevice("device-1", "serial-1")

	if err := writer.record(device, config.ModbusParam{Identify: "temperature"}, 1); err != nil {
		t.Fatal(err)
	}
	if err := writer.applyConfig(config.IotCfgType{Devices: []config.DeviceRuntime{device}}); err != nil {
		t.Fatalf("applyConfig() error = %v", err)
	}
	if err := writer.record(device, config.ModbusParam{Identify: "temperature"}, 2); err != nil {
		t.Fatal(err)
	}

	calls := redis.pipelineCallsSnapshot()
	if len(calls) != 2 {
		t.Fatalf("pipeline calls = %d, want 2", len(calls))
	}
	if calls[0].streamValues["event_id"] == calls[1].streamValues["event_id"] {
		t.Fatalf("refresh reused event identity: %#v", calls)
	}
}

func TestRemoveAndReaddDeviceUsesFreshEventIdentity(t *testing.T) {
	redis := newResultRedisFake()
	writer := newJudgeEnabledResultWriter(redis)
	device := judgeTestDevice("device-1", "serial-1")

	if err := writer.record(device, config.ModbusParam{Identify: "temperature"}, 1); err != nil {
		t.Fatal(err)
	}
	writer.remove("serial-1")
	if err := writer.record(device, config.ModbusParam{Identify: "temperature"}, 2); err != nil {
		t.Fatal(err)
	}

	calls := redis.pipelineCallsSnapshot()
	if len(calls) != 2 {
		t.Fatalf("pipeline calls = %d, want 2", len(calls))
	}
	if calls[0].streamValues["event_id"] == calls[1].streamValues["event_id"] {
		t.Fatalf("re-add reused event identity: %#v", calls)
	}
}

func TestClassifyJudgeSourceError(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{err: context.DeadlineExceeded, want: "REDIS_TIMEOUT"},
		{err: errors.New("OOM command not allowed when used memory > maxmemory"), want: "REDIS_MEMORY_EXHAUSTED"},
		{err: errors.New("READONLY You can't write against a read only replica"), want: "REDIS_READONLY"},
		{err: errors.New("MISCONF Redis is configured to save RDB snapshots"), want: "REDIS_PERSISTENCE_ERROR"},
		{err: errors.New("redis: connection pool timeout"), want: "REDIS_POOL_EXHAUSTED"},
		{err: errors.New("NOAUTH Authentication required"), want: "REDIS_AUTH_FAILED"},
		{err: errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"), want: "REDIS_KEY_TYPE_INVALID"},
		{err: errors.New("dial tcp 127.0.0.1:6379: connect: connection refused"), want: "REDIS_UNAVAILABLE"},
		{err: errors.New("unexpected write failure"), want: "REDIS_WRITE_FAILED"},
	}
	for _, tt := range tests {
		if got := classifyJudgeSourceError(tt.err); got != tt.want {
			t.Errorf("classifyJudgeSourceError(%q) = %q, want %q", tt.err, got, tt.want)
		}
	}
}

func newJudgeEnabledResultWriter(redis *resultRedisFake) *deviceResultWriter {
	var idMu sync.Mutex
	var nextID uint64
	return &deviceResultWriter{
		ctx:         context.Background(),
		redis:       redis,
		snapshots:   make(map[string]*deviceSnapshot),
		judgeSource: judgeSourceConfig{enabled: true, stream: "judge:source", writeTimeout: 100 * time.Millisecond, retryCount: 1, retryInterval: time.Millisecond, maximumEventBytes: 64 << 10},
		newEventID: func() (string, error) {
			idMu.Lock()
			defer idMu.Unlock()
			nextID++
			return fmt.Sprintf("550e8400-e29b-41d4-a716-%012x", nextID), nil
		},
	}
}

func judgeTestDevice(id, serial string) config.DeviceRuntime {
	return config.DeviceRuntime{Config: config.DeviceConfig{ID: id, SerialNumber: serial, DeviceName: "pump"}}
}

type resultRedisPipelineCall struct {
	snapshotKey  string
	streamKey    string
	streamValues map[string]any
}

type resultRedisXAddCall struct {
	streamKey    string
	streamValues map[string]any
}

type resultRedisFake struct {
	mu sync.Mutex

	setCalls        []string
	pipelineCalls   []resultRedisPipelineCall
	xaddCalls       []resultRedisXAddCall
	pipelineResults []store.SnapshotStreamWriteResult
	xaddResults     []error
	pipelineBlocks  map[string]<-chan struct{}
	pipelineStarted chan string
}

func newResultRedisFake() *resultRedisFake {
	return &resultRedisFake{
		pipelineBlocks:  make(map[string]<-chan struct{}),
		pipelineStarted: make(chan string, 32),
	}
}

func (f *resultRedisFake) Set(_ context.Context, key string, _ interface{}, _ time.Duration) error {
	f.mu.Lock()
	f.setCalls = append(f.setCalls, key)
	f.mu.Unlock()
	return nil
}

func (f *resultRedisFake) SetAndXAdd(
	_ context.Context,
	snapshotKey string,
	_ any,
	streamKey string,
	streamValues map[string]any,
) store.SnapshotStreamWriteResult {
	call := resultRedisPipelineCall{snapshotKey: snapshotKey, streamKey: streamKey, streamValues: cloneStreamValues(streamValues)}
	f.mu.Lock()
	f.pipelineCalls = append(f.pipelineCalls, call)
	block := f.pipelineBlocks[snapshotKey]
	var result store.SnapshotStreamWriteResult
	if len(f.pipelineResults) > 0 {
		result = f.pipelineResults[0]
		f.pipelineResults = f.pipelineResults[1:]
	}
	f.mu.Unlock()
	f.pipelineStarted <- snapshotKey
	if block != nil {
		<-block
	}
	return result
}

func (f *resultRedisFake) XAdd(_ context.Context, streamKey string, streamValues map[string]any) (string, error) {
	f.mu.Lock()
	f.xaddCalls = append(f.xaddCalls, resultRedisXAddCall{streamKey: streamKey, streamValues: cloneStreamValues(streamValues)})
	var err error
	if len(f.xaddResults) > 0 {
		err = f.xaddResults[0]
		f.xaddResults = f.xaddResults[1:]
	}
	f.mu.Unlock()
	if err != nil {
		return "", err
	}
	return "1720000000000-1", nil
}

func (f *resultRedisFake) pipelineCallsSnapshot() []resultRedisPipelineCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]resultRedisPipelineCall, len(f.pipelineCalls))
	copy(result, f.pipelineCalls)
	return result
}

func (f *resultRedisFake) singlePipelineCall(t *testing.T) resultRedisPipelineCall {
	t.Helper()
	calls := f.pipelineCallsSnapshot()
	if len(calls) != 1 {
		t.Fatalf("pipeline calls = %d, want 1", len(calls))
	}
	return calls[0]
}

func (f *resultRedisFake) singleXAddCall(t *testing.T) resultRedisXAddCall {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.xaddCalls) != 1 {
		t.Fatalf("XADD calls = %d, want 1", len(f.xaddCalls))
	}
	return f.xaddCalls[0]
}

func cloneStreamValues(values map[string]any) map[string]any {
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func waitPipelineStart(t *testing.T, started <-chan string, want string) {
	t.Helper()
	select {
	case got := <-started:
		if got != want {
			t.Fatalf("pipeline start = %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for pipeline %q", want)
	}
}
