package collector

import (
	"context"
	"errors"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
)

func TestJudgeDeviceBudgetsDefaultAndFitInsideGlobalBudgets(t *testing.T) {
	for _, test := range []struct {
		name         string
		raw          config.JudgeSourceCfg
		count, bytes int
	}{
		{"defaults", config.JudgeSourceCfg{}, 64, 1 << 20},
		{"custom", config.JudgeSourceCfg{DeviceQueueSize: 12, DeviceQueueMaxBytes: 4096}, 12, 4096},
		{"negative defaults", config.JudgeSourceCfg{DeviceQueueSize: -1, DeviceQueueMaxBytes: -1}, 64, 1 << 20},
		{"bounded by global", config.JudgeSourceCfg{WorkerCount: 1, QueueSize: 4, QueueMaxBytes: 1024, DeviceQueueSize: 100, DeviceQueueMaxBytes: 4096}, 4, 1024},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := normalizedJudgeSourceConfig(test.raw)
			if got.deviceQueueSize != test.count || got.deviceQueueMaxBytes != test.bytes {
				t.Fatalf("device budgets = %d/%d, want %d/%d", got.deviceQueueSize, got.deviceQueueMaxBytes, test.count, test.bytes)
			}
		})
	}
}

func TestJudgeDeviceCountIncludesInFlightAndPreservesOtherDevices(t *testing.T) {
	publisher := admissionTestPublisher(2, 64)
	if !publisher.enqueue(testJudgeSourceEvent("A", "a1")) {
		t.Fatal("first A rejected")
	}
	first := takeAdmissionPublication(t, publisher, "A", "a1")
	if !publisher.enqueue(testJudgeSourceEvent("A", "a2")) || publisher.enqueue(testJudgeSourceEvent("A", "a3")) {
		t.Fatal("device count budget did not include its in-flight publication")
	}
	if !publisher.enqueue(testJudgeSourceEvent("B", "b1")) {
		t.Fatal("hot device prevented B admission despite free global capacity")
	}
	if got := judgeSourceFailureCount(publisher.failureLog, "SOURCE_DEVICE_QUEUE_FULL"); got != 1 {
		t.Fatalf("device count rejection count = %d, want 1", got)
	}
	publisher.completePublication(first)
	publisher.completePublication(takeAdmissionPublication(t, publisher, "B", "b1"))
	publisher.completePublication(takeAdmissionPublication(t, publisher, "A", "a2"))
	if publisher.queueBytes != 0 || publisher.queuedPublications != 0 || len(publisher.deviceQueues) != 0 {
		t.Fatal("completed publications retained admission capacity or device entries")
	}
	if !publisher.enqueue(testJudgeSourceEvent("A", "a3")) {
		t.Fatal("device budget was not reusable after draining")
	}
}

func TestJudgeDeviceByteBudgetIncludesInFlightAndIsReleased(t *testing.T) {
	publisher := admissionTestPublisher(8, 8)
	if !publisher.enqueue(testJudgeSourceEvent("A", "123456")) {
		t.Fatal("first A rejected")
	}
	first := takeAdmissionPublication(t, publisher, "A", "123456")
	if publisher.enqueue(testJudgeSourceEvent("A", "123")) {
		t.Fatal("device byte budget ignored the in-flight payload")
	}
	if !publisher.enqueue(testJudgeSourceEvent("B", "b")) {
		t.Fatal("device A byte limit incorrectly rejected B")
	}
	if publisher.enqueue(testJudgeSourceEvent("C", "123456789")) {
		t.Fatal("oversize first event bypassed the per-device byte limit")
	}
	if got := judgeSourceFailureCount(publisher.failureLog, "SOURCE_DEVICE_QUEUE_BYTES_FULL"); got != 2 {
		t.Fatalf("device byte rejection count = %d, want 2", got)
	}
	publisher.completePublication(first)
	if !publisher.enqueue(testJudgeSourceEvent("A", "12345678")) {
		t.Fatal("completion did not release device byte capacity")
	}
}

func TestJudgeDeviceCapacityIsReleasedAfterEveryPublicationOutcome(t *testing.T) {
	for _, outcome := range []string{"success", "redis-error", "no-subscribers", "timeout"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			client := &admissionOutcomeRedis{outcome: outcome, release: make(chan struct{}), started: make(chan string, 4)}
			publisher, err := newJudgeSourcePublisher(ctx, judgeSourceConfig{
				enabled: true, channel: defaultJudgeSourceChannel, writeTimeout: 100 * time.Millisecond,
				workerCount: 1, queueSize: 8, queueMaxBytes: 128, deviceQueueSize: 1, deviceQueueMaxBytes: 16,
			}, client)
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			t.Cleanup(func() { cancel(); publisher.stop() })
			if !publisher.enqueue(testJudgeSourceEvent("A", "a1")) {
				t.Fatal("first A rejected")
			}
			expectAdmissionStart(t, client.started, "a1")
			if publisher.enqueue(testJudgeSourceEvent("A", "a2")) {
				t.Fatal("in-flight A did not consume device capacity")
			}
			if !publisher.enqueue(testJudgeSourceEvent("B", "fence")) {
				t.Fatal("B rejected")
			}
			close(client.release)
			// One worker starts B only after completing and releasing A.
			expectAdmissionStart(t, client.started, "fence")
			if !publisher.enqueue(testJudgeSourceEvent("A", "a2")) {
				t.Fatal("A capacity was not released")
			}
			expectAdmissionStart(t, client.started, "a2")
			cancel()
			publisher.stop()
			if publisher.queueBytes != 0 || publisher.queuedPublications != 0 || len(publisher.deviceQueues) != 0 {
				t.Fatal("publication outcome leaked capacity")
			}
		})
	}
}

func TestJudgeInitializationFailureLeavesSnapshotWriterUsable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	snapshots := &resultSnapshotFake{keys: make(chan string, 1)}
	client := &admissionOutcomeRedis{}
	writer := &deviceResultWriter{
		ctx: ctx, redis: snapshots, snapshots: make(map[string]*deviceSnapshot),
		judgeSource: judgeSourceConfig{enabled: true}, // Invalid publisher configuration.
	}
	writer.initializeJudgePublisher(client)
	if writer.judgePublisher != nil || writer.judgeSource.enabled || !client.closed {
		t.Fatal("invalid publisher was not disabled and closed independently")
	}
	if got := judgeSourceFailureCount(writer.judgeFailureLog, "SOURCE_PUBLISHER_UNAVAILABLE"); got != 1 {
		t.Fatalf("initialization failure count = %d, want 1", got)
	}
	if err := writer.record(judgeTestDevice("device-1", "serial-1"), config.ModbusParam{Identify: "temperature"}, 23.5); err != nil {
		t.Fatal(err)
	}
	if got := snapshots.singleKey(t); got != "device:data:device-1" {
		t.Fatalf("snapshot key = %q", got)
	}
}

func admissionTestPublisher(deviceCount, deviceBytes int) *judgeSourcePublisher {
	return &judgeSourcePublisher{
		ctx:          context.Background(),
		config:       judgeSourceConfig{queueSize: 8, queueMaxBytes: 128, deviceQueueSize: deviceCount, deviceQueueMaxBytes: deviceBytes},
		readyDevices: make(chan string, 8), deviceQueues: make(map[string]*judgeSourceDeviceQueue), failureLog: &judgeSourceFailureLog{},
	}
}

func takeAdmissionPublication(t *testing.T, publisher *judgeSourcePublisher, deviceID, payload string) judgeSourcePublication {
	t.Helper()
	select {
	case ready := <-publisher.readyDevices:
		if ready != deviceID {
			t.Fatalf("ready = %q, want %q", ready, deviceID)
		}
	default:
		t.Fatal("missing ready device")
	}
	publication, ok := publisher.nextPublication(deviceID)
	if !ok || string(publication.payload) != payload {
		t.Fatalf("publication = %+v, want %q", publication, payload)
	}
	return publication
}

func expectAdmissionStart(t *testing.T, started <-chan string, want string) {
	t.Helper()
	select {
	case got := <-started:
		if got != want {
			t.Fatalf("publication = %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("publication %q did not start", want)
	}
}

type admissionOutcomeRedis struct {
	outcome string
	release chan struct{}
	started chan string
	closed  bool
}

func (client *admissionOutcomeRedis) PublishWithSubscriberCount(ctx context.Context, _ string, payload interface{}) (int64, error) {
	marker := string(payload.([]byte))
	client.started <- marker
	if marker != "a1" {
		return 1, nil
	}
	if client.outcome == "timeout" {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	select {
	case <-client.release:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	switch client.outcome {
	case "redis-error":
		return 0, errors.New("redis unavailable")
	case "no-subscribers":
		return 0, nil
	default:
		return 1, nil
	}
}

func (client *admissionOutcomeRedis) Close() error { client.closed = true; return nil }
