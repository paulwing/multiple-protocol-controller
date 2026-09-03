package collector

import (
	"context"
	"errors"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/pkg/logger"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestJudgeSourceFirstFailureWakesLoggingWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	failures := &judgeSourceFailureLog{}
	core, _ := observer.New(zap.DebugLevel)
	written := make(chan struct{}, 1)
	previous := logger.Log
	log := zap.New(core, zap.Hooks(func(zapcore.Entry) error { written <- struct{}{}; return nil }))
	logger.Log = log
	done := make(chan struct{})
	t.Cleanup(func() { cancel(); <-done; logger.Log = previous })
	go func() { defer close(done); failures.run(ctx, log) }()
	failures.record("REDIS_TIMEOUT")
	select {
	case <-written:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("first failure waited for the periodic flush instead of waking the log worker")
	}
}

func TestJudgeSourceFailureLogSuppressesRepeatedWarningsWithinSummaryWindow(t *testing.T) {
	failures := &judgeSourceFailureLog{}
	core, observed := observer.New(zap.DebugLevel)
	log := zap.New(core)
	failures.record("REDIS_TIMEOUT")
	failures.flush(log)
	failures.record("REDIS_TIMEOUT")
	failures.flush(log)
	if observed.Len() != 1 {
		t.Fatalf("repeated failure produced %d warnings before the summary interval", observed.Len())
	}
	if got := judgeSourceFailureCount(failures, "REDIS_TIMEOUT"); got != 1 {
		t.Fatalf("suppressed failure count = %d, want 1 retained for next summary", got)
	}
}

func TestJudgeSourceFailureLogSummarizesCountsAndRequiresConfirmedRecovery(t *testing.T) {
	failures := &judgeSourceFailureLog{}
	core, observed := observer.New(zap.DebugLevel)
	log := zap.New(core)
	start := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	failures.record("REDIS_TIMEOUT")
	failures.flushAt(log, start)
	for range 3 {
		failures.record("REDIS_TIMEOUT")
	}
	failures.flushAt(log, start.Add(5*time.Second))
	if observed.Len() != 1 {
		t.Fatalf("warnings before summary = %d, want 1", observed.Len())
	}
	failures.flushAt(log, start.Add(30*time.Second))
	if observed.Len() != 2 {
		t.Fatalf("warnings after summary = %d, want 2", observed.Len())
	}
	entries := observed.All()
	if fields := entries[1].ContextMap(); fields["event"] != "failure_summary" || fields["failure_count"] != uint64(3) || fields["total_failure_count"] != uint64(4) {
		t.Fatalf("incorrect sustained-failure summary: %#v", fields)
	}
	failures.flushAt(log, start.Add(time.Minute))
	failures.success("SOURCE_NO_SUBSCRIBERS")
	failures.flushAt(log, start.Add(61*time.Second))
	if observed.Len() != 2 {
		t.Fatal("quiet time or unrelated success was reported as recovery")
	}
	failures.record("REDIS_TIMEOUT")
	failures.success("REDIS_TIMEOUT")
	failures.flushAt(log, start.Add(62*time.Second))
	entries = observed.All()
	if len(entries) != 3 {
		t.Fatalf("entries after recovery = %d, want 3", len(entries))
	}
	if fields := entries[2].ContextMap(); fields["event"] != "recovered" || fields["failure_count"] != uint64(1) || fields["total_failure_count"] != uint64(5) || entries[2].Level != zap.InfoLevel {
		t.Fatalf("recovery lost accumulated failures: %#v", fields)
	}
	failures.flushAt(log, start.Add(63*time.Second))
	if observed.Len() != 3 {
		t.Fatal("recovery was logged twice")
	}
	failures.record("REDIS_TIMEOUT")
	failures.flushAt(log, start.Add(64*time.Second))
	if fields := observed.All()[3].ContextMap(); fields["event"] != "first_failure" || fields["total_failure_count"] != uint64(6) {
		t.Fatalf("new failure episode was not logged immediately: %#v", fields)
	}
}

func TestJudgeSourceLaterFailureInvalidatesUnreportedRecovery(t *testing.T) {
	failures := &judgeSourceFailureLog{}
	core, observed := observer.New(zap.DebugLevel)
	log := zap.New(core)
	failures.record("REDIS_TIMEOUT")
	failures.flush(log)
	failures.success("REDIS_TIMEOUT")
	failures.record("REDIS_TIMEOUT")
	failures.flush(log)
	if observed.Len() != 1 {
		t.Fatal("an earlier success incorrectly recovered a newer failure")
	}
	failures.success("REDIS_TIMEOUT")
	failures.flush(log)
	if entries := observed.All(); len(entries) != 2 || entries[1].ContextMap()["event"] != "recovered" {
		t.Fatalf("latest successful attempt did not recover: %#v", entries)
	}
}

func TestJudgePublicationRecoveryRequiresSubscribedSuccess(t *testing.T) {
	publisher := admissionTestPublisher(4, 64)
	core, observed := observer.New(zap.DebugLevel)
	log := zap.New(core)
	publisher.failureLog.record("REDIS_TIMEOUT")
	publisher.failureLog.flush(log)
	for _, subscribers := range []int64{0, 1} {
		ctx, cancel := context.WithCancel(context.Background())
		client := &recoveryJudgeRedis{subscribers: subscribers, cancel: cancel}
		publisher.ctx, publisher.redis = ctx, client
		publisher.config.channel = defaultJudgeSourceChannel
		publisher.config.writeTimeout = time.Second
		if !publisher.enqueue(testJudgeSourceEvent("device-1", "event")) {
			t.Fatal("enqueue failed")
		}
		publisher.done.Add(1)
		publisher.runWorker()
		cancel()
		if client.calls != 1 {
			t.Fatalf("single source event was published %d times", client.calls)
		}
		publisher.failureLog.flush(log)
		var recovered []string
		for _, entry := range observed.All() {
			if entry.ContextMap()["event"] == "recovered" {
				recovered = append(recovered, entry.ContextMap()["reason_code"].(string))
			}
		}
		if subscribers == 0 && len(recovered) != 0 {
			t.Fatalf("zero subscribers recovered publication failures: %v", recovered)
		}
		if subscribers == 1 && len(recovered) != 2 {
			t.Fatalf("subscribed publication recovered %v, want Redis timeout and no-subscribers", recovered)
		}
	}
}

func TestJudgeSuccessfulAdmissionDoesNotRecoverPublicationFailure(t *testing.T) {
	publisher := admissionTestPublisher(4, 64)
	publisher.config.queueSize = 1
	core, observed := observer.New(zap.DebugLevel)
	log := zap.New(core)
	if !publisher.enqueue(testJudgeSourceEvent("A", "first")) || publisher.enqueue(testJudgeSourceEvent("B", "rejected")) {
		t.Fatal("did not reproduce full queue")
	}
	publisher.failureLog.record("REDIS_TIMEOUT")
	publisher.failureLog.flush(log)
	publication := takeAdmissionPublication(t, publisher, "A", "first")
	publisher.completePublication(publication)
	if !publisher.enqueue(testJudgeSourceEvent("B", "accepted")) {
		t.Fatal("admission did not resume")
	}
	publisher.failureLog.flush(log)
	for _, entry := range observed.All() {
		fields := entry.ContextMap()
		if fields["event"] == "recovered" && fields["reason_code"] != "SOURCE_QUEUE_FULL" {
			t.Fatalf("enqueue recovered unrelated failure: %#v", fields)
		}
	}
	if observed.FilterMessage("judge source event failure recovered").Len() != 1 {
		t.Fatal("successful admission did not report recovery")
	}
}

func TestJudgeValidEventRecoversBuildFailureWithoutHidingQueueFailure(t *testing.T) {
	publisher := admissionTestPublisher(4, 1<<20)
	publisher.config.queueSize = 1
	snapshots := &resultSnapshotFake{keys: make(chan string, 2)}
	writer := &deviceResultWriter{
		ctx: context.Background(), redis: snapshots, snapshots: make(map[string]*deviceSnapshot),
		judgeSource:    judgeSourceConfig{enabled: true, maximumEventBytes: maximumJudgeEventBytes},
		judgePublisher: publisher, newEventID: func() (string, error) { return "", errors.New("unavailable") },
	}
	core, observed := observer.New(zap.DebugLevel)
	log := zap.New(core)
	device := judgeTestDevice("A", "serial-A")
	if err := writer.record(device, config.ModbusParam{Identify: "temperature"}, 20); err != nil {
		t.Fatal(err)
	}
	publisher.failureLog.flush(log)
	if !publisher.enqueue(testJudgeSourceEvent("B", "occupied")) {
		t.Fatal("failed to occupy queue")
	}
	writer.newEventID = func() (string, error) { return "550e8400-e29b-41d4-a716-446655440000", nil }
	if err := writer.record(device, config.ModbusParam{Identify: "temperature"}, 21); err != nil {
		t.Fatal(err)
	}
	publisher.failureLog.flush(log)
	var buildRecovered, queueFailed bool
	for _, entry := range observed.All() {
		fields := entry.ContextMap()
		buildRecovered = buildRecovered || fields["reason_code"] == "SOURCE_EVENT_INVALID" && fields["event"] == "recovered"
		queueFailed = queueFailed || fields["reason_code"] == "SOURCE_QUEUE_FULL" && fields["event"] == "first_failure"
	}
	if !buildRecovered || !queueFailed {
		t.Fatalf("stage transitions missing: %#v", observed.All())
	}
	if len(snapshots.keys) != 2 {
		t.Fatal("Judge failures interrupted snapshot writes")
	}
}

type recoveryJudgeRedis struct {
	subscribers int64
	cancel      context.CancelFunc
	calls       int
}

func (client *recoveryJudgeRedis) PublishWithSubscriberCount(_ context.Context, channel string, payload interface{}) (int64, error) {
	client.calls++
	client.cancel()
	if channel != defaultJudgeSourceChannel || string(payload.([]byte)) != "event" {
		return 0, errors.New("unexpected publication")
	}
	return client.subscribers, nil
}

func (*recoveryJudgeRedis) Close() error { return nil }
