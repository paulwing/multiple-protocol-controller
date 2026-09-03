package collector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/pkg/logger"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestJudgeSourceFailureLogAggregatesAndDrainsReasons(t *testing.T) {
	failures := &judgeSourceFailureLog{}
	failures.record("SOURCE_QUEUE_FULL")
	failures.record("SOURCE_QUEUE_FULL")
	failures.record("REDIS_TIMEOUT")
	failures.record("unknown-a")
	failures.record("unknown-b")
	core, observed := observer.New(zap.WarnLevel)
	log := zap.New(core)
	failures.flush(log)
	failures.flush(log)
	entries := observed.All()
	if len(entries) != 3 {
		t.Fatalf("summary entries = %d, want 3 with no duplicate drain", len(entries))
	}
	want := map[string]uint64{"SOURCE_QUEUE_FULL": 2, "REDIS_TIMEOUT": 1, "SOURCE_FAILURE_UNKNOWN": 2}
	for _, entry := range entries {
		fields := entry.ContextMap()
		reason, _ := fields["reason_code"].(string)
		if fields["failure_count"] != want[reason] || want[reason] == 0 {
			t.Fatalf("unexpected summary: %#v", fields)
		}
		delete(want, reason)
	}
	if len(want) != 0 {
		t.Fatalf("missing summaries: %#v", want)
	}
}

func TestJudgeSourceFailuresKeepSnapshotMovingWhileLogOutputIsBlocked(t *testing.T) {
	for _, test := range []struct{ failure, reason string }{
		{"queue-full", "SOURCE_QUEUE_FULL"},
		{"bytes-full", "SOURCE_QUEUE_BYTES_FULL"},
		{"invalid-event", "SOURCE_EVENT_INVALID"},
		{"publisher-unavailable", "SOURCE_PUBLISHER_UNAVAILABLE"},
	} {
		t.Run(test.failure, func(t *testing.T) {
			failures := &judgeSourceFailureLog{}
			sink := &blockedJudgeLogSink{started: make(chan struct{}), release: make(chan struct{})}
			log := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), sink, zap.WarnLevel))
			previousLog := logger.Log
			logger.Log = log
			flushDone := make(chan struct{})
			recordDone := make(chan error, 1)
			recordStarted := false
			t.Cleanup(func() {
				close(sink.release)
				<-flushDone
				if recordStarted {
					<-recordDone
				}
				logger.Log = previousLog
			})
			failures.record("REDIS_TIMEOUT")
			go func() { defer close(flushDone); failures.flush(log) }()
			select {
			case <-sink.started:
			case <-time.After(time.Second):
				t.Fatal("background log did not reach the blocked sink")
			}

			snapshots := &resultSnapshotFake{keys: make(chan string, 1)}
			publisher := &judgeSourcePublisher{
				config:             judgeSourceConfig{queueSize: 1, queueMaxBytes: 1 << 20},
				queuedPublications: 1, failureLog: failures,
			}
			writer := &deviceResultWriter{
				ctx: context.Background(), redis: snapshots, snapshots: make(map[string]*deviceSnapshot),
				judgeSource:    judgeSourceConfig{enabled: true, maximumEventBytes: maximumJudgeEventBytes},
				judgePublisher: publisher, judgeFailureLog: failures,
			}
			switch test.failure {
			case "bytes-full":
				publisher.queuedPublications = 0
				publisher.config.queueMaxBytes = 1
			case "invalid-event":
				writer.newEventID = func() (string, error) { return "", errors.New("event ID unavailable") }
			case "publisher-unavailable":
				writer.judgePublisher = nil
			}
			recordStarted = true
			go func() {
				recordDone <- writer.record(judgeTestDevice("device-1", "serial-1"), config.ModbusParam{Identify: "temperature"}, 23.5)
			}()
			select {
			case err := <-recordDone:
				recordStarted = false
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("Judge failure reporting blocked the collecting caller")
			}
			if got := snapshots.singleKey(t); got != "device:data:device-1" {
				t.Fatalf("snapshot key = %q", got)
			}
			if got := judgeSourceFailureCount(failures, test.reason); got != 1 {
				t.Fatalf("%s count = %d, want 1", test.reason, got)
			}
		})
	}
}

func TestJudgeSourceFailureLogKeepsPublisherAndStopMovingWhileOutputIsBlocked(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		reason string
	}{
		{"redis-error", context.DeadlineExceeded, "REDIS_TIMEOUT"},
		{"no-subscribers", nil, "SOURCE_NO_SUBSCRIBERS"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			sink := &blockedJudgeLogSink{started: make(chan struct{}), release: make(chan struct{})}
			previousLog := logger.Log
			logger.Log = zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), sink, zap.WarnLevel))
			client := &failingJudgeLogRedis{calls: make(chan string, 2), err: test.err}
			publisher, err := newJudgeSourcePublisher(ctx, judgeSourceConfig{
				enabled: true, channel: defaultJudgeSourceChannel, writeTimeout: time.Second,
				workerCount: 1, queueSize: 2, queueMaxBytes: 1 << 20,
				deviceQueueSize: 2, deviceQueueMaxBytes: 1 << 20,
			}, client)
			stopDone := make(chan struct{})
			stopStarted := false
			t.Cleanup(func() {
				cancel()
				close(sink.release)
				if publisher != nil {
					publisher.stop()
					<-publisher.failureLogDone
				}
				if stopStarted {
					<-stopDone
				}
				// Test cleanup joins the logger only after releasing its sink;
				// publisher.stop itself must never wait for failureLogDone.
				logger.Log = previousLog
			})
			if err != nil {
				t.Fatal(err)
			}
			if !publisher.enqueue(testJudgeSourceEvent("device-1", "first")) {
				t.Fatal("first enqueue failed")
			}
			select {
			case <-sink.started:
			case <-time.After(3 * time.Second):
				t.Fatal("periodic failure logger did not reach the blocked sink")
			}
			// The first failure is now being logged. Neither the publisher nor
			// its shutdown may wait for that write to finish.
			if !publisher.enqueue(testJudgeSourceEvent("device-1", "second")) {
				t.Fatal("second enqueue failed while logging was blocked")
			}
			for _, want := range []string{"first", "second"} {
				select {
				case got := <-client.calls:
					if got != want {
						t.Fatalf("published = %q, want %q", got, want)
					}
				case <-time.After(time.Second):
					t.Fatalf("publication %q blocked behind log output", want)
				}
			}
			cancel()
			stopStarted = true
			go func() { defer close(stopDone); publisher.stop() }()
			select {
			case <-stopDone:
			case <-time.After(time.Second):
				t.Fatal("publisher stop waited for the blocked log sink")
			}
			if got := judgeSourceFailureCount(publisher.failureLog, test.reason); got != 1 {
				t.Fatalf("pending %s count = %d, want 1", test.reason, got)
			}
		})
	}
}

func TestJudgeSourceClassifiesNoSubscribers(t *testing.T) {
	for _, err := range []error{errJudgeSourceNoSubscribers, fmt.Errorf("publish failed: %w", errJudgeSourceNoSubscribers)} {
		if got := classifyJudgeSourceError(err); got != "SOURCE_NO_SUBSCRIBERS" {
			t.Fatalf("classification = %q, want SOURCE_NO_SUBSCRIBERS", got)
		}
	}
}

func judgeSourceFailureCount(failures *judgeSourceFailureLog, reasonCode string) uint64 {
	for index, reason := range judgeSourceFailureReasons {
		if reason == reasonCode {
			return failures.counts[index].Load()
		}
	}
	return 0
}

type failingJudgeLogRedis struct {
	calls chan string
	err   error
}

func (client *failingJudgeLogRedis) PublishWithSubscriberCount(_ context.Context, _ string, payload interface{}) (int64, error) {
	client.calls <- string(payload.([]byte))
	return 0, client.err
}

func (*failingJudgeLogRedis) Close() error { return nil }

type blockedJudgeLogSink struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (sink *blockedJudgeLogSink) Write(data []byte) (int, error) {
	sink.once.Do(func() { close(sink.started) })
	<-sink.release
	return len(data), nil
}

func (*blockedJudgeLogSink) Sync() error { return nil }
