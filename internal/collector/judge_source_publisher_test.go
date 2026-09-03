package collector

import (
	"context"
	"hash/fnv"
	"testing"
	"time"
)

func TestJudgeSourcePublisherSchedulesDevicesDynamically(t *testing.T) {
	deviceA, deviceB := legacyLaneCollision(2)
	blocked := make(chan struct{})
	redis := newScheduledJudgeRedis(map[string]<-chan struct{}{
		deviceA + ":1": blocked,
	})
	publisher := newTestJudgeSourcePublisher(t, 2, redis)

	if !publisher.enqueue(testJudgeSourceEvent(deviceA, deviceA+":1")) {
		t.Fatal("enqueue device A = false")
	}
	redis.expectStarted(t, deviceA+":1")

	if !publisher.enqueue(testJudgeSourceEvent(deviceB, deviceB+":1")) {
		t.Fatal("enqueue device B = false")
	}
	redis.expectStarted(t, deviceB+":1")
	close(blocked)
}

func TestJudgeSourcePublisherGivesOtherDevicesATurn(t *testing.T) {
	blocked := make(chan struct{})
	redis := newScheduledJudgeRedis(map[string]<-chan struct{}{
		"device-A:1": blocked,
	})
	publisher := newTestJudgeSourcePublisher(t, 1, redis)

	if !publisher.enqueue(testJudgeSourceEvent("device-A", "device-A:1")) {
		t.Fatal("enqueue device A first event = false")
	}
	redis.expectStarted(t, "device-A:1")

	if !publisher.enqueue(testJudgeSourceEvent("device-A", "device-A:2")) {
		t.Fatal("enqueue device A second event = false")
	}
	if !publisher.enqueue(testJudgeSourceEvent("device-B", "device-B:1")) {
		t.Fatal("enqueue device B = false")
	}
	close(blocked)

	redis.expectStarted(t, "device-B:1")
	redis.expectStarted(t, "device-A:2")
}

func TestJudgeSourcePublisherKeepsOneDeviceSerial(t *testing.T) {
	blocked := make(chan struct{})
	redis := newScheduledJudgeRedis(map[string]<-chan struct{}{
		"device-A:1": blocked,
	})
	publisher := newTestJudgeSourcePublisher(t, 2, redis)

	if !publisher.enqueue(testJudgeSourceEvent("device-A", "device-A:1")) {
		t.Fatal("enqueue first event = false")
	}
	redis.expectStarted(t, "device-A:1")
	if !publisher.enqueue(testJudgeSourceEvent("device-A", "device-A:2")) {
		t.Fatal("enqueue second event = false")
	}

	redis.expectNotStarted(t)
	close(blocked)
	redis.expectStarted(t, "device-A:2")
}

func TestJudgeSourcePublisherBoundsTheSharedWaitingQueue(t *testing.T) {
	blocked := make(chan struct{})
	defer close(blocked)
	redis := newScheduledJudgeRedis(map[string]<-chan struct{}{
		"device-A:1": blocked,
	})
	publisher := newTestJudgeSourcePublisherWithLimits(t, 1, 2, 1<<20, redis)

	if !publisher.enqueue(testJudgeSourceEvent("device-A", "device-A:1")) {
		t.Fatal("enqueue in-flight event = false")
	}
	redis.expectStarted(t, "device-A:1")
	if !publisher.enqueue(testJudgeSourceEvent("device-A", "device-A:2")) {
		t.Fatal("enqueue first waiting event = false")
	}
	if !publisher.enqueue(testJudgeSourceEvent("device-B", "device-B:1")) {
		t.Fatal("enqueue second waiting event = false")
	}
	if publisher.enqueue(testJudgeSourceEvent("device-C", "device-C:1")) {
		t.Fatal("enqueue succeeded after the shared waiting queue reached its limit")
	}
}

func TestJudgeSourcePublisherByteLimitIncludesInFlightPublications(t *testing.T) {
	blocked := make(chan struct{})
	defer close(blocked)
	redis := newScheduledJudgeRedis(map[string]<-chan struct{}{
		"device-A:1": blocked,
	})
	publisher := newTestJudgeSourcePublisherWithLimits(t, 1, 16, len("device-A:1"), redis)

	if !publisher.enqueue(testJudgeSourceEvent("device-A", "device-A:1")) {
		t.Fatal("enqueue event at byte limit = false")
	}
	redis.expectStarted(t, "device-A:1")
	if publisher.enqueue(testJudgeSourceEvent("device-B", "x")) {
		t.Fatal("enqueue succeeded while an in-flight publication consumed the byte limit")
	}
}

func newTestJudgeSourcePublisher(t *testing.T, workers int, redis judgeSourceRedis) *judgeSourcePublisher {
	t.Helper()
	return newTestJudgeSourcePublisherWithLimits(t, workers, 16, 1<<20, redis)
}

func newTestJudgeSourcePublisherWithLimits(
	t *testing.T,
	workers int,
	queueSize int,
	queueMaxBytes int,
	redis judgeSourceRedis,
) *judgeSourcePublisher {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	publisher, err := newJudgeSourcePublisher(ctx, judgeSourceConfig{
		enabled:             true,
		channel:             defaultJudgeSourceChannel,
		writeTimeout:        time.Second,
		workerCount:         workers,
		queueSize:           queueSize,
		queueMaxBytes:       queueMaxBytes,
		deviceQueueSize:     queueSize,
		deviceQueueMaxBytes: queueMaxBytes,
		maximumEventBytes:   maximumJudgeEventBytes,
	}, redis)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		publisher.stop()
	})
	return publisher
}

func testJudgeSourceEvent(deviceID, marker string) judgeSourceEvent {
	return judgeSourceEvent{
		DeviceID:     deviceID,
		UpdatedPoint: "temperature",
		encoded:      []byte(marker),
	}
}

func legacyLaneCollision(laneCount int) (string, string) {
	firstByLane := make(map[int]string, laneCount)
	for i := 0; ; i++ {
		deviceID := "device-" + string(rune('A'+i))
		hasher := fnv.New32a()
		_, _ = hasher.Write([]byte(deviceID))
		lane := int(hasher.Sum32() % uint32(laneCount))
		if first, ok := firstByLane[lane]; ok {
			return first, deviceID
		}
		firstByLane[lane] = deviceID
	}
}

type scheduledJudgeRedis struct {
	started chan string
	blocks  map[string]<-chan struct{}
}

func newScheduledJudgeRedis(blocks map[string]<-chan struct{}) *scheduledJudgeRedis {
	return &scheduledJudgeRedis{started: make(chan string, 32), blocks: blocks}
}

func (redis *scheduledJudgeRedis) PublishWithSubscriberCount(ctx context.Context, _ string, payload interface{}) (int64, error) {
	marker := string(payload.([]byte))
	redis.started <- marker
	if blocked := redis.blocks[marker]; blocked != nil {
		select {
		case <-blocked:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return 1, nil
}

func (redis *scheduledJudgeRedis) Close() error { return nil }

func (redis *scheduledJudgeRedis) expectStarted(t *testing.T, want string) {
	t.Helper()
	select {
	case got := <-redis.started:
		if got != want {
			t.Fatalf("publication started = %q, want %q", got, want)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for publication %q", want)
	}
}

func (redis *scheduledJudgeRedis) expectNotStarted(t *testing.T) {
	t.Helper()
	select {
	case got := <-redis.started:
		t.Fatalf("publication %q started while another publication for the same device was in flight", got)
	case <-time.After(50 * time.Millisecond):
	}
}
