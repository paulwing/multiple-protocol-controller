package collector

import (
	"context"
	"errors"
	"sync"

	"multiple-protocol-controller/pkg/logger"
)

var errJudgeSourceNoSubscribers = errors.New("Redis accepted the publication but Judge has no active subscriber")

type judgeSourceRedis interface {
	PublishWithSubscriberCount(context.Context, string, interface{}) (int64, error)
	Close() error
}

type judgeSourcePublication struct {
	deviceID string
	payload  []byte
}

type judgeSourceDeviceQueue struct {
	pending   []judgeSourcePublication
	scheduled bool
	// Admission remains charged until Publish returns, not merely until dequeue.
	outstandingCount int
	outstandingBytes int
}

type judgeSourcePublisher struct {
	ctx                context.Context
	config             judgeSourceConfig
	redis              judgeSourceRedis
	readyDevices       chan string
	deviceQueues       map[string]*judgeSourceDeviceQueue
	done               sync.WaitGroup
	queueMu            sync.Mutex
	queuedPublications int
	queueBytes         int
	failureLog         *judgeSourceFailureLog
	failureLogDone     chan struct{}
	closeOnce          sync.Once
}

func newJudgeSourcePublisher(
	ctx context.Context,
	config judgeSourceConfig,
	client judgeSourceRedis,
) (*judgeSourcePublisher, error) {
	if ctx == nil || !config.enabled || config.workerCount <= 0 || config.queueSize < config.workerCount || config.queueMaxBytes <= 0 ||
		config.deviceQueueSize <= 0 || config.deviceQueueMaxBytes <= 0 ||
		config.channel == "" || config.writeTimeout <= 0 || client == nil {
		return nil, errors.New("invalid Judge Source publisher configuration")
	}
	publisher := &judgeSourcePublisher{
		ctx: ctx, config: config, redis: client,
		readyDevices:   make(chan string, config.queueSize),
		deviceQueues:   make(map[string]*judgeSourceDeviceQueue),
		failureLog:     &judgeSourceFailureLog{},
		failureLogDone: make(chan struct{}),
	}
	// Log I/O has its own fixed worker and is not part of the publication drain.
	// A blocked sink must not stall collection, Redis publication or publisher.stop.
	failureLogger := logger.Log
	go func() {
		defer close(publisher.failureLogDone)
		publisher.failureLog.run(ctx, failureLogger)
	}()
	for range config.workerCount {
		publisher.done.Add(1)
		go publisher.runWorker()
	}
	return publisher, nil
}

func (publisher *judgeSourcePublisher) enqueue(event judgeSourceEvent) bool {
	if publisher == nil || len(event.encoded) == 0 {
		return false
	}
	publication := judgeSourcePublication{
		deviceID: event.DeviceID, payload: event.encoded,
	}
	publisher.queueMu.Lock()
	if publisher.queuedPublications >= publisher.config.queueSize {
		publisher.queueMu.Unlock()
		publisher.logFailure("SOURCE_QUEUE_FULL")
		return false
	}
	if len(publication.payload) > publisher.config.queueMaxBytes-publisher.queueBytes {
		publisher.queueMu.Unlock()
		publisher.logFailure("SOURCE_QUEUE_BYTES_FULL")
		return false
	}
	deviceQueue := publisher.deviceQueues[publication.deviceID]
	var deviceCount, deviceBytes int
	if deviceQueue != nil {
		deviceCount, deviceBytes = deviceQueue.outstandingCount, deviceQueue.outstandingBytes
	}
	if deviceCount >= publisher.config.deviceQueueSize {
		publisher.queueMu.Unlock()
		publisher.logFailure("SOURCE_DEVICE_QUEUE_FULL")
		return false
	}
	if len(publication.payload) > publisher.config.deviceQueueMaxBytes-deviceBytes {
		publisher.queueMu.Unlock()
		publisher.logFailure("SOURCE_DEVICE_QUEUE_BYTES_FULL")
		return false
	}
	if deviceQueue == nil {
		deviceQueue = &judgeSourceDeviceQueue{}
		publisher.deviceQueues[publication.deviceID] = deviceQueue
	}
	deviceQueue.pending = append(deviceQueue.pending, publication)
	deviceQueue.outstandingCount++
	deviceQueue.outstandingBytes += len(publication.payload)
	publisher.queuedPublications++
	publisher.queueBytes += len(publication.payload)
	if !deviceQueue.scheduled {
		deviceQueue.scheduled = true
		// One ready token represents one device, not one publication. Because each
		// token has at least one waiting publication, queueSize also bounds this send.
		publisher.readyDevices <- publication.deviceID
	}
	publisher.queueMu.Unlock()
	publisher.failureLog.success(
		"SOURCE_QUEUE_FULL", "SOURCE_QUEUE_BYTES_FULL",
		"SOURCE_DEVICE_QUEUE_FULL", "SOURCE_DEVICE_QUEUE_BYTES_FULL",
	)
	return true
}

func (publisher *judgeSourcePublisher) runWorker() {
	defer publisher.done.Done()
	for {
		select {
		case <-publisher.ctx.Done():
			return
		case deviceID := <-publisher.readyDevices:
			publication, ok := publisher.nextPublication(deviceID)
			if !ok {
				continue
			}
			attemptContext, cancel := context.WithTimeout(publisher.ctx, publisher.config.writeTimeout)
			subscribers, err := publisher.redis.PublishWithSubscriberCount(
				attemptContext,
				publisher.config.channel,
				publication.payload,
			)
			cancel()
			publisher.completePublication(publication)
			if err == nil && subscribers == 0 {
				err = errJudgeSourceNoSubscribers
			}
			if err != nil {
				publisher.logFailure(classifyJudgeSourceError(err))
			} else {
				publisher.failureLog.success(
					"SOURCE_NO_SUBSCRIBERS", "REDIS_TIMEOUT", "REDIS_MEMORY_EXHAUSTED",
					"REDIS_READONLY", "REDIS_PERSISTENCE_ERROR", "REDIS_POOL_EXHAUSTED",
					"REDIS_AUTH_FAILED", "REDIS_KEY_TYPE_INVALID", "REDIS_UNAVAILABLE", "REDIS_WRITE_FAILED",
				)
			}
		}
	}
}

func (publisher *judgeSourcePublisher) nextPublication(deviceID string) (judgeSourcePublication, bool) {
	publisher.queueMu.Lock()
	defer publisher.queueMu.Unlock()
	deviceQueue := publisher.deviceQueues[deviceID]
	if deviceQueue == nil || len(deviceQueue.pending) == 0 {
		return judgeSourcePublication{}, false
	}
	publication := deviceQueue.pending[0]
	deviceQueue.pending[0] = judgeSourcePublication{}
	deviceQueue.pending = deviceQueue.pending[1:]
	publisher.queuedPublications--
	if publisher.queuedPublications < 0 {
		publisher.queuedPublications = 0
	}
	return publication, true
}

func (publisher *judgeSourcePublisher) completePublication(publication judgeSourcePublication) {
	publisher.queueMu.Lock()
	publisher.queueBytes -= len(publication.payload)
	if publisher.queueBytes < 0 {
		publisher.queueBytes = 0
	}
	deviceQueue := publisher.deviceQueues[publication.deviceID]
	if deviceQueue != nil {
		deviceQueue.outstandingCount--
		deviceQueue.outstandingBytes -= len(publication.payload)
	}
	if deviceQueue == nil || len(deviceQueue.pending) == 0 {
		delete(publisher.deviceQueues, publication.deviceID)
	} else {
		// Requeue at the tail after one publication so other ready devices get a turn.
		publisher.readyDevices <- publication.deviceID
	}
	publisher.queueMu.Unlock()
}

func (publisher *judgeSourcePublisher) stop() {
	if publisher == nil {
		return
	}
	publisher.closeOnce.Do(func() {
		publisher.done.Wait()
		_ = publisher.redis.Close()
	})
}

// logFailure only records a counter; it never calls the log sink on its caller.
func (publisher *judgeSourcePublisher) logFailure(reasonCode string) {
	publisher.failureLog.record(reasonCode)
}
