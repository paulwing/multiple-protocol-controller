package collector

import (
	"context"
	"sync/atomic"
	"time"

	"multiple-protocol-controller/pkg/logger"

	"go.uber.org/zap"
)

var judgeSourceFailureReasons = [...]string{
	"SOURCE_QUEUE_FULL",
	"SOURCE_QUEUE_BYTES_FULL",
	"SOURCE_DEVICE_QUEUE_FULL",
	"SOURCE_DEVICE_QUEUE_BYTES_FULL",
	"SOURCE_EVENT_INVALID",
	"SOURCE_PUBLISHER_UNAVAILABLE",
	"SOURCE_NO_SUBSCRIBERS",
	"REDIS_TIMEOUT",
	"REDIS_MEMORY_EXHAUSTED",
	"REDIS_READONLY",
	"REDIS_PERSISTENCE_ERROR",
	"REDIS_POOL_EXHAUSTED",
	"REDIS_AUTH_FAILED",
	"REDIS_KEY_TYPE_INVALID",
	"REDIS_UNAVAILABLE",
	"REDIS_WRITE_FAILED",
	"SOURCE_FAILURE_UNKNOWN",
}

// judgeSourceFailureLog retains only fixed-size counters, never event payloads,
// per-device entries or a growing queue of errors waiting for a slow log sink.
type judgeSourceFailureLog struct {
	counts [len(judgeSourceFailureReasons)]atomic.Uint64
}

func (failures *judgeSourceFailureLog) record(reasonCode string) {
	if failures == nil {
		return
	}
	for index, reason := range judgeSourceFailureReasons {
		if reason == reasonCode {
			failures.counts[index].Add(1)
			return
		}
	}
	failures.counts[len(failures.counts)-1].Add(1)
}

func (failures *judgeSourceFailureLog) run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			failures.flush(logger.Log)
		}
	}
}

func (failures *judgeSourceFailureLog) flush(log *zap.Logger) {
	if log == nil {
		return
	}
	for index, reason := range judgeSourceFailureReasons {
		if count := failures.counts[index].Swap(0); count > 0 {
			log.Warn("judge source event failures",
				zap.String("reason_code", reason),
				zap.Uint64("failure_count", count),
			)
		}
	}
}
