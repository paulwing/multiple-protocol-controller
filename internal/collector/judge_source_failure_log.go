package collector

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const judgeSourceFailureSummaryInterval = 30 * time.Second

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
	mu     sync.Mutex
	states [len(judgeSourceFailureReasons)]judgeSourceFailureState
	wake   chan struct{}
}

type judgeSourceFailureState struct {
	total          uint64
	recoveredTotal uint64
	reported       bool
	lastReport     time.Time
}

type judgeSourceFailureSummary struct {
	reason string
	event  string
	count  uint64
	total  uint64
}

func (failures *judgeSourceFailureLog) record(reasonCode string) {
	if failures == nil {
		return
	}
	index := len(judgeSourceFailureReasons) - 1
	for candidate, reason := range judgeSourceFailureReasons {
		if reason == reasonCode {
			index = candidate
			break
		}
	}
	failures.mu.Lock()
	failures.counts[index].Add(1)
	failures.states[index].total++
	if !failures.states[index].reported {
		failures.signalLocked()
	}
	failures.mu.Unlock()
}

// success confirms a completed operation for only its own failure reasons.
// A quiet interval, successful enqueue or zero-subscriber PUBLISH cannot recover
// a Redis publication failure. The lock orders this confirmation with failures.
// This is a stage-level success observation, not a per-device health assertion.
func (failures *judgeSourceFailureLog) success(reasonCodes ...string) {
	if failures == nil {
		return
	}
	failures.mu.Lock()
	defer failures.mu.Unlock()
	for _, code := range reasonCodes {
		for index, reason := range judgeSourceFailureReasons {
			if reason != code {
				continue
			}
			state := &failures.states[index]
			if state.total > state.recoveredTotal {
				state.recoveredTotal = state.total
				failures.signalLocked()
			}
			break
		}
	}
}

func (failures *judgeSourceFailureLog) signalLocked() {
	if failures.wake == nil {
		failures.wake = make(chan struct{}, 1)
	}
	select {
	case failures.wake <- struct{}{}:
	default:
	}
}

func (failures *judgeSourceFailureLog) run(ctx context.Context, log *zap.Logger) {
	failures.mu.Lock()
	failures.signalLocked()
	wake := failures.wake
	failures.mu.Unlock()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
		if ctx.Err() != nil {
			return
		}
		failures.flush(log)
	}
}

func (failures *judgeSourceFailureLog) flush(log *zap.Logger) {
	failures.flushAt(log, time.Now())
}

func (failures *judgeSourceFailureLog) flushAt(log *zap.Logger, now time.Time) {
	if log == nil {
		return
	}
	// At most a first failure and its already-confirmed recovery per reason.
	// Copy under the short state lock, then perform all sink I/O after unlocking.
	var summaries [2 * len(judgeSourceFailureReasons)]judgeSourceFailureSummary
	summaryCount := 0
	failures.mu.Lock()
	for index, reason := range judgeSourceFailureReasons {
		state := &failures.states[index]
		if !state.reported && failures.counts[index].Load() > 0 {
			summaries[summaryCount] = judgeSourceFailureSummary{reason, "first_failure", failures.counts[index].Swap(0), state.total}
			summaryCount++
			state.reported = true
			state.lastReport = now
		}
		if !state.reported {
			continue
		}
		if state.recoveredTotal == state.total {
			summaries[summaryCount] = judgeSourceFailureSummary{reason, "recovered", failures.counts[index].Swap(0), state.total}
			summaryCount++
			state.reported = false
		} else if now.Sub(state.lastReport) >= judgeSourceFailureSummaryInterval && failures.counts[index].Load() > 0 {
			summaries[summaryCount] = judgeSourceFailureSummary{reason, "failure_summary", failures.counts[index].Swap(0), state.total}
			summaryCount++
			state.lastReport = now
		}
	}
	failures.mu.Unlock()
	for _, summary := range summaries[:summaryCount] {
		fields := []zap.Field{
			zap.String("reason_code", summary.reason),
			zap.String("event", summary.event),
			zap.Uint64("failure_count", summary.count),
			zap.Uint64("total_failure_count", summary.total),
		}
		if summary.event == "recovered" {
			fields = append(fields, zap.String("recovery_scope", "operation"))
			log.Info("judge source event failure recovered", fields...)
		} else {
			log.Warn("judge source event failures", fields...)
		}
	}
}
