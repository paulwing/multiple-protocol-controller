package collector

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/conn"
)

func mqttRegistrationWorker(t *testing.T) *mqttWorker {
	t.Helper()
	worker := newMqttWorker(context.Background(), mqttSpec{
		Device: config.DeviceRuntime{Config: config.DeviceConfig{SerialNumber: "registration-test"}},
		Params: []config.MqttParam{{Identify: "value", SubscribeTopic: "read", Path: "value"}},
	})
	t.Cleanup(worker.stop)
	return worker
}

func TestMqttWorkerRetriesOnlyConnectionConfigConflict(t *testing.T) {
	worker := mqttRegistrationWorker(t)
	registered := make(chan struct{})
	var attempts, released atomic.Int32
	worker.subscribe = func(context.Context, conn.MqttConfig, string, byte, conn.MessageHandler) (func(), error) {
		if attempts.Add(1) == 1 {
			return nil, fmt.Errorf("old connection is still in use: %w", conn.ErrMqttConnectionConfigConflict)
		}
		close(registered)
		return func() { released.Add(1) }, nil
	}
	worker.start()
	select {
	case <-registered:
	case <-time.After(3 * time.Second):
		t.Fatal("collector never registered again after the shared connection conflict")
	}
	worker.stop()
	if attempts.Load() != 2 || released.Load() != 1 {
		t.Fatalf("attempts=%d releases=%d; want one retry and one released subscription", attempts.Load(), released.Load())
	}
}

func TestMqttWorkerCancelsConnectionConfigWaitPromptly(t *testing.T) {
	worker := mqttRegistrationWorker(t)
	attempted := make(chan struct{}, 4)
	var attempts atomic.Int32
	worker.subscribe = func(context.Context, conn.MqttConfig, string, byte, conn.MessageHandler) (func(), error) {
		attempts.Add(1)
		attempted <- struct{}{}
		return nil, conn.ErrMqttConnectionConfigConflict
	}
	worker.start()
	select {
	case <-attempted:
	case <-time.After(time.Second):
		t.Fatal("worker did not attempt to register")
	}
	stopped := make(chan struct{})
	go func() { worker.stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("stopping the worker waited for the connection retry interval")
	}
	if attempts.Load() != 1 {
		t.Fatalf("cancelled worker kept registering: attempts=%d", attempts.Load())
	}
}

func TestMqttWorkerDoesNotRetryOtherRegistrationErrors(t *testing.T) {
	worker := mqttRegistrationWorker(t)
	attempted := make(chan struct{}, 4)
	var attempts atomic.Int32
	worker.subscribe = func(context.Context, conn.MqttConfig, string, byte, conn.MessageHandler) (func(), error) {
		attempts.Add(1)
		attempted <- struct{}{}
		return nil, errors.New("invalid subscription")
	}
	worker.start()
	select {
	case <-attempted:
	case <-time.After(time.Second):
		t.Fatal("worker did not attempt to register")
	}
	select {
	case <-attempted:
		t.Fatal("an unrelated registration error was retried")
	case <-time.After(1200 * time.Millisecond):
	}
	worker.stop()
	if attempts.Load() != 1 {
		t.Fatalf("attempts=%d, want original non-retry behavior", attempts.Load())
	}
}
