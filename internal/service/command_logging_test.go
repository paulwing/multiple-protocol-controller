package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/pkg/logger"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestCommandHandlerLogsSummaryWithoutRawPayloadOrError(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		processErr    error
		calls         int
		result        string
	}{
		{"accepted", `{"uid":"command-log-1","data":[{"device_id":"device-1","password":"private-value","value":9007199254740993}]}`, nil, 1, "received"},
		{"processing error", `{"uid":"command-log-1","data":[{"device_id":"device-1","password":"private-value"}]}`, errors.New("private-protocol-password"), 1, "unknown"},
		{"invalid payload", `{"uid":"command-log-1","data":"private-value"}`, nil, 0, "rejected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zap.DebugLevel)
			previous := logger.Log
			logger.Log = zap.New(core)
			t.Cleanup(func() { logger.Log = previous })
			stdout, err := os.Create(filepath.Join(t.TempDir(), "stdout"))
			if err != nil {
				t.Fatal(err)
			}
			previousStdout := os.Stdout
			os.Stdout = stdout
			t.Cleanup(func() { os.Stdout = previousStdout; stdout.Close() })
			calls := 0
			handler := newCommandHandler(func(command config.Command4MPC) error {
				calls++
				if command.Uid != "command-log-1" || command.Data[0]["password"] != "private-value" {
					t.Fatal("command contents changed")
				}
				if value, ok := command.Data[0]["value"]; ok && value.(json.Number).String() != "9007199254740993" {
					t.Fatal("integer precision changed")
				}
				return tc.processErr
			})
			if err := handler(&redis.Message{Payload: tc.payload}); err != nil {
				t.Fatalf("handler now interrupts subscriber: %v", err)
			}
			if calls != tc.calls {
				t.Fatalf("process calls = %d, want %d", calls, tc.calls)
			}
			entries := logs.All()
			if len(entries) == 0 {
				t.Fatal("missing receive log")
			}
			if got := entries[len(entries)-1].ContextMap()["result"]; got != tc.result {
				t.Fatalf("result = %v, want %s", got, tc.result)
			}
			for _, entry := range entries {
				fields := entry.ContextMap()
				if tc.calls > 0 && fields["command_id"] != "command-log-1" {
					t.Errorf("missing uid: %v", fields)
				}
				if fields["stage"] == nil || fields["elapsed_ms"] == nil {
					t.Errorf("missing stage or duration: %v", fields)
				}
				if strings.Contains(fmt.Sprint(fields), "private-") {
					t.Errorf("raw message or error exposed: %q", entry.Message)
				}
			}
			if info, err := stdout.Stat(); err != nil || info.Size() != 0 {
				t.Fatalf("raw stdout output remains: size/stat=%v, %v", info, err)
			}
		})
	}
}

func TestCommandHandlerWaitsForExistingDispatchAndDoesNotRetry(t *testing.T) {
	previous := logger.Log
	logger.Log = nil
	t.Cleanup(func() { logger.Log = previous })
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	calls := 0
	handler := newCommandHandler(func(config.Command4MPC) error {
		calls++
		close(entered)
		<-release
		return errors.New("send result unknown")
	})
	go func() { defer close(done); _ = handler(&redis.Message{Payload: `{"uid":"inflight","data":[]}`}) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not start")
	}
	select {
	case <-done:
		t.Fatal("handler returned before in-flight dispatch ended")
	default:
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not finish")
	}
	if calls != 1 {
		t.Fatalf("dispatch count = %d, want 1", calls)
	}
}
