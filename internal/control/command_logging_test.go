package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/pkg/logger"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestProcessCommandLogsIdentityWithoutCommandValues(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	previousLogger := logger.Log
	previousConfig := config.IotCfgStore.Load()
	logger.Log = zap.New(core)
	config.IotCfgStore.Store(config.IotCfgType{})
	t.Cleanup(func() {
		logger.Log = previousLogger
		if previousConfig == nil {
			config.IotCfgStore = atomic.Value{}
		} else {
			config.IotCfgStore.Store(previousConfig)
		}
	})

	err := ProcessCommand(nil, config.Command4MPC{Uid: "command-log-1", Data: []map[string]interface{}{
		{"password": "private-control-value"},
		{"device_id": "missing-device", "setting": "private-control-value"},
	}})
	if err != nil {
		t.Fatalf("existing missing-device handling changed: %v", err)
	}
	if logs.Len() == 0 {
		t.Fatal("missing command logs")
	}
	for _, entry := range logs.All() {
		fields := entry.ContextMap()
		if fields["command_id"] != "command-log-1" {
			t.Errorf("log %q lacks the existing command uid: %v", entry.Message, fields)
		}
		if fields["stage"] == nil || fields["result"] == nil {
			t.Errorf("log %q lacks stage/result: %v", entry.Message, fields)
		}
		if strings.Contains(fmt.Sprint(fields), "private-control-value") {
			t.Errorf("log %q exposes a raw command value", entry.Message)
		}
	}
}

func TestReceiptPublicationLogsOutcomeWithoutChangingReceiptOrRetrying(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       commandStatus
		attributeErr error
		publishErr   error
		wantResult   string
	}{
		{name: "verified", status: commandStatusVerified, wantResult: "published"},
		{name: "unverified", status: commandStatusUnverified, wantResult: "published"},
		{name: "execution failure", status: commandStatusFailed, attributeErr: errors.New("private-execution-detail"), wantResult: "published"},
		{name: "publish uncertain", status: commandStatusUnverified, publishErr: errors.New("private-redis-password"), wantResult: "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zap.DebugLevel)
			previous := logger.Log
			logger.Log = zap.New(core)
			t.Cleanup(func() { logger.Log = previous })
			calls := 0
			publishCommandResultsWith(func(ctx context.Context, channel string, payload interface{}) error {
				calls++
				if channel != config.SendCmdResCh || ctx.Err() != nil {
					t.Fatalf("publication changed: channel=%q, context error=%v", channel, ctx.Err())
				}
				var receipt commandResultEnvelope
				if err := json.Unmarshal([]byte(payload.(string)), &receipt); err != nil {
					t.Fatal(err)
				}
				if receipt.Result.UID != "receipt-log-1" || len(receipt.Result.Data) != 1 {
					t.Fatalf("receipt identity changed: %+v", receipt.Result)
				}
				attribute := receipt.Result.Data[0].Attributes[0]
				if attribute.Status != tc.status {
					t.Fatalf("status = %q, want %q", attribute.Status, tc.status)
				}
				if tc.attributeErr != nil && attribute.Message != tc.attributeErr.Error() {
					t.Fatalf("existing receipt error was changed: %q", attribute.Message)
				}
				return tc.publishErr
			}, config.IotCfgType{}, "receipt-log-1", []attrDispatchResult{{DeviceSerial: "device-1", Identify: "setting", Status: tc.status, Err: tc.attributeErr}})
			if calls != 1 {
				t.Fatalf("publication attempts = %d, want exactly 1", calls)
			}
			entries := logs.All()
			if len(entries) != 1 {
				t.Fatalf("receipt log count = %d, want 1", len(entries))
			}
			fields := entries[0].ContextMap()
			if fields["command_id"] != "receipt-log-1" || fields["stage"] != "receipt_publish" || fields["result"] != tc.wantResult || fields["elapsed_ms"] == nil {
				t.Fatalf("receipt log = %v", fields)
			}
			if strings.Contains(fmt.Sprint(fields), "private-") {
				t.Fatal("receipt log contains raw error details")
			}
		})
	}
}

func TestCommandLogResultKeepsUnverifiedAndEmptyDistinctFromSuccess(t *testing.T) {
	for _, tc := range []struct {
		results []attrDispatchResult
		want    string
	}{
		{nil, "no_result"},
		{[]attrDispatchResult{{Status: commandStatusVerified}}, "verified"},
		{[]attrDispatchResult{{Status: commandStatusVerified}, {Status: commandStatusUnverified}}, "unverified"},
		{[]attrDispatchResult{{Status: commandStatusUnverified}, {Err: context.DeadlineExceeded}}, "failed"},
	} {
		if got := commandLogResult(tc.results); got != tc.want {
			t.Errorf("result = %q, want %q", got, tc.want)
		}
	}
}

func TestCommandLoggingDoesNotRequireInitializedLogger(t *testing.T) {
	previousLogger := logger.Log
	previousConfig := config.IotCfgStore.Load()
	logger.Log = nil
	config.IotCfgStore = atomic.Value{}
	t.Cleanup(func() {
		logger.Log = previousLogger
		config.IotCfgStore = atomic.Value{}
		if previousConfig != nil {
			config.IotCfgStore.Store(previousConfig)
		}
	})
	if err := ProcessCommand(nil, config.Command4MPC{Uid: "not-started"}); err == nil || err.Error() != "iot config not loaded" {
		t.Fatalf("existing config error changed: %v", err)
	}
	calls := 0
	publishCommandResultsWith(func(context.Context, string, interface{}) error { calls++; return errors.New("private-error") }, config.IotCfgType{}, "command-nil-logger", []attrDispatchResult{{DeviceSerial: "device-1", Status: commandStatusUnverified}})
	if calls != 1 {
		t.Fatalf("publication attempts = %d", calls)
	}
}
