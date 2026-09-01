package control

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
)

func TestControlValuesEqualNormalizesSupportedPropertyTypes(t *testing.T) {
	tests := []struct {
		name     string
		target   any
		actual   any
		dataType string
		want     bool
	}{
		{name: "bool", target: "true", actual: true, dataType: "bool", want: true},
		{name: "enum", target: float64(2), actual: int32(2), dataType: "enum", want: true},
		{name: "float tolerance", target: 12.5, actual: float32(12.500001), dataType: "float", want: true},
		{name: "string mismatch", target: "open", actual: "closed", dataType: "string", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := controlValuesEqual(tt.target, tt.actual, tt.dataType); got != tt.want {
				t.Fatalf("controlValuesEqual(%v, %v, %q) = %t, want %t", tt.target, tt.actual, tt.dataType, got, tt.want)
			}
		})
	}
}

func TestVerifyControlReadbackRetriesUntilTargetValueIsObserved(t *testing.T) {
	values := []any{false, false, true}
	attempts := 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result := verifyControlReadback(ctx, true, "bool", 0, func(context.Context) (any, error) {
		value := values[attempts]
		attempts++
		return value, nil
	})

	if result.Status != commandStatusVerified {
		t.Fatalf("Status = %q, want %q (error: %v)", result.Status, commandStatusVerified, result.Err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestVerifyControlReadbackReportsTimeoutWhenValueNeverMatches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	result := verifyControlReadback(ctx, true, "bool", time.Millisecond, func(context.Context) (any, error) {
		return false, nil
	})

	if result.Status != commandStatusFailed {
		t.Fatalf("Status = %q, want %q", result.Status, commandStatusFailed)
	}
	if !errors.Is(result.Err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline exceeded", result.Err)
	}
}

func TestExecuteModbusWriteDoesNotIgnoreAcknowledgementFailure(t *testing.T) {
	ackErr := errors.New("ack read failed")
	connection := &failingAckConn{readErr: ackErr}
	device := config.DeviceRuntime{ResponseTimeoutMs: 50}
	command := config.ModbusCommand{FunctionCode: 5}

	result := executeModbusWrite(context.Background(), connection, []byte{1, 2, 3}, device, command, nil, true)

	if result.Status != commandStatusFailed {
		t.Fatalf("Status = %q, want %q", result.Status, commandStatusFailed)
	}
	if !errors.Is(result.Err, ackErr) {
		t.Fatalf("error = %v, want %v", result.Err, ackErr)
	}
}

func TestBuildCommandResultEnvelopePreservesVerificationStatusPerAttribute(t *testing.T) {
	cfg := config.IotCfgType{DeviceBySerial: map[string]config.DeviceRuntime{
		"serial-1": {Config: config.DeviceConfig{SerialNumber: "serial-1", DeviceName: "设备一"}},
	}}
	results := []attrDispatchResult{
		{DeviceSerial: "serial-1", Identify: "switch", Status: commandStatusVerified},
		{DeviceSerial: "serial-1", Identify: "setpoint", Status: commandStatusUnverified},
	}

	envelope := buildCommandResultEnvelope(cfg, "command-1", results, time.Unix(123, 0))

	if envelope.Result.Code != "0" || envelope.Result.Msg != "dispatched_unverified" {
		t.Fatalf("result summary = (%q, %q), want (0, dispatched_unverified)", envelope.Result.Code, envelope.Result.Msg)
	}
	if len(envelope.Result.Data) != 1 {
		t.Fatalf("len(Data) = %d, want 1", len(envelope.Result.Data))
	}
	entry := envelope.Result.Data[0]
	if entry.Result != "command_dispatched_unverified" || entry.VerificationStatus != commandStatusUnverified {
		t.Fatalf("entry summary = (%q, %q), want unverified", entry.Result, entry.VerificationStatus)
	}
	if len(entry.Attributes) != 2 || entry.Attributes[0].Identify != "switch" || entry.Attributes[0].Status != commandStatusVerified || entry.Attributes[1].Identify != "setpoint" || entry.Attributes[1].Status != commandStatusUnverified {
		t.Fatalf("attribute results = %+v, want verified switch and unverified setpoint", entry.Attributes)
	}
}

type failingAckConn struct {
	readErr error
}

func (c *failingAckConn) Read([]byte) (int, error)         { return 0, c.readErr }
func (c *failingAckConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *failingAckConn) Close() error                     { return nil }
func (c *failingAckConn) LocalAddr() net.Addr              { return stubAddr("local") }
func (c *failingAckConn) RemoteAddr() net.Addr             { return stubAddr("remote") }
func (c *failingAckConn) SetDeadline(time.Time) error      { return nil }
func (c *failingAckConn) SetReadDeadline(time.Time) error  { return nil }
func (c *failingAckConn) SetWriteDeadline(time.Time) error { return nil }

type stubAddr string

func (a stubAddr) Network() string { return string(a) }
func (a stubAddr) String() string  { return string(a) }

var _ net.Conn = (*failingAckConn)(nil)
