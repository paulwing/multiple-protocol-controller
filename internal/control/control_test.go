package control

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
)

func TestControlValuesEqualNormalizesSupportedPropertyTypes(t *testing.T) {
	tests := []struct {
		name       string
		target     any
		actual     any
		targetType string
		actualType string
		want       bool
	}{
		{name: "bool", target: "true", actual: true, targetType: "bool", actualType: "bool", want: true},
		{name: "enum", target: float64(2), actual: int32(2), targetType: "enum", actualType: "enum", want: true},
		{name: "float tolerance", target: 12.5, actual: float32(12.500001), targetType: "float", actualType: "float", want: true},
		{name: "string mismatch", target: "open", actual: "closed", targetType: "string", actualType: "string", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := controlValuesEqual(tt.target, tt.actual, tt.targetType, tt.actualType); got != tt.want {
				t.Fatalf("controlValuesEqual(%v, %v, %q, %q) = %t, want %t", tt.target, tt.actual, tt.targetType, tt.actualType, got, tt.want)
			}
		})
	}
}

func TestControlValuesEqualUsesWriteAndReadTypesIndependently(t *testing.T) {
	tests := []struct {
		name       string
		target     any
		actual     any
		targetType string
		actualType string
		want       bool
	}{
		{name: "int16 written, uint16 read", target: int16(300), actual: uint16(300), targetType: "int16", actualType: "uint16", want: true},
		{name: "int16 written, uint16 differs", target: int16(300), actual: uint16(301), targetType: "int16", actualType: "uint16", want: false},
		{name: "int64 exact", target: int64(9007199254740993), actual: int64(9007199254740993), targetType: "int64", actualType: "int64", want: true},
		{name: "int64 one off", target: int64(9007199254740993), actual: int64(9007199254740992), targetType: "int64", actualType: "int64", want: false},
		{name: "uint64 max exact", target: uint64(1<<64 - 1), actual: uint64(1<<64 - 1), targetType: "uint64", actualType: "uint64", want: true},
		{name: "int written, float read with tolerance", target: int64(1), actual: float32(1.0000001), targetType: "int64", actualType: "float32", want: true},
		{name: "int64 beyond float precision is not verified by a float read", target: int64(9007199254740993), actual: float64(9007199254740992), targetType: "int64", actualType: "float64", want: false},
		{name: "legacy int keeps tolerance", target: 1.9, actual: 1.9000001, targetType: "int", actualType: "int", want: true},
		{name: "canonical int16 is exact", target: int16(1), actual: int16(2), targetType: "int16", actualType: "int16", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := controlValuesEqual(tt.target, tt.actual, tt.targetType, tt.actualType); got != tt.want {
				t.Fatalf("controlValuesEqual = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestVerifyControlReadbackRetriesUntilTargetValueIsObserved(t *testing.T) {
	values := []any{false, false, true}
	attempts := 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result := verifyControlReadback(ctx, true, "bool", true, "bool", true, 0, func(context.Context) (any, error) {
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

	result := verifyControlReadback(ctx, true, "bool", true, "bool", true, time.Millisecond, func(context.Context) (any, error) {
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

// TestLegacyNonExplicitCommandValueKeepsOldSemantics covers old properties
// whose type name is also a canonical data type: without an explicit dataType
// the original truncating conversion must remain.
func TestLegacyNonExplicitCommandValueKeepsOldSemantics(t *testing.T) {
	for _, dataType := range []string{"int32", "int64"} {
		value, err := normalizeCommandValue(dataType, false, json.Number("1.9"))
		if err != nil {
			t.Fatalf("legacy normalizeCommandValue(%s) error = %v", dataType, err)
		}
		if got, ok := value.(int64); !ok || got != 1 {
			t.Fatalf("legacy %s value = %#v, want truncated 1", dataType, value)
		}
	}
	if _, err := normalizeCommandValue("int32", true, json.Number("1.9")); err == nil {
		t.Fatal("explicit int32 must reject a fractional value")
	}
}

// TestControlValuesEqualUsesExplicitMarkerForLegacyFallback shows that the
// comparison only falls back to old tolerance semantics when both sides
// inherited their types.
func TestControlValuesEqualUsesExplicitMarkerForLegacyFallback(t *testing.T) {
	if !controlValuesEqualWithExplicit(5, 5.0000001, "int", false, "float", false) {
		t.Fatal("two inherited legacy types must keep the old tolerance comparison")
	}
	if controlValuesEqualWithExplicit(uint64(9007199254740993), float64(9007199254740992), "uint64", true, "double", false) {
		t.Fatal("an explicit integer write must not verify a rounded inherited float read")
	}
	if controlValuesEqualWithExplicit(int64(5), int32(5), "int32", false, "int32", false) {
		t.Fatal("two inherited canonical-looking names must keep the old DeepEqual comparison")
	}
}
