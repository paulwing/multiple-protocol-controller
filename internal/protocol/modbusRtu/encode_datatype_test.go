package modbusRtu

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"testing"

	"multiple-protocol-controller/internal/protocol"
)

func encodeFrameForTest(t *testing.T, functionCode int, dataType string, value interface{}) []byte {
	t.Helper()
	instance := NewModbusRtu()
	frame, err := instance.EncodeCommand(&protocol.CommandMessage{
		DeviceAddress: 1, FunctionCode: functionCode, Address: 0,
		DataType: dataType, DataTypeExplicit: true, Endian: "ABCD", Value: value,
	})
	if err != nil {
		t.Fatalf("EncodeCommand(%s) error = %v", dataType, err)
	}
	return frame
}

func TestEncodeRegisterValueSupportsCanonicalFloatNames(t *testing.T) {
	frame := encodeFrameForTest(t, ModbusRtuFunCode.WriteMultiRegister, "float32", 1.5)
	// slave + function + address + quantity + byte count then the payload.
	if got := binary.BigEndian.Uint32(frame[7:11]); got != math.Float32bits(1.5) {
		t.Fatalf("float32 payload = %#x, want %#x", got, math.Float32bits(1.5))
	}

	frame = encodeFrameForTest(t, ModbusRtuFunCode.WriteMultiRegister, "float64", 1.5)
	if got := binary.BigEndian.Uint64(frame[7:15]); got != math.Float64bits(1.5) {
		t.Fatalf("float64 payload = %#x, want %#x", got, math.Float64bits(1.5))
	}
}

func TestEncodeCanonicalIntegersRejectLossyInput(t *testing.T) {
	cases := []struct {
		dataType string
		value    interface{}
	}{
		{"int32", 1.5},
		{"uint32", 1.5},
		{"int64", json.Number("1e3")},
		{"int64", float64(1 << 60)},
		{"uint64", -1.0},
		{"int16", json.Number("1.5")},
	}
	for _, tc := range cases {
		_, err := NewModbusRtu().EncodeCommand(&protocol.CommandMessage{
			DeviceAddress: 1, FunctionCode: ModbusRtuFunCode.WriteMultiRegister, Address: 0,
			DataType: tc.dataType, DataTypeExplicit: true, Value: tc.value,
		})
		if err == nil {
			t.Fatalf("%s with %#v must be rejected rather than truncated", tc.dataType, tc.value)
		}
	}
}

func TestEncodeCanonicalIntegersKeepExactValues(t *testing.T) {
	var minInt64 int64 = math.MinInt64
	frame := encodeFrameForTest(t, ModbusRtuFunCode.WriteMultiRegister, "int64", minInt64)
	if got := binary.BigEndian.Uint64(frame[7:15]); got != uint64(minInt64) {
		t.Fatalf("int64 payload = %#x, want %#x", got, uint64(minInt64))
	}
	frame = encodeFrameForTest(t, ModbusRtuFunCode.WriteMultiRegister, "uint64", json.Number("18446744073709551615"))
	if got := binary.BigEndian.Uint64(frame[7:15]); got != math.MaxUint64 {
		t.Fatalf("uint64 payload = %#x, want %#x", got, uint64(math.MaxUint64))
	}
}

// TestLegacyShortKeepsTruncating is the compatibility side: the old names keep
// the coercion they always used.
func TestLegacyShortKeepsTruncating(t *testing.T) {
	frame := encodeLegacyFrameForTest(t, ModbusRtuFunCode.WriteHoldRegister, "short", 1.9)
	if got := int16(binary.BigEndian.Uint16(frame[4:6])); got != 1 {
		t.Fatalf("legacy short payload = %d, want 1", got)
	}
}

// TestLegacyNonExplicitIntegerKeepsTruncating covers old configurations whose
// property type happens to be a canonical name: without DataTypeExplicit the
// historical coercion still applies.
func TestLegacyNonExplicitIntegerKeepsTruncating(t *testing.T) {
	for _, dataType := range []string{"int32", "int64"} {
		frame := encodeLegacyFrameForTest(t, ModbusRtuFunCode.WriteMultiRegister, dataType, json.Number("1.9"))
		var got uint64
		if dataType == "int32" {
			got = uint64(binary.BigEndian.Uint32(frame[7:11]))
		} else {
			got = binary.BigEndian.Uint64(frame[7:15])
		}
		if got != 1 {
			t.Fatalf("legacy %s payload = %d, want truncated 1", dataType, got)
		}
	}
}

func encodeLegacyFrameForTest(t *testing.T, functionCode int, dataType string, value interface{}) []byte {
	t.Helper()
	frame, err := NewModbusRtu().EncodeCommand(&protocol.CommandMessage{
		DeviceAddress: 1, FunctionCode: functionCode, Address: 0,
		DataType: dataType, DataTypeExplicit: false, Endian: "ABCD", Value: value,
	})
	if err != nil {
		t.Fatalf("EncodeCommand(%s) error = %v", dataType, err)
	}
	return frame
}

func TestFunction06RejectsWideTypesAtEncoding(t *testing.T) {
	_, err := NewModbusRtu().EncodeCommand(&protocol.CommandMessage{
		DeviceAddress: 1, FunctionCode: ModbusRtuFunCode.WriteHoldRegister, Address: 0,
		DataType: "float32", DataTypeExplicit: true, Value: 1.5,
	})
	if err == nil {
		t.Fatal("function code 06 must reject a 4-byte payload")
	}
}
