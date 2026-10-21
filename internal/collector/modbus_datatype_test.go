package collector

import (
	"encoding/binary"
	"math"
	"testing"

	"multiple-protocol-controller/internal/config"
)

func TestQuantityForParamSupportsCanonicalFloatNames(t *testing.T) {
	cases := []struct {
		dataType string
		want     uint16
	}{
		{"float32", 2},
		{"float64", 4},
		{"int32", 2},
		{"uint64", 4},
		{"uint16", 1},
	}
	for _, tc := range cases {
		if got := quantityForParam(config.ModbusParam{DataType: tc.dataType, DataTypeExplicit: true}); got != tc.want {
			t.Fatalf("quantityForParam(%q) = %d, want %d", tc.dataType, got, tc.want)
		}
	}
}

func TestDecodeRegisterValueSupportsCanonicalFloatNames(t *testing.T) {
	float32Payload := make([]byte, 4)
	binary.BigEndian.PutUint32(float32Payload, math.Float32bits(12.5))
	got32, err := decodeRegisterValue(config.ModbusParam{DataType: "float32", DataTypeExplicit: true, Quantity: 2}, float32Payload)
	if err != nil {
		t.Fatalf("float32: %v", err)
	}
	if value, ok := got32.(float32); !ok || value != 12.5 {
		t.Fatalf("float32 value = %#v, want 12.5", got32)
	}

	float64Payload := make([]byte, 8)
	binary.BigEndian.PutUint64(float64Payload, math.Float64bits(123456.789))
	got64, err := decodeRegisterValue(config.ModbusParam{DataType: "float64", DataTypeExplicit: true, Quantity: 4}, float64Payload)
	if err != nil {
		t.Fatalf("float64: %v", err)
	}
	if value, ok := got64.(float64); !ok || value != 123456.789 {
		t.Fatalf("float64 value = %#v", got64)
	}
}

func TestDecodeRegisterBatchUsesEachPointWidth(t *testing.T) {
	payload := make([]byte, 14)
	binary.BigEndian.PutUint16(payload[0:2], 7)
	binary.BigEndian.PutUint32(payload[2:6], math.Float32bits(2.5))
	binary.BigEndian.PutUint64(payload[6:14], math.Float64bits(3.5))

	device := config.DeviceRuntime{Config: config.DeviceConfig{ID: "d", SerialNumber: "SN"}}
	uint16Point := &config.ModbusParam{Address: 0, DataType: "uint16", Identify: "a"}
	float32Point := &config.ModbusParam{Address: 1, DataType: "float32", DataTypeExplicit: true, Quantity: 2, Identify: "b"}
	float64Point := &config.ModbusParam{Address: 3, DataType: "float64", DataTypeExplicit: true, Quantity: 4, Identify: "c"}
	batch := batchQuery{functionCode: 3, startAddr: 0, quantity: 7, params: []*config.ModbusParam{uint16Point, float32Point, float64Point}}

	// Build a complete Modbus response frame: slave, fc, byte count, payload, crc.
	frame := make([]byte, 0, len(payload)+5)
	frame = append(frame, 1, 3, byte(len(payload)))
	frame = append(frame, payload...)
	frame = appendCRC(frame)

	if err := decodeRegisterBatch(device, batch, frame); err != nil {
		t.Fatalf("decodeRegisterBatch: %v", err)
	}
}

// TestLegacyNonExplicitFloatAliasKeepsOldWidth covers old properties whose
// type name is "float32"/"float64": without an explicit dataType the original
// one-register fallback stays.
func TestLegacyNonExplicitFloatAliasKeepsOldWidth(t *testing.T) {
	for _, dataType := range []string{"float32", "float64"} {
		if got := quantityForParam(config.ModbusParam{DataType: dataType}); got != 1 {
			t.Fatalf("legacy quantityForParam(%q) = %d, want 1", dataType, got)
		}
		payload := make([]byte, 2)
		payload[0], payload[1] = 0x00, 0x07
		value, err := decodeRegisterValue(config.ModbusParam{DataType: dataType}, payload)
		if err != nil {
			t.Fatalf("legacy decodeRegisterValue(%q) error = %v", dataType, err)
		}
		if got, ok := value.(uint16); !ok || got != 7 {
			t.Fatalf("legacy %s value = %#v, want uint16(7)", dataType, value)
		}
	}
}
