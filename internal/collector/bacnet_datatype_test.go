package collector

import (
	"encoding/json"
	"testing"
)

func TestNormalizeBacnetValueExplicitNumericPoints(t *testing.T) {
	cases := []struct {
		dataType string
		value    interface{}
		want     interface{}
	}{
		{"int8", int64(-128), int8(-128)},
		{"uint8", uint64(255), uint8(255)},
		{"int16", int64(-32768), int16(-32768)},
		{"uint16", uint64(65535), uint16(65535)},
		{"int32", int64(42), int32(42)},
		{"uint32", uint64(4294967295), uint32(4294967295)},
		{"int64", json.Number("9223372036854775807"), int64(9223372036854775807)},
		{"uint64", json.Number("18446744073709551615"), uint64(18446744073709551615)},
		{"float32", float64(21.5), float32(21.5)},
		{"float64", float64(21.5), float64(21.5)},
	}
	for _, tc := range cases {
		t.Run(tc.dataType, func(t *testing.T) {
			got, err := normalizeBacnetValue(tc.value, tc.dataType, true)
			if err != nil {
				t.Fatalf("normalizeBacnetValue(%v, %s, true): %v", tc.value, tc.dataType, err)
			}
			if got != tc.want {
				t.Fatalf("got %T(%v), want %T(%v)", got, got, tc.want, tc.want)
			}
		})
	}
}

func TestNormalizeBacnetValueExplicitRejectsInvalidIntegers(t *testing.T) {
	cases := []struct {
		name     string
		dataType string
		value    interface{}
	}{
		{"fractional", "int32", 1.9},
		{"signed overflow", "int8", 128},
		{"unsigned overflow", "uint16", 65536},
		{"negative unsigned", "uint64", -1},
		{"int64 overflow", "int64", json.Number("9223372036854775808")},
		{"uint64 overflow", "uint64", json.Number("18446744073709551616")},
		{"invalid number", "int32", "not a number"},
		{"nil", "int32", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := normalizeBacnetValue(tc.value, tc.dataType, true); err == nil {
				t.Fatalf("normalizeBacnetValue(%v, %s, true) must reject invalid value", tc.value, tc.dataType)
			}
		})
	}
}

func TestNormalizeBacnetValueLegacyTypesKeepConversions(t *testing.T) {
	cases := []struct {
		dataType string
		value    interface{}
		want     interface{}
	}{
		{"float", 21.5, float32(21.5)},
		{"int", 1.9, int32(1)},
		{"enum", 2.9, int32(2)},
		{"bool", 1, true},
		{"string", 42, "42"},
	}
	for _, tc := range cases {
		t.Run(tc.dataType, func(t *testing.T) {
			got, err := normalizeBacnetValue(tc.value, tc.dataType, false)
			if err != nil || got != tc.want {
				t.Fatalf("got %T(%v), %v; want %T(%v)", got, got, err, tc.want, tc.want)
			}
		})
	}
}

// TestNormalizeBacnetValueLegacyCanonicalNameKeepsOldSemantics covers old
// properties whose type name is a canonical data type: without an explicit
// dataType the original default pass-through remains.
func TestNormalizeBacnetValueLegacyCanonicalNameKeepsOldSemantics(t *testing.T) {
	value, err := normalizeBacnetValue(1.9, "int32", false)
	if err != nil {
		t.Fatalf("legacy normalizeBacnetValue(int32) error = %v", err)
	}
	if value != 1.9 {
		t.Fatalf("legacy int32 value = %#v, want pass-through 1.9", value)
	}
	if _, err := normalizeBacnetValue(1.9, "int32", true); err == nil {
		t.Fatal("explicit int32 must reject a fractional value")
	}
}
