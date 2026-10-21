package collector

import (
	"encoding/json"
	"math"
	"testing"
)

func TestNormalizeMqttValueKeepsExactCanonicalIntegers(t *testing.T) {
	value, err := normalizeMqttValue(json.Number("9007199254740993"), "int64", true)
	if err != nil {
		t.Fatalf("normalizeMqttValue(int64) error = %v", err)
	}
	if got, ok := value.(int64); !ok || got != 9007199254740993 {
		t.Fatalf("int64 value = %#v, want 9007199254740993", value)
	}

	value, err = normalizeMqttValue(json.Number("18446744073709551615"), "uint64", true)
	if err != nil {
		t.Fatalf("normalizeMqttValue(uint64) error = %v", err)
	}
	if got, ok := value.(uint64); !ok || got != math.MaxUint64 {
		t.Fatalf("uint64 value = %#v, want max uint64", value)
	}

	value, err = normalizeMqttValue(json.Number("200"), "uint8", true)
	if err != nil {
		t.Fatalf("normalizeMqttValue(uint8) error = %v", err)
	}
	if got, ok := value.(uint8); !ok || got != 200 {
		t.Fatalf("uint8 value = %#v, want 200", value)
	}
}

func TestNormalizeMqttValueRejectsLossyCanonicalIntegers(t *testing.T) {
	cases := []struct {
		dataType string
		value    interface{}
	}{
		{"int64", json.Number("1.5")},
		{"int64", json.Number("1e3")},
		{"int64", float64(1 << 60)},
		{"uint64", json.Number("-1")},
		{"int16", json.Number("40000")},
		{"int8", json.Number("1.5")},
	}
	for _, tc := range cases {
		if got, err := normalizeMqttValue(tc.value, tc.dataType, true); err == nil {
			t.Fatalf("%s with %#v must be rejected, got %#v", tc.dataType, tc.value, got)
		}
	}
}

// TestNormalizeMqttValueLegacyIntegerKeepsTruncating preserves the old branch.
func TestNormalizeMqttValueLegacyIntegerKeepsTruncating(t *testing.T) {
	value, err := normalizeMqttValue(json.Number("1.9"), "int", false)
	if err != nil {
		t.Fatalf("normalizeMqttValue(int) error = %v", err)
	}
	if got, ok := value.(int64); !ok || got != 1 {
		t.Fatalf("legacy int value = %#v, want 1", value)
	}
}

// TestNormalizeMqttValueLegacyCanonicalNameKeepsOldSemantics covers old
// properties whose type is "int32"/"int64": the original truncation stays.
func TestNormalizeMqttValueLegacyCanonicalNameKeepsOldSemantics(t *testing.T) {
	for _, dataType := range []string{"int32", "int64"} {
		value, err := normalizeMqttValue(json.Number("1.9"), dataType, false)
		if err != nil {
			t.Fatalf("legacy normalizeMqttValue(%s) error = %v", dataType, err)
		}
		if got, ok := value.(int64); !ok || got != 1 {
			t.Fatalf("legacy %s value = %#v, want truncated 1", dataType, value)
		}
		if _, err := normalizeMqttValue(json.Number("1.9"), dataType, true); err == nil {
			t.Fatalf("explicit %s must reject a fractional value", dataType)
		}
	}
}
