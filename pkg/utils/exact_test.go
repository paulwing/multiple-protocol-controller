package utils

import (
	"encoding/json"
	"math"
	"testing"
)

func TestCoerceExactInt64RejectsLossyValues(t *testing.T) {
	cases := []struct {
		name  string
		value interface{}
	}{
		{"fractional float", 1.5},
		{"fractional float32", float32(1.5)},
		{"fractional number", json.Number("1.5")},
		{"exponent number", json.Number("1e3")},
		{"exponent string", "1e3"},
		{"fractional string", "1.5"},
		{"nan", math.NaN()},
		{"infinity", math.Inf(1)},
		{"float beyond exact integer range", float64(1 << 60)},
		{"out of int64 range", json.Number("9223372036854775808")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := CoerceExactInt64(tc.value); err == nil {
				t.Fatalf("expected rejection, got %d", got)
			}
		})
	}
}

func TestCoerceExactInt64AcceptsExactValues(t *testing.T) {
	cases := []struct {
		name  string
		value interface{}
		want  int64
	}{
		{"int", int(-5), -5},
		{"int64", int64(math.MinInt64), math.MinInt64},
		{"integral float", float64(42), 42},
		{"integral float32", float32(42), 42},
		{"number", json.Number("9007199254740992"), 9007199254740992},
		{"number min", json.Number("-9223372036854775808"), math.MinInt64},
		{"string", "42", 42},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CoerceExactInt64(tc.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCoerceExactUint64RejectsLossyValues(t *testing.T) {
	cases := []struct {
		name  string
		value interface{}
	}{
		{"negative", -1},
		{"negative number", json.Number("-1")},
		{"fractional", 1.5},
		{"exponent", json.Number("1e3")},
		{"fractional string", "1.5"},
		{"float beyond exact integer range", float64(1 << 60)},
		{"out of uint64 range", json.Number("18446744073709551616")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := CoerceExactUint64(tc.value); err == nil {
				t.Fatalf("expected rejection, got %d", got)
			}
		})
	}
}

func TestCoerceExactUint64AcceptsExactValues(t *testing.T) {
	cases := []struct {
		name  string
		value interface{}
		want  uint64
	}{
		{"uint", uint(0), 0},
		{"uint64 max", uint64(math.MaxUint64), math.MaxUint64},
		{"integral float", float64(42), 42},
		{"number", json.Number("9007199254740992"), 9007199254740992},
		{"number max", json.Number("18446744073709551615"), math.MaxUint64},
		{"string", "42", 42},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CoerceExactUint64(tc.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// TestLegacyCoercionsKeepOldBehaviourForJSONNumbers pins the conversion applied
// when a command is decoded with UseNumber: it must match what the old float64
// decode plus truncation did, so existing configurations keep their behaviour.
func TestLegacyCoercionsKeepOldBehaviourForJSONNumbers(t *testing.T) {
	if got, err := CoerceInt64(json.Number("1.9")); err != nil || got != 1 {
		t.Fatalf("CoerceInt64(1.9) = %d, %v; want 1 (legacy truncation)", got, err)
	}
	if got, err := CoerceInt64(json.Number("1e3")); err != nil || got != 1000 {
		t.Fatalf("CoerceInt64(1e3) = %d, %v; want 1000 (legacy exponent)", got, err)
	}
	if got, err := CoerceUint64(json.Number("2.9")); err != nil || got != 2 {
		t.Fatalf("CoerceUint64(2.9) = %d, %v; want 2 (legacy truncation)", got, err)
	}
	if got, err := CoerceFloat64(json.Number("1.25")); err != nil || got != 1.25 {
		t.Fatalf("CoerceFloat64(1.25) = %v, %v", got, err)
	}
	if got, err := CoerceBool(json.Number("0")); err != nil || got {
		t.Fatalf("CoerceBool(0) = %v, %v; want false", got, err)
	}
	if got, err := CoerceBool(json.Number("2")); err != nil || !got {
		t.Fatalf("CoerceBool(2) = %v, %v; want true", got, err)
	}
}
