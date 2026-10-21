package opcua

import (
	"encoding/json"
	"testing"
)

// TestLegacyNonExplicitOPCUAPassesCanonicalNamesThrough covers old properties
// whose type name is a canonical data type: without an explicit dataType the
// original pass-through remains (with the old float64 number shape).
func TestLegacyNonExplicitOPCUAPassesCanonicalNamesThrough(t *testing.T) {
	value, err := NormalizeValue(json.Number("1.9"), "int32")
	if err != nil {
		t.Fatalf("legacy NormalizeValue(int32) error = %v", err)
	}
	if got, ok := value.(float64); !ok || got != 1.9 {
		t.Fatalf("legacy int32 value = %#v, want float64(1.9)", value)
	}
	if _, err := NormalizeValueExplicit(json.Number("1.9"), "int32"); err == nil {
		t.Fatal("explicit int32 must reject a fractional value")
	}
}
