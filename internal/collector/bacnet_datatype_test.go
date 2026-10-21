package collector

import "testing"

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
