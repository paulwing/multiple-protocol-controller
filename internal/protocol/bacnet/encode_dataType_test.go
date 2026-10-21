package bacnet

import "testing"

func TestNormalizeValueProducesNarrowCanonicalIntegers(t *testing.T) {
	cases := []struct {
		dataType string
		value    interface{}
		want     interface{}
	}{
		{"int8", 5, int8(5)},
		{"uint8", 200, uint8(200)},
		{"int16", -300, int16(-300)},
		{"uint16", 60000, uint16(60000)},
	}
	for _, tc := range cases {
		got, err := NormalizeValueExplicit(tc.value, tc.dataType)
		if err != nil {
			t.Fatalf("NormalizeValueExplicit(%s) error = %v", tc.dataType, err)
		}
		if got != tc.want {
			t.Fatalf("NormalizeValueExplicit(%s) = %#v, want %#v", tc.dataType, got, tc.want)
		}
	}
}

func TestNormalizeValueRejectsNarrowRangeOverflow(t *testing.T) {
	if _, err := NormalizeValueExplicit(300, "int8"); err == nil {
		t.Fatal("int8 overflow must be rejected")
	}
	if _, err := NormalizeValueExplicit(-1, "uint8"); err == nil {
		t.Fatal("uint8 negative must be rejected")
	}
}
