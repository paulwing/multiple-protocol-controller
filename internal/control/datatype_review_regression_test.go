package control

import "testing"

// An explicit integer write paired with an inherited legacy float read must not
// bypass the precision guard merely because one type uses an old spelling.
func TestReviewExplicitIntegerLegacyFloatCannotVerifyRoundedValue(t *testing.T) {
	for _, readType := range []string{"float", "double"} {
		t.Run(readType, func(t *testing.T) {
			if controlValuesEqual(uint64(9007199254740993), float64(9007199254740992), "uint64", readType) {
				t.Fatal("different integer and rounded floating readback must not be verified")
			}
		})
	}
}
