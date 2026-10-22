package conn

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestParseReadPresentValueResponseScalarTagBytes(t *testing.T) {
	// These are fixed ACKs, not produced by our request/application encoder.
	cases := []struct {
		name string
		wire string
		want interface{}
	}{
		{"float32 one", "81 0a 00 17 01 00 30 07 0c 0c 00 00 00 02 19 55 3e 44 3f 80 00 00 3f", float32(1)},
		{"float32 opening byte", "81 0a 00 17 01 00 30 07 0c 0c 00 00 00 02 19 55 3e 44 3e 80 00 00 3f", float32(0.25)},
		{"float64 one", "81 0a 00 1c 01 00 30 07 0c 0c 00 00 00 02 19 55 3e 55 08 3f f0 00 00 00 00 00 00 3f", float64(1)},
		{"object ID tag bytes", "81 0a 00 14 01 00 30 07 0c 0c 00 3e 3f 02 19 55 3e 21 2a 3f", uint64(42)},
		{"extended context field lengths", "81 0a 00 16 01 00 30 07 0c 0d 04 00 3e 3f 02 1d 01 55 3e 21 2a 3f", uint64(42)},
		{"signed tag bytes", "81 0a 00 15 01 00 30 07 0c 0c 00 00 00 02 19 55 3e 32 3e 3f 3f", int64(15935)},
		{"unsigned tag bytes", "81 0a 00 15 01 00 30 07 0c 0c 00 00 00 02 19 55 3e 22 3f 3e 3f", uint64(16190)},
		{"string tag bytes", "81 0a 00 16 01 00 30 07 0c 0c 00 00 00 02 19 55 3e 73 00 3f 3e 3f", "?>"},
		{"boolean", "81 0a 00 13 01 00 30 07 0c 0c 00 00 00 02 19 55 3e 11 3f", true},
		{"boolean false", "81 0a 00 13 01 00 30 07 0c 0c 00 00 00 02 19 55 3e 10 3f", false},
		{"enumeration", "81 0a 00 14 01 00 30 07 0c 0c 00 00 00 02 19 55 3e 91 02 3f", uint64(2)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value, done, err := parseReadPresentValueResponse(bacnetWireBytes(t, tc.wire), 7)
			if err != nil || !done || value != tc.want {
				t.Fatalf("value = %T(%v), done = %v, error = %v; want %T(%v)", value, value, done, err, tc.want, tc.want)
			}
		})
	}
}

func TestParseReadPresentValueResponseRejectsTruncatedScalarACK(t *testing.T) {
	frame := bacnetWireBytes(t, "81 0a 00 1c 01 00 30 07 0c 0c 00 3e 3f 02 19 55 3e 55 08 3f f0 00 00 00 00 00 00 3f")
	for length := 0; length < len(frame); length++ {
		t.Run(fmt.Sprintf("length_%d", length), func(t *testing.T) {
			truncated := append([]byte(nil), frame[:length]...)
			if len(truncated) >= 4 {
				// Exercise inner-field bounds instead of only the BVLC length check.
				binary.BigEndian.PutUint16(truncated[2:4], uint16(len(truncated)))
			}
			if value, _, err := parseReadPresentValueResponse(truncated, 7); err == nil {
				t.Fatalf("truncated ACK accepted: value = %v, frame = % x", value, truncated)
			}
		})
	}
}

func TestParseReadPresentValueResponseRejectsMalformedScalarACK(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"missing object", "19 55 3e 21 2a 3f"},
		{"wrong object length", "0b 00 00 02 19 55 3e 21 2a 3f"},
		{"missing property", "0c 00 00 00 02 3e 21 2a 3f"},
		{"empty property", "0c 00 00 00 02 18 3e 21 2a 3f"},
		{"truncated context length", "0d"},
		{"truncated context value", "0d 04 00 00"},
		{"missing opening", "0c 00 00 00 02 19 55 21 2a 3f"},
		{"wrong closing", "0c 00 00 00 02 19 55 3e 21 2a 4f"},
		{"multiple values", "0c 00 00 00 02 19 55 3e 21 2a 21 2b 3f"},
		{"trailing value", "0c 00 00 00 02 19 55 3e 21 2a 3f 21 2b"},
		{"invalid bool length", "0c 00 00 00 02 19 55 3e 12 3f"},
		{"empty unsigned", "0c 00 00 00 02 19 55 3e 20 3f"},
		{"oversized integer", "0c 00 00 00 02 19 55 3e 25 09 01 00 00 00 00 00 00 00 00 3f"},
		{"oversized signed integer", "0c 00 00 00 02 19 55 3e 35 09 01 00 00 00 00 00 00 00 00 3f"},
		{"invalid primitive marker", "0c 00 00 00 02 19 55 3e 26 01 00 00 00 00 00 3f"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frame := append(bacnetWireBytes(t, "81 0a 00 00 01 00 30 07 0c"), bacnetWireBytes(t, tc.body)...)
			binary.BigEndian.PutUint16(frame[2:4], uint16(len(frame)))
			if value, _, err := parseReadPresentValueResponse(frame, 7); err == nil {
				t.Fatalf("malformed ACK accepted: value = %v, frame = % x", value, frame)
			}
		})
	}
}
