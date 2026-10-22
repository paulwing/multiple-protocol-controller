package conn

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestBuildBacnetApplicationValueExtendedNumericLengths(t *testing.T) {
	// Fixed wire bytes protect the encoder independently of our decoder.
	cases := []struct {
		name  string
		value interface{}
		wire  string
	}{
		{"float64 one", float64(1), "55 08 3f f0 00 00 00 00 00 00"},
		{"float64 negative", float64(-1.5), "55 08 bf f8 00 00 00 00 00 00"},
		{"int64 four bytes", int64(2147483647), "34 7f ff ff ff"},
		{"int64 five bytes", int64(2147483648), "35 05 00 80 00 00 00"},
		{"int64 negative five bytes", int64(-2147483649), "35 05 ff 7f ff ff ff"},
		{"int64 minimum", int64(-9223372036854775808), "35 08 80 00 00 00 00 00 00 00"},
		{"int64 maximum", int64(9223372036854775807), "35 08 7f ff ff ff ff ff ff ff"},
		{"uint64 four bytes", uint64(4294967295), "24 ff ff ff ff"},
		{"uint64 five bytes", uint64(4294967296), "25 05 01 00 00 00 00"},
		{"uint64 six bytes", uint64(1 << 40), "25 06 01 00 00 00 00 00"},
		{"uint64 seven bytes", uint64(1 << 48), "25 07 01 00 00 00 00 00 00"},
		{"uint64 maximum", uint64(18446744073709551615), "25 08 ff ff ff ff ff ff ff ff"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildBacnetApplicationValue(tc.value)
			if err != nil {
				t.Fatalf("buildBacnetApplicationValue: %v", err)
			}
			want := bacnetWireBytes(t, tc.wire)
			if !bytes.Equal(got, want) {
				t.Fatalf("wire = % x, want % x", got, want)
			}
			decoded, consumed, err := parseBacnetApplicationValue(got)
			if err != nil || decoded != tc.value || consumed != len(got) {
				t.Fatalf("decoded = %T(%v), consumed = %d, error = %v; want %T(%v), %d bytes",
					decoded, decoded, consumed, err, tc.value, tc.value, len(got))
			}
		})
	}
}

func TestBuildWritePresentValueRequestWideNumericWire(t *testing.T) {
	cases := []struct {
		name  string
		value interface{}
		wire  string
	}{
		{"float64", float64(1), "81 0a 00 1d 01 04 00 05 07 0f 0c 00 00 00 02 19 55 3e 55 08 3f f0 00 00 00 00 00 00 3f"},
		{"int64", int64(-9223372036854775808), "81 0a 00 1d 01 04 00 05 07 0f 0c 00 00 00 02 19 55 3e 35 08 80 00 00 00 00 00 00 00 3f"},
		{"uint64", uint64(18446744073709551615), "81 0a 00 1d 01 04 00 05 07 0f 0c 00 00 00 02 19 55 3e 25 08 ff ff ff ff ff ff ff ff 3f"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildWritePresentValueRequest(7, "analogInput", 2, tc.value)
			if err != nil {
				t.Fatalf("buildWritePresentValueRequest: %v", err)
			}
			want := bacnetWireBytes(t, tc.wire)
			if !bytes.Equal(got, want) {
				t.Fatalf("request = % x, want % x", got, want)
			}
			applicationValue := got[18 : len(got)-1]
			decoded, consumed, err := parseBacnetApplicationValue(applicationValue)
			if err != nil || decoded != tc.value || consumed != len(applicationValue) {
				t.Fatalf("application value = %T(%v), consumed = %d, error = %v; want %T(%v), %d bytes",
					decoded, decoded, consumed, err, tc.value, tc.value, len(applicationValue))
			}
		})
	}
}

func bacnetWireBytes(t *testing.T, text string) []byte {
	t.Helper()
	compact := bytes.ReplaceAll([]byte(text), []byte(" "), nil)
	wire, err := hex.DecodeString(string(compact))
	if err != nil {
		t.Fatalf("invalid test wire bytes: %v", err)
	}
	return wire
}

func TestBuildBacnetApplicationValueSupportsNarrowCanonicalIntegers(t *testing.T) {
	cases := []struct {
		name  string
		value interface{}
		want  []byte
	}{
		{"int8 negative", int8(-5), []byte{0x31, 0xfb}},
		{"int8 positive", int8(5), []byte{0x31, 0x05}},
		{"uint8", uint8(200), []byte{0x21, 0xc8}},
		{"int16 negative", int16(-300), []byte{0x32, 0xfe, 0xd4}},
		{"int16 positive", int16(300), []byte{0x32, 0x01, 0x2c}},
		{"uint16", uint16(60000), []byte{0x22, 0xea, 0x60}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildBacnetApplicationValue(tc.value)
			if err != nil {
				t.Fatalf("buildBacnetApplicationValue(%T) error = %v", tc.value, err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("payload = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestBuildWritePresentValueRequestCarriesInt16ApplicationValue(t *testing.T) {
	request, err := buildWritePresentValueRequest(7, "analogInput", 2, int16(-300))
	if err != nil {
		t.Fatalf("buildWritePresentValueRequest error = %v", err)
	}
	if !bytes.Contains(request, []byte{0x32, 0xfe, 0xd4}) {
		t.Fatalf("request %#v does not carry the int16 application value", request)
	}
}
