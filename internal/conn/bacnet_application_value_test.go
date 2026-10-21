package conn

import (
	"bytes"
	"testing"
)

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
