package conn

import (
	"encoding/json"
	"testing"
)

func TestParseMqttPayloadExactKeepsInt64Precision(t *testing.T) {
	payload := []byte(`{"payload":{"id":9007199254740993}}`)
	value, err := ParseMqttPayloadExact(payload, "payload.id")
	if err != nil {
		t.Fatalf("ParseMqttPayloadExact error = %v", err)
	}
	number, ok := value.(json.Number)
	if !ok {
		t.Fatalf("value type = %T, want json.Number", value)
	}
	if number.String() != "9007199254740993" {
		t.Fatalf("value = %s, want the exact integer", number)
	}
}

func TestParseMqttPayloadExactKeepsTextFallback(t *testing.T) {
	value, err := ParseMqttPayloadExact([]byte(`not json`), "")
	if err != nil {
		t.Fatalf("ParseMqttPayloadExact error = %v", err)
	}
	if value != "not json" {
		t.Fatalf("value = %#v, want the original text", value)
	}
}

// TestParseMqttPayloadLegacyStillReturnsFloat64 is the compatibility guard: an
// explicit point uses the exact parser, old configurations keep the existing
// float64 decode.
func TestParseMqttPayloadLegacyStillReturnsFloat64(t *testing.T) {
	value, err := ParseMqttPayload([]byte(`{"payload":{"id":1}}`), "payload.id")
	if err != nil {
		t.Fatalf("ParseMqttPayload error = %v", err)
	}
	if _, ok := value.(float64); !ok {
		t.Fatalf("value type = %T, want float64 for the legacy path", value)
	}
}
