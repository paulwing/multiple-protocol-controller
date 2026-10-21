package bacnet

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParsePropProtocolKeepsIndependentReadWriteDataTypes(t *testing.T) {
	raw := json.RawMessage(`{
		"type":"bacnet",
		"objects":{
			"read":[{"objectType":"analogInput","instance":1,"propertyId":"presentValue","dataType":"float64"}],
			"write":[{"objectType":"analogOutput","instance":2,"propertyId":"presentValue","dataType":"BYTE"}]
		}
	}`)
	parsed, err := NewBacnet().ParsePropProtocol(raw)
	if err != nil {
		t.Fatalf("ParsePropProtocol: %v", err)
	}
	cfg := parsed.(*BacnetProtocol)
	if got := cfg.Objects.Read[0].DataType; got != "float64" {
		t.Fatalf("read dataType = %q", got)
	}
	if got := cfg.Objects.Write[0].DataType; got != "uint8" {
		t.Fatalf("write dataType = %q, want canonical uint8", got)
	}
}

func TestParsePropProtocolRejectsExplicitEnumUntilNativeMappingExists(t *testing.T) {
	raw := json.RawMessage(`{"type":"bacnet","objects":{"read":[{"objectType":"multiStateInput","instance":1,"propertyId":"presentValue","dataType":"enum"}],"write":[]}}`)
	_, err := NewBacnet().ParsePropProtocol(raw)
	if err == nil || !strings.Contains(err.Error(), "enum") {
		t.Fatalf("error = %v, want explicit enum rejected (signed Int32 is not a native enumerated tag)", err)
	}
}

func TestParsePropProtocolRejectsStringDataType(t *testing.T) {
	raw := json.RawMessage(`{"type":"bacnet","objects":{"read":[{"objectType":"analogInput","instance":1,"propertyId":"presentValue","dataType":"string"}],"write":[]}}`)
	_, err := NewBacnet().ParsePropProtocol(raw)
	if err == nil || !strings.Contains(err.Error(), "string") {
		t.Fatalf("error = %v, want rejection naming string", err)
	}
}
