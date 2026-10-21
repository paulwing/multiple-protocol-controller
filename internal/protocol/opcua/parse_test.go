package opcua

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParsePropProtocolKeepsIndependentReadWriteDataTypes(t *testing.T) {
	raw := json.RawMessage(`{
		"type":"opcua",
		"nodes":{
			"read":[{"nodeId":"ns=2;s=A","dataType":"uint64"}],
			"write":[{"nodeId":"ns=2;s=A","dataType":"FLOAT"}]
		}
	}`)
	parsed, err := NewOpcua().ParsePropProtocol(raw)
	if err != nil {
		t.Fatalf("ParsePropProtocol: %v", err)
	}
	cfg := parsed.(*OpcuaProtocol)
	if got := cfg.Nodes.Read[0].DataType; got != "uint64" {
		t.Fatalf("read dataType = %q", got)
	}
	if got := cfg.Nodes.Write[0].DataType; got != "float32" {
		t.Fatalf("write dataType = %q, want canonical float32", got)
	}
}

func TestParsePropProtocolAcceptsAllOpcuaTypes(t *testing.T) {
	for _, dt := range []string{"bool", "int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64", "float32", "float64", "string"} {
		raw := json.RawMessage(`{"type":"opcua","nodes":{"read":[{"nodeId":"ns=2;s=A","dataType":"` + dt + `"}],"write":[]}}`)
		if _, err := NewOpcua().ParsePropProtocol(raw); err != nil {
			t.Fatalf("OPC UA must accept %q: %v", dt, err)
		}
	}
}

func TestParsePropProtocolDoesNotClaimExplicitEnumSupport(t *testing.T) {
	raw := json.RawMessage(`{"type":"opcua","nodes":{"read":[{"nodeId":"ns=2;s=A","dataType":"enum"}],"write":[]}}`)
	_, err := NewOpcua().ParsePropProtocol(raw)
	if err == nil || !strings.Contains(err.Error(), "enum") {
		t.Fatalf("error = %v, want explicit enum rejected until wire value is decided", err)
	}
}

func TestParsePropProtocolRejectsUnknownExplicitDataType(t *testing.T) {
	raw := json.RawMessage(`{"type":"opcua","nodes":{"read":[],"write":[{"nodeId":"ns=2;s=A","dataType":"decimal"}]}}`)
	_, err := NewOpcua().ParsePropProtocol(raw)
	if err == nil || !strings.Contains(err.Error(), "decimal") {
		t.Fatalf("error = %v, want rejection naming decimal", err)
	}
}
