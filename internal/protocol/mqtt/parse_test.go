package mqtt

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParsePropProtocolKeepsIndependentReadWriteDataTypes(t *testing.T) {
	raw := json.RawMessage(`{
		"type":"mqtt",
		"topics":{
			"subscribe":[{"topic":"dev/temp","path":"temp","qos":1,"dataType":"int64"}],
			"publish":[{"topic":"dev/cmd","path":"cmd","qos":1,"dataType":"DOUBLE"}]
		}
	}`)
	parsed, err := NewMqtt().ParsePropProtocol(raw)
	if err != nil {
		t.Fatalf("ParsePropProtocol: %v", err)
	}
	cfg := parsed.(*MqttProtocol)
	if got := cfg.Topics.Subscribe[0].DataType; got != "int64" {
		t.Fatalf("subscribe dataType = %q", got)
	}
	if got := cfg.Topics.Publish[0].DataType; got != "float64" {
		t.Fatalf("publish dataType = %q, want canonical float64", got)
	}
}

func TestParsePropProtocolRejectsEnumUntilWireValueIsDefined(t *testing.T) {
	raw := json.RawMessage(`{"type":"mqtt","topics":{"subscribe":[{"topic":"dev/state","dataType":"enum"}],"publish":[]}}`)
	_, err := NewMqtt().ParsePropProtocol(raw)
	if err == nil || !strings.Contains(err.Error(), "enum") {
		t.Fatalf("error = %v, want rejection naming enum", err)
	}
}
