package modbusRtu

import (
	"encoding/json"
	"testing"
)

// The backend now rejects these shapes with HTTP 400. This test records why:
// the real runtime decoder cannot bind them into ModbusPoint (functionCode is
// a JSON string, quantity is a JSON integer), so accepting them at save time
// would store a model the controller silently skips.
func TestParseRejectsBackendIncompatibleFieldShapes(t *testing.T) {
	cases := map[string]string{
		"numeric functionCode": `{"type":"modbus_rtu","points":{"read":[{"functionCode":3,"address":"0","quantity":1,"dataType":"uint16"}],"write":[]}}`,
		"string quantity":      `{"type":"modbus_rtu","points":{"read":[{"functionCode":"03","address":"0","quantity":"1","dataType":"uint16"}],"write":[]}}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewModbusRtu().ParsePropProtocol(json.RawMessage(raw)); err == nil {
				t.Fatal("MPC runtime must reject a protocol JSON it cannot decode")
			}
		})
	}
}

func TestParseAcceptsBackendCanonicalShape(t *testing.T) {
	raw := `{"type":"modbus_rtu","points":{"read":[{"functionCode":"03","address":"0","quantity":1,"dataType":"uint16"}],"write":[]}}`
	parsed, err := NewModbusRtu().ParsePropProtocol(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("canonical shape must parse: %v", err)
	}
	cfg := parsed.(*ModbusProtocol)
	if len(cfg.Points.Read) != 1 || cfg.Points.Read[0].DataType != "uint16" {
		t.Fatalf("parsed point = %#v", cfg.Points.Read)
	}
}
