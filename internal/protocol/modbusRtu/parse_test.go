package modbusRtu

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParsePropProtocolKeepsIndependentReadWriteDataTypes(t *testing.T) {
	raw := json.RawMessage(`{
		"type": "modbus_rtu",
		"points": {
			"read":  [{"functionCode":"03","address":"0","quantity":1,"dataType":"uint16"}],
			"write": [{"functionCode":"16","address":"1","quantity":2,"dataType":"INT32"}]
		}
	}`)

	parsed, err := NewModbusRtu().ParsePropProtocol(raw)
	if err != nil {
		t.Fatalf("ParsePropProtocol: %v", err)
	}
	cfg := parsed.(*ModbusProtocol)
	if got := cfg.Points.Read[0].DataType; got != "uint16" {
		t.Fatalf("read dataType = %q, want uint16", got)
	}
	if got := cfg.Points.Write[0].DataType; got != "int32" {
		t.Fatalf("write dataType = %q, want canonical int32", got)
	}
}

func TestParsePropProtocolLeavesMissingDataTypeEmptyForFallback(t *testing.T) {
	raw := json.RawMessage(`{"type":"modbus_rtu","points":{"read":[{"functionCode":"03","address":"0"}],"write":[]}}`)
	parsed, err := NewModbusRtu().ParsePropProtocol(raw)
	if err != nil {
		t.Fatalf("ParsePropProtocol: %v", err)
	}
	if got := parsed.(*ModbusProtocol).Points.Read[0].DataType; got != "" {
		t.Fatalf("missing dataType = %q, want empty for property fallback", got)
	}
}

func TestParsePropProtocolRejectsUnsupportedExplicitDataTypes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "int8 register read has no byte layout",
			raw:  `{"type":"modbus_rtu","points":{"read":[{"functionCode":"03","address":"0","dataType":"int8"}],"write":[]}}`,
			want: "int8",
		},
		{
			name: "enum register read has no width",
			raw:  `{"type":"modbus_rtu","points":{"read":[{"functionCode":"04","address":"0","dataType":"enum"}],"write":[]}}`,
			want: "enum",
		},
		{
			name: "non-bool coil read",
			raw:  `{"type":"modbus_rtu","points":{"read":[{"functionCode":"01","address":"0","dataType":"uint16"}],"write":[]}}`,
			want: "uint16",
		},
		{
			name: "fc06 cannot carry int32",
			raw:  `{"type":"modbus_rtu","points":{"read":[],"write":[{"functionCode":"06","address":"0","dataType":"int32"}]}}`,
			want: "int32",
		},
		{
			name: "bool cannot use register write",
			raw:  `{"type":"modbus_rtu","points":{"read":[],"write":[{"functionCode":"06","address":"0","dataType":"bool"}]}}`,
			want: "bool",
		},
		{
			name: "int32 with one configured register would overflow decode",
			raw:  `{"type":"modbus_rtu","points":{"read":[{"functionCode":"03","address":"0","quantity":1,"dataType":"int32"}],"write":[]}}`,
			want: "quantity",
		},
		{
			name: "float64 with two configured registers would overflow decode",
			raw:  `{"type":"modbus_rtu","points":{"read":[{"functionCode":"03","address":"0","quantity":2,"dataType":"float64"}],"write":[]}}`,
			want: "quantity",
		},
		{
			name: "unknown name",
			raw:  `{"type":"modbus_rtu","points":{"read":[{"functionCode":"03","address":"0","dataType":"decimal"}],"write":[]}}`,
			want: "decimal",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewModbusRtu().ParsePropProtocol(json.RawMessage(tc.raw))
			if err == nil {
				t.Fatalf("expected rejection")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q must name rejected data type %q", err, tc.want)
			}
		})
	}
}
