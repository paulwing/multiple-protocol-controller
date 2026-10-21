package datatypes

import "testing"

func TestCanonicalDataTypeMapsLegacyAliases(t *testing.T) {
	cases := map[string]string{
		" bool ":      "bool",
		"BOOLEAN":     "bool",
		"Int8":        "int8",
		"byte":        "uint8",
		"short":       "int16",
		"ushort":      "uint16",
		"word":        "uint16",
		"int":         "int32",
		"uint":        "uint32",
		"dword":       "uint32",
		"long":        "int64",
		"ulong":       "uint64",
		"float":       "float32",
		"FLOAT32":     "float32",
		"double":      "float64",
		"float64":     "float64",
		"string":      "string",
		"enum":        "enum",
		"enumeration": "enum",
	}
	for raw, want := range cases {
		got, err := Canonical(raw)
		if err != nil {
			t.Fatalf("Canonical(%q): %v", raw, err)
		}
		if got != want {
			t.Fatalf("Canonical(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestCanonicalDataTypeRejectsUnknownAndEmpty(t *testing.T) {
	for _, raw := range []string{"", "  ", "float16", "decimal", "uint128"} {
		if got, err := Canonical(raw); err == nil {
			t.Fatalf("Canonical(%q) = %q, want error", raw, got)
		}
	}
}

func TestProtocolCapabilities(t *testing.T) {
	all := []string{
		"bool", "int8", "uint8", "int16", "uint16", "int32", "uint32",
		"int64", "uint64", "float32", "float64", "string", "enum",
	}

	opcua := map[string]bool{}
	bacnet := map[string]bool{}
	mqtt := map[string]bool{}
	for _, dt := range all {
		// Explicit point-level enum is pending a product decision; legacy property
		// enum keeps its old branch in each protocol.
		opcua[dt] = dt != "enum"
		bacnet[dt] = dt != "string" && dt != "enum"
		mqtt[dt] = dt != "enum"
	}
	assertSupport(t, "opcua", all, opcua, SupportsOpcua)
	assertSupport(t, "bacnet", all, bacnet, SupportsBacnet)
	assertSupport(t, "mqtt", all, mqtt, SupportsMqtt)

	modbusReadRegisters := map[string]bool{
		"bool": true, "int16": true, "uint16": true, "int32": true,
		"uint32": true, "int64": true, "uint64": true, "float32": true, "float64": true,
	}
	for _, dt := range all {
		if got := SupportsModbusRead(dt, 3); got != modbusReadRegisters[dt] {
			t.Fatalf("SupportsModbusRead(%q, 3) = %v, want %v", dt, got, modbusReadRegisters[dt])
		}
		if got := SupportsModbusRead(dt, 4); got != modbusReadRegisters[dt] {
			t.Fatalf("SupportsModbusRead(%q, 4) = %v, want %v", dt, got, modbusReadRegisters[dt])
		}
	}
	for _, fc := range []int{1, 2} {
		for _, dt := range all {
			if got := SupportsModbusRead(dt, fc); got != (dt == "bool") {
				t.Fatalf("SupportsModbusRead(%q, %d) = %v, want %v", dt, fc, got, dt == "bool")
			}
		}
	}

	modbusWriteSingleRegister := map[string]bool{
		"int16": true, "uint16": true,
	}
	for _, dt := range all {
		if got := SupportsModbusWrite(dt, 6); got != modbusWriteSingleRegister[dt] {
			t.Fatalf("SupportsModbusWrite(%q, 6) = %v, want %v", dt, got, modbusWriteSingleRegister[dt])
		}
	}
	modbusWriteRegisters := map[string]bool{
		"int16": true, "uint16": true, "int32": true, "uint32": true,
		"int64": true, "uint64": true, "float32": true, "float64": true,
	}
	for _, dt := range all {
		if got := SupportsModbusWrite(dt, 16); got != modbusWriteRegisters[dt] {
			t.Fatalf("SupportsModbusWrite(%q, 16) = %v, want %v", dt, got, modbusWriteRegisters[dt])
		}
	}
	for _, fc := range []int{5, 15} {
		for _, dt := range all {
			if got := SupportsModbusWrite(dt, fc); got != (dt == "bool") {
				t.Fatalf("SupportsModbusWrite(%q, %d) = %v, want %v", dt, fc, got, dt == "bool")
			}
		}
	}
}

func assertSupport(t *testing.T, protocol string, all []string, want map[string]bool, supports func(string) bool) {
	t.Helper()
	for _, dt := range all {
		if got := supports(dt); got != want[dt] {
			t.Fatalf("%s supports %q = %v, want %v", protocol, dt, got, want[dt])
		}
	}
}
