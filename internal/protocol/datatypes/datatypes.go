// Package datatypes defines the data-type vocabulary shared by the four protocol
// implementations. It only describes names and per-protocol support; encoding and
// decoding stay in each protocol package.
package datatypes

import (
	"fmt"
	"strings"
)

// Canonical data-type names stored in property protocol items.
const (
	Bool    = "bool"
	Int8    = "int8"
	Uint8   = "uint8"
	Int16   = "int16"
	Uint16  = "uint16"
	Int32   = "int32"
	Uint32  = "uint32"
	Int64   = "int64"
	Uint64  = "uint64"
	Float32 = "float32"
	Float64 = "float64"
	String  = "string"
	Enum    = "enum"
)

// aliases keeps older spellings usable in explicit dataType fields. The runtime
// fallback (property.type) is intentionally not rewritten by this package so
// existing history tags and behaviour stay unchanged.
var aliases = map[string]string{
	"bool":        Bool,
	"boolean":     Bool,
	"int8":        Int8,
	"uint8":       Uint8,
	"byte":        Uint8,
	"int16":       Int16,
	"short":       Int16,
	"uint16":      Uint16,
	"ushort":      Uint16,
	"word":        Uint16,
	"int32":       Int32,
	"int":         Int32,
	"uint32":      Uint32,
	"uint":        Uint32,
	"dword":       Uint32,
	"int64":       Int64,
	"long":        Int64,
	"uint64":      Uint64,
	"ulong":       Uint64,
	"float32":     Float32,
	"float":       Float32,
	"float64":     Float64,
	"double":      Float64,
	"string":      String,
	"enum":        Enum,
	"enumeration": Enum,
}

// Canonical normalises an explicit dataType value. Empty values are not accepted
// here: callers treat empty as "fall back to the property type".
func Canonical(raw string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" {
		return "", fmt.Errorf("data type is empty")
	}
	if canonical, ok := aliases[normalized]; ok {
		return canonical, nil
	}
	return "", fmt.Errorf("unsupported data type %q", raw)
}

var registerReadTypes = map[string]struct{}{
	Bool: {}, Int16: {}, Uint16: {}, Int32: {}, Uint32: {},
	Int64: {}, Uint64: {}, Float32: {}, Float64: {},
}

var registerWriteTypes = map[string]struct{}{
	Int16: {}, Uint16: {}, Int32: {}, Uint32: {},
	Int64: {}, Uint64: {}, Float32: {}, Float64: {},
}

var opcuaTypes = map[string]struct{}{
	Bool: {}, Int8: {}, Uint8: {}, Int16: {}, Uint16: {}, Int32: {},
	Uint32: {}, Int64: {}, Uint64: {}, Float32: {}, Float64: {},
	String: {},
}

var bacnetTypes = map[string]struct{}{
	Bool: {}, Int8: {}, Uint8: {}, Int16: {}, Uint16: {}, Int32: {},
	Uint32: {}, Int64: {}, Uint64: {}, Float32: {}, Float64: {},
}

var mqttTypes = map[string]struct{}{
	Bool: {}, Int8: {}, Uint8: {}, Int16: {}, Uint16: {}, Int32: {},
	Uint32: {}, Int64: {}, Uint64: {}, Float32: {}, Float64: {}, String: {},
}

// SupportsModbusRead reports whether an explicit data type can be decoded for the
// given read function code. Coils/discrete inputs only carry booleans; register
// reads follow the existing 16/32/64-bit layouts. int8/uint8, string and enum
// have no established layout and are rejected.
func SupportsModbusRead(dataType string, functionCode int) bool {
	switch functionCode {
	case 1, 2:
		return dataType == Bool
	case 3, 4:
		_, ok := registerReadTypes[dataType]
		return ok
	default:
		return false
	}
}

// SupportsModbusWrite reports whether an explicit data type can be encoded for the
// given write function code. FC06 carries exactly one register; FC16 can carry the
// wider layouts. FC05/FC15 keep their existing boolean-only behaviour.
func SupportsModbusWrite(dataType string, functionCode int) bool {
	switch functionCode {
	case 5, 15:
		return dataType == Bool
	case 6:
		return dataType == Int16 || dataType == Uint16
	case 16:
		_, ok := registerWriteTypes[dataType]
		return ok
	default:
		return false
	}
}

func SupportsOpcua(dataType string) bool {
	_, ok := opcuaTypes[dataType]
	return ok
}

func SupportsBacnet(dataType string) bool {
	_, ok := bacnetTypes[dataType]
	return ok
}

func SupportsMqtt(dataType string) bool {
	_, ok := mqttTypes[dataType]
	return ok
}
