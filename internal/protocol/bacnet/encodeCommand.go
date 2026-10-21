package bacnet

import (
	"fmt"
	"math"
	"strings"

	"multiple-protocol-controller/pkg/utils"
)

// NormalizeValue keeps the original property-type semantics for configs
// without an explicit point data type.
func NormalizeValue(val interface{}, dataType string) (interface{}, error) {
	val = utils.LegacyJSONNumberValue(val)
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "float":
		v, err := utils.CoerceFloat32(val)
		if err != nil {
			return nil, fmt.Errorf("bacnet: %w", err)
		}
		return v, nil
	case "int", "enum":
		v, err := utils.CoerceInt32(val)
		if err != nil {
			return nil, fmt.Errorf("bacnet: %w", err)
		}
		return v, nil
	case "bool":
		v, err := utils.CoerceBool(val)
		if err != nil {
			return nil, fmt.Errorf("bacnet: %w", err)
		}
		return v, nil
	default:
		return nil, fmt.Errorf("bacnet: unsupported data type %q in current phase", dataType)
	}
}

// NormalizeValueExplicit converts an explicitly configured point data type.
// "enum" is still encoded as a signed Int32 application value, not as a native
// BACnet enumerated tag.
func NormalizeValueExplicit(val interface{}, dataType string) (interface{}, error) {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "float", "float32":
		v, err := utils.CoerceFloat32(val)
		if err != nil {
			return nil, fmt.Errorf("bacnet: %w", err)
		}
		return v, nil
	case "float64":
		v, err := utils.CoerceFloat64(val)
		if err != nil {
			return nil, fmt.Errorf("bacnet: %w", err)
		}
		return v, nil
	case "int", "enum":
		v, err := utils.CoerceInt32(val)
		if err != nil {
			return nil, fmt.Errorf("bacnet: %w", err)
		}
		return v, nil
	case "int8":
		return coerceSigned(val, math.MinInt8, math.MaxInt8, func(n int64) interface{} { return int8(n) })
	case "int16":
		return coerceSigned(val, math.MinInt16, math.MaxInt16, func(n int64) interface{} { return int16(n) })
	case "int32":
		return coerceSigned(val, math.MinInt32, math.MaxInt32, func(n int64) interface{} { return int32(n) })
	case "int64":
		v, err := utils.CoerceExactInt64(val)
		if err != nil {
			return nil, fmt.Errorf("bacnet: %w", err)
		}
		return v, nil
	case "uint8":
		return coerceUnsigned(val, math.MaxUint8, func(n uint64) interface{} { return uint8(n) })
	case "uint16":
		return coerceUnsigned(val, math.MaxUint16, func(n uint64) interface{} { return uint16(n) })
	case "uint32":
		return coerceUnsigned(val, math.MaxUint32, func(n uint64) interface{} { return uint32(n) })
	case "uint64":
		v, err := utils.CoerceExactUint64(val)
		if err != nil {
			return nil, fmt.Errorf("bacnet: %w", err)
		}
		return v, nil
	case "bool":
		v, err := utils.CoerceBool(val)
		if err != nil {
			return nil, fmt.Errorf("bacnet: %w", err)
		}
		return v, nil
	default:
		return nil, fmt.Errorf("bacnet: unsupported data type %q in current phase", dataType)
	}
}

func coerceSigned(val interface{}, min int64, max int64, convert func(int64) interface{}) (interface{}, error) {
	n, err := utils.CoerceExactInt64(val)
	if err != nil {
		return nil, fmt.Errorf("bacnet: %w", err)
	}
	if n < min || n > max {
		return nil, fmt.Errorf("bacnet: %d exceeds %T range", n, convert(0))
	}
	return convert(n), nil
}

func coerceUnsigned(val interface{}, max uint64, convert func(uint64) interface{}) (interface{}, error) {
	n, err := utils.CoerceExactUint64(val)
	if err != nil {
		return nil, fmt.Errorf("bacnet: %w", err)
	}
	if n > max {
		return nil, fmt.Errorf("bacnet: %d exceeds %T range", n, convert(0))
	}
	return convert(n), nil
}
