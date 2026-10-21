package opcua

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
	dt := strings.ToLower(strings.TrimSpace(dataType))
	switch dt {
	case "bool":
		return utils.CoerceBool(val)
	case "int", "enum":
		n, err := utils.CoerceInt32(val)
		if err != nil {
			return 0, fmt.Errorf("opcua: %w", err)
		}
		return n, nil
	case "float":
		f, err := utils.CoerceFloat32(val)
		if err != nil {
			return 0, fmt.Errorf("opcua: %w", err)
		}
		return f, nil
	case "string":
		return utils.CoerceString(val), nil
	default:
		return val, nil
	}
}

// NormalizeValueExplicit converts an explicitly configured point data type.
// Unlike the legacy path it range checks canonical integer and float types
// instead of passing them through.
func NormalizeValueExplicit(val interface{}, dataType string) (interface{}, error) {
	dt := strings.ToLower(strings.TrimSpace(dataType))
	switch dt {
	case "bool":
		return utils.CoerceBool(val)
	case "int", "enum":
		n, err := utils.CoerceInt32(val)
		if err != nil {
			return 0, fmt.Errorf("opcua: %w", err)
		}
		return n, nil
	case "int8":
		return coerceSigned(val, math.MinInt8, math.MaxInt8, func(n int64) interface{} { return int8(n) })
	case "int16":
		return coerceSigned(val, math.MinInt16, math.MaxInt16, func(n int64) interface{} { return int16(n) })
	case "int32":
		return coerceSigned(val, math.MinInt32, math.MaxInt32, func(n int64) interface{} { return int32(n) })
	case "int64":
		n, err := utils.CoerceExactInt64(val)
		if err != nil {
			return 0, fmt.Errorf("opcua: %w", err)
		}
		return n, nil
	case "uint8":
		return coerceUnsigned(val, math.MaxUint8, func(n uint64) interface{} { return uint8(n) })
	case "uint16":
		return coerceUnsigned(val, math.MaxUint16, func(n uint64) interface{} { return uint16(n) })
	case "uint32":
		return coerceUnsigned(val, math.MaxUint32, func(n uint64) interface{} { return uint32(n) })
	case "uint64":
		n, err := utils.CoerceExactUint64(val)
		if err != nil {
			return 0, fmt.Errorf("opcua: %w", err)
		}
		return n, nil
	case "float", "float32":
		f, err := utils.CoerceFloat32(val)
		if err != nil {
			return 0, fmt.Errorf("opcua: %w", err)
		}
		return f, nil
	case "float64":
		f, err := utils.CoerceFloat64(val)
		if err != nil {
			return 0, fmt.Errorf("opcua: %w", err)
		}
		return f, nil
	case "string":
		return utils.CoerceString(val), nil
	default:
		return val, nil
	}
}

func coerceSigned(val interface{}, min int64, max int64, convert func(int64) interface{}) (interface{}, error) {
	n, err := utils.CoerceExactInt64(val)
	if err != nil {
		return nil, fmt.Errorf("opcua: %w", err)
	}
	if n < min || n > max {
		return nil, fmt.Errorf("opcua: %d exceeds %T range", n, convert(0))
	}
	return convert(n), nil
}

func coerceUnsigned(val interface{}, max uint64, convert func(uint64) interface{}) (interface{}, error) {
	n, err := utils.CoerceExactUint64(val)
	if err != nil {
		return nil, fmt.Errorf("opcua: %w", err)
	}
	if n > max {
		return nil, fmt.Errorf("opcua: %d exceeds %T range", n, convert(0))
	}
	return convert(n), nil
}
