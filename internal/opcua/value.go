package opcua

import (
	"fmt"
	"strconv"
	"strings"
)

func NormalizeValue(val interface{}, dataType string) (interface{}, error) {
	dt := strings.ToLower(strings.TrimSpace(dataType))
	switch dt {
	case "bool":
		return toBool(val)
	case "int", "enum":
		return toInt32(val)
	case "float":
		return toFloat32(val)
	case "string":
		return toString(val), nil
	default:
		return val, nil
	}
}

func toBool(val interface{}) (bool, error) {
	switch v := val.(type) {
	case bool:
		return v, nil
	case int:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case uint64:
		return v != 0, nil
	case float64:
		return v != 0, nil
	case string:
		s := strings.TrimSpace(strings.ToLower(v))
		switch s {
		case "1", "true", "t", "yes", "y":
			return true, nil
		case "0", "false", "f", "no", "n":
			return false, nil
		default:
			return false, fmt.Errorf("opcua: invalid bool string %q", v)
		}
	default:
		return false, fmt.Errorf("opcua: unsupported bool type %T", val)
	}
}

func toInt32(val interface{}) (int32, error) {
	n, err := toInt64(val)
	if err != nil {
		return 0, err
	}
	if n > int64(^uint32(0)>>1) || n < -int64(^uint32(0)>>1)-1 {
		return 0, fmt.Errorf("opcua: %d exceeds int32 range", n)
	}
	return int32(n), nil
}

func toInt64(val interface{}) (int64, error) {
	switch v := val.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > uint64(^uint32(0)>>1) {
			return 0, fmt.Errorf("opcua: %d exceeds int32 range", v)
		}
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		if strings.Contains(v, ".") {
			f, err := strconv.ParseFloat(v, 64)
			return int64(f), err
		}
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("opcua: unsupported int conversion from %T", val)
	}
}

func toFloat32(val interface{}) (float32, error) {
	f, err := toFloat64(val)
	if err != nil {
		return 0, err
	}
	return float32(f), nil
}

func toFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("opcua: unsupported float conversion from %T", val)
	}
}

func toString(val interface{}) string {
	if v, ok := val.(string); ok {
		return v
	}
	return fmt.Sprint(val)
}
