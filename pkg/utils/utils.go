package utils

import (
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
)

func ContainStr(arr []string, target string) bool {
	for _, v := range arr {
		if strings.EqualFold(v, target) {
			return true
		}
	}
	return false
}

// 根据list2的元素的key，过滤list1中元素的key与list2中元素的key相同的元素
func FilterByMapKey[T any, R any, K comparable](items []T, keyFn func(T) K, refList []R, refKeyFn func(R) K) []T {
	m := make(map[K]struct{}, len(refList))
	for _, r := range refList {
		m[refKeyFn(r)] = struct{}{}
	}

	result := make([]T, 0, len(items))
	for _, item := range items {
		if _, exists := m[keyFn(item)]; exists {
			result = append(result, item)
		}
	}
	return result
}

// Filter 泛型函数，支持任意类型
func FilterArr[T any](arr []T, f func(T) bool) []T {
	var result []T
	for _, v := range arr {
		if f(v) {
			result = append(result, v)
		}
	}
	return result
}

// 判断字符串是否是合法的十六进制格式
func IsHexString(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}

	hasPrefix := false
	hasSuffix := false
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		hasPrefix = true
	}
	if strings.HasSuffix(s, "h") || strings.HasSuffix(s, "H") {
		hasSuffix = true
	}

	if !hasPrefix && !hasSuffix {
		return false
	}

	// 去掉 0x / 0X 前缀
	if hasPrefix {
		s = s[2:]
		// s = strings.TrimPrefix(s, "0x")
	}

	if hasSuffix {
		s = s[:len(s)-1]
		// s = strings.TrimSuffix(s, "h")
	}

	// 逐字符检查
	for _, c := range s {
		if !(('0' <= c && c <= '9') ||
			('a' <= c && c <= 'f') ||
			('A' <= c && c <= 'F')) {
			return false
		}
	}
	return true
}

func ParseUintDecHex(addr string) (uint64, error) {
	if IsHexString(addr) {
		// 去掉 0x 前缀 h 后缀后以16进制解析
		addr = strings.TrimPrefix(addr, "0x")
		addr = strings.TrimPrefix(addr, "0X")
		addr = strings.TrimSuffix(addr, "h")
		addr = strings.TrimSuffix(addr, "H")
		return strconv.ParseUint(addr, 16, 64)
	}
	// 否则按十进制解析
	return strconv.ParseUint(addr, 10, 64)
}

func IsNumeric(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func IsPositiveNumber(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return false // 不是合法整数
	}
	return n > 0
}

func IsFloat(s string) bool {
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return true
	}
	return false
}

func IsIPv4(s string) bool {
	ip := net.ParseIP(s)
	return ip != nil && ip.To4() != nil
}

func IsValidPort(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	port, err := strconv.Atoi(s)
	if err != nil {
		return false // 不是数字
	}
	return port >= 0 && port <= 65535
}

func Ternary[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

func CoerceBool(val interface{}) (bool, error) {
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
			return false, fmt.Errorf("invalid bool string %q", v)
		}
	default:
		return false, fmt.Errorf("unsupported bool type %T", val)
	}
}

func CoerceInt64(val interface{}) (int64, error) {
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
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("%d exceeds int64 range", v)
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
		return 0, fmt.Errorf("unsupported int conversion from %T", val)
	}
}

func CoerceInt32(val interface{}) (int32, error) {
	n, err := CoerceInt64(val)
	if err != nil {
		return 0, err
	}
	if n > math.MaxInt32 || n < math.MinInt32 {
		return 0, fmt.Errorf("%d exceeds int32 range", n)
	}
	return int32(n), nil
}

func CoerceUint64(val interface{}) (uint64, error) {
	switch v := val.(type) {
	case int:
		if v < 0 {
			return 0, fmt.Errorf("negative int cannot convert to uint")
		}
		return uint64(v), nil
	case int8:
		if v < 0 {
			return 0, fmt.Errorf("negative int8 cannot convert to uint")
		}
		return uint64(v), nil
	case int16:
		if v < 0 {
			return 0, fmt.Errorf("negative int16 cannot convert to uint")
		}
		return uint64(v), nil
	case int32:
		if v < 0 {
			return 0, fmt.Errorf("negative int32 cannot convert to uint")
		}
		return uint64(v), nil
	case int64:
		if v < 0 {
			return 0, fmt.Errorf("negative int64 cannot convert to uint")
		}
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return v, nil
	case float32:
		if v < 0 {
			return 0, fmt.Errorf("negative float32 cannot convert to uint")
		}
		return uint64(v), nil
	case float64:
		if v < 0 {
			return 0, fmt.Errorf("negative float64 cannot convert to uint")
		}
		return uint64(v), nil
	case string:
		if strings.Contains(v, ".") {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return 0, err
			}
			if f < 0 {
				return 0, fmt.Errorf("negative float string cannot convert to uint")
			}
			return uint64(f), nil
		}
		return strconv.ParseUint(v, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported uint conversion from %T", val)
	}
}

func CoerceFloat64(val interface{}) (float64, error) {
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
		return 0, fmt.Errorf("unsupported float conversion from %T", val)
	}
}

func CoerceFloat32(val interface{}) (float32, error) {
	f, err := CoerceFloat64(val)
	if err != nil {
		return 0, err
	}
	return float32(f), nil
}

func CoerceString(val interface{}) string {
	if v, ok := val.(string); ok {
		return v
	}
	return fmt.Sprint(val)
}
