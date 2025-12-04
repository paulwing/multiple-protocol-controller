package utils

import (
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
