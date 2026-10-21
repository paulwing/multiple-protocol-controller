package control

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"

	"multiple-protocol-controller/internal/collector"
	"multiple-protocol-controller/internal/config"
)

type commandStatus string

const (
	commandStatusVerified   commandStatus = "verified"
	commandStatusUnverified commandStatus = "unverified"
	commandStatusFailed     commandStatus = "failed"
)

type commandExecutionResult struct {
	Status commandStatus
	Err    error
}

type controlReadback func(context.Context) (any, error)

func verifyControlReadback(
	ctx context.Context,
	target any,
	targetDataType string,
	targetExplicit bool,
	actualDataType string,
	actualExplicit bool,
	retryInterval time.Duration,
	read controlReadback,
) commandExecutionResult {
	var lastActual any
	var lastReadError error
	for {
		actual, err := read(ctx)
		lastActual = actual
		lastReadError = err
		if err == nil && controlValuesEqualWithExplicit(target, actual, targetDataType, targetExplicit, actualDataType, actualExplicit) {
			return commandExecutionResult{Status: commandStatusVerified}
		}

		if ctx.Err() != nil {
			return commandExecutionResult{Status: commandStatusFailed, Err: controlVerificationError(target, lastActual, lastReadError, ctx.Err())}
		}
		if retryInterval <= 0 {
			continue
		}

		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return commandExecutionResult{Status: commandStatusFailed, Err: controlVerificationError(target, lastActual, lastReadError, ctx.Err())}
		case <-timer.C:
		}
	}
}

func controlVerificationError(target any, actual any, readErr error, timeoutErr error) error {
	if readErr != nil {
		return fmt.Errorf("control readback failed: %v: %w", readErr, timeoutErr)
	}
	return fmt.Errorf("control readback mismatch: got %v, want %v: %w", actual, target, timeoutErr)
}

// controlValuesEqual keeps the two-type signature for callers that only know
// the effective names. Canonical names are treated as explicit configuration.
func controlValuesEqual(target any, actual any, targetDataType string, actualDataType string) bool {
	return controlValuesEqualWithExplicit(
		target, actual,
		targetDataType, isCanonicalDataTypeName(targetDataType),
		actualDataType, isCanonicalDataTypeName(actualDataType),
	)
}

// controlValuesEqualWithExplicit compares the written target and the read-back
// value using their own data types: a write point and its read point may
// legitimately be configured with different wire types (for example int16
// written, uint16 read).
func controlValuesEqualWithExplicit(
	target any,
	actual any,
	targetDataType string,
	targetExplicit bool,
	actualDataType string,
	actualExplicit bool,
) bool {
	// Both sides inherited their type from the old property model: reproduce
	// the original comparison exactly, regardless of the type name.
	if !targetExplicit && !actualExplicit {
		return legacyControlValuesEqual(target, actual, targetDataType)
	}
	if isBoolDataType(targetDataType) || isBoolDataType(actualDataType) {
		targetValue, targetOK := controlBool(target)
		actualValue, actualOK := controlBool(actual)
		return targetOK && actualOK && targetValue == actualValue
	}
	if isStringDataType(targetDataType) || isStringDataType(actualDataType) {
		return fmt.Sprint(target) == fmt.Sprint(actual)
	}

	targetCategory, targetNumeric := numericCategory(targetDataType)
	actualCategory, actualNumeric := numericCategory(actualDataType)
	if !targetNumeric || !actualNumeric {
		return reflect.DeepEqual(target, actual)
	}

	// Old property names keep the original float comparison, tolerance included,
	// only when both sides are legacy. An explicit integer write paired with an
	// inherited float/double read must still reach the precision guard below:
	// otherwise a rounded readback could be reported as verified.
	if isLegacyNumericDataType(targetDataType) && isLegacyNumericDataType(actualDataType) {
		return controlNumbersWithinTolerance(target, actual)
	}

	if targetCategory == numericInteger && actualCategory == numericInteger {
		targetValue, targetOK := controlExactRational(target)
		actualValue, actualOK := controlExactRational(actual)
		return targetOK && actualOK && targetValue.Cmp(actualValue) == 0
	}

	// A float read cannot represent every int64/uint64 exactly; do not report a
	// match that the float could have rounded into place.
	if (targetCategory == numericInteger && controlMagnitudeBeyondFloatPrecision(target)) ||
		(actualCategory == numericInteger && controlMagnitudeBeyondFloatPrecision(actual)) {
		return false
	}
	return controlNumbersWithinTolerance(target, actual)
}

func isBoolDataType(dataType string) bool {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "bool", "boolean":
		return true
	}
	return false
}

func isStringDataType(dataType string) bool {
	return strings.EqualFold(strings.TrimSpace(dataType), "string")
}

type numericKind int

const (
	numericNone numericKind = iota
	numericInteger
	numericFloat
)

func numericCategory(dataType string) (numericKind, bool) {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64":
		return numericInteger, true
	case "float32", "float64":
		return numericFloat, true
	}
	if isLegacyNumericDataType(dataType) {
		return numericFloat, true
	}
	return numericNone, false
}

func isLegacyNumericDataType(dataType string) bool {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "byte", "short", "ushort", "int", "uint", "long", "ulong", "enum", "float", "double", "number":
		return true
	}
	return false
}

// legacyControlValuesEqual is the pre-dataType comparison, preserved verbatim
// for properties that have no explicit point data type.
func legacyControlValuesEqual(target any, actual any, dataType string) bool {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "bool", "boolean":
		targetValue, targetOK := controlBool(target)
		actualValue, actualOK := controlBool(actual)
		return targetOK && actualOK && targetValue == actualValue
	case "byte", "short", "ushort", "int", "uint", "long", "ulong", "enum", "float", "double", "number":
		return controlNumbersWithinTolerance(target, actual)
	case "string":
		return fmt.Sprint(target) == fmt.Sprint(actual)
	default:
		return reflect.DeepEqual(target, actual)
	}
}

func isCanonicalDataTypeName(dataType string) bool {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64", "float32", "float64":
		return true
	}
	return false
}

func controlNumbersWithinTolerance(target any, actual any) bool {
	targetValue, targetOK := controlNumber(target)
	actualValue, actualOK := controlNumber(actual)
	if !targetOK || !actualOK {
		return false
	}
	tolerance := math.Max(1e-6, math.Abs(targetValue)*1e-6)
	return math.Abs(targetValue-actualValue) <= tolerance
}

// controlExactRational converts a value to an exact rational so large integers
// are compared without passing through float64.
func controlExactRational(value any) (*big.Rat, bool) {
	switch typed := value.(type) {
	case int:
		return new(big.Rat).SetInt64(int64(typed)), true
	case int8:
		return new(big.Rat).SetInt64(int64(typed)), true
	case int16:
		return new(big.Rat).SetInt64(int64(typed)), true
	case int32:
		return new(big.Rat).SetInt64(int64(typed)), true
	case int64:
		return new(big.Rat).SetInt64(typed), true
	case uint:
		return new(big.Rat).SetUint64(uint64(typed)), true
	case uint8:
		return new(big.Rat).SetUint64(uint64(typed)), true
	case uint16:
		return new(big.Rat).SetUint64(uint64(typed)), true
	case uint32:
		return new(big.Rat).SetUint64(uint64(typed)), true
	case uint64:
		return new(big.Rat).SetUint64(typed), true
	case float32:
		return controlRationalFromFloat(float64(typed))
	case float64:
		return controlRationalFromFloat(typed)
	case json.Number:
		return controlRationalFromText(typed.String())
	case string:
		return controlRationalFromText(typed)
	default:
		return controlRationalFromText(fmt.Sprint(value))
	}
}

func controlRationalFromFloat(value float64) (*big.Rat, bool) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, false
	}
	return new(big.Rat).SetFloat64(value), true
}

func controlRationalFromText(text string) (*big.Rat, bool) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil, false
	}
	if strings.ContainsAny(trimmed, "eE") {
		parsed, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return nil, false
		}
		return controlRationalFromFloat(parsed)
	}
	parsed, ok := new(big.Rat).SetString(trimmed)
	return parsed, ok
}

func controlMagnitudeBeyondFloatPrecision(value any) bool {
	switch typed := value.(type) {
	case uint64:
		return typed > 1<<53
	case float64:
		return math.Abs(typed) > 1<<53
	case float32:
		return math.Abs(float64(typed)) > 1<<53
	}
	rational, ok := controlExactRational(value)
	if !ok {
		return true
	}
	limit := new(big.Rat).SetInt64(1 << 53)
	return new(big.Rat).Abs(rational).Cmp(limit) > 0
}

func controlBool(value any) (bool, bool) {
	if typed, ok := value.(bool); ok {
		return typed, true
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(fmt.Sprint(value)))
	return parsed, err == nil
}

func controlNumber(value any) (float64, bool) {
	parsed, err := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(value)), 64)
	return parsed, err == nil
}

func executeModbusWrite(
	ctx context.Context,
	netConn net.Conn,
	frame []byte,
	device config.DeviceRuntime,
	command config.ModbusCommand,
	readParam *config.ModbusParam,
	target any,
) commandExecutionResult {
	if _, err := netConn.Write(frame); err != nil {
		return commandExecutionResult{Status: commandStatusFailed, Err: err}
	}
	if err := collector.WaitCommandAck(ctx, netConn, device, command); err != nil {
		return commandExecutionResult{Status: commandStatusFailed, Err: err}
	}
	if readParam == nil {
		return commandExecutionResult{Status: commandStatusUnverified}
	}

	return verifyControlReadback(ctx, target, command.DataType, command.DataTypeExplicit, readParam.DataType, readParam.DataTypeExplicit, 100*time.Millisecond, func(readCtx context.Context) (any, error) {
		return collector.ReadParamValueWithConn(readCtx, netConn, device, *readParam)
	})
}
