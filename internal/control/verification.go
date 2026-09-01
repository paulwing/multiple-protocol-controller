package control

import (
	"context"
	"fmt"
	"math"
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
	dataType string,
	retryInterval time.Duration,
	read controlReadback,
) commandExecutionResult {
	var lastActual any
	var lastReadError error
	for {
		actual, err := read(ctx)
		lastActual = actual
		lastReadError = err
		if err == nil && controlValuesEqual(target, actual, dataType) {
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

func controlValuesEqual(target any, actual any, dataType string) bool {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "bool", "boolean":
		targetValue, targetOK := controlBool(target)
		actualValue, actualOK := controlBool(actual)
		return targetOK && actualOK && targetValue == actualValue
	case "byte", "short", "ushort", "int", "uint", "long", "ulong", "enum", "float", "double", "number":
		targetValue, targetOK := controlNumber(target)
		actualValue, actualOK := controlNumber(actual)
		if !targetOK || !actualOK {
			return false
		}
		tolerance := math.Max(1e-6, math.Abs(targetValue)*1e-6)
		return math.Abs(targetValue-actualValue) <= tolerance
	case "string":
		return fmt.Sprint(target) == fmt.Sprint(actual)
	default:
		return reflect.DeepEqual(target, actual)
	}
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

	return verifyControlReadback(ctx, target, command.DataType, 100*time.Millisecond, func(readCtx context.Context) (any, error) {
		return collector.ReadParamValueWithConn(readCtx, netConn, device, *readParam)
	})
}
