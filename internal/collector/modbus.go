package collector

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"time"

	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/internal/conn"

	"go.uber.org/zap"
)

var errUnsupportedFunction = errors.New("unsupported modbus function code")

func collectModbusParam(ctx context.Context, device config.DeviceRuntime, param config.ModbusParam) error {
	value, err := readParamValue(ctx, device, param)
	if err != nil {
		if errors.Is(err, errGatewayBusy) {
			return fmt.Errorf("gateway busy")
		}
		return err
	}

	logDebug("modbus collect",
		zap.String("deviceSerial", device.Config.SerialNumber),
		zap.String("param", param.Identify),
		zap.Any("value", value))

	recordCollectedValue(device, param, value)

	return nil
}

var errGatewayBusy = errors.New("gateway busy")

func ReadParamValue(ctx context.Context, device config.DeviceRuntime, param config.ModbusParam) (interface{}, error) {
	return readParamValue(ctx, device, param)
}

func ReadParamValueWithConn(ctx context.Context, netConn net.Conn, device config.DeviceRuntime, param config.ModbusParam) (interface{}, error) {
	return readParamValueWithConn(ctx, netConn, device, param)
}

func readParamValue(ctx context.Context, device config.DeviceRuntime, param config.ModbusParam) (interface{}, error) {
	manager, ok := conn.Default()
	if !ok {
		return nil, errors.New("connection manager not initialised")
	}

	netConn, release, err := acquireExclusiveConnection(ctx, manager, device.Config.SerialNumber)
	if err != nil {
		if errors.Is(err, conn.ErrGatewayBusy) {
			return nil, errGatewayBusy
		}
		return nil, err
	}
	defer release()

	val, err := readParamValueWithConn(ctx, netConn, device, param)
	if err != nil {
		manager.ResetConnection(device.Config.SerialNumber)
	}
	return val, err
}

func readParamValueWithConn(ctx context.Context, netConn net.Conn, device config.DeviceRuntime, param config.ModbusParam) (interface{}, error) {
	if device.SlaveID == 0 {
		return nil, errors.New("device address missing")
	}

	quantity := quantityForParam(param)
	if quantity == 0 {
		return nil, errors.New("invalid register quantity")
	}

	frame, err := buildReadFrame(byte(device.SlaveID), param.FunctionCode, uint16(param.Address), quantity)
	if err != nil {
		return nil, err
	}

	timeout := time.Duration(device.ResponseTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}

	deadline := time.Now().Add(timeout)
	_ = netConn.SetDeadline(deadline)
	defer netConn.SetDeadline(time.Time{})

	if _, err := netConn.Write(frame); err != nil {
		return nil, err
	}

	expectedLen, err := expectedResponseLength(param.FunctionCode, quantity)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, expectedLen)
	if _, err := io.ReadFull(netConn, buf); err != nil {
		return nil, err
	}

	if !verifyCRC(buf) {
		return nil, errors.New("crc mismatch")
	}

	value, err := decodePayload(param, buf)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func WaitCommandAck(ctx context.Context, netConn net.Conn, device config.DeviceRuntime, cmd config.ModbusCommand) error {
	length := writeAckLength(cmd.FunctionCode)
	if length == 0 {
		return nil
	}

	timeout := time.Duration(device.ResponseTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}
	deadline := time.Now().Add(timeout)
	_ = netConn.SetDeadline(deadline)
	defer netConn.SetDeadline(time.Time{})

	buf := make([]byte, length)
	if _, err := io.ReadFull(netConn, buf); err != nil {
		return err
	}
	if !verifyCRC(buf) {
		return errors.New("crc mismatch")
	}
	return nil
}

func writeAckLength(functionCode int) int {
	switch functionCode {
	case 5, 6, 15, 16:
		return 8
	default:
		return 0
	}
}

func collectModbusBatch(ctx context.Context, device config.DeviceRuntime, batch batchQuery) error {
	if len(batch.params) == 0 {
		return nil
	}
	manager, ok := conn.Default()
	if !ok {
		return errors.New("connection manager not initialised")
	}
	if device.SlaveID == 0 {
		return errors.New("device address missing")
	}

	frame, err := buildReadFrame(byte(device.SlaveID), batch.functionCode, batch.startAddr, batch.quantity)
	if err != nil {
		return err
	}

	timeout := time.Duration(device.ResponseTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	netConn, release, err := acquireExclusiveConnection(ctx, manager, device.Config.SerialNumber)
	if err != nil {
		return err
	}
	defer release()

	deadline := time.Now().Add(timeout)
	_ = netConn.SetDeadline(deadline)
	defer netConn.SetDeadline(time.Time{})

	traceParam, traced := firstTraceableParam(device, batch)
	if traced {
		traceModbusFrame("SEND", device, traceParam, frame, map[string]interface{}{
			"batch":        true,
			"startAddr":    batch.startAddr,
			"quantity":     batch.quantity,
			"paramCount":   len(batch.params),
			"functionCode": batch.functionCode,
			"params":       batchParamNames(batch),
		})
	}
	if _, err := netConn.Write(frame); err != nil {
		if traced {
			traceModbusFrame("ERROR", device, traceParam, nil, map[string]interface{}{
				"batch": true,
				"stage": "write",
				"error": err.Error(),
			})
		}
		manager.ResetConnection(device.Config.SerialNumber)
		return err
	}

	expectedLen, err := expectedResponseLength(batch.functionCode, batch.quantity)
	if err != nil {
		return err
	}

	buf := make([]byte, expectedLen)
	if _, err := io.ReadFull(netConn, buf); err != nil {
		if traced {
			traceModbusFrame("ERROR", device, traceParam, nil, map[string]interface{}{
				"batch": true,
				"stage": "read",
				"error": err.Error(),
			})
		}
		manager.ResetConnection(device.Config.SerialNumber)
		return err
	}

	if traced {
		traceModbusFrame("RECV", device, traceParam, buf, map[string]interface{}{
			"batch":      true,
			"dataLength": len(buf),
		})
	}

	if !verifyCRC(buf) {
		if traced {
			traceModbusFrame("ERROR", device, traceParam, buf, map[string]interface{}{
				"batch": true,
				"stage": "crc",
			})
		}
		manager.ResetConnection(device.Config.SerialNumber)
		return errors.New("crc mismatch")
	}

	switch batch.functionCode {
	case 1, 2:
		return decodeDiscreteBatch(device, batch, buf)
	case 3, 4:
		return decodeRegisterBatch(device, batch, buf)
	default:
		return errUnsupportedFunction
	}
}

func quantityForParam(param config.ModbusParam) uint16 {
	if param.Quantity > 0 {
		return uint16(param.Quantity)
	}
	switch strings.ToLower(param.DataType) {
	case "int32", "uint32", "float":
		return 2
	case "double", "uint64", "int64":
		return 4
	default:
		return 1
	}
}

func buildReadFrame(deviceAddr byte, functionCode int, startAddr uint16, quantity uint16) ([]byte, error) {
	switch functionCode {
	case 1, 2, 3, 4:
	default:
		return nil, errUnsupportedFunction
	}

	if quantity == 0 {
		return nil, errors.New("quantity must be greater than 0")
	}

	frame := make([]byte, 6)
	frame[0] = deviceAddr
	frame[1] = byte(functionCode)
	binary.BigEndian.PutUint16(frame[2:4], startAddr)
	binary.BigEndian.PutUint16(frame[4:6], quantity)
	return appendCRC(frame), nil
}

func expectedResponseLength(functionCode int, quantity uint16) (int, error) {
	var byteCount int
	switch functionCode {
	case 1, 2:
		byteCount = int((quantity + 7) / 8)
	case 3, 4:
		byteCount = int(quantity * 2)
	default:
		return 0, errUnsupportedFunction
	}
	return 3 + byteCount + 2, nil
}

func decodePayload(param config.ModbusParam, frame []byte) (interface{}, error) {
	if len(frame) < 5 {
		return nil, errors.New("invalid frame length")
	}
	byteCount := int(frame[2])
	if len(frame) < 3+byteCount+2 {
		return nil, errors.New("payload too short")
	}
	payload := frame[3 : 3+byteCount]

	switch param.FunctionCode {
	case 1, 2:
		return decodeDiscreteValue(param, payload)
	case 3, 4:
		return decodeRegisterValue(param, payload)
	default:
		return nil, errUnsupportedFunction
	}
}

func decodeDiscreteValue(param config.ModbusParam, payload []byte) (interface{}, error) {
	if len(payload) == 0 {
		return nil, errors.New("empty payload")
	}
	bitIndex := param.Bit
	if bitIndex < 0 {
		bitIndex = 0
	}
	bytePos := bitIndex / 8
	if bytePos >= len(payload) {
		return nil, errors.New("bit index out of range")
	}
	value := (payload[bytePos] >> uint(bitIndex%8)) & 0x1
	return value == 1, nil
}

func decodeRegisterValue(param config.ModbusParam, payload []byte) (interface{}, error) {
	requiredBytes := int(quantityForParam(param)) * 2
	if requiredBytes == 0 {
		requiredBytes = 2
	}
	if len(payload) < requiredBytes {
		return nil, errors.New("payload shorter than expected")
	}

	data := applyEndian(payload[:requiredBytes], param.Endian)
	switch strings.ToLower(param.DataType) {
	case "short", "int16":
		return int16(binary.BigEndian.Uint16(data[:2])), nil
	case "ushort", "uint16":
		return binary.BigEndian.Uint16(data[:2]), nil
	case "int32":
		return int32(binary.BigEndian.Uint32(data[:4])), nil
	case "uint32":
		return binary.BigEndian.Uint32(data[:4]), nil
	case "float":
		bits := binary.BigEndian.Uint32(data[:4])
		return math.Float32frombits(bits), nil
	case "double":
		bits := binary.BigEndian.Uint64(padBytes(data, 8))
		return math.Float64frombits(bits), nil
	case "int64":
		val := int64(binary.BigEndian.Uint64(padBytes(data, 8)))
		return val, nil
	case "uint64":
		val := binary.BigEndian.Uint64(padBytes(data, 8))
		return val, nil
	case "bool":
		raw := binary.BigEndian.Uint16(data[:2])
		return raw != 0, nil
	default:
		return binary.BigEndian.Uint16(data[:2]), nil
	}
}

func decodeRegisterBatch(device config.DeviceRuntime, batch batchQuery, frame []byte) error {
	if len(frame) < 5 {
		return errors.New("invalid frame length")
	}
	byteCount := int(frame[2])
	if len(frame) < 3+byteCount+2 {
		return errors.New("payload too short")
	}
	payload := frame[3 : 3+byteCount]

	for _, param := range batch.params {
		offsetRegisters := int64(param.Address) - int64(batch.startAddr)
		if offsetRegisters < 0 {
			continue
		}
		byteOffset := int(offsetRegisters) * 2
		need := registerByteSize(*param)
		if byteOffset < 0 || byteOffset+need > len(payload) {
			logInfo("batch payload shorter than expected",
				zap.String("deviceSerial", device.Config.SerialNumber),
				zap.String("param", param.Identify),
				zap.Uint64("address", param.Address),
				zap.Int("functionCode", param.FunctionCode))
			continue
		}

		value, err := decodeRegisterValue(*param, payload[byteOffset:byteOffset+need])
		if err != nil {
			logInfo("decode batch register failed",
				zap.String("deviceSerial", device.Config.SerialNumber),
				zap.String("param", param.Identify),
				zap.Error(err))
			continue
		}
		recordCollectedValue(device, *param, value)
	}

	return nil
}

func registerByteSize(param config.ModbusParam) int {
	return int(quantityForParam(param)) * 2
}

func decodeDiscreteBatch(device config.DeviceRuntime, batch batchQuery, frame []byte) error {
	if len(frame) < 5 {
		return errors.New("invalid frame length")
	}
	byteCount := int(frame[2])
	if len(frame) < 3+byteCount+2 {
		return errors.New("payload too short")
	}
	payload := frame[3 : 3+byteCount]
	for _, param := range batch.params {
		offset := int(param.Address) - int(batch.startAddr)
		bytePos := offset / 8
		bitOffset := offset % 8
		if bytePos < 0 || bytePos >= len(payload) {
			logInfo("batch payload shorter than expected",
				zap.String("deviceSerial", device.Config.SerialNumber),
				zap.String("param", param.Identify),
				zap.Uint64("address", param.Address),
				zap.Int("functionCode", param.FunctionCode))
			continue
		}
		value := (payload[bytePos] >> uint(bitOffset)) & 0x1
		recordCollectedValue(device, *param, value == 1)
	}
	return nil
}

func padBytes(data []byte, size int) []byte {
	if len(data) >= size {
		return data[:size]
	}
	padded := make([]byte, size)
	copy(padded[size-len(data):], data)
	return padded
}

func batchParamNames(batch batchQuery) []string {
	names := make([]string, 0, len(batch.params))
	for _, p := range batch.params {
		names = append(names, p.Identify)
	}
	return names
}

func firstTraceableParam(device config.DeviceRuntime, batch batchQuery) (config.ModbusParam, bool) {
	for _, param := range batch.params {
		if shouldTraceModbus(device, *param) {
			return *param, true
		}
	}
	return config.ModbusParam{}, false
}

func acquireExclusiveConnection(ctx context.Context, manager *conn.Manager, deviceSerial string) (net.Conn, func(), error) {
	for {
		netConn, release, err := manager.ExclusiveConnection(ctx, deviceSerial)
		if err == nil {
			return netConn, release, nil
		}
		if errors.Is(err, conn.ErrGatewayBusy) {
			select {
			case <-time.After(100 * time.Millisecond):
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
			continue
		}
		return nil, nil, err
	}
}

func normalizeEndian(endian string) string {
	upper := strings.ToUpper(strings.TrimSpace(endian))
	if upper == "" {
		return "ABCD"
	}
	switch upper {
	case "ABCD", "DCBA", "BADC", "CDAB", "AB", "BA":
		return upper
	default:
		return "ABCD"
	}
}

func applyEndian(data []byte, endian string) []byte {
	e := normalizeEndian(endian)
	out := make([]byte, len(data))
	copy(out, data)
	switch len(out) {
	case 2:
		if e == "BA" || e == "DCBA" {
			out[0], out[1] = out[1], out[0]
		}
	case 4:
		switch e {
		case "DCBA":
			out[0], out[3] = out[3], out[0]
			out[1], out[2] = out[2], out[1]
		case "BADC":
			out[0], out[1] = out[1], out[0]
			out[2], out[3] = out[3], out[2]
		case "CDAB":
			out[0], out[2] = out[2], out[0]
			out[1], out[3] = out[3], out[1]
		}
	case 8:
		switch e {
		case "DCBA":
			for i := 0; i < 4; i++ {
				out[i], out[7-i] = out[7-i], out[i]
			}
		case "BADC":
			for i := 0; i < 8; i += 2 {
				out[i], out[i+1] = out[i+1], out[i]
			}
		case "CDAB":
			for i := 0; i < 4; i++ {
				out[i], out[i+4] = out[i+4], out[i]
			}
		}
	}
	return out
}

// CRC helpers
func appendCRC(frame []byte) []byte {
	crc := computeCRC(frame)
	return append(frame, byte(crc&0xFF), byte(crc>>8))
}

func verifyCRC(frame []byte) bool {
	if len(frame) < 2 {
		return false
	}
	payload := frame[:len(frame)-2]
	expected := computeCRC(payload)
	got := uint16(frame[len(frame)-2]) | uint16(frame[len(frame)-1])<<8
	return expected == got
}

func computeCRC(data []byte) uint16 {
	var crc uint16 = 0xFFFF
	for _, b := range data {
		crc ^= uint16(b)
		for i := 0; i < 8; i++ {
			if crc&0x0001 != 0 {
				crc = (crc >> 1) ^ 0xA001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}
