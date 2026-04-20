package modbusRtu

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"

	"multiple-protocol-controller/internal/protocol"
	"multiple-protocol-controller/pkg/utils"
)

func (m *ModbusRtu) EncodeCommand(cmd any) ([]byte, error) {
	payload, err := coerceCommandMessage(cmd)
	if err != nil {
		return nil, err
	}
	return buildModbusFrame(payload)
}

func coerceCommandMessage(cmd any) (*protocol.CommandMessage, error) {
	switch v := cmd.(type) {
	case *protocol.CommandMessage:
		if v == nil {
			return nil, errors.New("modbusRtu: command payload is nil")
		}
		return v, nil
	case protocol.CommandMessage:
		return &v, nil
	default:
		return nil, fmt.Errorf("modbusRtu: unsupported command payload type %T", cmd)
	}
}

func buildModbusFrame(msg *protocol.CommandMessage) ([]byte, error) {
	functionCode := msg.FunctionCode
	if msg.Address > math.MaxUint16 {
		return nil, fmt.Errorf("modbusRtu: register address %d overflow", msg.Address)
	}

	deviceAddress := byte(msg.DeviceAddress & 0xFF)
	registerAddress := uint16(msg.Address & 0xFFFF)
	endian := normalizeEndian(msg.Endian)

	switch functionCode {
	case ModbusRtuFunCode.WriteHoldRegister:
		data, err := encodeRegisterValue(msg.Value, msg.DataType, endian)
		if err != nil {
			return nil, err
		}
		if len(data) != 2 {
			return nil, fmt.Errorf("modbusRtu: function code 0x06 expects 1 register, got %d bytes", len(data))
		}
		frame := make([]byte, 6)
		frame[0] = deviceAddress
		frame[1] = byte(functionCode)
		binary.BigEndian.PutUint16(frame[2:4], registerAddress)
		copy(frame[4:], data)
		return appendCRC(frame), nil
	case ModbusRtuFunCode.WriteCoil:
		boolVal, err := utils.CoerceBool(msg.Value)
		if err != nil {
			return nil, fmt.Errorf("modbusRtu: invalid coil value: %w", err)
		}
		value := uint16(0x0000)
		if boolVal {
			value = 0xFF00
		}
		frame := make([]byte, 6)
		frame[0] = deviceAddress
		frame[1] = byte(functionCode)
		binary.BigEndian.PutUint16(frame[2:4], registerAddress)
		binary.BigEndian.PutUint16(frame[4:], value)
		return appendCRC(frame), nil
	case ModbusRtuFunCode.WriteMultiRegister:
		data, err := encodeRegisterValue(msg.Value, msg.DataType, endian)
		if err != nil {
			return nil, err
		}
		if len(data)%2 != 0 {
			return nil, fmt.Errorf("modbusRtu: multi register payload length must be even, got %d", len(data))
		}
		registers := uint16(len(data) / 2)
		frame := make([]byte, 7+len(data))
		frame[0] = deviceAddress
		frame[1] = byte(functionCode)
		binary.BigEndian.PutUint16(frame[2:4], registerAddress)
		binary.BigEndian.PutUint16(frame[4:6], registers)
		frame[6] = byte(len(data))
		copy(frame[7:], data)
		return appendCRC(frame), nil
	case ModbusRtuFunCode.WriteMultiCoil:
		bools, err := normalizeBoolSlice(msg.Value)
		if err != nil {
			return nil, fmt.Errorf("modbusRtu: invalid multi coil payload: %w", err)
		}
		if len(bools) == 0 {
			return nil, errors.New("modbusRtu: multi coil payload cannot be empty")
		}
		bytes := packCoils(bools)
		frame := make([]byte, 7+len(bytes))
		frame[0] = deviceAddress
		frame[1] = byte(functionCode)
		binary.BigEndian.PutUint16(frame[2:4], registerAddress)
		binary.BigEndian.PutUint16(frame[4:6], uint16(len(bools)))
		frame[6] = byte(len(bytes))
		copy(frame[7:], bytes)
		return appendCRC(frame), nil
	default:
		return nil, fmt.Errorf("modbusRtu: unsupported function code %d", functionCode)
	}
}

func encodeRegisterValue(value interface{}, dataType string, endian string) ([]byte, error) {
	dt := strings.ToLower(strings.TrimSpace(dataType))
	if dt == "" {
		dt = "ushort"
	}

	switch dt {
	case "short", "int16":
		n, err := utils.CoerceInt64(value)
		if err != nil {
			return nil, fmt.Errorf("modbusRtu: %w", err)
		}
		if n < math.MinInt16 || n > math.MaxInt16 {
			return nil, fmt.Errorf("modbusRtu: %d exceeds int16 range", n)
		}
		buf := make([]byte, 2)
		binary.BigEndian.PutUint16(buf, uint16(int16(n)))
		return applyEndian(buf, endian), nil
	case "ushort", "uint16":
		n, err := utils.CoerceUint64(value)
		if err != nil {
			return nil, fmt.Errorf("modbusRtu: %w", err)
		}
		if n > math.MaxUint16 {
			return nil, fmt.Errorf("modbusRtu: %d exceeds uint16 range", n)
		}
		buf := make([]byte, 2)
		binary.BigEndian.PutUint16(buf, uint16(n))
		return applyEndian(buf, endian), nil
	case "int32":
		n, err := utils.CoerceInt64(value)
		if err != nil {
			return nil, fmt.Errorf("modbusRtu: %w", err)
		}
		if n < math.MinInt32 || n > math.MaxInt32 {
			return nil, fmt.Errorf("modbusRtu: %d exceeds int32 range", n)
		}
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(int32(n)))
		return applyEndian(buf, endian), nil
	case "uint32":
		n, err := utils.CoerceUint64(value)
		if err != nil {
			return nil, fmt.Errorf("modbusRtu: %w", err)
		}
		if n > math.MaxUint32 {
			return nil, fmt.Errorf("modbusRtu: %d exceeds uint32 range", n)
		}
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(n))
		return applyEndian(buf, endian), nil
	case "float":
		f, err := utils.CoerceFloat64(value)
		if err != nil {
			return nil, fmt.Errorf("modbusRtu: %w", err)
		}
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, math.Float32bits(float32(f)))
		return applyEndian(buf, endian), nil
	case "double":
		f, err := utils.CoerceFloat64(value)
		if err != nil {
			return nil, fmt.Errorf("modbusRtu: %w", err)
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(f))
		return applyEndian(buf, endian), nil
	case "uint64":
		n, err := utils.CoerceUint64(value)
		if err != nil {
			return nil, fmt.Errorf("modbusRtu: %w", err)
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, n)
		return applyEndian(buf, endian), nil
	case "int64":
		n, err := utils.CoerceInt64(value)
		if err != nil {
			return nil, fmt.Errorf("modbusRtu: %w", err)
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(n))
		return applyEndian(buf, endian), nil
	default:
		return nil, fmt.Errorf("modbusRtu: unsupported command data type %s", dataType)
	}
}

func normalizeBoolSlice(val interface{}) ([]bool, error) {
	switch v := val.(type) {
	case []bool:
		return v, nil
	case []interface{}:
		bools := make([]bool, len(v))
		for i, item := range v {
			b, err := utils.CoerceBool(item)
			if err != nil {
				return nil, fmt.Errorf("modbusRtu: %w", err)
			}
			bools[i] = b
		}
		return bools, nil
	default:
		return nil, fmt.Errorf("modbusRtu: unsupported bool slice type %T", val)
	}
}

func packCoils(bools []bool) []byte {
	byteLen := (len(bools) + 7) / 8
	result := make([]byte, byteLen)
	for i, b := range bools {
		if b {
			byteIndex := i / 8
			bitIndex := uint(i % 8)
			result[byteIndex] |= 1 << bitIndex
		}
	}
	return result
}

// CRC helpers reused by both encoder and collector.
func appendCRC(frame []byte) []byte {
	crc := computeCRC(frame)
	return append(frame, byte(crc&0xFF), byte(crc>>8))
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
			// swap bytes within each 16-bit word
			for i := 0; i < 8; i += 2 {
				out[i], out[i+1] = out[i+1], out[i]
			}
		case "CDAB":
			// swap word order (16-bit words)
			for i := 0; i < 4; i++ {
				out[i], out[i+4] = out[i+4], out[i]
			}
		}
	}
	return out
}
