package conn

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"time"
)

type BacnetConfig struct {
	IP             string
	Port           uint16
	DeviceInstance uint32
}

type BacnetManager struct {
	mu    sync.Mutex
	conns map[string]*bacnetConnHolder
}

type bacnetConnHolder struct {
	mu       sync.Mutex
	conn     *net.UDPConn
	endpoint string
	invokeID byte
}

var bacnetDefaultHolder struct {
	mu  sync.Mutex
	mgr *BacnetManager
}

func DefaultBacnetManager() *BacnetManager {
	bacnetDefaultHolder.mu.Lock()
	defer bacnetDefaultHolder.mu.Unlock()
	if bacnetDefaultHolder.mgr == nil {
		bacnetDefaultHolder.mgr = NewBacnetManager()
	}
	return bacnetDefaultHolder.mgr
}

func NewBacnetManager() *BacnetManager {
	return &BacnetManager{conns: make(map[string]*bacnetConnHolder)}
}

func (m *BacnetManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, holder := range m.conns {
		if holder.conn != nil {
			_ = holder.conn.Close()
		}
		delete(m.conns, key)
	}
	return nil
}

func (m *BacnetManager) ReadPresentValue(ctx context.Context, cfg BacnetConfig, objectType string, instance uint32) (interface{}, error) {
	holder, err := m.ensureConn(cfg)
	if err != nil {
		return nil, err
	}

	holder.mu.Lock()
	defer holder.mu.Unlock()

	invokeID := holder.nextInvokeID()
	req, err := buildReadPresentValueRequest(invokeID, objectType, instance)
	if err != nil {
		return nil, err
	}

	timeout := 5 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return nil, context.DeadlineExceeded
		}
	}
	if err := holder.conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	defer holder.conn.SetDeadline(time.Time{})

	if _, err := holder.conn.Write(req); err != nil {
		return nil, err
	}

	buf := make([]byte, 1500)
	for {
		n, err := holder.conn.Read(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
			}
			return nil, err
		}

		val, done, err := parseReadPresentValueResponse(buf[:n], invokeID)
		if err != nil {
			return nil, err
		}
		if done {
			return val, nil
		}
	}
}

func (m *BacnetManager) WritePresentValue(ctx context.Context, cfg BacnetConfig, objectType string, instance uint32, value interface{}) error {
	holder, err := m.ensureConn(cfg)
	if err != nil {
		return err
	}

	holder.mu.Lock()
	defer holder.mu.Unlock()

	invokeID := holder.nextInvokeID()
	req, err := buildWritePresentValueRequest(invokeID, objectType, instance, value)
	if err != nil {
		return err
	}

	timeout := 5 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return context.DeadlineExceeded
		}
	}
	if err := holder.conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	defer holder.conn.SetDeadline(time.Time{})

	if _, err := holder.conn.Write(req); err != nil {
		return err
	}

	buf := make([]byte, 1500)
	for {
		n, err := holder.conn.Read(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() && ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		done, err := parseWritePresentValueResponse(buf[:n], invokeID)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func (m *BacnetManager) ensureConn(cfg BacnetConfig) (*bacnetConnHolder, error) {
	endpoint, err := bacnetEndpoint(cfg)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if holder, ok := m.conns[endpoint]; ok && holder.conn != nil {
		return holder, nil
	}

	addr, err := net.ResolveUDPAddr("udp", endpoint)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}
	holder := &bacnetConnHolder{
		conn:     conn,
		endpoint: endpoint,
	}
	m.conns[endpoint] = holder
	return holder, nil
}

func (h *bacnetConnHolder) nextInvokeID() byte {
	h.invokeID++
	if h.invokeID == 0 {
		h.invokeID = 1
	}
	return h.invokeID
}

func bacnetEndpoint(cfg BacnetConfig) (string, error) {
	ip := strings.TrimSpace(cfg.IP)
	if ip == "" || cfg.Port == 0 {
		return "", fmt.Errorf("bacnet: gateway address missing")
	}
	return net.JoinHostPort(ip, fmt.Sprintf("%d", cfg.Port)), nil
}

func buildReadPresentValueRequest(invokeID byte, objectType string, instance uint32) ([]byte, error) {
	objectTypeID, err := bacnetObjectTypeID(objectType)
	if err != nil {
		return nil, err
	}
	service := []byte{
		0x0c,
		0, 0, 0, 0,
		0x19,
		0x55,
	}

	objectID := (uint32(objectTypeID) << 22) | (instance & 0x3FFFFF)
	binary.BigEndian.PutUint32(service[1:5], objectID)

	apdu := []byte{
		0x00,
		0x05,
		invokeID,
		0x0c,
	}
	apdu = append(apdu, service...)

	npdu := []byte{
		0x01,
		0x04,
	}

	payload := append(npdu, apdu...)
	frame := []byte{
		0x81,
		0x0a,
		0x00,
		0x00,
	}
	frame = append(frame, payload...)
	binary.BigEndian.PutUint16(frame[2:4], uint16(len(frame)))
	return frame, nil
}

func buildWritePresentValueRequest(invokeID byte, objectType string, instance uint32, value interface{}) ([]byte, error) {
	objectTypeID, err := bacnetObjectTypeID(objectType)
	if err != nil {
		return nil, err
	}
	objectID := (uint32(objectTypeID) << 22) | (instance & 0x3FFFFF)

	valuePayload, err := buildBacnetApplicationValue(value)
	if err != nil {
		return nil, err
	}

	service := []byte{
		0x0c,
		0, 0, 0, 0,
		0x19,
		0x55,
		0x3e,
	}
	binary.BigEndian.PutUint32(service[1:5], objectID)
	service = append(service, valuePayload...)
	service = append(service, 0x3f)

	apdu := []byte{
		0x00,
		0x05,
		invokeID,
		0x0f,
	}
	apdu = append(apdu, service...)

	npdu := []byte{
		0x01,
		0x04,
	}

	payload := append(npdu, apdu...)
	frame := []byte{
		0x81,
		0x0a,
		0x00,
		0x00,
	}
	frame = append(frame, payload...)
	binary.BigEndian.PutUint16(frame[2:4], uint16(len(frame)))
	return frame, nil
}

func parseReadPresentValueResponse(frame []byte, invokeID byte) (interface{}, bool, error) {
	if len(frame) < 10 {
		return nil, false, fmt.Errorf("bacnet: response too short")
	}
	if frame[0] != 0x81 {
		return nil, false, fmt.Errorf("bacnet: invalid BVLC type 0x%02x", frame[0])
	}
	if binary.BigEndian.Uint16(frame[2:4]) > uint16(len(frame)) {
		return nil, false, fmt.Errorf("bacnet: invalid BVLC length")
	}

	offset := 4
	if frame[offset] != 0x01 {
		return nil, false, fmt.Errorf("bacnet: unsupported NPDU version 0x%02x", frame[offset])
	}
	offset += 2
	if len(frame) <= offset {
		return nil, false, fmt.Errorf("bacnet: missing APDU")
	}

	pduType := frame[offset] >> 4
	switch pduType {
	case 0x3:
		if len(frame) < offset+3 {
			return nil, false, fmt.Errorf("bacnet: complex ack too short")
		}
		if frame[offset+1] != invokeID {
			return nil, false, nil
		}
		if frame[offset+2] != 0x0c {
			return nil, false, fmt.Errorf("bacnet: unexpected service choice 0x%02x", frame[offset+2])
		}
		body := frame[offset+3:]
		valuePayload, err := extractReadPropertyValue(body)
		if err != nil {
			return nil, false, err
		}
		value, _, err := parseBacnetApplicationValue(valuePayload)
		return value, true, err
	case 0x5:
		return nil, false, fmt.Errorf("bacnet: error response received")
	default:
		return nil, false, fmt.Errorf("bacnet: unsupported APDU type 0x%02x", frame[offset])
	}
}

func parseWritePresentValueResponse(frame []byte, invokeID byte) (bool, error) {
	if len(frame) < 10 {
		return false, fmt.Errorf("bacnet: response too short")
	}
	if frame[0] != 0x81 {
		return false, fmt.Errorf("bacnet: invalid BVLC type 0x%02x", frame[0])
	}
	offset := 4
	if frame[offset] != 0x01 {
		return false, fmt.Errorf("bacnet: unsupported NPDU version 0x%02x", frame[offset])
	}
	offset += 2
	if len(frame) <= offset {
		return false, fmt.Errorf("bacnet: missing APDU")
	}

	pduType := frame[offset] >> 4
	switch pduType {
	case 0x2:
		if len(frame) < offset+3 {
			return false, fmt.Errorf("bacnet: simple ack too short")
		}
		if frame[offset+1] != invokeID {
			return false, nil
		}
		if frame[offset+2] != 0x0f {
			return false, fmt.Errorf("bacnet: unexpected service choice 0x%02x", frame[offset+2])
		}
		return true, nil
	case 0x5:
		return false, fmt.Errorf("bacnet: error response received")
	default:
		return false, fmt.Errorf("bacnet: unsupported APDU type 0x%02x", frame[offset])
	}
}

func extractReadPropertyValue(body []byte) ([]byte, error) {
	if len(body) == 0 {
		return nil, fmt.Errorf("bacnet: empty ReadProperty ACK body")
	}
	openIdx := -1
	closeIdx := -1
	for i, b := range body {
		if b == 0x3e {
			openIdx = i
			break
		}
	}
	if openIdx < 0 {
		return nil, fmt.Errorf("bacnet: opening tag for property value not found")
	}
	for i := openIdx + 1; i < len(body); i++ {
		if body[i] == 0x3f {
			closeIdx = i
			break
		}
	}
	if closeIdx < 0 || closeIdx <= openIdx+1 {
		return nil, fmt.Errorf("bacnet: closing tag for property value not found")
	}
	return body[openIdx+1 : closeIdx], nil
}

func parseBacnetApplicationValue(data []byte) (interface{}, int, error) {
	if len(data) == 0 {
		return nil, 0, fmt.Errorf("bacnet: empty application value")
	}
	tag := data[0]
	tagNumber := int(tag >> 4)
	if tag&0x08 != 0 {
		return nil, 0, fmt.Errorf("bacnet: context tag value not supported in read phase")
	}
	length := int(tag & 0x07)
	offset := 1
	if length == 5 {
		if len(data) < 2 {
			return nil, 0, fmt.Errorf("bacnet: extended length missing")
		}
		length = int(data[1])
		offset++
	}

	switch tagNumber {
	case 1:
		return length != 0, offset, nil
	case 2:
		return parseUnsigned(data, offset, length)
	case 3:
		return parseSigned(data, offset, length)
	case 4:
		if length != 4 || len(data) < offset+4 {
			return nil, 0, fmt.Errorf("bacnet: invalid REAL value length")
		}
		bits := binary.BigEndian.Uint32(data[offset : offset+4])
		return math.Float32frombits(bits), offset + 4, nil
	case 5:
		if length != 8 || len(data) < offset+8 {
			return nil, 0, fmt.Errorf("bacnet: invalid DOUBLE value length")
		}
		bits := binary.BigEndian.Uint64(data[offset : offset+8])
		return math.Float64frombits(bits), offset + 8, nil
	case 7:
		if len(data) < offset+length || length == 0 {
			return nil, 0, fmt.Errorf("bacnet: invalid character string length")
		}
		return string(data[offset+1 : offset+length]), offset + length, nil
	case 9:
		return parseUnsigned(data, offset, length)
	default:
		return nil, 0, fmt.Errorf("bacnet: unsupported application tag %d", tagNumber)
	}
}

func buildBacnetApplicationValue(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return []byte{0x11}, nil
		}
		return []byte{0x10}, nil
	case float32:
		buf := make([]byte, 5)
		buf[0] = 0x44
		binary.BigEndian.PutUint32(buf[1:], math.Float32bits(v))
		return buf, nil
	case float64:
		buf := make([]byte, 9)
		buf[0] = 0x55
		binary.BigEndian.PutUint64(buf[1:], math.Float64bits(v))
		return buf, nil
	case int:
		return buildSignedValue(int64(v))
	case int32:
		return buildSignedValue(int64(v))
	case int64:
		return buildSignedValue(v)
	case uint:
		return buildUnsignedValue(uint64(v))
	case uint32:
		return buildUnsignedValue(uint64(v))
	case uint64:
		return buildUnsignedValue(v)
	case string:
		return nil, fmt.Errorf("bacnet: string presentValue is not supported in current phase")
	default:
		return nil, fmt.Errorf("bacnet: unsupported value type %T", value)
	}
}

func buildUnsignedValue(v uint64) ([]byte, error) {
	raw := encodeUint(v)
	buf := make([]byte, 1+len(raw))
	buf[0] = 0x20 | byte(len(raw))
	copy(buf[1:], raw)
	return buf, nil
}

func buildSignedValue(v int64) ([]byte, error) {
	raw := encodeInt(v)
	buf := make([]byte, 1+len(raw))
	buf[0] = 0x30 | byte(len(raw))
	copy(buf[1:], raw)
	return buf, nil
}

func encodeUint(v uint64) []byte {
	if v == 0 {
		return []byte{0}
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	i := 0
	for i < len(buf)-1 && buf[i] == 0 {
		i++
	}
	return buf[i:]
}

func encodeInt(v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	if v >= 0 {
		i := 0
		for i < len(buf)-1 && buf[i] == 0 && buf[i+1]&0x80 == 0 {
			i++
		}
		return buf[i:]
	}
	i := 0
	for i < len(buf)-1 && buf[i] == 0xff && buf[i+1]&0x80 == 0x80 {
		i++
	}
	return buf[i:]
}

func parseUnsigned(data []byte, offset int, length int) (interface{}, int, error) {
	if length < 0 || len(data) < offset+length {
		return nil, 0, fmt.Errorf("bacnet: invalid unsigned value length")
	}
	var value uint64
	for _, b := range data[offset : offset+length] {
		value = (value << 8) | uint64(b)
	}
	return value, offset + length, nil
}

func parseSigned(data []byte, offset int, length int) (interface{}, int, error) {
	if length <= 0 || len(data) < offset+length {
		return nil, 0, fmt.Errorf("bacnet: invalid signed value length")
	}
	var value int64
	for _, b := range data[offset : offset+length] {
		value = (value << 8) | int64(b)
	}
	shift := uint(64 - (length * 8))
	value = (value << shift) >> shift
	return value, offset + length, nil
}

func bacnetObjectTypeID(objectType string) (uint16, error) {
	switch strings.TrimSpace(objectType) {
	case "analogInput":
		return 0, nil
	case "analogOutput":
		return 1, nil
	case "analogValue":
		return 2, nil
	case "binaryInput":
		return 3, nil
	case "binaryOutput":
		return 4, nil
	case "binaryValue":
		return 5, nil
	case "multiStateInput":
		return 13, nil
	case "multiStateOutput":
		return 14, nil
	case "multiStateValue":
		return 19, nil
	default:
		return 0, fmt.Errorf("bacnet: unsupported object type %q", objectType)
	}
}
