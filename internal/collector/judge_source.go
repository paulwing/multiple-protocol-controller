package collector

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"multiple-protocol-controller/internal/config"
)

const (
	defaultJudgeSourceStream         = "judge:source"
	defaultJudgeWriteTimeout         = 200 * time.Millisecond
	maximumJudgeWriteTimeout         = 2 * time.Second
	defaultJudgeRetryCount           = 1
	maximumJudgeRetryCount           = 3
	defaultJudgeRetryInterval        = 20 * time.Millisecond
	maximumJudgeRetryInterval        = time.Second
	maximumJudgeEventBytes           = 64 << 10
	maximumJudgeValueKeys            = 64
	maximumJudgePropertyRunes        = 128
	maximumJudgePropertyBytes        = 512
	maximumJudgeStringBytes          = 8 << 10
	maximumJudgeNumberBytes          = 64
	maximumJudgeDeviceIDRunes        = 64
	estimatedRedisStreamIDByteLength = 32
)

type judgeSourceConfig struct {
	enabled           bool
	stream            string
	writeTimeout      time.Duration
	retryCount        int
	retryInterval     time.Duration
	maximumEventBytes int
}

func normalizedJudgeSourceConfig(raw config.JudgeSourceCfg) judgeSourceConfig {
	stream := strings.TrimSpace(raw.Stream)
	if stream == "" {
		stream = defaultJudgeSourceStream
	}

	writeTimeout := time.Duration(raw.WriteTimeoutMS) * time.Millisecond
	if writeTimeout <= 0 {
		writeTimeout = defaultJudgeWriteTimeout
	}
	if writeTimeout > maximumJudgeWriteTimeout {
		writeTimeout = maximumJudgeWriteTimeout
	}

	retryCount := raw.RetryCount
	if retryCount <= 0 {
		retryCount = defaultJudgeRetryCount
	}
	if retryCount > maximumJudgeRetryCount {
		retryCount = maximumJudgeRetryCount
	}

	retryInterval := time.Duration(raw.RetryIntervalMS) * time.Millisecond
	if retryInterval <= 0 {
		retryInterval = defaultJudgeRetryInterval
	}
	if retryInterval > maximumJudgeRetryInterval {
		retryInterval = maximumJudgeRetryInterval
	}

	maximumEventBytes := raw.MaxEventBytes
	if maximumEventBytes <= 0 || maximumEventBytes > maximumJudgeEventBytes {
		maximumEventBytes = maximumJudgeEventBytes
	}

	return judgeSourceConfig{
		enabled:           raw.Enabled,
		stream:            stream,
		writeTimeout:      writeTimeout,
		retryCount:        retryCount,
		retryInterval:     retryInterval,
		maximumEventBytes: maximumEventBytes,
	}
}

func newJudgeEventID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate Judge event ID: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80

	var encoded [36]byte
	hex.Encode(encoded[0:8], raw[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], raw[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], raw[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], raw[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], raw[10:16])
	return string(encoded[:]), nil
}

func validJudgeEventID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' ||
		value[14] != '4' || !strings.ContainsRune("89ab", rune(value[19])) {
		return false
	}
	for index, current := range []byte(value) {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if !((current >= '0' && current <= '9') || (current >= 'a' && current <= 'f')) {
			return false
		}
	}
	return true
}

type judgeSourceEvent struct {
	eventID      string
	deviceID     string
	updatedPoint string
	collectedAt  string
	values       string
}

func (e judgeSourceEvent) redisValues() map[string]any {
	return map[string]any{
		"event_id":      e.eventID,
		"device_id":     e.deviceID,
		"updated_point": e.updatedPoint,
		"collected_at":  e.collectedAt,
		"values":        e.values,
	}
}

func buildJudgeSourceEvent(
	eventID string,
	deviceID string,
	updatedPoint string,
	collectedAt time.Time,
	values map[string]any,
	maximumBytes int,
) (judgeSourceEvent, error) {
	if !validJudgeEventID(eventID) {
		return judgeSourceEvent{}, fmt.Errorf("event_id must be a canonical lowercase UUIDv4")
	}

	deviceID = strings.TrimSpace(deviceID)
	if deviceID == "" || !utf8.ValidString(deviceID) || utf8.RuneCountInString(deviceID) > maximumJudgeDeviceIDRunes {
		return judgeSourceEvent{}, fmt.Errorf("device_id must be valid UTF-8 and at most %d characters", maximumJudgeDeviceIDRunes)
	}
	updatedPoint = strings.TrimSpace(updatedPoint)
	if err := validateJudgePropertyName(updatedPoint); err != nil {
		return judgeSourceEvent{}, fmt.Errorf("updated_point: %w", err)
	}
	if collectedAt.IsZero() {
		return judgeSourceEvent{}, fmt.Errorf("collected_at must not be zero")
	}

	encodedValues, err := encodeJudgeValues(values, updatedPoint)
	if err != nil {
		return judgeSourceEvent{}, fmt.Errorf("values: %w", err)
	}
	event := judgeSourceEvent{
		eventID:      eventID,
		deviceID:     deviceID,
		updatedPoint: updatedPoint,
		collectedAt:  collectedAt.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z"),
		values:       string(encodedValues),
	}
	if err := validateJudgeEnvelopeSize(event, maximumBytes); err != nil {
		return judgeSourceEvent{}, err
	}
	return event, nil
}

func encodeJudgeValues(values map[string]any, updatedPoint string) ([]byte, error) {
	if values == nil {
		return nil, fmt.Errorf("must be a JSON object")
	}
	if len(values) > maximumJudgeValueKeys {
		return nil, fmt.Errorf("key count %d exceeds %d", len(values), maximumJudgeValueKeys)
	}
	updatedValue, exists := values[updatedPoint]
	if !exists {
		return nil, fmt.Errorf("updated_point %q is missing", updatedPoint)
	}
	if updatedValue == nil {
		return nil, fmt.Errorf("updated_point %q must not be null", updatedPoint)
	}

	for key, value := range values {
		if err := validateJudgePropertyName(key); err != nil {
			return nil, fmt.Errorf("property %q: %w", key, err)
		}
		if err := validateJudgeScalar(value); err != nil {
			return nil, fmt.Errorf("property %q: %w", key, err)
		}
	}

	encoded, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("encode JSON: %w", err)
	}
	if len(encoded) > maximumJudgeEventBytes {
		return nil, fmt.Errorf("encoded object exceeds %d bytes", maximumJudgeEventBytes)
	}
	return encoded, nil
}

func validateJudgePropertyName(value string) error {
	if value == "" || !utf8.ValidString(value) {
		return fmt.Errorf("must be non-empty valid UTF-8")
	}
	if utf8.RuneCountInString(value) > maximumJudgePropertyRunes || len(value) > maximumJudgePropertyBytes {
		return fmt.Errorf("exceeds %d characters or %d bytes", maximumJudgePropertyRunes, maximumJudgePropertyBytes)
	}
	return nil
}

func validateJudgeScalar(value any) error {
	switch typed := value.(type) {
	case nil, bool:
		return nil
	case string:
		if !utf8.ValidString(typed) {
			return fmt.Errorf("string is not valid UTF-8")
		}
		if len(typed) > maximumJudgeStringBytes {
			return fmt.Errorf("string exceeds %d bytes", maximumJudgeStringBytes)
		}
		return nil
	case json.Number:
		return validateJudgeNumber(typed.String())
	case int:
		return validateJudgeNumber(strconv.FormatInt(int64(typed), 10))
	case int8:
		return validateJudgeNumber(strconv.FormatInt(int64(typed), 10))
	case int16:
		return validateJudgeNumber(strconv.FormatInt(int64(typed), 10))
	case int32:
		return validateJudgeNumber(strconv.FormatInt(int64(typed), 10))
	case int64:
		return validateJudgeNumber(strconv.FormatInt(typed, 10))
	case uint:
		return validateJudgeNumber(strconv.FormatUint(uint64(typed), 10))
	case uint8:
		return validateJudgeNumber(strconv.FormatUint(uint64(typed), 10))
	case uint16:
		return validateJudgeNumber(strconv.FormatUint(uint64(typed), 10))
	case uint32:
		return validateJudgeNumber(strconv.FormatUint(uint64(typed), 10))
	case uint64:
		return validateJudgeNumber(strconv.FormatUint(typed, 10))
	case float32:
		value := float64(typed)
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("number must be finite")
		}
		return validateJudgeNumber(strconv.FormatFloat(value, 'g', -1, 32))
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return fmt.Errorf("number must be finite")
		}
		return validateJudgeNumber(strconv.FormatFloat(typed, 'g', -1, 64))
	default:
		return fmt.Errorf("type %T is not an allowed scalar", value)
	}
}

func validateJudgeNumber(value string) error {
	if value == "" || len(value) > maximumJudgeNumberBytes {
		return fmt.Errorf("number text exceeds %d bytes", maximumJudgeNumberBytes)
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return fmt.Errorf("number is invalid")
	}
	return nil
}

func validateJudgeEnvelopeSize(event judgeSourceEvent, maximumBytes int) error {
	if maximumBytes <= 0 || maximumBytes > maximumJudgeEventBytes {
		maximumBytes = maximumJudgeEventBytes
	}
	total := estimatedRedisStreamIDByteLength
	for key, value := range event.redisValues() {
		encoded, ok := value.(string)
		if !ok {
			return fmt.Errorf("field %q is not a string", key)
		}
		total += len(key) + len(encoded)
		if total > maximumBytes {
			return fmt.Errorf("event envelope exceeds %d bytes", maximumBytes)
		}
	}
	return nil
}
