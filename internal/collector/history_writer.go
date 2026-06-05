package collector

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"multiple-protocol-controller/internal/config"
	"multiple-protocol-controller/pkg/logger"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

const deviceHistoryMeasurement = "device_history"

type historyWriter struct {
	enabled bool
	url     string
	token   string
	org     string
	bucket  string
	client  *http.Client
}

type historyPoint struct {
	DeviceID     string
	SerialNumber string
	PropertyKey  string
	PropertyID   string
	PropertyName string
	Protocol     string
	Unit         string
	DataType     string
	Value        interface{}
	Timestamp    time.Time
}

func newHistoryWriter(cfg config.InfluxCfg) *historyWriter {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	enabled := cfg.Enabled &&
		strings.TrimSpace(cfg.URL) != "" &&
		strings.TrimSpace(cfg.Token) != "" &&
		strings.TrimSpace(cfg.Org) != "" &&
		strings.TrimSpace(cfg.Bucket) != ""

	return &historyWriter{
		enabled: enabled,
		url:     strings.TrimRight(strings.TrimSpace(cfg.URL), "/"),
		token:   strings.TrimSpace(cfg.Token),
		org:     strings.TrimSpace(cfg.Org),
		bucket:  strings.TrimSpace(cfg.Bucket),
		client:  &http.Client{Timeout: timeout},
	}
}

func (w *historyWriter) writeAsync(point historyPoint) {
	if w == nil || !w.enabled {
		return
	}
	go func() {
		if err := w.write(context.Background(), point); err != nil {
			logger.Log.Warn("write device history failed",
				zap.String("deviceID", point.DeviceID),
				zap.String("propertyKey", point.PropertyKey),
				zap.Error(err))
		}
	}()
}

func (w *historyWriter) write(ctx context.Context, point historyPoint) error {
	line, err := buildHistoryLineProtocol(point)
	if err != nil {
		return err
	}

	endpoint, err := url.Parse(w.url + "/api/v2/write")
	if err != nil {
		return fmt.Errorf("parse influx write url: %w", err)
	}
	query := endpoint.Query()
	query.Set("org", w.org)
	query.Set("bucket", w.bucket)
	query.Set("precision", "ms")
	endpoint.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewBufferString(line))
	if err != nil {
		return fmt.Errorf("create influx write request: %w", err)
	}
	request.Header.Set("Authorization", "Token "+w.token)
	request.Header.Set("Content-Type", "text/plain; charset=utf-8")

	response, err := w.client.Do(request)
	if err != nil {
		return fmt.Errorf("write influx history: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusNoContent || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("write influx history status: %d", response.StatusCode)
	}
	return nil
}

func buildHistoryLineProtocol(point historyPoint) (string, error) {
	point.DeviceID = strings.TrimSpace(point.DeviceID)
	point.SerialNumber = strings.TrimSpace(point.SerialNumber)
	point.PropertyKey = strings.TrimSpace(point.PropertyKey)
	if point.DeviceID == "" || point.PropertyKey == "" {
		return "", fmt.Errorf("device_id and property_key are required")
	}
	if point.Timestamp.IsZero() {
		point.Timestamp = time.Now()
	}

	tags := []string{
		"device_id=" + escapeInfluxTag(point.DeviceID),
		"property_key=" + escapeInfluxTag(point.PropertyKey),
	}
	for _, tag := range []struct {
		key   string
		value string
	}{
		{key: "serial_number", value: point.SerialNumber},
		{key: "property_id", value: point.PropertyID},
		{key: "property_name", value: point.PropertyName},
		{key: "protocol", value: point.Protocol},
		{key: "unit", value: point.Unit},
		{key: "data_type", value: point.DataType},
	} {
		value := strings.TrimSpace(tag.value)
		if value != "" {
			tags = append(tags, tag.key+"="+escapeInfluxTag(value))
		}
	}

	field, err := buildHistoryField(point.Value)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s,%s %s %d",
		deviceHistoryMeasurement,
		strings.Join(tags, ","),
		field,
		point.Timestamp.UnixMilli(),
	), nil
}

func buildHistoryField(value interface{}) (string, error) {
	switch typed := value.(type) {
	case bool:
		return "value_bool=" + strconv.FormatBool(typed), nil
	case int:
		return "value_number=" + strconv.Itoa(typed), nil
	case int8:
		return "value_number=" + strconv.FormatInt(int64(typed), 10), nil
	case int16:
		return "value_number=" + strconv.FormatInt(int64(typed), 10), nil
	case int32:
		return "value_number=" + strconv.FormatInt(int64(typed), 10), nil
	case int64:
		return "value_number=" + strconv.FormatInt(typed, 10), nil
	case uint:
		return "value_number=" + strconv.FormatUint(uint64(typed), 10), nil
	case uint8:
		return "value_number=" + strconv.FormatUint(uint64(typed), 10), nil
	case uint16:
		return "value_number=" + strconv.FormatUint(uint64(typed), 10), nil
	case uint32:
		return "value_number=" + strconv.FormatUint(uint64(typed), 10), nil
	case uint64:
		return "value_number=" + strconv.FormatUint(typed, 10), nil
	case float32:
		return formatHistoryFloat(float64(typed))
	case float64:
		return formatHistoryFloat(typed)
	case string:
		return `value_string="` + escapeInfluxString(typed) + `"`, nil
	default:
		return `value_string="` + escapeInfluxString(fmt.Sprint(typed)) + `"`, nil
	}
}

func formatHistoryFloat(value float64) (string, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "", fmt.Errorf("invalid numeric history value")
	}
	return "value_number=" + strconv.FormatFloat(value, 'f', -1, 64), nil
}

func escapeInfluxTag(value string) string {
	replacer := strings.NewReplacer(
		`,`, `\,`,
		` `, `\ `,
		`=`, `\=`,
	)
	return replacer.Replace(value)
}

func escapeInfluxString(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`)
	return replacer.Replace(value)
}

func historyPointFromModbus(device config.DeviceRuntime, param config.ModbusParam, value interface{}) historyPoint {
	return historyPoint{
		DeviceID:     fallbackHistoryString(param.DeviceID, device.Config.ID),
		SerialNumber: fallbackHistoryString(param.SerialNumber, device.Config.SerialNumber),
		PropertyKey:  param.Identify,
		PropertyID:   param.PropertyID,
		PropertyName: param.Name,
		Protocol:     device.Config.Protocol,
		Unit:         param.Unit,
		DataType:     param.DataType,
		Value:        value,
		Timestamp:    time.Now(),
	}
}

func historyPointFromOpcua(device config.DeviceRuntime, param config.OpcuaParam, value interface{}) historyPoint {
	return historyPoint{
		DeviceID:     fallbackHistoryString(param.DeviceID, device.Config.ID),
		SerialNumber: fallbackHistoryString(param.SerialNumber, device.Config.SerialNumber),
		PropertyKey:  param.Identify,
		PropertyID:   param.PropertyID,
		PropertyName: param.Name,
		Protocol:     device.Config.Protocol,
		Unit:         param.Unit,
		DataType:     param.DataType,
		Value:        value,
		Timestamp:    time.Now(),
	}
}

func historyPointFromBacnet(device config.DeviceRuntime, param config.BacnetParam, value interface{}) historyPoint {
	return historyPoint{
		DeviceID:     fallbackHistoryString(param.DeviceID, device.Config.ID),
		SerialNumber: fallbackHistoryString(param.SerialNumber, device.Config.SerialNumber),
		PropertyKey:  param.Identify,
		PropertyID:   param.PropertyID,
		PropertyName: param.Name,
		Protocol:     device.Config.Protocol,
		Unit:         param.Unit,
		DataType:     param.DataType,
		Value:        value,
		Timestamp:    time.Now(),
	}
}

func historyPointFromMqtt(device config.DeviceRuntime, param config.MqttParam, value interface{}) historyPoint {
	return historyPoint{
		DeviceID:     fallbackHistoryString(param.DeviceID, device.Config.ID),
		SerialNumber: fallbackHistoryString(param.SerialNumber, device.Config.SerialNumber),
		PropertyKey:  param.Identify,
		PropertyID:   param.PropertyID,
		PropertyName: param.Name,
		Protocol:     device.Config.Protocol,
		Unit:         param.Unit,
		DataType:     param.DataType,
		Value:        value,
		Timestamp:    time.Now(),
	}
}

func fallbackHistoryString(value string, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}
