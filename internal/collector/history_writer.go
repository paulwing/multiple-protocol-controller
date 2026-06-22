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
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const (
	deviceHistoryMeasurement = "device_history"
	defaultHistoryBatchSize  = 100
	defaultHistoryFlushMS    = 500
	defaultHistoryQueueSize  = 10000
	defaultHistoryRetryCount = 3
	defaultHistoryRetryMS    = 200
)

type historyWriter struct {
	enabled       bool
	url           string
	token         string
	org           string
	bucket        string
	client        *http.Client
	ctx           context.Context
	cancel        context.CancelFunc
	queue         chan historyPoint
	wg            sync.WaitGroup
	batchSize     int
	flushInterval time.Duration
	retryCount    int
	retryInterval time.Duration
	dropped       uint64
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

func newHistoryWriter(ctx context.Context, cfg config.InfluxCfg) *historyWriter {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	enabled := cfg.Enabled &&
		strings.TrimSpace(cfg.URL) != "" &&
		strings.TrimSpace(cfg.Token) != "" &&
		strings.TrimSpace(cfg.Org) != "" &&
		strings.TrimSpace(cfg.Bucket) != ""

	if ctx == nil {
		ctx = context.Background()
	}
	writerCtx, cancel := context.WithCancel(ctx)

	w := &historyWriter{
		enabled:       enabled,
		url:           strings.TrimRight(strings.TrimSpace(cfg.URL), "/"),
		token:         strings.TrimSpace(cfg.Token),
		org:           strings.TrimSpace(cfg.Org),
		bucket:        strings.TrimSpace(cfg.Bucket),
		client:        &http.Client{Timeout: timeout},
		ctx:           writerCtx,
		cancel:        cancel,
		batchSize:     positiveOrDefault(cfg.BatchSize, defaultHistoryBatchSize),
		flushInterval: time.Duration(positiveOrDefault(cfg.FlushIntervalMS, defaultHistoryFlushMS)) * time.Millisecond,
		retryCount:    positiveOrDefault(cfg.RetryCount, defaultHistoryRetryCount),
		retryInterval: time.Duration(positiveOrDefault(cfg.RetryIntervalMS, defaultHistoryRetryMS)) * time.Millisecond,
	}
	if enabled {
		w.queue = make(chan historyPoint, positiveOrDefault(cfg.QueueSize, defaultHistoryQueueSize))
		w.wg.Add(1)
		go w.run()
	}
	return w
}

func (w *historyWriter) writeAsync(point historyPoint) {
	if w == nil || !w.enabled {
		return
	}
	select {
	case <-w.ctx.Done():
		return
	case w.queue <- point:
	default:
		dropped := atomic.AddUint64(&w.dropped, 1)
		if dropped == 1 || dropped%1000 == 0 {
			logger.Log.Warn("drop device history because queue is full",
				zap.Uint64("dropped", dropped),
				zap.String("deviceID", point.DeviceID),
				zap.String("propertyKey", point.PropertyKey))
		}
	}
}

func (w *historyWriter) write(ctx context.Context, point historyPoint) error {
	return w.writeBatch(ctx, []historyPoint{point})
}

func (w *historyWriter) stop() {
	if w == nil || !w.enabled {
		return
	}
	w.cancel()
	w.wg.Wait()
}

func (w *historyWriter) run() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	batch := make([]historyPoint, 0, w.batchSize)
	flush := func(ctx context.Context) {
		if len(batch) == 0 {
			return
		}
		w.writeBatchWithRetry(ctx, batch)
		batch = batch[:0]
	}

	for {
		select {
		case point := <-w.queue:
			batch = append(batch, point)
			if len(batch) >= w.batchSize {
				flush(context.Background())
			}
		case <-ticker.C:
			flush(context.Background())
		case <-w.ctx.Done():
			for {
				select {
				case point := <-w.queue:
					batch = append(batch, point)
				default:
					flush(context.Background())
					return
				}
			}
		}
	}
}

func (w *historyWriter) writeBatchWithRetry(ctx context.Context, batch []historyPoint) {
	attempts := w.retryCount + 1
	if attempts <= 0 {
		attempts = 1
	}
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		err = w.writeBatch(ctx, batch)
		if err == nil {
			return
		}
		if attempt >= attempts {
			break
		}
		select {
		case <-ctx.Done():
			logger.Log.Warn("write device history canceled",
				zap.Int("batchSize", len(batch)),
				zap.Error(err))
			return
		case <-time.After(w.retryInterval):
		}
	}

	logger.Log.Warn("write device history batch failed",
		zap.Int("batchSize", len(batch)),
		zap.Int("attempts", attempts),
		zap.Error(err))
}

func (w *historyWriter) writeBatch(ctx context.Context, batch []historyPoint) error {
	if len(batch) == 0 {
		return nil
	}

	lines := make([]string, 0, len(batch))
	for _, point := range batch {
		line, err := buildHistoryLineProtocol(point)
		if err != nil {
			return err
		}
		lines = append(lines, line)
	}

	body := strings.Join(lines, "\n")

	endpoint, err := url.Parse(w.url + "/api/v2/write")
	if err != nil {
		return fmt.Errorf("parse influx write url: %w", err)
	}
	query := endpoint.Query()
	query.Set("org", w.org)
	query.Set("bucket", w.bucket)
	query.Set("precision", "ns")
	endpoint.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewBufferString(body))
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

func positiveOrDefault(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
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
		point.Timestamp.UnixNano(),
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
