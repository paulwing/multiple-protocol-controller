package collector

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
)

func TestBuildHistoryLineProtocolFormatsNumberBoolAndString(t *testing.T) {
	point := historyPoint{
		DeviceID:     "device 1",
		SerialNumber: "SN,001",
		PropertyKey:  "temp",
		PropertyID:   "property=1",
		PropertyName: "温度",
		Protocol:     "modbus_rtu",
		Unit:         "℃",
		DataType:     "float",
		Value:        36.8,
		Timestamp:    time.UnixMilli(1770000000123),
	}

	line, err := buildHistoryLineProtocol(point)
	if err != nil {
		t.Fatalf("buildHistoryLineProtocol returned error: %v", err)
	}

	if !strings.Contains(line, "device_history,") {
		t.Fatalf("expected measurement, got %q", line)
	}
	if !strings.Contains(line, `device_id=device\ 1`) {
		t.Fatalf("expected escaped device id tag, got %q", line)
	}
	if !strings.Contains(line, `serial_number=SN\,001`) {
		t.Fatalf("expected escaped serial tag, got %q", line)
	}
	if !strings.Contains(line, `property_id=property\=1`) {
		t.Fatalf("expected escaped property id tag, got %q", line)
	}
	if !strings.Contains(line, " value_number=36.8 ") {
		t.Fatalf("expected numeric field, got %q", line)
	}
	if !strings.HasSuffix(line, "1770000000123000000") {
		t.Fatalf("expected nanosecond timestamp, got %q", line)
	}

	boolLine, err := buildHistoryLineProtocol(historyPoint{
		DeviceID:     "device-1",
		SerialNumber: "SN001",
		PropertyKey:  "enabled",
		Value:        true,
		Timestamp:    time.UnixMilli(1770000000456),
	})
	if err != nil {
		t.Fatalf("build bool line returned error: %v", err)
	}
	if !strings.Contains(boolLine, " value_bool=true ") {
		t.Fatalf("expected bool field, got %q", boolLine)
	}

	stringLine, err := buildHistoryLineProtocol(historyPoint{
		DeviceID:     "device-1",
		SerialNumber: "SN001",
		PropertyKey:  "status",
		Value:        `a"b`,
		Timestamp:    time.UnixMilli(1770000000789),
	})
	if err != nil {
		t.Fatalf("build string line returned error: %v", err)
	}
	if !strings.Contains(stringLine, ` value_string="a\"b" `) {
		t.Fatalf("expected string field, got %q", stringLine)
	}
}

func TestHistoryWriterRetriesFailedBatchWrite(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&attempts, 1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	writer := newHistoryWriter(context.Background(), config.InfluxCfg{
		Enabled:         true,
		URL:             server.URL,
		Token:           "token",
		Org:             "iot",
		Bucket:          "device_history",
		TimeoutSeconds:  1,
		BatchSize:       10,
		FlushIntervalMS: 1000,
		QueueSize:       10,
		RetryCount:      1,
		RetryIntervalMS: 1,
	})
	defer writer.stop()

	writer.writeBatchWithRetry(context.Background(), []historyPoint{
		{
			DeviceID:    "device-1",
			PropertyKey: "temp",
			Value:       36.8,
			Timestamp:   time.UnixMilli(1770000000123),
		},
	})

	if got := atomic.LoadInt32(&attempts); got != 2 {
		t.Fatalf("expected 2 attempts, got %d", got)
	}
}

func TestHistoryWriterWriteBatchPostsMultipleLines(t *testing.T) {
	var requestPath string
	var requestBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPath = r.URL.String()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		requestBody = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	writer := newHistoryWriter(context.Background(), config.InfluxCfg{
		Enabled:         true,
		URL:             server.URL,
		Token:           "token",
		Org:             "iot",
		Bucket:          "device_history",
		TimeoutSeconds:  1,
		BatchSize:       10,
		FlushIntervalMS: 1000,
		QueueSize:       10,
		RetryCount:      0,
		RetryIntervalMS: 1,
	})
	defer writer.stop()

	err := writer.writeBatch(context.Background(), []historyPoint{
		{
			DeviceID:    "device-1",
			PropertyKey: "temp",
			Value:       36.8,
			Timestamp:   time.UnixMilli(1770000000123),
		},
		{
			DeviceID:    "device-1",
			PropertyKey: "enabled",
			Value:       true,
			Timestamp:   time.UnixMilli(1770000000456),
		},
	})
	if err != nil {
		t.Fatalf("writeBatch returned error: %v", err)
	}

	if !strings.Contains(requestPath, "precision=ns") {
		t.Fatalf("expected ns precision in request path, got %q", requestPath)
	}
	lines := strings.Split(strings.TrimSpace(requestBody), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %q", len(lines), requestBody)
	}
	if !strings.Contains(lines[0], "value_number=36.8") || !strings.Contains(lines[1], "value_bool=true") {
		t.Fatalf("unexpected request body: %q", requestBody)
	}
}

func TestBuildHistoryLineProtocolRejectsMissingRequiredTags(t *testing.T) {
	_, err := buildHistoryLineProtocol(historyPoint{
		DeviceID:    "device-1",
		PropertyKey: "",
		Value:       1,
		Timestamp:   time.UnixMilli(1770000000123),
	})
	if err == nil {
		t.Fatalf("expected missing property key error")
	}
}
