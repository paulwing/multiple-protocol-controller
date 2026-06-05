package collector

import (
	"strings"
	"testing"
	"time"
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
	if !strings.HasSuffix(line, "1770000000123") {
		t.Fatalf("expected millisecond timestamp, got %q", line)
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
