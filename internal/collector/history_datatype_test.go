package collector

import (
	"strings"
	"testing"
	"time"

	"multiple-protocol-controller/internal/config"
)

func TestHistoryKeepsPropertyBusinessTypeWhenWireTypeIsExplicit(t *testing.T) {
	device := config.DeviceRuntime{Config: config.DeviceConfig{ID: "d1", SerialNumber: "SN-1", Protocol: "modbus_rtu"}}

	points := []historyPoint{
		historyPointFromModbus(device, config.ModbusParam{
			DeviceID: "d1", SerialNumber: "SN-1", Identify: "temperature", Name: "温度",
			PropertyType: "float", DataType: "float32",
		}, float32(21.5)),
		historyPointFromOpcua(device, config.OpcuaParam{
			DeviceID: "d1", SerialNumber: "SN-1", Identify: "temperature", Name: "温度",
			PropertyType: "float", DataType: "float64",
		}, float64(21.5)),
		historyPointFromBacnet(device, config.BacnetParam{
			DeviceID: "d1", SerialNumber: "SN-1", Identify: "temperature", Name: "温度",
			PropertyType: "float", DataType: "float32",
		}, float32(21.5)),
		historyPointFromMqtt(device, config.MqttParam{
			DeviceID: "d1", SerialNumber: "SN-1", Identify: "temperature", Name: "温度",
			PropertyType: "float", DataType: "int64",
		}, int64(21)),
	}

	for _, point := range points {
		if point.DataType != "float" {
			t.Fatalf("history data_type = %q, want business type float (wire type only decodes)", point.DataType)
		}
		line, err := buildHistoryLineProtocol(point)
		if err != nil {
			t.Fatalf("buildHistoryLineProtocol: %v", err)
		}
		if !strings.Contains(line, "data_type=float") || strings.Contains(line, "data_type=float32") || strings.Contains(line, "data_type=int64") {
			t.Fatalf("history line must tag property type, got %q", line)
		}
	}
}

func TestHistoryFallsBackToEffectiveTypeForDirectlyConstructedPoints(t *testing.T) {
	point := historyPointFromModbus(
		config.DeviceRuntime{Config: config.DeviceConfig{ID: "d1", SerialNumber: "SN-1"}},
		config.ModbusParam{Identify: "temperature", PropertyType: "", DataType: "float32"},
		float32(1),
	)
	if point.DataType != "float32" {
		t.Fatalf("data_type = %q, want effective type fallback", point.DataType)
	}
	if point.Timestamp.IsZero() || time.Since(point.Timestamp) > time.Minute {
		t.Fatalf("history timestamp not set: %v", point.Timestamp)
	}
}
