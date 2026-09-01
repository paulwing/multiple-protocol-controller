package config

import (
	"encoding/json"
	"testing"
)

func TestBuildDeviceRuntimeParsesParamListSlaveIDAndTimeout(t *testing.T) {
	dev := DeviceConfig{
		ID:           "device-1",
		SerialNumber: "serial-1",
		Protocol:     "MODBUS_RTU",
		Params: []DeviceParam{
			{Label: "从机ID", Key: "slaveID", Type: "string", Value: rawParamValue(`"7"`)},
			{Label: "超时时间", Key: "timeout", Type: "int", Value: rawParamValue(`3000`)},
		},
	}
	dev.GatewayInfo.IP = "127.0.0.1"
	dev.GatewayInfo.Port = 502

	rt, err := buildDeviceRuntime(dev)
	if err != nil {
		t.Fatalf("buildDeviceRuntime() error = %v", err)
	}
	if rt.SlaveID != 7 {
		t.Fatalf("SlaveID = %d, want 7", rt.SlaveID)
	}
	if rt.ResponseTimeoutMs != 3000 {
		t.Fatalf("ResponseTimeoutMs = %d, want 3000", rt.ResponseTimeoutMs)
	}
}

func TestParamListParsesProtocolSpecificDeviceParams(t *testing.T) {
	bacnet := DeviceConfig{
		Params: []DeviceParam{{Key: "deviceInstance", Type: "int", Value: rawParamValue(`12345`)}},
	}
	if got := parseBacnetDeviceInstance(bacnet); got != 12345 {
		t.Fatalf("parseBacnetDeviceInstance() = %d, want 12345", got)
	}

	policy, mode := parseOpcuaSecurity([]DeviceParam{
		{Key: "securityPolicy", Type: "string", Value: rawParamValue(`"Basic256Sha256"`)},
		{Key: "securityMode", Type: "string", Value: rawParamValue(`"SignAndEncrypt"`)},
	})
	if policy != "Basic256Sha256" || mode != "SignAndEncrypt" {
		t.Fatalf("parseOpcuaSecurity() = (%q, %q), want (Basic256Sha256, SignAndEncrypt)", policy, mode)
	}

	mqtt := DeviceConfig{
		SerialNumber: "serial-mqtt",
		Username:     "device-user",
		Password:     "device-pass",
		Params: []DeviceParam{
			{Key: "clientId", Type: "string", Value: rawParamValue(`"client-1"`)},
			{Key: "username", Type: "string", Value: rawParamValue(`"mqtt-user"`)},
			{Key: "password", Type: "string", Value: rawParamValue(`"mqtt-pass"`)},
			{Key: "qos", Type: "int", Value: rawParamValue(`2`)},
			{Key: "keepAlive", Type: "int", Value: rawParamValue(`60`)},
			{Key: "cleanSession", Type: "bool", Value: rawParamValue(`false`)},
		},
	}
	clientID, username, password, qos, keepAlive, cleanSession := parseMqttParams(mqtt)
	if clientID != "client-1" || username != "mqtt-user" || password != "mqtt-pass" || qos != 2 || keepAlive != 60 || cleanSession {
		t.Fatalf("parseMqttParams() = (%q, %q, %q, %d, %d, %t), want client-1/mqtt-user/mqtt-pass/2/60/false",
			clientID, username, password, qos, keepAlive, cleanSession)
	}
}

func TestDeviceConfigUnmarshalsTypedParamList(t *testing.T) {
	raw := []byte(`{
		"id": "device-1",
		"params": [
			{"label":"从机ID","key":"slaveID","type":"string","value":"1"},
			{"label":"超时时间","key":"timeout","type":"int","value":3000}
		]
	}`)

	var dev DeviceConfig
	if err := json.Unmarshal(raw, &dev); err != nil {
		t.Fatalf("json.Unmarshal(DeviceConfig) error = %v", err)
	}
	if len(dev.Params) != 2 {
		t.Fatalf("len(Params) = %d, want 2", len(dev.Params))
	}
	if dev.Params[0].Key != "slaveID" || string(dev.Params[0].Value) != `"1"` {
		t.Fatalf("Params[0] = %+v, want slaveID value \"1\"", dev.Params[0])
	}
	if dev.Params[1].Key != "timeout" || string(dev.Params[1].Value) != `3000` {
		t.Fatalf("Params[1] = %+v, want timeout value 3000", dev.Params[1])
	}
}

func TestBuildDeviceRuntimeRejectsWritePointForReadOnlyProperty(t *testing.T) {
	dev := DeviceConfig{
		ID:           "device-1",
		SerialNumber: "serial-1",
		Protocol:     "MODBUS_RTU",
		Properties: []Property{{
			ID:     "property-1",
			Key:    "switch",
			Name:   "开关",
			Type:   "bool",
			Access: "10",
			Protocol: json.RawMessage(`{
				"type":"MODBUS_RTU",
				"points":{
					"read":[{"functionCode":"01","address":"0"}],
					"write":[{"functionCode":"05","address":"0"}]
				}
			}`),
		}},
		Params: []DeviceParam{{Key: "slaveID", Type: "string", Value: rawParamValue(`"1"`)}},
	}
	dev.GatewayInfo.IP = "127.0.0.1"
	dev.GatewayInfo.Port = 502

	runtime, err := buildDeviceRuntime(dev)
	if err != nil {
		t.Fatalf("buildDeviceRuntime() error = %v", err)
	}
	if len(runtime.WritePoints) != 0 {
		t.Fatalf("len(WritePoints) = %d, want 0 for read-only property", len(runtime.WritePoints))
	}
}

func TestBuildDeviceRuntimeKeepsWritePointForWritableProperty(t *testing.T) {
	dev := DeviceConfig{
		ID:           "device-1",
		SerialNumber: "serial-1",
		Protocol:     "MODBUS_RTU",
		Properties: []Property{{
			ID:     "property-1",
			Key:    "switch",
			Name:   "开关",
			Type:   "bool",
			Access: "01",
			Protocol: json.RawMessage(`{
				"type":"MODBUS_RTU",
				"points":{
					"read":[],
					"write":[{"functionCode":"05","address":"0"}]
				}
			}`),
		}},
		Params: []DeviceParam{{Key: "slaveID", Type: "string", Value: rawParamValue(`"1"`)}},
	}
	dev.GatewayInfo.IP = "127.0.0.1"
	dev.GatewayInfo.Port = 502

	runtime, err := buildDeviceRuntime(dev)
	if err != nil {
		t.Fatalf("buildDeviceRuntime() error = %v", err)
	}
	if len(runtime.WritePoints) != 1 {
		t.Fatalf("len(WritePoints) = %d, want 1 for write-only property", len(runtime.WritePoints))
	}
}

func TestReadOnlyPropertyNeverProducesNonModbusWriteCommand(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		config   string
		count    func(DeviceConfig) int
	}{
		{
			name:     "OPC UA",
			protocol: "OPCUA",
			config:   `{"type":"OPCUA","nodes":{"read":[{"nodeId":"ns=2;s=read"}],"write":[{"nodeId":"ns=2;s=write"}]}}`,
			count:    func(dev DeviceConfig) int { _, commands := parseOpcuaPoints(dev); return len(commands) },
		},
		{
			name:     "BACnet",
			protocol: "BACNET",
			config:   `{"type":"BACNET","objects":{"read":[{"objectType":"analogValue","instance":1,"propertyId":"presentValue"}],"write":[{"objectType":"analogValue","instance":1,"propertyId":"presentValue"}]}}`,
			count:    func(dev DeviceConfig) int { _, commands := parseBacnetPoints(dev); return len(commands) },
		},
		{
			name:     "MQTT",
			protocol: "MQTT",
			config:   `{"type":"MQTT","topics":{"subscribe":[{"topic":"device/read","path":"value","qos":1}],"publish":[{"topic":"device/write","path":"value","qos":1}]}}`,
			count:    func(dev DeviceConfig) int { _, commands := parseMqttPoints(dev); return len(commands) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dev := DeviceConfig{
				ID: "device-1", SerialNumber: "serial-1", Protocol: tt.protocol,
				Properties: []Property{{ID: "property-1", Key: "value", Type: "float", Access: "10", Protocol: json.RawMessage(tt.config)}},
			}
			if got := tt.count(dev); got != 0 {
				t.Fatalf("write command count = %d, want 0 for read-only property", got)
			}
		})
	}
}

func rawParamValue(raw string) json.RawMessage {
	return json.RawMessage(raw)
}
