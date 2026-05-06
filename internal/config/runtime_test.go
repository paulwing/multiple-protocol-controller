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

func rawParamValue(raw string) json.RawMessage {
	return json.RawMessage(raw)
}
