package config

import (
	"encoding/json"
	"testing"

	"go.uber.org/zap"

	"multiple-protocol-controller/pkg/logger"
)

func runtimeTestDevice(id string, protocolName string, propertyType string, protocolJSON string, access string) DeviceConfig {
	dev := DeviceConfig{
		ID:           id,
		SerialNumber: "SN-" + id,
		DeviceName:   id,
		Protocol:     protocolName,
		AcqFreq:      1000,
		Properties: []Property{{
			ID:       "p-" + id,
			Type:     propertyType,
			Key:      "value",
			Name:     "值",
			Access:   access,
			Protocol: json.RawMessage(protocolJSON),
		}},
	}
	dev.GatewayInfo.ID = "gw-" + id
	dev.GatewayInfo.IP = "127.0.0.1"
	dev.GatewayInfo.Port = 1502
	dev.GatewayInfo.DeviceName = "gw"
	return dev
}

func buildRuntimeForTest(t *testing.T, dev DeviceConfig) DeviceRuntime {
	t.Helper()
	// 解析失败路径会写日志；单元测试不依赖全局日志初始化。
	previous := logger.Log
	logger.Log = zap.NewNop()
	t.Cleanup(func() { logger.Log = previous })
	rt, err := BuildRuntimeConfig([]DeviceConfig{dev})
	if err != nil {
		t.Fatalf("BuildRuntimeConfig: %v", err)
	}
	if len(rt.Devices) != 1 {
		t.Fatalf("devices = %d, want 1 (property parse may have failed)", len(rt.Devices))
	}
	return rt.Devices[0]
}

func TestModbusRuntimeUsesIndependentReadWriteDataTypes(t *testing.T) {
	dev := runtimeTestDevice("m1", "modbus_rtu", "float", `{
		"type":"modbus_rtu",
		"points":{
			"read":[{"functionCode":"03","address":"0","quantity":1,"dataType":"uint16"}],
			"write":[{"functionCode":"16","address":"1","quantity":2,"dataType":"int32"}]
		}
	}`, "11")
	rt := buildRuntimeForTest(t, dev)

	if len(rt.ReadPoints) != 1 || len(rt.WritePoints) != 1 {
		t.Fatalf("read/write points = %d/%d", len(rt.ReadPoints), len(rt.WritePoints))
	}
	if got := rt.ReadPoints[0].DataType; got != "uint16" {
		t.Fatalf("read DataType = %q, want uint16", got)
	}
	if got := rt.WritePoints[0].DataType; got != "int32" {
		t.Fatalf("write DataType = %q, want int32", got)
	}
	if got := rt.ReadPoints[0].PropertyType; got != "float" {
		t.Fatalf("read PropertyType = %q, want business type float", got)
	}
	if !rt.ReadPoints[0].DataTypeExplicit || !rt.WritePoints[0].DataTypeExplicit {
		t.Fatalf("explicit point data types must be marked, got read=%v write=%v", rt.ReadPoints[0].DataTypeExplicit, rt.WritePoints[0].DataTypeExplicit)
	}
	if got := rt.WritePoints[0].PropertyType; got != "float" {
		t.Fatalf("write PropertyType = %q, want business type float", got)
	}
	if got := rt.ReadPoints[0].DataType; got == rt.WritePoints[0].DataType {
		t.Fatalf("read and write data types must stay independent")
	}
}

func TestRuntimeFallsBackToPropertyTypeWhenDataTypeMissing(t *testing.T) {
	dev := runtimeTestDevice("m2", "modbus_rtu", "float", `{
		"type":"modbus_rtu",
		"points":{
			"read":[{"functionCode":"03","address":"0"}],
			"write":[{"functionCode":"06","address":"1"}]
		}
	}`, "11")
	rt := buildRuntimeForTest(t, dev)

	if got := rt.ReadPoints[0].DataType; got != "float" {
		t.Fatalf("read fallback DataType = %q, want property type float", got)
	}
	if got := rt.ReadPoints[0].PropertyType; got != "float" {
		t.Fatalf("read PropertyType = %q", got)
	}
	if got := rt.WritePoints[0].DataType; got != "float" {
		t.Fatalf("write fallback DataType = %q, want property type float", got)
	}
	if rt.ReadPoints[0].DataTypeExplicit || rt.WritePoints[0].DataTypeExplicit {
		t.Fatalf("fallback data types must not be marked explicit")
	}
}

func TestRuntimeTreatsNullAndEmptyDataTypeAsMissing(t *testing.T) {
	dev := runtimeTestDevice("m2b", "modbus_rtu", "uint16", `{
		"type":"modbus_rtu",
		"points":{
			"read":[{"functionCode":"03","address":"0","dataType":null},{"functionCode":"03","address":"1","dataType":"  "}],
			"write":[{"functionCode":"06","address":"2","dataType":null}]
		}
	}`, "11")
	rt := buildRuntimeForTest(t, dev)

	for index, param := range rt.ReadPoints {
		if param.DataType != "uint16" || param.DataTypeExplicit {
			t.Fatalf("read point %d = %q explicit=%v, want fallback uint16", index, param.DataType, param.DataTypeExplicit)
		}
	}
	if got := rt.WritePoints[0].DataType; got != "uint16" {
		t.Fatalf("write fallback DataType = %q, want uint16", got)
	}
}

func TestRuntimeOnlyOverridesTheSideThatHasDataType(t *testing.T) {
	dev := runtimeTestDevice("m2c", "modbus_rtu", "int16", `{
		"type":"modbus_rtu",
		"points":{
			"read":[{"functionCode":"03","address":"0","dataType":"uint32","quantity":2}],
			"write":[{"functionCode":"06","address":"1"}]
		}
	}`, "11")
	rt := buildRuntimeForTest(t, dev)

	if got := rt.ReadPoints[0].DataType; got != "uint32" || !rt.ReadPoints[0].DataTypeExplicit {
		t.Fatalf("read = %q explicit=%v, want explicit uint32", got, rt.ReadPoints[0].DataTypeExplicit)
	}
	if got := rt.WritePoints[0].DataType; got != "int16" || rt.WritePoints[0].DataTypeExplicit {
		t.Fatalf("write = %q explicit=%v, want fallback int16", got, rt.WritePoints[0].DataTypeExplicit)
	}
}

func TestRuntimeKeepsMultiplePointsIndependent(t *testing.T) {
	dev := runtimeTestDevice("m2d", "modbus_rtu", "float", `{
		"type":"modbus_rtu",
		"points":{
			"read":[
				{"functionCode":"03","address":"0","quantity":1,"dataType":"uint16"},
				{"functionCode":"03","address":"1","quantity":2,"dataType":"float32"},
				{"functionCode":"03","address":"3","dataType":"int64","quantity":4}
			],
			"write":[
				{"functionCode":"16","address":"10","quantity":2,"dataType":"int32"},
				{"functionCode":"05","address":"20","dataType":"bool"}
			]
		}
	}`, "11")
	rt := buildRuntimeForTest(t, dev)

	wantRead := []string{"uint16", "float32", "int64"}
	for index, want := range wantRead {
		if got := rt.ReadPoints[index].DataType; got != want {
			t.Fatalf("read point %d = %q, want %q", index, got, want)
		}
		if got := rt.ReadPoints[index].PropertyType; got != "float" {
			t.Fatalf("read point %d PropertyType = %q", index, got)
		}
	}
	wantWrite := []string{"int32", "bool"}
	for index, want := range wantWrite {
		if got := rt.WritePoints[index].DataType; got != want {
			t.Fatalf("write point %d = %q, want %q", index, got, want)
		}
	}
}

func TestOpcuaBacnetMqttRuntimeUsePointDataTypes(t *testing.T) {
	cases := []struct {
		name     string
		protocol string
		propType string
		raw      string
		wantRead string
		wantWrit string
	}{
		{
			name:     "opcua",
			protocol: "opcua",
			propType: "int",
			raw:      `{"type":"opcua","nodes":{"read":[{"nodeId":"ns=2;s=A","dataType":"uint64"}],"write":[{"nodeId":"ns=2;s=A","dataType":"int16"}]}}`,
			wantRead: "uint64",
			wantWrit: "int16",
		},
		{
			name:     "bacnet",
			protocol: "bacnet",
			propType: "float",
			raw:      `{"type":"bacnet","objects":{"read":[{"objectType":"analogInput","instance":1,"propertyId":"presentValue","dataType":"float32"}],"write":[{"objectType":"analogOutput","instance":1,"propertyId":"presentValue","dataType":"float64"}]}}`,
			wantRead: "float32",
			wantWrit: "float64",
		},
		{
			name:     "mqtt",
			protocol: "mqtt",
			propType: "string",
			raw:      `{"type":"mqtt","topics":{"subscribe":[{"topic":"dev/a","dataType":"int64"}],"publish":[{"topic":"dev/b","dataType":"string"}]}}`,
			wantRead: "int64",
			wantWrit: "string",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dev := runtimeTestDevice(tc.name, tc.protocol, tc.propType, tc.raw, "11")
			rt := buildRuntimeForTest(t, dev)

			var gotRead, gotWrite, gotReadPropertyType string
			switch tc.name {
			case "opcua":
				gotRead, gotWrite = rt.OpcuaReadPoints[0].DataType, rt.OpcuaWritePoints[0].DataType
				gotReadPropertyType = rt.OpcuaReadPoints[0].PropertyType
			case "bacnet":
				gotRead, gotWrite = rt.BacnetReadPoints[0].DataType, rt.BacnetWritePoints[0].DataType
				gotReadPropertyType = rt.BacnetReadPoints[0].PropertyType
			case "mqtt":
				gotRead, gotWrite = rt.MqttReadPoints[0].DataType, rt.MqttWritePoints[0].DataType
				gotReadPropertyType = rt.MqttReadPoints[0].PropertyType
			}
			if gotRead != tc.wantRead || gotWrite != tc.wantWrit {
				t.Fatalf("data types = %q/%q, want %q/%q", gotRead, gotWrite, tc.wantRead, tc.wantWrit)
			}
			if gotReadPropertyType != tc.propType {
				t.Fatalf("PropertyType = %q, want %q", gotReadPropertyType, tc.propType)
			}
		})
	}
}

func TestAllProtocolsFallbackForMissingOrNullDataType(t *testing.T) {
	cases := []struct {
		name     string
		protocol string
		raw      string
	}{
		{
			name:     "opcua",
			protocol: "opcua",
			raw:      `{"type":"opcua","nodes":{"read":[{"nodeId":"ns=2;s=A","dataType":null}],"write":[{"nodeId":"ns=2;s=A"}]}}`,
		},
		{
			name:     "bacnet",
			protocol: "bacnet",
			raw:      `{"type":"bacnet","objects":{"read":[{"objectType":"analogInput","instance":1,"propertyId":"presentValue"}],"write":[{"objectType":"analogOutput","instance":1,"propertyId":"presentValue","dataType":null}]}}`,
		},
		{
			name:     "mqtt",
			protocol: "mqtt",
			raw:      `{"type":"mqtt","topics":{"subscribe":[{"topic":"dev/a","dataType":null}],"publish":[{"topic":"dev/b"}]}}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dev := runtimeTestDevice(tc.name, tc.protocol, "string", tc.raw, "11")
			rt := buildRuntimeForTest(t, dev)

			var read, write string
			var readExplicit, writeExplicit bool
			switch tc.name {
			case "opcua":
				read, write = rt.OpcuaReadPoints[0].DataType, rt.OpcuaWritePoints[0].DataType
				readExplicit, writeExplicit = rt.OpcuaReadPoints[0].DataTypeExplicit, rt.OpcuaWritePoints[0].DataTypeExplicit
			case "bacnet":
				read, write = rt.BacnetReadPoints[0].DataType, rt.BacnetWritePoints[0].DataType
				readExplicit, writeExplicit = rt.BacnetReadPoints[0].DataTypeExplicit, rt.BacnetWritePoints[0].DataTypeExplicit
			case "mqtt":
				read, write = rt.MqttReadPoints[0].DataType, rt.MqttWritePoints[0].DataType
				readExplicit, writeExplicit = rt.MqttReadPoints[0].DataTypeExplicit, rt.MqttWritePoints[0].DataTypeExplicit
			}
			if read != "string" || write != "string" || readExplicit || writeExplicit {
				t.Fatalf("fallback = %q/%q explicit=%v/%v, want property string", read, write, readExplicit, writeExplicit)
			}
		})
	}
}

func TestRuntimeSkipsPropertyWithUnsupportedExplicitDataType(t *testing.T) {
	dev := runtimeTestDevice("m3", "modbus_rtu", "float", `{
		"type":"modbus_rtu",
		"points":{
			"read":[{"functionCode":"03","address":"0","dataType":"int8"}],
			"write":[]
		}
	}`, "10")
	rt := buildRuntimeForTest(t, dev)
	if len(rt.ReadPoints) != 0 {
		t.Fatalf("unsupported explicit dataType must not reach runtime, got %#v", rt.ReadPoints)
	}
}
