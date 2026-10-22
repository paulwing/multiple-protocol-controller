package control

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"multiple-protocol-controller/internal/config"
)

// The producer is backend's TestDatatypePersistenceToPublishedSnapshotMySQL.
// Run that test successfully with a fresh temporary output before this consumer.
// This is the exact publisher envelope after real MySQL persistence, not an
// HTTP response reshaped into a DeviceConfig and not an independent fixture.
func TestPersistedBackendSnapshotBuildsRuntimeTypes(t *testing.T) {
	path := os.Getenv("DATATYPE_PERSISTENCE_CONTRACT_FILE")
	if path == "" {
		t.Skip("DATATYPE_PERSISTENCE_CONTRACT_FILE unset; MySQL-backed publisher artifact not supplied")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var envelope config.RedisWrapper
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Key != config.DeviceCfg || envelope.Version == "" {
		t.Fatalf("unexpected publisher envelope: %+v", envelope)
	}
	var devices []config.DeviceConfig
	if err := json.Unmarshal(envelope.Data, &devices); err != nil {
		t.Fatal(err)
	}
	wants := map[string]struct {
		protocol string
		read     []string
		write    []string
		explicit bool
	}{
		"SN-modbus-fc06-registers": {"modbus_rtu", []string{"uint16"}, []string{"int16", "uint16"}, true},
		"SN-bacnet-explicit":       {"bacnet", []string{"int32"}, []string{"float64"}, true},
		"SN-opcua-explicit":        {"opcua", []string{"int64"}, []string{"bool"}, true},
		"SN-mqtt-explicit":         {"mqtt", []string{"float64"}, []string{"string"}, true},
		"SN-opcua-legacy":          {"opcua", []string{"float"}, []string{"float"}, false},
	}
	if len(devices) != len(wants) {
		t.Fatalf("published device count=%d want=%d", len(devices), len(wants))
	}
	runtime, err := config.BuildRuntimeConfig(devices)
	if err != nil {
		t.Fatal(err)
	}
	if len(runtime.DeviceBySerial) != len(wants) {
		t.Fatalf("runtime lost a persisted device: %d", len(runtime.DeviceBySerial))
	}
	seen := map[string]bool{}
	for _, device := range devices {
		t.Run(device.SerialNumber, func(t *testing.T) {
			want, ok := wants[device.SerialNumber]
			if !ok || seen[device.SerialNumber] {
				t.Fatalf("unexpected or duplicate published serial: %s", device.SerialNumber)
			}
			seen[device.SerialNumber] = true
			rt, ok := runtime.DeviceBySerial[device.SerialNumber]
			if !ok || rt.Config.ID != device.ID || device.ProductId == "" || device.Protocol != want.protocol {
				t.Fatalf("published identity/protocol lost: %+v", rt)
			}
			if device.DeviceType != 2 || rt.GatewayID != device.ID || rt.Config.GatewayInfo.IP != "127.0.0.1" || rt.Config.GatewayInfo.Port != 502 {
				t.Fatalf("published self-gateway fields did not survive loading: %+v", rt)
			}
			// MQTT stores its endpoint as MqttBroker; the other protocols keep
			// GatewayIP/GatewayPort. Preserve this legitimate internal difference.
			if want.protocol == "mqtt" {
				if rt.MqttBroker != "tcp://127.0.0.1:502" {
					t.Fatalf("persisted MQTT endpoint changed: %s", rt.MqttBroker)
				}
			} else if rt.GatewayIP != "127.0.0.1" || rt.GatewayPort != 502 {
				t.Fatalf("persisted endpoint changed: %s:%d", rt.GatewayIP, rt.GatewayPort)
			}
			var read, write []string
			collect := func(target *[]string, dataType, propertyType string, explicit bool) {
				t.Helper()
				if propertyType != "float" || explicit != want.explicit {
					t.Fatalf("business/explicit type changed: %s/%v want float/%v", propertyType, explicit, want.explicit)
				}
				*target = append(*target, dataType)
			}
			switch want.protocol {
			case "modbus_rtu":
				if rt.SlaveID != 1 {
					t.Fatalf("persisted slaveID changed: %d", rt.SlaveID)
				}
				for _, point := range rt.ReadPoints {
					collect(&read, point.DataType, point.PropertyType, point.DataTypeExplicit)
				}
				for _, command := range runtime.DeviceCmdBySerial[device.SerialNumber] {
					collect(&write, command.DataType, command.PropertyType, command.DataTypeExplicit)
					if command.FunctionCode != 6 || command.Quantity != 1 {
						t.Fatalf("FC06 write meaning changed: %+v", command)
					}
				}
			case "bacnet":
				for _, point := range rt.BacnetReadPoints {
					collect(&read, point.DataType, point.PropertyType, point.DataTypeExplicit)
				}
				for _, command := range rt.BacnetWritePoints {
					collect(&write, command.DataType, command.PropertyType, command.DataTypeExplicit)
				}
			case "opcua":
				for _, point := range rt.OpcuaReadPoints {
					collect(&read, point.DataType, point.PropertyType, point.DataTypeExplicit)
				}
				for _, command := range rt.OpcuaWritePoints {
					collect(&write, command.DataType, command.PropertyType, command.DataTypeExplicit)
				}
			case "mqtt":
				for _, point := range rt.MqttReadPoints {
					collect(&read, point.DataType, point.PropertyType, point.DataTypeExplicit)
				}
				for _, command := range rt.MqttWritePoints {
					collect(&write, command.DataType, command.PropertyType, command.DataTypeExplicit)
				}
			}
			if !reflect.DeepEqual(read, want.read) || !reflect.DeepEqual(write, want.write) {
				t.Fatalf("runtime point types=%v/%v want=%v/%v", read, write, want.read, want.write)
			}
		})
	}
}
