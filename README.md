## multiple-protocol-controller
### for xlong
### How to run
- local
```
air
```
- production
```
go build -o ./multiple-protocol-controller ./cmd
./multiple-protocol-controller
```

### Device Params

`DeviceConfig.params` stores device-level special parameters as a list. Each item is matched by `key`, and `value` is converted according to the runtime use case.

```json
"params": [
  {
    "label": "从机ID",
    "key": "slaveID",
    "type": "string",
    "value": "1"
  },
  {
    "label": "超时时间",
    "key": "timeout",
    "type": "int",
    "value": 3000
  }
]
```

Current runtime keys:

| key | Purpose | Effect |
| --- | --- | --- |
| `slaveID` / `SlaveID` | Modbus slave ID | Device address in Modbus read/write frames |
| `timeout` / `responseTimeout` / `responseTimeoutMs` | Device response timeout in milliseconds | `DeviceRuntime.ResponseTimeoutMs`, used by collection and control waits |
| `deviceInstance` | BACnet device instance | Target device for BACnet read/write operations |
| `securityPolicy` | OPC UA security policy | OPC UA connection settings |
| `securityMode` | OPC UA security mode | OPC UA connection settings |
| `clientId` | MQTT client ID | MQTT connection |
| `username` | MQTT username, overrides device-level `Username` | MQTT connection |
| `password` | MQTT password, overrides device-level `Password` | MQTT connection |
| `qos` | MQTT default QoS, valid values are `0`-`2` | MQTT subscribe/publish default |
| `keepAlive` | MQTT keep alive in seconds | MQTT connection |
| `cleanSession` | MQTT clean session flag | MQTT connection |

Property-level protocol configuration is not read from `params`. It is read from each property's `protocol` field, such as Modbus function code and address, OPC UA `nodeId`, MQTT topic/path, and BACnet object configuration.
