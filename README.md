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

### Jenkins Image Package

`scripts/package-image.sh` is the Jenkins image packaging script template. It supports amd64 and arm64 by environment variables.

```sh
# amd64
ARCH_NAME=linux-amd64 IMAGE_PLATFORM=linux/amd64 ./scripts/package-image.sh

# arm64
ARCH_NAME=linux-arm64 IMAGE_PLATFORM=linux/arm64 ./scripts/package-image.sh

# arm64 on amd64 Jenkins with buildx
ARCH_NAME=linux-arm64 IMAGE_PLATFORM=linux/arm64 USE_BUILDX=true ./scripts/package-image.sh
```

Output examples:

```text
new_mpc-basic_sys-linux-amd64.tar
new_mpc-basic_sys-linux-arm64.tar
```

Generated app image packages are uploaded to:

```text
NewFramework/apps/${ARCH_NAME}/
```

The script loads `alpine:3.23.4` from `NewFramework/base-images/${ARCH_NAME}/` when the local base image architecture does not match `ARCH_NAME`.

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

### History Data

MPC can write collected property values to InfluxDB for historical queries.

Configuration:

```toml
[influx]
enabled = false
url = "http://127.0.0.1:8086"
token = ""
org = "iot"
bucket = "device_history"
timeout_seconds = 3
```

Environment overrides:

```sh
INFLUXDB_ENABLED=true
INFLUXDB_URL=http://iot-influxdb:8086
INFLUXDB_TOKEN=iot-influxdb-local-token
INFLUXDB_ORG=iot
INFLUXDB_BUCKET=device_history
INFLUXDB_TIMEOUT_SECONDS=3
```

When enabled, each successful realtime Redis write also writes one `device_history` point to InfluxDB. If InfluxDB is disabled or incomplete, MPC only writes Redis realtime data and skips history writes.

InfluxDB tags:

```text
device_id, property_key, serial_number, property_id, property_name, protocol, unit, data_type
```

InfluxDB fields:

```text
value_number, value_bool, value_string
```
