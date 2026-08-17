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

The script loads `alpine:3.23.4-${ARCH_NAME}` from `NewFramework/base-images/${ARCH_NAME}/` and verifies the loaded base image architecture before building.

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
batch_size = 100
flush_interval_ms = 500
queue_size = 10000
retry_count = 3
retry_interval_ms = 200
```

Environment overrides:

```sh
INFLUXDB_ENABLED=true
INFLUXDB_URL=http://iot-influxdb:8086
INFLUXDB_TOKEN=iot-influxdb-local-token
INFLUXDB_ORG=iot
INFLUXDB_BUCKET=device_history
INFLUXDB_TIMEOUT_SECONDS=3
INFLUXDB_BATCH_SIZE=100
INFLUXDB_FLUSH_INTERVAL_MS=500
INFLUXDB_QUEUE_SIZE=10000
INFLUXDB_RETRY_COUNT=3
INFLUXDB_RETRY_INTERVAL_MS=200
```

When enabled, each successful realtime Redis write enqueues one `device_history` point. A fixed background worker writes queued points to InfluxDB in batches. If InfluxDB is disabled or incomplete, MPC only writes Redis realtime data and skips history writes.

Batching behavior:

```text
collected value -> Redis realtime snapshot -> history queue -> batched InfluxDB write
```

The writer flushes when the batch reaches `batch_size` or `flush_interval_ms` elapses. Failed batches are retried up to `retry_count` times with `retry_interval_ms` between attempts. If `queue_size` is full, MPC drops new history points and logs the dropped count; realtime Redis writes are not blocked.

InfluxDB tags:

```text
device_id, property_key, serial_number, property_id, property_name, protocol, unit, data_type
```

InfluxDB fields:

```text
value_number, value_bool, value_string
```

### Judge Rule Events

MPC publishes collected property changes to the Judge Redis Stream. Local configuration remains disabled by default, while `12_new_mpc.sh` enables the already-integrated five-field publisher in production unless `IOT_JUDGE_SOURCE_ENABLED=false` is set explicitly.

MPC and Judge must use the same Redis address and logical database. MPC reads the database from `redis.db` or the `REDIS_DB` environment variable; the production script passes `${IOT_REDIS_DB:-0}` to match the Judge deployment.

```toml
[judge_source]
enabled = false
stream = "judge:source"
write_timeout_ms = 200
retry_count = 1
retry_interval_ms = 20
max_event_bytes = 65536
```

Environment overrides:

```text
JUDGE_SOURCE_ENABLED
JUDGE_SOURCE_STREAM
JUDGE_SOURCE_WRITE_TIMEOUT_MS
JUDGE_SOURCE_RETRY_COUNT
JUDGE_SOURCE_RETRY_INTERVAL_MS
JUDGE_SOURCE_MAX_EVENT_BYTES
REDIS_DB
```

`retry_count` is the number of additional `XADD` attempts after the initial Pipeline attempt. Runtime normalization limits the write timeout to 2 seconds, retries to 3, retry interval to 1 second, and event size to 64 KiB.

When enabled, MPC reuses the realtime writer's existing Redis client. The normal path pipelines the existing realtime `SET` and Judge `XADD` in one Redis round trip, but Pipeline is not transactional and both command results are checked separately. Every logical event receives one canonical lowercase UUIDv4 and all finite retries reuse the exact five-field payload. MPC does not create a second Judge Redis client, read `judge:ingress`, or trim `judge:source`.

There is no local persistent queue or in-memory recovery queue. A failed Source write receives only the configured finite synchronous retries. If all attempts fail, MPC logs a stable `reason_code`, drops that rule event, and continues collecting. A successful realtime `SET` still permits the existing history write even when Source delivery fails.

Production activation:

1. Deploy Judge with the strict five-field UUIDv4 contract.
2. Ensure MPC and Judge receive the same `IOT_REDIS_DB` value.
3. Deploy MPC with `12_new_mpc.sh`; Judge Source is enabled by default.
4. Set `IOT_JUDGE_SOURCE_ENABLED=false` only when an explicit rollback requires MPC to stop producing new Source events.
