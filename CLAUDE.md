# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Multiple Protocol Controller (MPC) - A Go-based industrial IoT gateway that supports multi-protocol device data collection (Modbus RTU, OPC UA, MQTT, BACnet/IP) and control command dispatch.

## Build & Run Commands

```bash
# Local development (with hot reload)
air

# Production build
go build -o ./multiple-protocol-controller ./cmd

# Run production binary
./multiple-protocol-controller
```

## Architecture

### Core Components

| Component | Purpose |
|-----------|---------|
| `cmd/main.go` | Application entry point |
| `internal/app/app.go` | Application lifecycle, goroutine management |
| `internal/collector/` | Device data collection workers |
| `internal/conn/` | Protocol connection management (Modbus TCP, OPC UA, MQTT, BACnet/IP) |
| `internal/control/` | Control command processing and dispatch |
| `internal/config/` | Configuration loading and runtime model |
| `internal/protocol/` | Protocol interface and implementations |
| `internal/service/` | Redis subscriptions and IoT config fetching |
| `internal/store/` | Redis client wrapper |

### Protocol System

Protocols are registered via a factory pattern in `internal/protocol/protocol.go`:

```go
type Protocol interface {
    ParsePropProtocol(raw json.RawMessage) (any, error)
    EncodeCommand(cmd any) ([]byte, error)
}

func RegisterProtocol(name string, p Protocol)
func GetProtocol(name string) (Protocol, error)
```

Existing implementations:

- `internal/protocol/modbusRtu/` - Modbus RTU/TCP support (registered as "MODBUS_RTU")
- `internal/protocol/opcua/` - OPC UA support (registered as "OPCUA")
- `internal/protocol/mqtt/` - MQTT support (registered as "MQTT")
- `internal/protocol/bacnet/` - BACnet/IP support (registered as "bacnet", normalized to "BACNET")

Protocol names are normalized with `protocol.NormalizeName` in the protocol factory, so each protocol should register only one name and reuse that helper for protocol-name comparisons.

To add a new protocol:

1. Create `internal/protocol/<protocol>/` package
2. Implement the `Protocol` interface
3. Register in `init()` function

### Data Flow

1. **Configuration**: Device configs loaded from Redis (`IOT:DEVICE`, `IOT:PRODUCT` keys)
2. **Collection**: Workers periodically poll devices based on `acqFreq`
3. **Storage**: Collected data written to Redis via `collector.ResultWriter`
4. **Control**: Commands received via Redis channel (`set_device_current_value`), dispatched to devices

### Configuration Model

Device configuration in `internal/config/device.go`:

- `Protocol` field specifies which protocol to use ("MODBUS_RTU", "OPCUA", "MQTT", "BACNET", etc.)
- `Properties` contains data points with protocol-specific `Protocol` config
- `Params` contains protocol-specific parameters (e.g., Modbus slaveID, MQTT client options, BACnet device instance)

#### MQTT Device Configuration

MQTT uses `gatewayInfo` to store broker address:

- `gatewayInfo.ip` - MQTT broker host
- `gatewayInfo.port` - MQTT broker port (default 1883)
- `username`/`password` - Authentication (optional)
- `params.clientId` - Client ID (optional)
- `params.qos` - Default QoS level (0-2)
- `params.keepAlive` - Keep alive interval in seconds
- `params.cleanSession` - Clean session flag

## Key Patterns

### Adding Protocol Support

1. Define protocol struct in `internal/protocol/<protocol>/<protocol>.go`
2. Implement `ParsePropProtocol()` to parse property protocol config
3. Implement `EncodeCommand()` for write operations
4. Register in `init()` using `protocol.RegisterProtocol()`

### Runtime Config Updates

The system uses `config.IotCfgStore` (atomic.Value) to store the current device configuration. When Redis publishes to `CFG_CHANGE` channel, the config is reloaded and collection workers are restarted.

### Connection Management

- Modbus uses `conn.Manager` for TCP connection pooling with exclusive access (one device operation at a time per gateway)
- OPC UA uses `conn.OpcuaManager` for connection handling
- MQTT uses `conn.MqttManager` for broker connection, subscription and publishing
- BACnet/IP uses `conn.BacnetManager` for read/write present value operations

## Environment Variables

- `REDIS_HOST` - Redis host override
- `REDIS_PORT` - Redis port override
- `REDIS_PWD` - Redis password override

## Redis Keys & Channels

| Key/Channel | Purpose |
|-------------|---------|
| `IOT:DEVICE` | Device configuration |
| `IOT:PRODUCT` | Product model configuration |
| `set_device_current_value` | Control command input |
| `device_current_value_response` | Control command result |
| `CFG_CHANGE` | Configuration change notification |
| `iot:judge:device-events` | Judge Source Pub/Sub; see the publication contract below |
| `server:status:mpc` | Service heartbeat |

## Judge 协作入口

- 配置、消息格式和失败处理：[MPC 规则事件发布](docs/对接文档/MPC规则事件发布.md)。
- 对采集、存储的影响：[Judge 发布与采集链路边界](docs/代码审查/Judge发布分支与采集链路边界审查.md)。
- 跨仓配套版本：[部署约定](../documents/integration/server-deployment-guide.md#release-versions)。只检出 MPC 时先核对本仓文档；缺少相邻文档或配套提交时说明缺口，不默认各仓当前分支互相兼容。

| 代码入口 | 职责 |
| --- | --- |
| `internal/collector/result_writer.go` | 采集快照、事件构造入口、实时与历史写入顺序 |
| `internal/collector/judge_source.go` | 五字段消息校验、编码及配置归一化 |
| `internal/collector/judge_source_publisher.go` | 单设备顺序、队列容量及 Redis 发布 |
| `internal/collector/judge_source_failure_log.go` | 失败计数、后台汇总及恢复日志 |
| `internal/config/`、`internal/store/` | 配置读取及独立发布客户端 |

相关包验证：

```sh
go test -count=1 ./internal/collector ./internal/config ./internal/store
```

需要全仓回归时执行 `go test -count=1 ./...`。按实际改动选择检查；单测不能代替真设备、实际 Redis 和 Judge 联调。

## 可选代码索引

GitNexus 只用于按需导航，工具可用且索引与所查代码一致时再使用。工具缺失或索引陈旧时直接查源码、测试和 Git 差异；不因普通文档整理自动安装工具、重建索引或逐个符号执行分析。影响范围和验证以实际改动为准。
