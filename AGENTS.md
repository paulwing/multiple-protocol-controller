# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

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
- `internal/protocol/bacnet/` - BACnet/IP support (registered as "BACNET")

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
- `Protocol` field specifies which protocol to use ("ModbusRtu", "OPCUA", "MQTT", "BACNET", etc.)
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
| `server:status:mpc` | Service heartbeat |
