# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Multiple Protocol Controller (MPC) - A Go-based industrial IoT gateway that supports multi-protocol device data collection (Modbus RTU, OPC UA, MQTT) and control command dispatch.

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
| `internal/conn/` | TCP connection pool management (Modbus) |
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
- `internal/protocol/modbusRtu/` - Modbus RTU/TCP support (registered as "ModbusRtu")
- `internal/protocol/opcua/` - OPC UA support (registered as "OPCUA", "opcua")
- `internal/protocol/mqtt/` - MQTT support (registered as "MQTT", "mqtt")

To add a new protocol (e.g., MQTT):
1. Create `internal/protocol/mqtt/` package
2. Implement the `Protocol` interface
3. Register in `init()` function

### Data Flow

1. **Configuration**: Device configs loaded from Redis (`IOT:DEVICE`, `IOT:PRODUCT` keys)
2. **Collection**: Workers periodically poll devices based on `acqFreq`
3. **Storage**: Collected data written to Redis via `collector.ResultWriter`
4. **Control**: Commands received via Redis channel (`set_device_current_value`), dispatched to devices

### Configuration Model

Device configuration in `internal/config/device.go`:
- `Protocol` field specifies which protocol to use ("ModbusRtu", "OPCUA", "MQTT", etc.)
- `Properties` contains data points with protocol-specific `Protocol` config
- `Params` contains protocol-specific parameters (e.g., Modbus slaveID, MQTT client options)

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

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **multiple-protocol-controller** (1187 symbols, 3315 relationships, 104 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> Index stale? Run `node .gitnexus/run.cjs analyze` from the project root — it auto-selects an available runner. No `.gitnexus/run.cjs` yet? `npx gitnexus analyze` (npm 11 crash → `npm i -g gitnexus`; #1939).

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows. For regression review, compare against the default branch: `detect_changes({scope: "compare", base_ref: "master"})`.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `query({search_query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `context({name: "symbolName"})`.
- For security review, `explain({target: "fileOrSymbol"})` lists taint findings (source→sink flows; needs `analyze --pdg`).

## Never Do

- NEVER edit a function, class, or method without first running `impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `rename` which understands the call graph.
- NEVER commit changes without running `detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/multiple-protocol-controller/context` | Codebase overview, check index freshness |
| `gitnexus://repo/multiple-protocol-controller/clusters` | All functional areas |
| `gitnexus://repo/multiple-protocol-controller/processes` | All execution flows |
| `gitnexus://repo/multiple-protocol-controller/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
