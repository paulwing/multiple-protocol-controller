# MQTT 协议支持实现计划

> 历史协议方案（2026-08-31 状态注）：相关协议已有采集/控制实现。本文示例可能与当前字段不同（params 当前为列表），运行配置以 [README](README.md) 和当前协议代码为准，不重新执行旧计划。

## 1. 需求概述

在现有 multiple-protocol-controller 项目中添加 MQTT 协议支持，实现：
- **设备实时数据采集**：通过订阅 MQTT Topic 获取设备数据
- **设备状态控制**：通过发布 MQTT Topic 发送控制指令

## 2. 设计思路

### 2.1 MQTT 设备通信模式

参考现有 Modbus/OPC UA 协议的实现模式：

| 现有协议 | 通信方式 | 采集方式 | 控制方式 |
|---------|---------|---------|---------|
| Modbus RTU | TCP 主动连接 | 主动轮询寄存器 | 主动写入寄存器 |
| OPC UA | TCP 主动连接 | 主动读取 Node | 主动写入 Node |
| **MQTT** | **TCP 主动连接 Broker** | **订阅 Topic 接收** | **发布 Topic 发送** |

### 2.2 MQTT 协议配置结构

复用现有 `gatewayInfo` 字段存储 MQTT Broker 地址，`params` 存放 MQTT 特有参数：

```json
{
  "protocol": "MQTT",
  "gatewayInfo": {
    "id": "mqtt-broker-01",
    "ip": "broker.example.com",
    "port": 1883
  },
  "username": "admin",
  "password": "public",
  "params": {
    "clientId": "device_serial",
    "qos": 1,
    "keepAlive": 30,
    "cleanSession": true
  },
  "properties": [
    {
      "id": "temp",
      "key": "temperature",
      "type": "float",
      "protocol": {
        "type": "MQTT",
        "topic": {
          "subscribe": "device/{serial}/telemetry",
          "publish": "device/{serial}/cmd"
        },
        "path": "payload.temperature",
        "qos": 1
      }
    }
  ]
}
```

## 3. 实现计划

### 3.1 新增文件结构

```
internal/
├── protocol/
│   └── mqtt/
│       ├── mqtt.go              # 协议实现 (实现 Protocol 接口)
│       ├── encodeCommand.go     # 命令编码 (发布消息)
│       └── decodeMessage.go     # 消息解码 (订阅消息)
├── conn/
│   ├── mqtt_client.go           # MQTT 客户端管理 (类比 opcua_manager.go)
│   └── mqtt_manager.go          # MQTT 连接池管理
└── collector/
    └── mqtt.go                  # MQTT 采集 worker
```

### 3.2 详细实现步骤

#### Step 1: 添加 MQTT 依赖

在 `go.mod` 添加 MQTT 客户端库：

```go
github.com/eclipse/paho.mqtt.golang v1.4.3
```

#### Step 2: 扩展配置模型

修改 `internal/config/runtime.go`：

- 添加 `MqttParam` 结构体 (采集点)
- 添加 `MqttCommand` 结构体 (控制点)
- 添加 `DeviceRuntime.Mqtt*` 字段
- 添加 `IotCfgType.DeviceMqttCmdBySerial` 字段
- 在 `buildDeviceRuntime()` 中解析 MQTT 配置

修改 `internal/config/device.go`：

- 在注释中添加 MQTT 协议配置说明

#### Step 3: 实现 MQTT 协议解析

创建 `internal/protocol/mqtt/mqtt.go`：

- 实现 `Protocol` 接口
- `ParsePropProtocol()` 解析 MQTT Topic 配置
- `EncodeCommand()` 生成 MQTT 消息载荷

#### Step 4: MQTT 连接管理

创建 `internal/conn/mqtt_client.go`：

- 实现 MQTT 客户端封装
- 支持订阅/发布
- 支持自动重连
- 线程安全

创建 `internal/conn/mqtt_manager.go`：

- 管理多个 MQTT 客户端连接
- 类似 `opcua_manager.go` 的模式

#### Step 5: 实现采集 Worker

创建 `internal/collector/mqtt.go`：

- 实现 `mqttWorker` 结构
- 订阅配置的消息 Topic
- 解析接收到的消息
- 调用 `recordCollectedMqttValue()` 写入 Redis

修改 `internal/collector/collector.go`：

- 添加 `mqttWorkers` map
- 在 `update()` 中处理 MQTT 设备
- 添加 `buildMqttDeviceSpecs()` 函数
- 添加 `isMqttDevice()` 判定函数

#### Step 6: 实现控制功能

修改 `internal/control/control.go`：

- 在 `ProcessCommand()` 中添加 MQTT 协议处理分支
- 调用 MQTT 客户端发布控制命令

#### Step 7: 数据写入

修改 `internal/collector/result_writer.go`：

- 添加 `recordCollectedMqttValue()` 函数
- 在 `applyConfig()` 中处理 MQTT 点位

### 3.3 代码修改清单

| 文件 | 修改类型 | 说明 |
|------|---------|------|
| `go.mod` | 新增依赖 | paho.mqtt.golang |
| `internal/config/runtime.go` | 扩展 | 添加 MqttParam/MqttCommand |
| `internal/config/device.go` | 文档 | 添加 MQTT 配置说明 |
| `internal/protocol/mqtt/mqtt.go` | 新增 | 协议实现 |
| `internal/protocol/mqtt/encodeCommand.go` | 新增 | 命令编码 |
| `internal/conn/mqtt_client.go` | 新增 | MQTT 客户端 |
| `internal/conn/mqtt_manager.go` | 新增 | 连接管理 |
| `internal/collector/mqtt.go` | 新增 | 采集 Worker |
| `internal/collector/collector.go` | 修改 | 添加 MQTT 支持 |
| `internal/collector/result_writer.go` | 修改 | 添加 MQTT 数据写入 |
| `internal/control/control.go` | 修改 | 添加 MQTT 控制 |

## 4. 配置示例

### 4.1 设备配置 JSON

```json
{
  "id": "mqtt-device-001",
  "serialNumber": "MQTT_DEVICE_001",
  "deviceName": "MQTT测试设备",
  "protocol": "MQTT",
  "acqFreq": 5000,
  "gatewayInfo": {
    "id": "mqtt-broker-01",
    "ip": "broker.example.com",
    "port": 1883,
    "deviceName": "本地MQTT服务器"
  },
  "username": "admin",
  "password": "public",
  "params": {
    "clientId": "mpc_mqtt_device_001",
    "qos": 1,
    "keepAlive": 30,
    "cleanSession": true
  },
  "properties": [
    {
      "id": "temp",
      "key": "temperature",
      "name": "温度",
      "type": "float",
      "access": "11",
      "unit": "℃",
      "protocol": {
        "type": "MQTT",
        "topic": {
          "subscribe": "device/MQTT_DEVICE_001/telemetry",
          "publish": "device/MQTT_DEVICE_001/cmd"
        },
        "path": "payload.temperature",
        "qos": 1
      }
    },
    {
      "id": "switch",
      "key": "switch",
      "name": "开关",
      "type": "bool",
      "access": "11",
      "unit": "",
      "protocol": {
        "type": "MQTT",
        "topic": {
          "subscribe": "device/MQTT_DEVICE_001/status",
          "publish": "device/MQTT_DEVICE_001/cmd"
        },
        "path": "payload.switch",
        "qos": 1
      }
    }
  ]
}
```

### 4.2 Topic 变量替换

支持以下变量：
- `{serial}` - 设备序列号
- `{deviceId}` - 设备 ID

## 5. 兼容性考虑

- 不修改现有 Modbus RTU 和 OPC UA 的任何逻辑
- MQTT 作为独立的协议分支
- 配置文件使用 `protocol: "MQTT"` 区分
- 确保 `init()` 函数正确注册 MQTT 协议

## 6. 测试计划

1. 单元测试：Protocol 接口实现
2. 集成测试：MQTT Broker 连接测试
3. 数据采集测试：订阅消息解析
4. 控制测试：发布命令功能
5. 压力测试：多设备并发
