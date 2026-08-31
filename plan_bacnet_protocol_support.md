# BACnet/IP 协议支持方案

> 历史协议方案（2026-08-31 状态注）：相关协议已有采集/控制实现。本文示例可能与当前字段不同（params 当前为列表），运行配置以 [README](README.md) 和当前协议代码为准，不重新执行旧计划。

## 1. 目标

在当前多协议控制器中新增 `BACnet/IP` 支持，同时不破坏现有 `Modbus`、`OPC UA` 和 `MQTT` 的行为。

实现方式应尽量与现有架构保持一致：

- 协议解析放在 `internal/protocol`
- 运行时配置构建放在 `internal/config/runtime.go`
- 连接管理放在 `internal/conn`
- 采集 worker 放在 `internal/collector`
- 控制下发放在 `internal/control`
- 实时结果输出放在 `internal/collector/result_writer.go`

## 2. 计划改动范围

### 2.1 协议层

新增 BACnet 协议包：

- `internal/protocol/bacnet/bacnet.go`

职责：

- 注册 `BACNET` / `bacnet`
- 解析 `Property.Protocol`
- 定义 BACnet 点位配置结构

建议的属性协议配置结构：

```json
{
  "type": "BACNET",
  "objects": {
    "read": [
      {
        "objectType": "analogInput",
        "instance": 1,
        "propertyId": "presentValue"
      }
    ],
    "write": [
      {
        "objectType": "analogValue",
        "instance": 2,
        "propertyId": "presentValue"
      }
    ]
  }
}
```

## 3. 运行时配置改动

扩展 `internal/config/runtime.go`，加入 BACnet 的运行时结构。

计划新增：

- `BacnetParam`
- `BacnetCommand`
- `DeviceRuntime.BacnetReadPoints`
- `DeviceRuntime.BacnetWritePoints`
- BACnet 设备级运行时字段，例如：
  - `BacnetDeviceInstance`
  - `BacnetPort`
  - `BacnetIP`
- `IotCfgType.DeviceBacnetCmdBySerial`

同时新增：

- `isBacnetProtocol(proto string) bool`
- `parseBacnetPoints(dev DeviceConfig)`
- `parseBacnetParams(dev DeviceConfig)`

## 4. 连接管理

新增 BACnet 连接管理器：

- `internal/conn/bacnet_manager.go`

职责：

- 管理 `BACnet/IP` 的 UDP 通信
- 处理目标设备寻址
- 提供读写请求辅助方法
- 处理超时和请求响应匹配

之所以单独做一个 manager：

- 当前 `internal/conn/manager.go` 是面向 Modbus 风格的 TCP 连接池
- `BACnet/IP` 是基于 UDP 的，通信模型不同
- 结构上应与以下文件保持一致的职责拆分：
  - `internal/conn/opcua_manager.go`
  - `internal/conn/mqtt_manager.go`

## 5. 采集改动

新增 BACnet 采集 worker：

- `internal/collector/bacnet.go`

并扩展：

- `internal/collector/collector.go`

计划行为：

- 增加 `bacnetWorkers`
- 根据运行时配置构建设备采集规格
- 周期性读取配置的 BACnet 对象属性
- 按属性类型对采集值进行归一化
- 将结果写入现有实时结果写入器

## 6. 控制改动

第一阶段支持 BACnet `Present_Value` 写入控制。

范围约束：

- 仅支持 `WriteProperty(Present_Value)`
- 不支持 `Units` 等其他属性写入
- 现有 Modbus / OPC UA / MQTT 控制分支保持不变

## 7. 结果写入改动

扩展：

- `internal/collector/result_writer.go`

计划行为：

- 在设备快照初始化时纳入 BACnet 读点位名称
- 支持 BACnet 采集值沿用现有 Redis 快照写入流程

## 8. 建议的配置模型

### 8.1 设备级配置

使用 `GatewayInfo` 表示 `BACnet/IP` 端点：

```json
"gatewayInfo": {
  "ip": "192.168.1.10",
  "port": 47808
}
```

使用 `params` 表示 BACnet 设备专有信息：

```json
"params": {
  "deviceInstance": 1001
}
```

### 8.2 属性级配置

建议点位字段使用：

- `objectType`
- `instance`
- `propertyId`

原因：

- 这几个字段与 BACnet 协议语义直接对应
- 可以减少额外的字段翻译层
- 便于后续结合协议文档或抓包结果排查问题

## 9. 建议示例

```json
{
  "id": "dev-001",
  "serialNumber": "SN-001",
  "protocol": "BACNET",
  "gatewayInfo": {
    "id": "gw-bacnet-01",
    "ip": "192.168.1.10",
    "port": 47808,
    "deviceName": "BACnet Gateway"
  },
  "params": {
    "deviceInstance": 1001
  },
  "properties": [
    {
      "id": "p1",
      "key": "roomTemp",
      "name": "Room Temperature",
      "type": "float",
      "access": "10",
      "protocol": {
        "type": "BACNET",
        "objects": {
          "read": [
            {
              "objectType": "analogInput",
              "instance": 1,
              "propertyId": "presentValue"
            }
          ],
          "write": []
        }
      }
    }
  ]
}
```

## 10. 实施顺序

1. 新增协议解析和运行时结构
2. 新增 BACnet 连接管理和读取能力
3. 新增 BACnet 采集 worker
4. 新增 BACnet 写入 / 控制支持
5. 扩展结果写入器
6. 完成后做一次全量编译与测试验证

## 11. 已确认范围

当前已确认的 BACnet 第一阶段实现边界如下：

- 使用 `params.deviceInstance` 作为设备标识字段
- 点位建模采用 `objectType + instance + propertyId`
- 第一阶段只支持读取 `Present_Value`
- 第一阶段控制只支持写入 `Present_Value`
- 第一阶段不读取 `Units`
