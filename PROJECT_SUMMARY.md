# multiple-protocol-controller 项目总结

## 1. 项目定位

`multiple-protocol-controller` 是一个 IoT 设备采集与控制服务，当前支持三类协议：

- Modbus RTU/TCP
- OPC UA
- MQTT

服务核心能力：

- 从 Redis 拉取设备配置并构建运行时快照
- 按设备协议执行周期采集
- 订阅控制指令并按协议下发
- 将采集结果回写 Redis

---

## 2. 启动与主流程

入口：`cmd/main.go` -> `internal/app/app.go`

应用启动后并发执行：

1. 初始化日志
2. 启动采集结果写入器（Redis）
3. 启动 Collector 管理器
4. 订阅 Redis 频道（配置变更 / 控制下发）
5. 周期发送服务心跳
6. 首次拉取 IoT 配置并构建运行时快照

配置更新流程：

- `service.SubRedisChannel` 收到配置变更消息
- 调用 `service.GetIoTCfg`
- `config.BuildRuntimeConfig` 重建运行时结构
- 刷新连接管理器与采集 worker 快照

---

## 3. 配置模型与运行时模型

### 3.1 原始配置模型

定义在 `internal/config/device.go`：

- 设备基础信息（`DeviceConfig`）
- 网关信息（`GatewayInfo`）
- 属性定义（`Property`）
- 协议扩展字段（`Property.Protocol` + `DeviceConfig.Params`）

### 3.2 运行时模型

定义在 `internal/config/runtime.go`：

- `IotCfgType`：全量运行时快照
- `DeviceRuntime`：单设备运行时信息
- 各协议读写点位结构：
  - `ModbusParam` / `ModbusCommand`
  - `OpcuaParam` / `OpcuaCommand`
  - `MqttParam` / `MqttCommand`

运行时快照还维护按序列号索引的控制映射：

- `DeviceCmdBySerial`（Modbus）
- `DeviceOpcuaCmdBySerial`（OPC UA）
- `DeviceMqttCmdBySerial`（MQTT）

---

## 4. 协议层设计

协议接口：`internal/protocol/protocol.go`

- `ParsePropProtocol(raw)`：解析属性协议配置
- `EncodeCommand(cmd)`：协议命令编码（仅对需要二进制帧编码的协议有意义）

各协议注册：

- Modbus：`internal/protocol/modbusRtu/modbusRtu.go`
- OPC UA：`internal/protocol/opcua/opcua.go`
- MQTT：`internal/protocol/mqtt/mqtt.go`

说明：

- Modbus 使用 `EncodeCommand` 生成下行帧
- OPC UA / MQTT 控制通过客户端 API 写入/发布，不依赖二进制帧编码

---

## 5. 连接管理层

### 5.1 TCP（Modbus）

`internal/conn/manager.go`

- 管理网关 TCP 连接池
- 设备到网关映射
- 独占连接与网关忙状态控制
- 预拨号 / 重连 / 配置刷新

### 5.2 OPC UA

`internal/conn/opcua_manager.go`

- 管理 `opcua.Client` 缓存（按 endpoint+安全配置+用户名）
- 提供 `ReadNodes` / `WriteNode`

### 5.3 MQTT

`internal/conn/mqtt_manager.go`

- 管理 MQTT 客户端缓存（按 broker+clientId）
- 提供 `Connect` / `Subscribe` / `Publish` / `Unsubscribe` / `Disconnect`
- 提供 `ParseMqttPayload`（支持 JSON path 提取）

---

## 6. 采集架构

统一调度：`internal/collector/collector.go`

- `Manager` 内部维护三类 worker：
  - `modbusWorkers`
  - `opcuaWorkers`
  - `mqttWorkers`
- 配置变更后按协议重建 worker

### 6.1 Modbus 采集

`internal/collector/modbus.go`

- 支持批量读和单点读
- CRC 校验、字节序处理、类型解码

### 6.2 OPC UA 采集

`internal/collector/opcua.go`

- 周期调用 `ReadNodes` 读取节点值
- 根据属性类型做值归一化后写入结果

### 6.3 MQTT 采集

`internal/collector/mqtt.go`

- 启动时连接 MQTT Broker
- 按配置订阅主题
- 收到消息后按 `path` 提取字段并做类型归一化
- 写入结果缓存/Redis

---

## 7. 控制下发架构

`internal/control/control.go`

控制消息来源：Redis 频道 `set_device_current_value`

`ProcessCommand` 按设备协议分支：

- MQTT：组装 payload -> `conn.DefaultMqttManager().Publish`
- OPC UA：归一化值 -> `conn.DefaultOpcuaManager().WriteNode`
- Modbus：`EncodeCommand` 组帧 -> TCP 发送并等待 ACK

控制结果回执发布到 `device_current_value_response`

---

## 8. 结果写入

`internal/collector/result_writer.go`

- 维护设备快照（点名、当前值、状态）
- 采集后写入 Redis key：`device:data:{serial}`
- 已覆盖三种协议的点位写入：Modbus / OPC UA / MQTT

---

## 9. MQTT 配置语义（当前实现）

MQTT 设备的运行时参数来源：

- Broker：由 `GatewayInfo.IP/Port` 组装（`tcp://ip:port`）
- 账号密码：优先 `params.username/password`，否则使用设备级 `username/password`
- 连接参数：`params.clientId/qos/keepAlive/cleanSession`
- 属性协议：`property.protocol.type = MQTT`
  - `topics.subscribe[]`：采集订阅
  - `topics.publish[]`：控制发布
  - 每个 topic 项可包含 `topic/path/qos`

Topic 模板变量替换：

- `{serial}` -> 设备序列号
- `{deviceId}` -> 设备 ID

---

## 10. 当前代码现状与注意点

1. 多协议主框架已打通：配置解析、采集调度、控制下发、结果写入均支持 Modbus/OPC UA/MQTT。
2. 连接管理按协议拆分，职责清晰：TCP、OPC UA、MQTT 各自管理。
3. MQTT 采集链路中，`MqttManager.Subscribe` 当前将消息写入内部 `messageCh`，但传入的 `handler` 未直接回调；需要确认 `collector/mqtt.go` 的 `w.msgCh` 是否能稳定收到消息（建议专项联调验证）。
4. 配置刷新时会重建 worker；但 MQTT/OPC UA 全局 manager 的历史 client 生命周期需要结合实测关注（长期运行资源回收）。

---

## 11. 关键文件索引

- 应用启动：`internal/app/app.go`
- 配置加载/刷新：`internal/service/getIoTCfg.go`
- Redis 订阅：`internal/service/subRedisChannel.go`
- 运行时模型：`internal/config/runtime.go`
- 协议工厂：`internal/protocol/protocol.go`
- Modbus 协议：`internal/protocol/modbusRtu/*`
- OPC UA 协议：`internal/protocol/opcua/*`
- MQTT 协议：`internal/protocol/mqtt/mqtt.go`
- 连接管理：`internal/conn/*.go`
- 采集调度：`internal/collector/*.go`
- 控制下发：`internal/control/control.go`

