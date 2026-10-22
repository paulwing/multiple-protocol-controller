# multiple-protocol-controller 项目总结

2026-10-22 按 `0a09019` 核对主要入口、点位类型与网关健康实现；这不是部署或实设备验收记录。完整配置和运行步骤见 [README](README.md)，系统级说明见[文档仓库 MPC 架构](../documents/architecture/mpc.md)。

## 主流程

`cmd/main.go` → `internal/app/app.go` 组装日志、Redis、采集、结果写入、配置订阅及服务心跳。

平台在 Redis IOT:DEVICE 发布设备快照，并通过 CFG_CHANGE 通知 MPC；当前每次相同 payload IOT:DEVICE 都触发重新加载，不再把固定通知去重。配置不直接从 MySQL 读取。

## 协议与模块

| 模块 | 职责 |
| --- | --- |
| internal/config | 原始配置、参数、按 ID/编号索引的运行时快照 |
| internal/service | Redis 快照加载、配置/控制订阅、心跳 |
| internal/protocol | Modbus RTU 帧、OPC UA、MQTT、BACnet/IP 的解析/编码；不宣称原生 Modbus TCP/MBAP |
| internal/conn | TCP、OPC UA、MQTT、BACnet 连接与读写 |
| internal/collector | 四类 worker、实时写入、InfluxDB 历史、Judge Source |
| internal/control | 命令分派、超时和回执 |
| internal/store | Redis 操作与订阅 |

协议工厂通过 NormalizeName 统一名字。设备级 params 和属性 protocol 配置分开；不能把点位写映射放进设备 params 后期望自动生效。

## 数据输出

- 实时：device:data:{设备数据库ID}。内部快照可能按编号管理，但 Redis key 由 device.Config.ID 构造，不是 serialNumber。
- 历史：实时写入成功后入内存队列，由后台 worker 批量写 InfluxDB；队列满或重试耗尽会丢弃相应历史点。
- Judge：通过独立有界队列向 Redis Pub/Sub 频道 `iot:judge:device-events` 发布严格五字段事件；与实时缓存、历史写入分别限流，没有持久补发或消费组。详见[Source 契约](docs/对接文档/MPC规则事件发布.md)。
- 服务心跳：server:status:mpc。
- 纯 Modbus 网关健康：gateway:status:{网关ID}，独立于子设备属性采集；配置快照与探测目标不一致时本轮不刷新，下一轮重取目标。该检查与 Redis 写入不是跨系统原子事务。

## 控制语义

SubRedisChannel 已订阅 set_device_current_value，并调用 ProcessCommand；结果发布到 device_current_value_response。消息外层为 uid/data。data 每项的 device_id 当前实际按 serialNumber 查询 DeviceBySerial，这是历史字段命名，不能误传平台数据库 ID。

控制分支涵盖 Modbus、OPC UA、MQTT、BACnet。运行时同时校验属性可写标记与协议写点位，不再为只读属性建立控制映射。Modbus、OPC UA、BACnet 的可读写属性在写入成功后回读目标属性并比较；只写属性及 MQTT 发布成功返回 `unverified`，不会误报设备已执行。协议回执、回读或目标值验证失败返回 `failed`。

`device_current_value_response` 保留历史 `control_command_result`，并包含 `verification_status` 与逐属性 `attributes`。平台后端已提供 HTTP 桥接、用户权限、设备 scope、命令追踪和控制记录；MPC 负责设备通信，不替代平台权限边界。

点位级 `dataType` 可独立于业务属性 `type`；未指定时沿用旧类型。运行时以 `DataTypeExplicit` 区分新显式转换和旧行为，`PropertyType` 保留业务含义及历史标签。精确整数、读写异型比较仍受协议及 JSON/浏览器精度限制，见[类型契约](../documents/architecture/mpc.md#点位类型与属性类型)。

## MQTT

Broker 来自 GatewayInfo；账号密码优先设备 params，连接选项包括 clientId/qos/keepAlive/cleanSession。主题模板支持 {serial}、{deviceId}；属性 topics.subscribe/publish 定义采集和写入。

旧总结中的“只支持三类协议”“实时 key 使用 serial”不再符合当前代码。代码已有 BACnet、按 ID 写 key。MQTT Subscribe 保存 topic handler，但旧总结对消息回调全链路的担忧并未在本轮做专项验证；设备兼容性、消息实际送达和长期连接资源释放仍须实测，不能从静态文档宣称已修复或稳定性验收。

## 核对与测试

```sh
go test ./...
```

重点回归：连续两次配置通知、实时 ID 契约、各协议参数解析、只读属性拒绝控制、Modbus 回执错误、写后回读状态、历史队列与 Judge Source 失败隔离。单测通过不等于实设备控制或 Judge 全链路通过。
