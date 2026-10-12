# Judge 发布与采集链路边界

2026-10-12 按 MPC `9cd2df9` 核对。本文供开发、排障时区分 Judge 发布的影响和原有采集行为，不是性能压测或现场验收报告。配置、消息格式、容量和失败日志以 [MPC 规则事件发布](../对接文档/MPC规则事件发布.md) 为准。

历史归因以 Judge 接入前的 `589d7d2`、首次接入的 `1a81c62` 为界；当前行为以本页列出的源码入口为准。

## 采集到发布的顺序

`recordValue` 在同一设备的锁内依次执行：

1. 更新属性值，由 `cloneResult()` 复制实时快照。
2. 开启 Judge 时，用已复制的 `result.CurrentValue` 生成事件编号、校验并编码，尝试加入发布队列。
3. 同步执行 Redis `SET device:data:<device_id>`。
4. 只有 `SET` 成功，才尝试把本次属性历史点加入历史队列；随后释放设备锁。

后台发布 worker 从 Judge 队列取消息，使用独立 Redis 客户端执行 `PUBLISH`，不等步骤 3 完成。因此，Judge 发布成功、实时快照写入成功、历史点保存成功，是三个不同的结果，不能互相代替。

当前事件构造已复用实时快照，不再额外复制一次属性 map；入队保存编码后的消息，后续采集不会改写旧事件。这减少了重复复制，但事件编号生成、校验、编码和入队仍有成本。

代码：[result_writer.go](../../internal/collector/result_writer.go)、[judge_source.go](../../internal/collector/judge_source.go)。

## 发布支路已经隔离的等待

| 环节 | 当前做法 | 保证范围 |
| --- | --- | --- |
| 采集入队 | 容量不足时放弃当前新事件，不等待空位 | 不覆盖或合并旧事件；仍有短锁和内存操作 |
| Redis 发布 | 独立客户端、固定 worker 池、同设备串行 | 不占用快照客户端连接池；并不独占 Redis 服务端 |
| 发布启动 | 创建发布客户端时不联网、不做 `PING`；发布器构造失败只停用 Judge 支路 | 快照客户端自身的启动检查仍保留 |
| 失败日志 | 调用方只更新固定计数与状态；首次失败、持续失败汇总和恢复由后台输出 | 写日志前释放状态锁，不让慢日志占着该锁；原有采集日志不在此保证内 |
| 退出 | 发布器停止不等待失败日志输出，也不保证发完待发消息 | 不代表整个 MPC 退出过程都不等待日志或存储 |

代码：[judge_source_publisher.go](../../internal/collector/judge_source_publisher.go)、[judge_source_failure_log.go](../../internal/collector/judge_source_failure_log.go)、[redis.go](../../internal/store/redis.go)。

日志里的恢复是“对应操作阶段重新成功”，不是全部设备恢复，更不是 Judge 完成处理。无订阅者、超时、队列不足的具体原因码和频率见发布契约，避免在两处维护不同说法。

## 仍与采集共用的资源

| 事项 | 当前实现及影响 | 排查时注意 |
| --- | --- | --- |
| 事件构造 | 在采集线程内生成、校验和编码事件，再尝试入队 | 不等待网络，不代表没有 CPU、分配或锁开销；不能只凭调用位置断定已经造成瓶颈 |
| 同设备锁 | `snapshot.mu` 持有到同步 `SET` 返回和历史入队尝试结束 | 慢 `SET` 会让同设备更新、状态更新和配置刷新等待；缩短锁范围前须验证顺序 |
| Redis 与进程资源 | 两个客户端共用配置中的 Redis 服务端，也共用 MPC 的 CPU、内存和垃圾回收 | 独立连接池不等于完全资源隔离；影响程度需要在目标负载下测量 |

接入前的 writer 在复制快照后释放锁，再执行 `SET`；首次 Judge 接入后改为持有设备锁完成这一轮写入。这是维持同设备顺序的取舍，不能只为缩短等待而直接去掉锁。

队列上限用于保护资源，轮流调度用于分配执行机会，两者都不能保证持续过载时不丢消息。Pub/Sub 不保留离线消息、不补发、没有业务处理确认，是当前接口约定，不应当作尚未实现的可靠消息队列。

## Judge 接入前已有的行为

| 环节 | 当前仍保留的行为 | 可能出现的现象 |
| --- | --- | --- |
| 实时与历史 | 实时快照同步 `SET`；失败时不写本次历史点 | Redis 慢会拖慢采集；不是 Judge 新增的同步等待 |
| 历史队列 | 队列满时丢弃新历史点；第 1 次及每 1000 次丢弃同步写日志 | 实时值已经更新，历史记录却可能缺少这一点；慢日志也可能延后采集 |
| Modbus 网关 | 批次采集持有网关独占连接，直到解码和结果写入结束才释放 | 慢 `SET` 会延长网关占用，影响共享该网关的其他设备或控制；Judge 新增工作可能进一步增加耗时 |
| MQTT 接收 | 每个 worker 的接收队列容量为 10，满时丢消息并同步写日志；处理属性仍走同步 `SET` | MQTT 接收队列可能先满，与 Judge 发布队列是两回事 |
| 原有日志 | 实时写入失败、协议路径及应用退出仍有同步日志 | 不能因 Judge 失败日志已放到后台，就认为全部日志都不会阻塞 |

代码：[history_writer.go](../../internal/collector/history_writer.go)、[modbus.go](../../internal/collector/modbus.go)、[manager.go](../../internal/conn/manager.go)、[mqtt.go](../../internal/collector/mqtt.go)、[app.go](../../internal/app/app.go)。

OPC UA、BACnet 同样经过结果写入链路。本页没有对全部协议做兼容性审查，也不能据此认定所有协议都不存在其他问题。

## 验证入口与待测边界

| 要核对的行为 | 测试入口 |
| --- | --- |
| 五字段消息、旧事件不被后续更新改写、发布失败与实时写入的关系 | [result_writer_judge_test.go](../../internal/collector/result_writer_judge_test.go) |
| 同设备顺序、轮流调度和队列容量 | [judge_source_publisher_test.go](../../internal/collector/judge_source_publisher_test.go) |
| 慢日志下的采集、发布及停止行为 | [judge_source_failure_log_test.go](../../internal/collector/judge_source_failure_log_test.go) |
| 发布客户端惰性连接和共同截止时间 | [redis_publish_test.go](../../internal/store/redis_publish_test.go) |

相关包验证命令：

```sh
go test -race -count=1 ./internal/collector ./internal/config ./internal/store
```

以上是验证入口，不是本次测试执行记录。单测使用替身及可控连接，不能替代真设备和真实 Redis/Judge 联调。目标负载下还需观察采集耗时、实时 `SET` 延迟、队列拒绝、历史丢点和 Judge 端结果，才能判断是否存在性能瓶颈。

MPC 发布压力的页面展示不在本页定义功能范围内；协议日志、历史链路等改动应单独确认，不因为本页列出了可能影响就自动修改。
