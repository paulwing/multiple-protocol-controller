# MPC 规则事件发布

## 1. 启用状态

MPC 已具备向 `judge:source` 发布规则事件的能力。本地配置默认关闭，生产脚本 `12_new_mpc.sh` 在未显式设置 `IOT_JUDGE_SOURCE_ENABLED=false` 时默认启用：

```toml
[judge_source]
enabled = false
stream = "judge:source"
write_timeout_ms = 200
retry_count = 1
retry_interval_ms = 20
max_event_bytes = 65536
```

生产启用步骤：

1. 部署支持本文严格五字段协议的 Judge。
2. 确认 MPC 与 Judge 的 Redis 地址及 `IOT_REDIS_DB` 完全一致。
3. 使用 `12_new_mpc.sh` 部署 MPC；脚本默认向容器传入 `JUDGE_SOURCE_ENABLED=true`。
4. 完成正常、重复、断网、Redis 拒绝写入和 MPC 重启联调。仅在明确回退时设置 `IOT_JUDGE_SOURCE_ENABLED=false`。

## 2. Redis 使用方式

Source 发布复用 `deviceResultWriter` 已有的 Redis Client 和连接池。正常路径把实时快照 `SET device:data:<device_id>` 与 `XADD judge:source` 放在同一个 Redis Pipeline 中，因此通常只有一次网络往返。

MPC 的配置读取、订阅、心跳、实时快照和 Source 发布都使用 `redis.db`；环境变量 `REDIS_DB` 可以覆盖该值。它必须与 Judge 的 `REDIS_DB` 相同，默认均为 0。

Pipeline 不等于事务：MPC 分别检查 `SET` 和 `XADD` 的结果，一个命令成功不代表另一个也成功。

MPC 不读取 `judge:ingress`，不写 Judge 报警或运行状态，不对 `judge:source` 使用 `MAXLEN`、`XTRIM` 或 `DEL`，也不建立本地、磁盘或后台补偿队列。

## 3. 严格五字段事件

每条 `XADD judge:source` 记录恰好包含：

| 字段 | 内容 |
|---|---|
| `event_id` | 每个逻辑事件生成一次的规范小写 UUIDv4；有限重试复用 |
| `device_id` | IoT 后台稳定设备 ID |
| `updated_point` | 本次成功采集并更新的属性键 |
| `collected_at` | UTC RFC3339 时间，固定毫秒精度并以 `Z` 结尾 |
| `values` | MPC 当时掌握的设备当前有界属性快照 JSON 对象 |

示例：

```text
event_id      = 550e8400-e29b-41d4-a716-446655440000
device_id     = device-1
updated_point = temperature
collected_at  = 2026-08-11T01:02:03.456Z
values        = {"pressure":18.2,"running":true,"temperature":23.5}
```

协议不允许任何额外字段。UUIDv4 只提供稳定随机事件身份，不表达设备顺序、MPC 进程代次或业务时间。

## 4. 大小与类型限制

MPC 在写 Redis 前执行与 Judge 一致的预校验：

- `event_id` 必须是规范小写 UUIDv4；
- 事件总预算最多 64 KiB，并为 Redis Stream ID 预留字节；
- `values` 最多 64 个属性；
- 属性键最多 128 个 Unicode 字符且最多 512 字节；
- `device_id` 最多 64 个 Unicode 字符；
- 字符串值最多 8 KiB；
- 数字文本最多 64 字节，不能是 NaN 或 Infinity；
- `values` 只允许单层 JSON 标量；
- `updated_point` 必须存在于 `values`，其值不能为 null；
- 不允许嵌套对象、数组或非法 UTF-8。

UUID 生成或事件校验失败时，MPC 仍尝试写实时快照，记录 `SOURCE_EVENT_INVALID`，但不发布不完整的 Source 事件。

## 5. 重试与丢失语义

`retry_count` 表示首次 Pipeline 中 `XADD` 失败后额外执行的 `XADD` 次数。所有尝试复用同一个 UUID 和同一个五字段 map，不重新生成身份或快照。

| 配置 | 默认值 | 最大值 |
|---|---:|---:|
| `write_timeout_ms` | 200 ms | 2000 ms |
| `retry_count` | 1 | 3 |
| `retry_interval_ms` | 20 ms | 1000 ms |
| `max_event_bytes` | 65536 | 65536 |

达到重试上限后，MPC 记录稳定 `reason_code`、放弃该 Source 事件并继续采集。没有本地或后台补发，所以 Redis 故障超过重试窗口时可能丢失规则事件。Judge 不通过会话、序号或重启推断缺口，也不会因此清空连续、窗口、计时状态或关闭活动报警。

结果组合：

- `SET` 成功、`XADD` 失败：实时快照和历史写入继续成功，只丢 Source 事件；
- `SET` 失败、`XADD` 成功：Judge 已收到事件，但实时快照写入返回失败；
- 两者都失败：记录两个失败边界，随后继续下一次采集。

## 6. 失败原因码

| 原因码 | 含义 |
|---|---|
| `SOURCE_EVENT_INVALID` | UUID、事件字段、类型或大小不满足协议 |
| `REDIS_TIMEOUT` | Redis 命令或网络超时 |
| `REDIS_UNAVAILABLE` | 连接拒绝、重置、断网或服务退出 |
| `REDIS_MEMORY_EXHAUSTED` | Redis OOM 或达到 `maxmemory` |
| `REDIS_READONLY` | 当前连接指向只读副本 |
| `REDIS_PERSISTENCE_ERROR` | Redis RDB/AOF `MISCONF` 拒绝写入 |
| `REDIS_POOL_EXHAUSTED` | Redis 连接池等待超时 |
| `REDIS_AUTH_FAILED` | 密码、ACL 或权限错误 |
| `REDIS_KEY_TYPE_INVALID` | `judge:source` 类型错误 |
| `REDIS_WRITE_FAILED` | 无法归入以上类别的写入错误 |

## 7. 并发与性能

现有设备快照锁覆盖同一设备的快照更新、UUID 生成、Pipeline 和有限重试，保证同设备串行；不同设备仍可并行。没有额外 session map、sequence map 或待发送队列。

每个事件增加一次 16 字节安全随机读取和五字段编码。Pipeline 合并 `SET` 与 `XADD`，不增加正常路径的 Redis 网络往返次数。

## 8. 环境变量

| 环境变量 | 对应配置 |
|---|---|
| `JUDGE_SOURCE_ENABLED` | `judge_source.enabled` |
| `JUDGE_SOURCE_STREAM` | `judge_source.stream` |
| `JUDGE_SOURCE_WRITE_TIMEOUT_MS` | `judge_source.write_timeout_ms` |
| `JUDGE_SOURCE_RETRY_COUNT` | `judge_source.retry_count` |
| `JUDGE_SOURCE_RETRY_INTERVAL_MS` | `judge_source.retry_interval_ms` |
| `JUDGE_SOURCE_MAX_EVENT_BYTES` | `judge_source.max_event_bytes` |
| `REDIS_DB` | `redis.db` |

无法解析的整数、负数 Redis DB 或非法布尔环境变量不会覆盖配置文件中的值。
