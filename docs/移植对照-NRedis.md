# 移植对照：DH.NRedis（C#） / Pek.RRedis（Rust） → Pek.ZRedis（Zig）

> 权威口径：以本地源码为准，不凭印象。
> - C# `G:\Code\Pek.FrameWork\DH.NRedis\DH.NRedis\`（`Redis.cs`、`FullRedis.cs`、`RedisClient.cs`、`RedisEncoder.cs`、各结构体/队列）
> - Rust `G:\Code\Rust\Pek.RRedis\src\`（`resp.rs`、`client.rs`、`options.rs`、`encoder.rs`、`redis.rs`、`full.rs`…）
> - Zig 本仓库 `src\`
>
> 本文件既是对照表，也是**缺口清单**（未做项不是遗忘）。

## 一、模块对照与状态

| 能力 | C#（DH.NRedis） | Rust（Pek.RRedis） | Zig（Pek.ZRedis） | 状态 |
| --- | --- | --- | --- | --- |
| RESP 协议 | `RedisClient.cs` `GetRequest` / `ParseResponse` | `resp.rs` | `src/resp.zig` | ✅ 组包逐字节一致；RESP2 全量 + RESP3 扩展类型 |
| 连接串 / 选项 | `Redis.cs` `Init` / `RedisOptions` / `RedisPoolConfig` | `options.rs` | `src/options.zig` | ✅ 键集合与默认值（Min=10/Max=100000/Idle=30/Lifetime=300/Wait=15）一致 |
| 连接 / 握手 | `RedisClient` `CheckLogin`/`CheckSelect` | `client.rs` | `src/client.zig` | ✅ `HELLO 3` → `AUTH` → `SELECT`；`-ERR` 转 `error.Server` |
| 连接池 | `ObjectPool<RedisClient>` / `MyPool.OnGet` | `pool.rs` | `src/pool.zig` | ✅ 空闲 PING、生命周期淘汰、池满等待 |
| 值编码 | `RedisEncoder.cs` / `DefaultPacketEncoder` | `encoder.rs` | `src/encoder.zig` | ✅ 字符串/布尔/整数/浮点/时间/JSON；读兼容宽松形态 |
| 基础命令 | `Redis.cs`（1046 行） | `redis.rs`（1717 行） | `src/redis.zig` | ✅ 键空间/字符串/过期/批量/数值/位图/INFO/版本门禁/脚本/管道 |
| 前缀 / 搜索 / 锁 | `FullRedis.cs` / `Cache.AcquireLock` | `full.rs` | `src/full.zig` | ✅ `EnsureStart`、SCAN 搜索（去前缀）、按模式删除、锁（`{token}|{绝对过期}`） |
| 哈希 | `RedisHash.cs` | `hash.rs` | `src/hash.zig` | ✅ 常用面（set/get/批量/计数/扫描/清零） |
| 列表 | `RedisList.cs` | `list.rs` | `src/list.zig` | ✅ 常用面（前后入队/阻塞出队/范围/移除/裁剪） |
| 集合 | `RedisSet.cs` | `set.rs` | `src/set.zig` | ✅ 常用面（增删/判存/批量判存/成员/弹出/扫描/并交差） |
| 有序集合 | `RedisSortedSet.cs` | `sortedset.rs` | `src/sortedset.zig` | ✅ 常用面（分数/排名/区间/按分查询/弹出/扫描） |
| 栈 | `RedisStack.cs` | `stack.rs` | `src/stack.zig` | ✅ `RPUSH` 进 / `RPOP` 出 |
| Geo | `RedisGeo.cs` | `geo.rs` | `src/geo.zig` | ✅ 常用面（GEOADD/GEODIST/GEOPOS/GEOHASH/GEORADIUS） |
| HyperLogLog | `HyperLogLog.cs` | `hyperloglog.rs` | `src/hyperloglog.zig` | ✅ PFADD/PFCOUNT/PFMERGE |
| 发布订阅 | `PubSub.cs` | `pubsub.rs` | `src/pubsub.zig` | ✅ 订阅/模式订阅/消息读取（独立连接） |
| 队列 | `RedisQueue.cs` | `queues/queue.rs` | — | ⏳ 下一里程碑 |
| 可靠队列 | `RedisReliableQueue.cs` | `queues/reliable.rs` | — | ⏳ 键布局与死信规则已记录（见下） |
| 延迟队列 | `RedisDelayQueue.cs` | `queues/delay.rs` | — | ⏳ |
| Stream 消息队列 | `RedisStream.cs` | `queues/stream.rs` | — | ⏳ |
| 服务层 | `Services/*`（EventBus / Stat / Deferred / RedLock） | `services.rs` | — | ⏳（RedLock 已记录算法） |
| Tair 扩展 | `FullRedis.Ex*` | `tair.rs` | — | ⏳ |
| 拓扑 | `Clusters/*`（Cluster/Sentinel/Replication） | `cluster/*` | — | ⏳ |
| TLS | `Ssl` / `rediss://` | rustls | — | ⏳ **当前显式报错，不降级明文** |
| 异步 API | `*Async`（.NET） | `async_api.rs`（tokio） | — | ➖ Zig 侧不 1:1 迁移（由调用方线程模型决定） |
| ASP.NET 集成 | `RedisCacheProvider` / `CacheExtensions` | 不迁移 | — | ➖ .NET 宿主专属 |

## 二、字节格式契约（已验证）

| 数据类型 | 写入 | 读取兼容 |
| --- | --- | --- |
| 字符串 | 原始 UTF-8（无引号） | — |
| 布尔 | `True` / `False` | `true` / `OK` / `1` / `0` |
| 整数 | 十进制文本 | 容忍空白 |
| 浮点 | 最短往返文本；`NaN` / `Infinity` / `-Infinity` | `inf` / `nan` 等 |
| 时间（值） | `yyyy-MM-dd HH:mm:ss.fff` | ISO 8601、带偏移、FastJson 文本 |
| 时间（JSON 字段） | `JsonDateTime` → ISO 8601 | 同上 |
| 时间（Stream 字段） | `DateTimeText` → `yyyy-MM-dd HH:mm:ss.fff` | 同上 |
| 复合对象 | 紧凑 JSON（PascalCase 字段名） | 容忍 BOM、忽略未知字段 |

证据（`zig build test`）：`src/encoder.zig` 的「标量编码与 C# 端一致」「时间编码…」「JSON 结构体…」「JsonDateTime 兼容…」用例。

## 三、键空间与结构

- 前缀：`FullRedis.keyAlloc` = C# `EnsureStart`（已有前缀不重复添加、比较不区分大小写）；空键返回前缀本身。
- 搜索：`FullRedis.search` 返回**去掉前缀**的键；`count = 0` 表示不限量（与 Rust 一致，修正 C# 边界行为）。
- 结构体：哈希/列表/集合/有序集合/Geo/HLL 均为原生 Redis 结构，成员字节一致即可互通。

## 四、分布式锁（与 C#/Rust 逐行对齐）

- 值：`{token}|{绝对过期毫秒}`；`token` = 32 位小写十六进制（等价 `Guid.NewGuid().ToString("N")`）。
- TTL：`ceil(msExpire / 1000)` 秒；`SET NX EX` 申请。
- 抢占：`GET` 解析过期时间，`dt <= now` 时 `SET XX` 抢占并校验旧值 `<= dt`，再 `EXPIRE` 补 TTL。
- 释放：校验 token 前缀后 `DEL`（不误删他人锁）。
- **时钟域**：`bootTicksMs()` = Windows `GetTickCount64` / POSIX `CLOCK_MONOTONIC` 毫秒，与 C# `Environment.TickCount64` 同域。
- 证据：`tests/all.zig`「分布式锁：互斥 / 超时 / 释放后可重入」。

## 五、尚未实现（下一里程碑输入）

1. **队列族**：键布局照 Rust README：
   - 普通队列 `LPUSH` / `RPOP`（阻塞 `BRPOP`）；
   - 可靠队列 `{key}`、`{key}:Ack:{ukey}`、`{key}:Status:{ukey}`（JSON PascalCase，7 天过期）、`{key}:AllStatus`（`SET NX EX RetryInterval` 抢清理权）、`{key}:Delay`（ZSet，`score = Unix 秒 + 延迟`）；
   - 死信：`LastActive + RetryInterval * 10 < Now` 回滚确认队列。
2. **Stream**：`__data` 基元约定、对象字段扁平化、消费组、`retry_ack`。
3. **服务层**：`RedisEventBus` / `RedisStat` / `RedisDeferred` / `RedisRedLock`。
4. **Tair**：`ex_set` / `ex_get` / `ex_hset` 等（需 Tair 实例）。
5. **拓扑**：Cluster slot/hash tag/`MOVED`/`ASK`、Sentinel、Replication、`ReadFromReplicas`。
6. **TLS**：接入 `zzig` 的 TLS 客户端（需把 `db/tls_client.zig` 导出为公共模块），并补 `rediss://` 正向用例。
7. **跨语言对拍工具**：C#/Rust ↔ Zig 同一实例双向读写矩阵（照 `Pek.RRedis/scripts/interop-strict.ps1` 的口径）。

## 六、Zig 0.17 实测坑（已规避，勿回退）

1. **按值返回会悬空指针**：`resp.Reader` 原保存 `*const Stream`、`Client` 原保存 `*const ConnConfig`，
   构造函数返回值被拷贝后指针指向旧栈帧 → Windows AFD 句柄读报 `INVALID_HANDLE`。
   现改为 **Reader 按值持有 `Stream`、Client 按值快照 `ConnConfig`**。
2. **Windows 连接超时不可用**：`netConnectIpWindows` 对 `timeout != .none` 直接 `@panic`。
3. **Windows 带超时 I/O 不可用**：`operateTimeout` 走 `batchAwaitConcurrent` → `error.ConcurrencyUnavailable`。
   现改为套接字 `SO_RCVTIMEO` / `SO_SNDTIMEO`（`client.setSocketTimeouts`）。
4. **数组所有权**：`Redis.expectArray` 返回**借用**切片，元素由 `Value.deinit` 统一释放（曾因只 free 数组导致 bulk 泄漏）。
5. **PS 5.1**：中文 `.ps1` 必须 UTF-8 with BOM；外部命令 stderr 在 `$ErrorActionPreference='Stop'` 下会被当终止错误，回归脚本内已封装。
