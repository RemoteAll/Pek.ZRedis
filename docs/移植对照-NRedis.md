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
| 队列 | `RedisQueue.cs` | `queues/queue.rs` | `src/queues/queue.zig` | ✅ `LPUSH`/`RPOP`/`BRPOP`，追踪字段注入，发送失败重试 |
| 可靠队列 | `RedisReliableQueue.cs` | `queues/reliable.rs` | `src/queues/reliable.zig` | ✅ 确认/回滚/发布消费/死信/错误计数/延迟补投 |
| 延迟队列 | `RedisDelayQueue.cs` | `queues/delay.rs` | `src/queues/delay.zig` | ✅ `ZADD` 到期分数、`ZRANGEBYSCORE`+`ZREM` 抢占、批量转移 |
| Stream 消息队列 | `RedisStream.cs` | `queues/stream.rs` | `src/queues/stream.zig` | ✅ 基元/对象字段展开、消费组、确认/挂起/抢夺、死信、裁剪与区间读 |
| 服务层 | `Services/*`（Stat / Deferred / RedLock / EventBus） | `services.rs` | `src/services/{stat,deferred,redlock,eventbus}.zig` | ✅ RedLock/延迟批处理/流式统计/事件总线 |
| Tair 扩展 | `FullRedis.Ex*` | `tair.rs` | `src/full.zig`（`Ex*`） | ✅ 字符串/哈希版本、`EXHINCRBY`、逐字段 TTL、`EXH*` 全家族 |
| 拓扑 | `Clusters/*`（Cluster/Sentinel/Replication） | `cluster/*` | `src/cluster.zig` + `src/topology_discovery.zig` | ✅ 槽位/节点解析/选路/重定向/健康退避/INFO 发现；自动刷新 ⏳ |
| TLS | `Ssl` / `rediss://` | rustls | `src/client.zig`（`zzig.tls_client`） | ✅ 握手/读写接入；正向 PING/SET/GET 已验证（`tools/regression.ps1` TLS 阶段）；`Certificate` 固定证书、`rediss://` 自动 TLS 已对齐 |
| TLS 客户端证书（mTLS） | —（无） | —（无） | — | ⛔ **参考实现都没有**，不在对齐范围（证据见「六之四」） |
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

## 五、队列 / 服务 / Tair（已验证）

- **普通队列**：`LPUSH` 生产、`RPOP`/`BRPOP` 消费；`Settings.attachTraceAlloc` 仅对 JSON 对象注入追踪字段。
- **可靠队列键布局**：`{key}`、`{key}:Ack:{8位消费者}`、`{key}:Status:{消费者}`（PascalCase JSON，7 天）、
  `{key}:AllStatus`（`SET NX EX RetryInterval` 抢清理权）、`{key}:Delay`（ZSet，`score = Unix 秒 + 延迟`）；
  死信判定 `LastActive + RetryInterval * 10 < Now`；错误计数写**备份库**（`db == 15 ? 0 : db + 1`）的 `{key}:Error:{md5}`，30 天、累计 10 次丢弃。
- **服务层**：`RedisDeferred`（`SPOP` 批处理 + 失败回写）、`RedisStat`（`RENAME` 快照 + `HGETALL` + i32 校验）、
  `RedLock`（**普通 SET**（与 C# 一致）、`quorum = n/2+1`、漂移 1%、200ms+jitter 重试、比较令牌的 Lua 释放；释放脚本按 `FullRedis` 前缀拼完整键）。
- **Tair**：`EXSET/EXGET`、`EXINCRBY`（返回 `[值, 版本]`）、`EXHSET/EXHGET/EXHMGET/EXHGETWITHVER/EXHINCRBY`、
  `EXHPTTL`、`EXHKEYS/EXHVALS/EXHLEN/EXHDEL`（`EXHINCRBY` 按 Rust 取整数返回）。
- 证据：`tests/queues_services.zig` 6 个用例（普通/延迟/可靠队列、延迟批处理、统计、RedLock、Tair）。

### 五之二、Stream / EventBus（已验证）

- **字段展开**：基元 → `__data`；对象 → 属性扁平化（PascalCase）；数组（JSON）→ 两两成对的字段名/值；
  每 1000 条带一次 `MAXLEN ~ 1000000` 裁剪。
- **消息解析**：`toStruct` 先全部按字符串解析（保留 `"0012"` 前导零），失败再回退启发式类型；
  解析时 **`.allocate = .alloc_always`**（默认会借用输入缓冲，临时缓冲释放后会得到悬空切片）。
- **消费组**：`retry_ack` 节流 `RetryInterval`（默认 60s）→ `XPENDING` + `XCLAIM` 抢回超时未确认消息，
  投递次数达 `max_retry` 直接确认丢弃；顺带清理空闲 > 1 小时的消费者；`from_last_offset` 首消费 `SETID $`。
- **EventBus**：默认 `from_last_offset = true`（首次消费前的消息不补），保留期 3 天 / 维护周期 10 分钟，
  处理成功才 `XACK`；失败保持挂起等 `retry_ack` 抢回。
- 证据：`tests/stream.zig` 4 个用例（基础写入/读取/裁剪/删除、消费组、独立消费+抢夺、EventBus）。

### 五之三、拓扑（已验证部分）

- **哈希槽**：CRC16-XMODEM + `{hash tag}`（空 tag 取整 key）；实测 `foo=12182`、`bar=5061`、`{user1000}.following=3443`。
- **节点模型**：`CLUSTER NODES` 单行解析（`@` 后缀、flags、master-id、槽位区间、`[slot->node]` 迁移）；
  `fail`/`fail?` 置 `link_up=false`，`slave`/`replica` 置 `is_replica`。
- **选路**：写优先主库；读可按 `ReadFromReplicas` 选从；失败按 1s/2s/4s…（上限 64s）退避后重选；
  `MOVED` 记入槽位覆盖，`ASK` 不记（临时跳转）。
- **执行侧**：`Redis.executeOnEndpoint`（按端点惰性建池）与 `Redis.executeOnKey`
  （选路 → 执行 → `MOVED`/`ASK` 跟进 + `ASKING` → 失败重选，最多 8 跳）。
- **发现**：`parseInfo` + `detectMode`（cluster / sentinel / replication）+ `parseDiscovery`
  （`master_host:port`、`slaveN`、`masterN` 地址）+ `buildReplicationNodes`；`INFO` 解析已下沉到 `src/info.zig`（解除循环导入）。
- **自动拓扑**：`Redis.refreshTopology` 按模式拉取（cluster → `CLUSTER NODES`；sentinel → `INFO Sentinel` 找主库再建复制拓扑；
  replication → `INFO`；standalone → 单节点；auto → 先 `INFO` 判型）；`executeAuto` 按 `TopologyRefreshSeconds` 惰性刷新（0 = 不自动刷），
  刷新失败**保留旧拓扑**并写诊断（新拓扑先在锁外构建再原子替换，避免刷新期间锁内递归）。
- **命令级自动选路**：启用拓扑后 `Redis.execute` / `executeBlocking` 按单键命令表（key 固定第 1 参数）自动选节点，
  自动处理 `MOVED`/`ASK`/失败重选；因此**各结构体（哈希/列表/集合/有序集合/队列/Stream/Tair）的单键命令自动生效**。
  多键：`MGET` / `DEL` 按节点**分组执行再按原顺序汇总**；`setAll` 在拓扑模式下转逐键 `SET`（EXPIRE 与键同节点）；
  跨键命令（`RENAME`/`SMOVE`/`BITOP`/`EVAL`/`SUNION` 等）按**第一个 key** 选路（key 下标特例：`BITOP`=2、`EVAL`=3），
  同槽约束交给服务端（不同槽返回 `CROSSSLOT`，与 C#/Rust 一致）。
- 证据：`src/cluster.zig` 5 个单元用例、`src/topology_discovery.zig` 2 个单元用例、`tests/cluster.zig` 1 + `tests/topology_auto.zig` 3 个集成用例
  （含双服务端按槽位真实分流验证）。

## 六、尚未实现（下一里程碑输入）

**暂无必须项。** 唯一未验证的一块是「真实集群/哨兵实机」，2026-11-18 用户决定**暂缓**（原话：
**「真实集群目前没有，我也不知道Redis该怎么弄集群。这个可以先不用管了吧，等后面有需要再推进」**）。

- 现状够用的部分：拓扑逻辑本身已有覆盖 —— `src/cluster.zig` 5 个单元用例（CRC16/槽位/`CLUSTER NODES` 解析/重定向解析）、
  `tests/topology_auto.zig` 3 个集成用例（cluster 刷新、**双单机 mock 按槽位真实分流**含 `RENAME`/`MGET`/`DEL`、replication 发现）。
- 没验证的部分（如实标注，不猜）：真实 `redis-server --cluster-enabled yes` 的 16384 槽拓扑、
  跨节点 `MOVED`/`ASK` 的实际往返、`redis-sentinel` 主从切换后的拓扑刷新。
- **触发条件**：只有在有真实集群/哨兵环境时才推进；**不要为了这条待办去搭集群环境**，
  也不要把它写成「已知缺陷」（现有单机 / 主从 / 静态拓扑用法不受影响）。

## 六之四、TLS 配置口径对齐（2026-11-18 第十四轮）

**先纠正一条错误结论**：此前把「客户端证书（mTLS）」列为缺口，是**我自己的假设**，参考实现里并没有：

- C# `DH.NRedis\RedisClient.cs:134`：`AuthenticateAsClient(host, [], sp, false)` —— **客户端证书集合是空数组**；
  `Redis.cs:68-70` 的 `Certificate` 只用于 `RedisClient.cs:238-247` 的
  `OnCertificateValidationCallback`（`chain.ChainElements.Any(x => x.Certificate.Thumbprint == cert.Thumbprint)`）。
- Rust `Pek.RRedis\client.rs:45-50`：只有 `tls` / `tls_server_name` / `tls_insecure`，`tls_client_config()` 只装 roots 与
  insecure 校验器，**无客户端证书**。

→ **mTLS 不属于对齐范围**，已从待办移除。

### 已对齐的键名与行为

| C# 键 | Zig 实现 | 说明 |
| --- | --- | --- |
| `Ssl` / `Tls` / `UseTls` | ✅ | 布尔开 TLS |
| `rediss://` 前缀 | ✅ | 自动置位 TLS（与 C# 一致） |
| `SslProtocol` / `TlsProtocol` | ✅ 解析、⚠️ 见偏离 2 | `None` / `Tls12` / `Tls13` / `Tls12, Tls13` |
| `Certificate` / `Cert` / `CertificateFile` | ✅ 见偏离 1 | 固定服务端证书 |
| `CertificatePassword` / `CertPassword` | ❌ 明确报错 | PKCS#12 不支持 |

### 有意的偏离（都不是「没做」，而是不能或不该照抄）

1. **证书匹配方式**：C# 比对**证书链中任一环的 SHA-1 指纹**；Zig 把该证书作为**唯一信任锚**（只信这张，或由它签发的）。
   自签证书、固定 CA 场景等价；「公共 CA 签发 + 固定叶子指纹」这种组合 C# 通过、Zig 拒绝（更严）。
2. **单版本锁定**：C# 可用 `SslProtocols.Tls12` 锁定；zzig 的 TLS 客户端固定按 `TLS1.3 / 1.2` 协商，**不暴露版本约束**
   → Zig 侧 `TlsProtocol=Tls12|Tls13` **明确报错**（`error.Config` + 文案「不支持强制单一 TLS 版本」），
   **绝不静默忽略**（静默忽略会让运维以为版本已锁定）。
3. **`CertificatePassword`（PFX）**：Zig std 无 PKCS#12 解密 → 明确报错，引导改用 PEM。
4. **没给证书时的默认行为**：C# `OnCertificateValidationCallback` 在 `Certificate == null` 时 **`return true`（完全不校验）**；
   Zig 默认走**系统根证书 + 主机名校验**，只有显式 `TlsInsecure=1` 才跳过。这是**有意的安全强化**，保留。

### 顺带修掉的两个真实缺陷

- **失败路径泄漏**：`openTls` 在加载固定证书 / 扫系统根后若握手失败，返回前没释放证书束 →
  `error(SafeAllocator): leaked ... hash_map / array_list`（实测确认）；已补 `errdefer { st.ca.deinit(gpa); ... }`。
- **诊断丢失**：`connect` 失败时 `Client` 被丢弃，池层只能报 `Tls` / `Connect`；现用线程局部 `lastConnectError()`
  由池层拼接（实测输出：`建立连接失败（…）：TLS 握手失败（…）：TlsCertificateNotVerified`）。
  同时 `Client.init` 失败不再打印默认的 `warning/close_notify`（此前会误导）。
- **CLI 退出码**：`RESULT=FAIL` 现在 `exit 1`（此前一律 0），可直接接 CI 与脚本断言。

### 实测（`tools/regression.ps1` TLS 阶段，全部自动化）

```
PASS: TLS 正向 PING / SET / GET（127.0.0.1:<port>）
PASS: TLS 固定证书（Certificate=正确证书）连接成功
PASS: TLS 固定证书拒绝非指定证书（TlsCertificateNotVerified）
PASS: TlsProtocol 单版本明确报错（不静默忽略）
PASS: rediss:// 自动启用 TLS
```

单元 **48/48**（`TlsProtocol.parse` 各分支 + `Certificate` 别名解析 2 个新用例）、集成 **26/26**。

## 六之三、跨语言严格对拍（2026-11-18 第十三轮已落地）

- Zig 侧：`src/interop.zig` + CLI `zredis interop write|verify|clean|report <config> [prefix]`。
  **写走封装 API**（`RedisHash`/`RedisList`/`RedisSet`/`RedisSortedSet`/`RedisQueue`/`RedisDelayQueue`/
  `RedisReliableQueue`/`RedisStream`），**校验走裸命令**（GET/HGETALL/LRANGE/SMEMBERS/ZSCORE/LINDEX/ZRANGE/XRANGE），
  避免「自己写自己读」自证。
- 外部口径：`tools/interop-strict.ps1` 用 **redis-cli** 作独立裁判，双向验证：
  ① Zig 写 → redis-cli 读（逐字节断言）；② redis-cli 写（`--pipe` 灌精确 RESP）→ Zig 读；③ Zig 写 → Zig 读（新进程）。
- 覆盖的契约点：字符串（UTF-8 / **二进制** `00 01 0d 0a 22 ff` / TTL）、计数器、布尔 `True`、时间 `yyyy-MM-dd HH:mm:ss.fff`、
  哈希、列表保序、集合、**ZSET score 文本**（`1.5` / `-2.25`）、普通队列、延迟队列（`score = now + delay` 秒）、
  可靠队列主列表、Stream（`id` 形如 `毫秒-序号` + 字段）、锁值 `token|绝对毫秒`（`parseLockExpire` 口径）。
- 隔离：脚本自建**临时实例**（随机端口 + 临时目录 + `save ""`/`appendonly no`），跑完 `Stop-Process` + 删目录，
  **绝不碰用户已有实例**（`D:\Redis` 的 6379 有真实数据）。缺 `redis-server`/`redis-cli` 时打 `SKIP` + 原因，不计 PASS。
- 实测：`RESULT=PASS（21 项检查）`；`zig build unit` **46/46**、`integration` **26/26**。
- 踩到的环境坑：本机 `D:\Redis\redis-server.exe` 是 **MSYS2 构建**，**不认 Windows 绝对路径**
  （会把 cwd 前缀拼到参数前 → `can't open config file '/cygdrive/g/.../C:\...'`）；
  改为「相对配置名 + `-WorkingDirectory`」启动。PS 5.1 下灌二进制（含 `0x0d 0x0a`）必须用 `--pipe` + 字节文件，
  不能走 argv（会被 ANSI 重编码）。

## 六之二、TLS 正向往返（2026-11-18 第十二轮已通过）

- 夹具：`tests/support/tls/`（openssl 自签 SAN `localhost`/`127.0.0.1` 10 年 + 最小 TLS RESP 服务端，键空间进程级共享）。
- 入口：`tools/regression.ps1` 的 `Invoke-TlsRoundTrip`（缺 python/夹具时打 `SKIP` + 原因，不计 PASS）。
- 实测：`TLS PING / SET / GET` 全 `RESULT=OK`，`GET` 取回 `LEN=5 VALUE=world`；`zig build unit` **44/44**、`integration` **26/26**。
- **根因（两处，缺一不可）**：
  1. **写**：`tls_client.Client.writer.flush()` 只把密文写进 `output`（socket 写缓冲），**不冲刷 socket**
     （zzig `tls_client.zig` 的 `flush`/`end` 均如此，只有 `init` 末尾 `output.flush()`）。
     → `client.writeAll` 在 `writer.flush()` 后必须再 `stream_writer.interface.flush()`；`TlsState.destroy` 发 close_notify 后同理。
  2. **读**：`std.Io.Reader` 允许实现「忽略 `data`、写自身缓冲并 `return 0`」，zzig TLS 客户端正是该形态
     （应用数据 `r.end += cleartext.len` 后 `return 0`；`NewSessionTicket` 也 `return 0`）。
     → 用 `peekGreedy(1)` 取明文、`toss(n)` 消费；**「返回 0」不是 EOF**，只有 close_notify 才是。
- 取证方式：带阶段日志的诊断 TLS 服务端打印 `accepted / handshake_ok / recv hex / recv_eof`，
  一眼看出「握手成功但零应用字节」→ 写方向缓冲未冲刷；随后的 `readVec n=0 tls_buffered=0` 定位读方向误判。
- 回归护栏：`src/resp.zig`「TLS 明文读取：实现写自身缓冲并返回 0 时不能当成 EOF」用 mock 复刻该实现，
  **撤掉修复该用例失败**（实测 43/44），恢复后 44/44。

## 七、Zig 0.17 实测坑（已规避，勿回退）

1. **按值返回会悬空指针**：`resp.Reader` 原保存 `*const Stream`、`Client` 原保存 `*const ConnConfig`，
   构造函数返回值被拷贝后指针指向旧栈帧 → Windows AFD 句柄读报 `INVALID_HANDLE`。
   现改为 **Reader 按值持有 `Stream`、Client 按值快照 `ConnConfig`**。
2. **Windows 连接超时不可用**：`netConnectIpWindows` 对 `timeout != .none` 直接 `@panic`。
3. **Windows 带超时 I/O 不可用**：`operateTimeout` 走 `batchAwaitConcurrent` → `error.ConcurrencyUnavailable`。
   现改为套接字 `SO_RCVTIMEO` / `SO_SNDTIMEO`（`client.setSocketTimeouts`）。
4. **数组所有权**：`Redis.expectArray` 返回**借用**切片，元素由 `Value.deinit` 统一释放（曾因只 free 数组导致 bulk 泄漏）。
5. **PS 5.1**：中文 `.ps1` 必须 UTF-8 with BOM；外部命令 stderr 在 `$ErrorActionPreference='Stop'` 下会被当终止错误，回归脚本内已封装。
