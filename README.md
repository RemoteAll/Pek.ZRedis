# Pek.ZRedis

Pek 生态的 **Zig** Redis 客户端 —— 语义与字节格式对齐两个参考实现，三方连接**同一个 Redis 实例**、
使用**同一套键与编码**：

| 参考实现 | 位置 |
| --- | --- |
| Rust `pek-rredis` | `G:\Code\Rust\Pek.RRedis` |
| C# `DH.NRedis` | `G:\Code\Pek.FrameWork\DH.NRedis` |

```text
┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐
│ C#（DH.NRedis）      │   │ Rust（Pek.RRedis）   │   │ Zig（Pek.ZRedis）    │
│ FullRedis / 各结构   │◄─►│ FullRedis / 各结构   │◄─►│ FullRedis / 各结构   │
└──────────┬───────────┘   └──────────┬───────────┘   └──────────┬───────────┘
           └──────────── 同一 Redis（RESP2 / RESP3） ────────────┘
```

## 当前能力（v0.1.0）

| 模块 | 对应实现 | 状态 |
| --- | --- | --- |
| `resp` | `RedisClient` 组包/解析 | ✅ RESP2 全量 + RESP3（Map/Set/Double/Bool/Null/Verbatim/Push/BigNumber/Attribute） |
| `options` | `Redis.Init` / `RedisOptions` / `RedisPoolConfig` | ✅ 连接串全量键、多地址、`rediss://` 识别、池参数默认值一致 |
| `client` | `RedisClient` | ✅ TCP、`HELLO`/`AUTH`/`SELECT` 惰性握手、管道、阻塞命令、超时与坏连接标记 |
| `pool` | `ObjectPool<RedisClient>` | ✅ Min/Max/IdleTime/MaxLifetime/WaitTimeout、空闲 PING、坏连接销毁 |
| `encoder` | `RedisJsonEncoder` / `DefaultPacketEncoder` | ✅ 字符串/布尔/整数/浮点/时间/JSON 字节格式逐条对齐；`JsonDateTime` / `DateTimeText` 兼容 FastJson |
| `redis` | `Redis`（基础命令） | ✅ 键空间 / 字符串 / 过期 / 批量 / 数值 / 位图命令面 / `INFO` / 版本门禁 / 脚本 / 管道 |
| `full` | `FullRedis` | ✅ 前缀（含 `EnsureStart` 语义）、`SCAN` 搜索、按模式删除、分布式锁、结构体工厂 |
| 结构体 | `RedisHash` / `RedisList` / `RedisSet` / `RedisSortedSet` / `RedisStack` | ✅ 常用命令面对齐（含阻塞出队、扫描、范围与排名） |
| `RedisGeo` / `HyperLogLog` | 同名 | ✅ 常用命令面 |
| `PubSub` | `PubSub` | ✅ 订阅 / 模式订阅 / 消息读取 |
| 队列 | `RedisQueue` / `RedisDelayQueue` / `RedisReliableQueue` | ✅ 普通/延迟/可靠队列：确认、回滚、死信、发布消费、延迟补投、错误计数（备份库） |
| 服务层 | `RedisRedLock` / `RedisDeferred` / `RedisStat` / `RedisEventBus<T>` | ✅ RedLock（普通 SET + 比较释放）、延迟批处理、流式统计（快照 + 备份库队列）、事件总线 |
| Stream 消息队列 | `RedisStream` / `Message` | ✅ 基元/对象写入、消费组、确认/挂起/抢夺、延迟死信、裁剪与区间读 |
| Tair 扩展 | `FullRedis.Ex*` | ✅ 字符串/哈希版本、`EXHINCRBY`、逐字段 TTL、`EXH*` 全家族 |
| 拓扑 | `Clusters/*`（Cluster / Sentinel / Replication / Static） | ✅ 哈希槽/`CLUSTER NODES`/选路/`MOVED` 记忆/`ASK`/健康退避/`INFO` 发现/定时刷新/`executeAuto`；`FullRedis` 结构体方法自动选路 ⏳ |
| TLS | `rediss://` / `Ssl` | ⏳ 当前**明确报错**，不降级明文 |

详细对照与缺口见 [`docs/移植对照-NRedis.md`](docs/移植对照-NRedis.md)。

## 快速开始

```zig
const std = @import("std");
const zredis = @import("pek_zredis");

var threaded = std.Io.Threaded.init(std.heap.page_allocator, .{});
defer threaded.deinit();
const io = threaded.io();

var fr = try zredis.FullRedis.open(std.heap.page_allocator, io, "127.0.0.1:6379", null, 7, "app:");
defer fr.deinit();

_ = try fr.set("user", "NewLife", 3600);
const name = (try fr.getRaw(std.heap.page_allocator, "user")).?;
defer std.heap.page_allocator.free(name);

// 结构体（键自动补前缀）
var h = try fr.getHash("counter");
defer h.deinit();
_ = try h.incrBy("pv", 1);
```

> `FullRedis` 内部的 `Redis` 由值持有；**拿到结构体后不要再移动 `FullRedis`**（结构体保存了 `*Redis`）。

## 构建与测试

```powershell
zig build                       # 编译库 + CLI（zig-out\bin\zredis.exe）
zig build test                  # 单元 + 集成（回环假 Redis，无需真实服务端）
zig build unit                  # 仅单元测试
zig build integration           # 仅集成测试
zig fmt --check src build.zig tests
powershell -NoProfile -ExecutionPolicy Bypass -File tools\regression.ps1
```

CLI 对拍/运维子命令：

```powershell
.\zig-out\bin\zredis.exe ping      "server=127.0.0.1:6379;db=0"
.\zig-out\bin\zredis.exe set       "server=127.0.0.1:6379;db=0" user NewLife 3600
.\zig-out\bin\zredis.exe get       "server=127.0.0.1:6379;db=0" user
.\zig-out\bin\zredis.exe info      "server=127.0.0.1:6379;db=0"
.\zig-out\bin\zredis.exe selfcheck "server=127.0.0.1:6379;db=0"
```

## 互通契约（要点）

| 项 | 口径 |
| --- | --- |
| 字符串 | 原始 UTF-8，**不加引号** |
| 布尔 | 写 `True` / `False`；读兼容 `true` / `OK` / `1` / `0` |
| 整数 / 浮点 | 十进制文本；`NaN` / `Infinity` / `-Infinity` |
| 时间 | `yyyy-MM-dd HH:mm:ss.fff`；读兼容 ISO 8601 / FastJson 文本 |
| 复合对象 | 紧凑 JSON（字段名 PascalCase 即可与 C# 对齐） |
| 键前缀 | `EnsureStart`：已有前缀不重复添加，比较不区分大小写 |
| 分布式锁 | 值 `{token}|{绝对过期毫秒}`，TTL `ceil(msExpire/1000)`，时钟域 = 启动毫秒（`GetTickCount64`） |

## 已知平台约束（Zig 0.17 / Windows）

- `netConnectIpWindows` 与带超时的 `operateTimeout` 在 Windows Threaded 后端是 TODO/不可用：
  连接阶段不设超时；读写超时通过套接字 `SO_RCVTIMEO` / `SO_SNDTIMEO` 设置。
- `Threaded.io()` 返回的 `Io` 指向 `Threaded` 实例本身，**不能按值拷贝**；库内部已按值持有句柄/配置，
  调用方只需保证 `Threaded` 的生命周期覆盖客户端。
