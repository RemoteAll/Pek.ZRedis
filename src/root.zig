//! Pek.ZRedis —— DH.NRedis（C#）与 Pek.RRedis（Rust）的 Zig 实现。
//!
//! 语义与字节格式对齐两个参考实现，三方连接**同一个 Redis 实例**、使用**同一套键与编码**：
//!
//! ```text
//! ┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐
//! │ C#（DH.NRedis）      │   │ Rust（Pek.RRedis）   │   │ Zig（Pek.ZRedis）    │
//! │ FullRedis / 各结构   │◄─►│ FullRedis / 各结构   │◄─►│ FullRedis / 各结构   │
//! └──────────┬───────────┘   └──────────┬───────────┘   └──────────┬───────────┘
//!            └──────────── 同一 Redis（RESP2 / RESP3） ────────────┘
//! ```
//!
//! 模块地图：
//! - `resp`：RESP2/RESP3 编解码；
//! - `options`：连接串解析（与 C# `Init` 同格式）；
//! - `client` / `pool`：单连接与连接池；
//! - `encoder`：值编码（字符串/布尔/整数/浮点/时间/JSON）；
//! - `redis`：基础命令层；
//! - `full`：键前缀、SCAN 搜索、分布式锁；
//! - `hash` / `list` / `set` / `sortedset` / `stack` / `geo` / `hyperloglog` / `pubsub`：数据结构。

const std = @import("std");

pub const resp = @import("resp.zig");
pub const options = @import("options.zig");
pub const encoder = @import("encoder.zig");
pub const client = @import("client.zig");
pub const pool = @import("pool.zig");
pub const redis = @import("redis.zig");
pub const full = @import("full.zig");
pub const hash = @import("hash.zig");
pub const list = @import("list.zig");
pub const set = @import("set.zig");
pub const sortedset = @import("sortedset.zig");
pub const stack = @import("stack.zig");
pub const geo = @import("geo.zig");
pub const hyperloglog = @import("hyperloglog.zig");
pub const pubsub = @import("pubsub.zig");
pub const cluster = @import("cluster.zig");
pub const info = @import("info.zig");
pub const topology_discovery = @import("topology_discovery.zig");
pub const interop = @import("interop.zig");

pub const queues = struct {
    pub const base = @import("queues/base.zig");
    pub const status = @import("queues/status.zig");
    pub const queue = @import("queues/queue.zig");
    pub const delay = @import("queues/delay.zig");
    pub const reliable = @import("queues/reliable.zig");
    pub const stream = @import("queues/stream.zig");
};

pub const services = struct {
    pub const redlock = @import("services/redlock.zig");
    pub const deferred = @import("services/deferred.zig");
    pub const stat = @import("services/stat.zig");
    pub const eventbus = @import("services/eventbus.zig");
};

pub const Redis = redis.Redis;
pub const FullRedis = full.FullRedis;
pub const RedisHash = hash.RedisHash;
pub const RedisList = list.RedisList;
pub const RedisSet = set.RedisSet;
pub const RedisSortedSet = sortedset.RedisSortedSet;
pub const RedisStack = stack.RedisStack;
pub const RedisGeo = geo.RedisGeo;
pub const HyperLogLog = hyperloglog.HyperLogLog;
pub const PubSub = pubsub.PubSub;
pub const RedisQueue = queues.queue.RedisQueue;
pub const RedisDelayQueue = queues.delay.RedisDelayQueue;
pub const RedisReliableQueue = queues.reliable.RedisReliableQueue;
pub const RedisStream = queues.stream.RedisStream;
pub const StreamMessage = queues.stream.Message;
pub const QueueSettings = queues.base.Settings;
pub const QueueStatus = queues.status.QueueStatus;
pub const RedisDeferred = services.deferred.RedisDeferred;
pub const RedisStat = services.stat.RedisStat;
pub const RedisEventBus = services.eventbus.RedisEventBus;
pub const RedLock = services.redlock.RedLock;
pub const acquireRedLock = services.redlock.acquireRedLock;
pub const ClusterNode = cluster.ClusterNode;
pub const Topology = cluster.Topology;
pub const StaticTopology = cluster.StaticTopology;
pub const RedisClusterTopology = cluster.RedisClusterTopology;
pub const RedisReplicationTopology = cluster.RedisReplicationTopology;
pub const Client = client.Client;
pub const Pool = pool.Pool;
pub const Value = resp.Value;
pub const Payload = encoder.Payload;
pub const JsonDateTime = encoder.JsonDateTime;
pub const DateTimeText = encoder.DateTimeText;
pub const Error = client.Error;
pub const ServerType = redis.ServerType;
pub const InfoMap = redis.InfoMap;

test {
    // 引入各模块内的单元测试（`zig build unit` / `zig test src/root.zig`）
    std.testing.refAllDecls(@This());
}
