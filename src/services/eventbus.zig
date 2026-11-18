//! Redis 事件总线（对齐 DH.NRedis `Services.RedisEventBus<T>` 的 Redis 语义层 / Rust `services::RedisEventBus`）。
//!
//! Rust 侧不复刻 `.NET EventBus<T>` / DI / 本地订阅注册模型，本实现同样只提供：
//! - `publish`：写入 Stream；
//! - `consumeOnce` / `consumeLoop`：按消费组读取并在成功后确认；
//!
//! 进程内分发由调用方用自己的回调 / channel / actor 机制完成。

const std = @import("std");
const zzig = @import("zzig");
const stream_mod = @import("../queues/stream.zig");
const full_mod = @import("../full.zig");
const redis_mod = @import("../redis.zig");

pub const FullRedis = full_mod.FullRedis;
pub const RedisStream = stream_mod.RedisStream;
pub const Message = stream_mod.Message;
pub const Error = redis_mod.Error;

pub fn RedisEventBus(comptime T: type) type {
    return struct {
        const Self = @This();

        gpa: std.mem.Allocator,
        stream: RedisStream,
        /// 保留期（秒）；0 表示关闭按时间裁剪。默认 3 天
        expire_seconds: i64 = 3 * 24 * 3600,
        /// 维护周期（秒）；0 表示每次发布/消费前都维护。默认 10 分钟
        maintenance_interval_seconds: i64 = 600,
        last_maintenance_ms: i64 = 0,

        /// 创建事件总线。默认 `from_last_offset = true`，与 C# 默认行为一致。
        pub fn init(redis: *FullRedis, topic: []const u8, group: []const u8) !Self {
            var stream = try redis.getStream(topic);
            errdefer stream.deinit();
            stream.from_last_offset = true;
            _ = try stream.setGroup(group);
            return .{ .gpa = redis.gpa, .stream = stream };
        }

        pub fn deinit(self: *Self) void {
            self.stream.deinit();
        }

        /// 底层流（可调整 `retry_interval_seconds` / `max_retry` / `block_time_ms` 等）。
        pub fn streamRef(self: *Self) *RedisStream {
            return &self.stream;
        }

        /// 设置首次消费策略（默认 true，从最新位置开始）。
        pub fn setFromLastOffset(self: *Self, enabled: bool) void {
            self.stream.from_last_offset = enabled;
        }

        /// 设置保留期（秒）；`null` 表示关闭按时间裁剪。
        pub fn setExpire(self: *Self, seconds: ?i64) void {
            self.expire_seconds = seconds orelse 0;
        }

        /// 设置维护周期（秒）；0 表示每次发布/消费前都尝试维护。
        pub fn setMaintenanceInterval(self: *Self, seconds: i64) void {
            self.maintenance_interval_seconds = seconds;
        }

        /// 发布事件，返回 Stream 消息 Id（调用方释放）。
        pub fn publish(self: *Self, event: anytype) Error![]u8 {
            try self.maybeMaintain();
            const id = (try self.stream.add(event, null)) orelse {
                self.stream.redis.setErrorDiag("发布事件失败：未返回消息 Id（queue={s}）", .{self.stream.key});
                return error.Server;
            };
            return id;
        }

        /// 处理一条事件消息。处理成功后自动确认；失败则保持挂起，由后续 `retryAck` 抢回。
        ///
        /// `ctx` 需提供 `handle(self, *const T, *const Message) !void`。
        pub fn consumeOnce(self: *Self, ctx: anytype) Error!bool {
            try self.maybeMaintain();
            var message = (try self.stream.takeMessage()) orelse return false;
            defer message.deinit();

            const parsed = (message.toStruct(T, self.gpa) catch null) orelse {
                self.stream.redis.setErrorDiag("事件反序列化失败：id={s}", .{message.id});
                return error.Type;
            };
            defer {
                var p = parsed;
                p.deinit();
            }

            ctx.handle(&parsed.value, &message) catch |err| {
                self.stream.redis.setErrorDiag("事件处理失败：id={s} 原因={s}", .{ message.id, @errorName(err) });
                return error.Server;
            };

            _ = try self.stream.acknowledge(&.{message.id});
            return true;
        }

        /// 消费循环；没有消息时短暂休眠，避免空转。
        pub fn consumeLoop(self: *Self, cancel: *std.atomic.Value(bool), ctx: anytype) Error!void {
            while (!cancel.load(.monotonic)) {
                const processed = try self.consumeOnce(ctx);
                if (!processed) zzig.compat.sleep(100 * std.time.ns_per_ms);
            }
        }

        fn maybeMaintain(self: *Self) Error!void {
            const now_ms: i64 = @intCast(@divTrunc(zzig.compat.nanoTimestamp(), std.time.ns_per_ms));
            if (self.maintenance_interval_seconds > 0 and self.last_maintenance_ms != 0 and
                now_ms - self.last_maintenance_ms < self.maintenance_interval_seconds * 1000)
            {
                return;
            }

            if (self.expire_seconds > 0) {
                _ = try self.stream.trimBeforeMs(now_ms - self.expire_seconds * 1000);
            }
            self.last_maintenance_ms = now_ms;
        }
    };
}
