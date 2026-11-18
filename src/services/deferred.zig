//! Redis 延迟批处理器（对齐 DH.NRedis `Services.RedisDeferred` / Rust `RedisDeferred`）。
//!
//! 用 Set 去重待处理 key；`processOnce` 取一批交给回调，失败时原样写回（失败重试语义）。
//! Zig 侧不内置计时器线程：`run` 由调用方线程驱动，带取消标记。

const std = @import("std");
const zzig = @import("zzig");
const set_mod = @import("../set.zig");
const redis_mod = @import("../redis.zig");

pub const Redis = redis_mod.Redis;
pub const Error = redis_mod.Error;
pub const RedisSet = set_mod.RedisSet;

pub const RedisDeferred = struct {
    gpa: std.mem.Allocator,
    name: []u8,
    pending: RedisSet,
    /// 空闲时轮询周期（毫秒）。默认 10 秒
    period_ms: u64 = 10_000,
    /// 每批处理条数。默认 10
    batch_size: usize = 10,

    /// `key` 为已含前缀的完整键。
    pub fn init(redis: *Redis, key: []const u8) !RedisDeferred {
        const gpa = redis.gpa;
        return .{
            .gpa = gpa,
            .name = try gpa.dupe(u8, key),
            .pending = try RedisSet.init(redis, key),
        };
    }

    pub fn deinit(self: *RedisDeferred) void {
        self.pending.deinit();
        self.gpa.free(self.name);
    }

    /// 放入待处理 key（集合去重），返回新增数量。
    pub fn add(self: *RedisDeferred, keys: []const []const u8) Error!i64 {
        return self.pending.addMany(keys);
    }

    /// 取出一批待处理 key（`SPOP`）。调用方释放每个元素与数组。
    pub fn takeBatch(self: *RedisDeferred, gpa: std.mem.Allocator) Error![][]u8 {
        return self.pending.pop(gpa, @intCast(self.batch_size));
    }

    /// 处理一批；失败时写回集合并返回错误。
    pub fn processOnce(self: *RedisDeferred, ctx: anytype) Error!usize {
        const keys = try self.takeBatch(self.gpa);
        defer {
            for (keys) |k| self.gpa.free(k);
            self.gpa.free(keys);
        }
        if (keys.len == 0) return 0;

        ctx.handle(keys) catch {
            _ = self.pending.addMany(keys) catch {};
            return error.Server;
        };
        return keys.len;
    }

    /// 循环处理；无消息时按 `period_ms` 休眠，直到 `cancel` 置位。
    pub fn run(self: *RedisDeferred, cancel: *std.atomic.Value(bool), ctx: anytype) Error!void {
        while (!cancel.load(.monotonic)) {
            const keys = try self.takeBatch(self.gpa);
            if (keys.len == 0) {
                zzig.compat.sleep(self.period_ms * std.time.ns_per_ms);
                continue;
            }
            defer {
                for (keys) |k| self.gpa.free(k);
                self.gpa.free(keys);
            }
            ctx.handle(keys) catch {
                _ = self.pending.addMany(keys) catch {};
                zzig.compat.sleep(self.period_ms * std.time.ns_per_ms);
            };
        }
    }
};
