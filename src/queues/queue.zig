//! 普通队列（对齐 DH.NRedis `RedisQueue<T>` / Rust `RedisQueue`）：
//! 左进右出（`LPUSH` + `RPOP`/`BRPOP`），弹出即消费、不确认。

const std = @import("std");
const zzig = @import("zzig");
const redis_mod = @import("../redis.zig");
const encoder = @import("../encoder.zig");
const base = @import("base.zig");

pub const Redis = redis_mod.Redis;
pub const Error = redis_mod.Error;

pub const RedisQueue = struct {
    gpa: std.mem.Allocator,
    redis: *Redis,
    key: []u8,
    settings: base.Settings = .{},

    pub fn init(redis: *Redis, key: []const u8) !RedisQueue {
        return .{ .gpa = redis.gpa, .redis = redis, .key = try redis.gpa.dupe(u8, key) };
    }

    pub fn deinit(self: *RedisQueue) void {
        self.settings.deinit(self.gpa);
        self.gpa.free(self.key);
    }

    pub fn count(self: *RedisQueue) Error!i64 {
        var v = try self.redis.execute(&.{ "LLEN", self.key });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn isEmpty(self: *RedisQueue) Error!bool {
        return (try self.count()) == 0;
    }

    /// 生产一条消息，返回入队后长度。
    pub fn add(self: *RedisQueue, value: anytype) Error!i64 {
        const maybe = encoder.encodeValueAlloc(self.gpa, value) catch return error.Type;
        defer if (maybe) |p| self.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else "";
        return self.pushRaw(&.{payload});
    }

    /// 批量生产（单次 `LPUSH`，元素顺序与 C# 一致）。
    pub fn addMany(self: *RedisQueue, values: []const []const u8) Error!i64 {
        if (values.len == 0) return 0;
        return self.pushRaw(values);
    }

    fn pushRaw(self: *RedisQueue, values: []const []const u8) Error!i64 {
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        var traced: std.ArrayList([]u8) = .empty;
        defer {
            for (traced.items) |s| self.gpa.free(s);
            traced.deinit(self.gpa);
        }

        try args.append(self.gpa, "LPUSH");
        try args.append(self.gpa, self.key);
        for (values) |v| {
            const out = try self.settings.attachTraceAlloc(self.gpa, v);
            try traced.append(self.gpa, out);
            try args.append(self.gpa, out);
        }

        var last: i64 = 0;
        var attempt: usize = 0;
        while (attempt <= self.settings.retry_times_when_send_failed) : (attempt += 1) {
            var v = try self.redis.execute(args.items);
            defer v.deinit(self.redis.gpa);
            last = v.asInt() orelse 0;
            if (last > 0) return last;
            if (attempt < self.settings.retry_times_when_send_failed) {
                zzig.compat.sleep(self.settings.retry_interval_ms * std.time.ns_per_ms);
            }
        }
        if (self.settings.throw_on_failure) {
            self.redis.setErrorDiag("发布到队列[{s}]失败！", .{self.key});
            return error.Server;
        }
        return last;
    }

    /// 消费一条（`timeout_seconds < 0` 用 `RPOP`，否则 `BRPOP`）。返回自有字节。
    pub fn takeOne(self: *RedisQueue, gpa: std.mem.Allocator, timeout_seconds: i64) Error!?[]u8 {
        var v = if (timeout_seconds < 0)
            try self.redis.execute(&.{ "RPOP", self.key })
        else blk: {
            var tb: [24]u8 = undefined;
            const text = std.fmt.bufPrint(&tb, "{d}", .{timeout_seconds}) catch return error.Config;
            break :blk try self.redis.executeBlocking(&.{ "BRPOP", self.key, text }, timeout_seconds);
        };
        defer v.deinit(self.redis.gpa);

        if (v.isNull()) return null;
        if (timeout_seconds >= 0) {
            const items = v.asArray() orelse return null;
            if (items.len < 2) return null;
            return items[1].asBytesAlloc(gpa) catch return error.IoError;
        }
        return v.asBytesAlloc(gpa) catch return error.IoError;
    }

    /// 消费一条并解析 JSON（`Parsed(T).deinit()` 由调用方负责）。
    pub fn takeJson(self: *RedisQueue, comptime T: type, gpa: std.mem.Allocator, timeout_seconds: i64) Error!?std.json.Parsed(T) {
        const raw = (try self.takeOne(gpa, timeout_seconds)) orelse return null;
        defer gpa.free(raw);
        return encoder.parseJsonAlloc(T, gpa, raw) catch null;
    }

    pub fn clear(self: *RedisQueue) Error!void {
        _ = try self.redis.remove(self.key);
    }
};
