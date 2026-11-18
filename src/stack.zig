//! 栈结构（对齐 DH.NRedis `RedisStack`：右进右出，后进先出）。

const std = @import("std");
const encoder = @import("encoder.zig");
const redis_mod = @import("redis.zig");

pub const Redis = redis_mod.Redis;
pub const Error = redis_mod.Error;

pub const RedisStack = struct {
    redis: *Redis,
    key: []u8,

    pub fn init(redis: *Redis, key: []const u8) !RedisStack {
        return .{ .redis = redis, .key = try redis.gpa.dupe(u8, key) };
    }

    pub fn deinit(self: *RedisStack) void {
        self.redis.gpa.free(self.key);
    }

    pub fn len(self: *RedisStack) Error!i64 {
        var v = try self.redis.execute(&.{ "LLEN", self.key });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn isEmpty(self: *RedisStack) Error!bool {
        return (try self.len()) == 0;
    }

    pub fn push(self: *RedisStack, value: anytype) Error!i64 {
        const maybe = encoder.encodeValueAlloc(self.redis.gpa, value) catch return error.Type;
        defer if (maybe) |p| self.redis.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else "";
        var v = try self.redis.execute(&.{ "RPUSH", self.key, payload });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn pushMany(self: *RedisStack, values: []const []const u8) Error!i64 {
        if (values.len == 0) return 0;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, "RPUSH");
        try args.append(self.redis.gpa, self.key);
        try args.appendSlice(self.redis.gpa, values);
        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    /// 出栈（`RPOP`）。`timeout_seconds < 0` 立即返回，否则 `BRPOP` 阻塞。
    pub fn takeOne(self: *RedisStack, gpa: std.mem.Allocator, timeout_seconds: i64) Error!?[]u8 {
        var v = if (timeout_seconds < 0)
            try self.redis.execute(&.{ "RPOP", self.key })
        else blk: {
            var tb: [24]u8 = undefined;
            const t = std.fmt.bufPrint(&tb, "{d}", .{timeout_seconds}) catch return error.Config;
            break :blk try self.redis.executeBlocking(&.{ "BRPOP", self.key, t }, timeout_seconds);
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

    pub fn clear(self: *RedisStack) Error!void {
        _ = try self.redis.remove(self.key);
    }
};
