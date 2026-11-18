//! HyperLogLog（对齐 DH.NRedis `HyperLogLog` / Rust `HyperLogLog`）。

const std = @import("std");
const redis_mod = @import("redis.zig");

pub const Redis = redis_mod.Redis;
pub const Error = redis_mod.Error;

pub const HyperLogLog = struct {
    redis: *Redis,
    key: []u8,

    pub fn init(redis: *Redis, key: []const u8) !HyperLogLog {
        return .{ .redis = redis, .key = try redis.gpa.dupe(u8, key) };
    }

    pub fn deinit(self: *HyperLogLog) void {
        self.redis.gpa.free(self.key);
    }

    /// `PFADD`；任一元素导致基数变化返回 `true`。
    pub fn add(self: *HyperLogLog, items: []const []const u8) Error!bool {
        if (items.len == 0) return false;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, "PFADD");
        try args.append(self.redis.gpa, self.key);
        try args.appendSlice(self.redis.gpa, items);
        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        return (v.asInt() orelse 0) == 1;
    }

    pub fn count(self: *HyperLogLog) Error!i64 {
        var v = try self.redis.execute(&.{ "PFCOUNT", self.key });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    /// `PFMERGE`（把 `keys` 合并进本键）。
    pub fn merge(self: *HyperLogLog, keys: []const []const u8) Error!bool {
        if (keys.len == 0) return false;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, "PFMERGE");
        try args.append(self.redis.gpa, self.key);
        try args.appendSlice(self.redis.gpa, keys);
        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        const s = v.asString() orelse return false;
        return std.ascii.eqlIgnoreCase(s, "OK");
    }

    pub fn clear(self: *HyperLogLog) Error!void {
        _ = try self.redis.remove(self.key);
    }
};
