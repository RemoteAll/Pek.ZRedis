//! 集合结构（对齐 DH.NRedis `RedisSet` / Rust `RedisSet`）。

const std = @import("std");
const resp = @import("resp.zig");
const encoder = @import("encoder.zig");
const redis_mod = @import("redis.zig");

pub const Redis = redis_mod.Redis;
pub const Error = redis_mod.Error;

pub const RedisSet = struct {
    redis: *Redis,
    key: []u8,

    pub fn init(redis: *Redis, key: []const u8) !RedisSet {
        return .{ .redis = redis, .key = try redis.gpa.dupe(u8, key) };
    }

    pub fn deinit(self: *RedisSet) void {
        self.redis.gpa.free(self.key);
    }

    pub fn len(self: *RedisSet) Error!i64 {
        var v = try self.redis.execute(&.{ "SCARD", self.key });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn isEmpty(self: *RedisSet) Error!bool {
        return (try self.len()) == 0;
    }

    pub fn add(self: *RedisSet, member: anytype) Error!i64 {
        const maybe = encoder.encodeValueAlloc(self.redis.gpa, member) catch return error.Type;
        defer if (maybe) |p| self.redis.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else "";
        var v = try self.redis.execute(&.{ "SADD", self.key, payload });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn addMany(self: *RedisSet, items: []const []const u8) Error!i64 {
        if (items.len == 0) return 0;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, "SADD");
        try args.append(self.redis.gpa, self.key);
        try args.appendSlice(self.redis.gpa, items);
        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn remove(self: *RedisSet, items: []const []const u8) Error!i64 {
        if (items.len == 0) return 0;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, "SREM");
        try args.append(self.redis.gpa, self.key);
        try args.appendSlice(self.redis.gpa, items);
        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn contains(self: *RedisSet, member: []const u8) Error!bool {
        var v = try self.redis.execute(&.{ "SISMEMBER", self.key, member });
        defer v.deinit(self.redis.gpa);
        return (v.asInt() orelse 0) == 1;
    }

    /// 批量判断（与 members 等长）。
    pub fn misMember(self: *RedisSet, gpa: std.mem.Allocator, items: []const []const u8) Error![]bool {
        if (items.len == 0) return gpa.alloc(bool, 0) catch error.IoError;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, "SMISMEMBER");
        try args.append(self.redis.gpa, self.key);
        try args.appendSlice(self.redis.gpa, items);

        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        const got = try self.redis.expectArray(&v);
        if (got.len != items.len) return error.Protocol;

        const out = gpa.alloc(bool, items.len) catch return error.IoError;
        for (got, 0..) |item, i| out[i] = (item.asInt() orelse 0) == 1;
        return out;
    }

    pub fn members(self: *RedisSet, gpa: std.mem.Allocator) Error![][]u8 {
        var v = try self.redis.execute(&.{ "SMEMBERS", self.key });
        defer v.deinit(self.redis.gpa);
        return dupArray(self.redis, gpa, &v);
    }

    pub fn pop(self: *RedisSet, gpa: std.mem.Allocator, count: i64) Error![][]u8 {
        var cb: [24]u8 = undefined;
        const c = std.fmt.bufPrint(&cb, "{d}", .{count}) catch return error.Config;
        var v = try self.redis.execute(&.{ "SPOP", self.key, c });
        defer v.deinit(self.redis.gpa);
        return dupArray(self.redis, gpa, &v);
    }

    /// 全部成员（`SSCAN` 循环，避免大 key 阻塞）。
    pub fn scanAll(self: *RedisSet, gpa: std.mem.Allocator, pattern: []const u8, count: usize) Error![][]u8 {
        var out: std.ArrayList([]u8) = .empty;
        errdefer {
            for (out.items) |s| gpa.free(s);
            out.deinit(gpa);
        }
        var cursor: u64 = 0;
        while (true) {
            var cb: [32]u8 = undefined;
            var nb: [32]u8 = undefined;
            const c = std.fmt.bufPrint(&cb, "{d}", .{cursor}) catch return error.Config;
            const n = std.fmt.bufPrint(&nb, "{d}", .{if (count == 0) 100 else count}) catch return error.Config;

            var v = try self.redis.execute(&.{ "SSCAN", self.key, c, "MATCH", pattern, "COUNT", n });
            defer v.deinit(self.redis.gpa);
            const arr = try self.redis.expectArray(&v);
            if (arr.len != 2) return error.Protocol;

            cursor = @intCast(arr[0].asInt() orelse 0);
            if (arr[1].asArray()) |arr_items| {
                for (arr_items) |m| {
                    if (m.asString()) |s| try out.append(gpa, try gpa.dupe(u8, s));
                }
            }
            if (cursor == 0) break;
        }
        return out.toOwnedSlice(gpa);
    }

    pub fn clear(self: *RedisSet) Error!void {
        _ = try self.redis.remove(self.key);
    }

    pub fn unionSet(self: *RedisSet, gpa: std.mem.Allocator, other_keys: []const []const u8) Error![][]u8 {
        return self.setOp(gpa, "SUNION", other_keys);
    }

    pub fn inter(self: *RedisSet, gpa: std.mem.Allocator, other_keys: []const []const u8) Error![][]u8 {
        return self.setOp(gpa, "SINTER", other_keys);
    }

    pub fn diff(self: *RedisSet, gpa: std.mem.Allocator, other_keys: []const []const u8) Error![][]u8 {
        return self.setOp(gpa, "SDIFF", other_keys);
    }

    fn setOp(self: *RedisSet, gpa: std.mem.Allocator, cmd: []const u8, other_keys: []const []const u8) Error![][]u8 {
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, cmd);
        try args.append(self.redis.gpa, self.key);
        try args.appendSlice(self.redis.gpa, other_keys);
        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        return dupArray(self.redis, gpa, &v);
    }
};

fn dupArray(redis: *Redis, gpa: std.mem.Allocator, v: *resp.Value) Error![][]u8 {
    const items = try redis.expectArray(v);

    var out: std.ArrayList([]u8) = .empty;
    errdefer {
        for (out.items) |s| gpa.free(s);
        out.deinit(gpa);
    }
    for (items) |item| {
        if (item.asString()) |s| try out.append(gpa, try gpa.dupe(u8, s));
    }
    return out.toOwnedSlice(gpa);
}
