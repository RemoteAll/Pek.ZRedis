//! 列表结构（对齐 DH.NRedis `RedisList` / Rust `RedisList`）。

const std = @import("std");
const resp = @import("resp.zig");
const encoder = @import("encoder.zig");
const redis_mod = @import("redis.zig");

pub const Redis = redis_mod.Redis;
pub const Error = redis_mod.Error;

pub const RedisList = struct {
    redis: *Redis,
    key: []u8,

    pub fn init(redis: *Redis, key: []const u8) !RedisList {
        return .{ .redis = redis, .key = try redis.gpa.dupe(u8, key) };
    }

    pub fn deinit(self: *RedisList) void {
        self.redis.gpa.free(self.key);
    }

    pub fn len(self: *RedisList) Error!i64 {
        var v = try self.redis.execute(&.{ "LLEN", self.key });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn isEmpty(self: *RedisList) Error!bool {
        return (try self.len()) == 0;
    }

    pub fn pushBack(self: *RedisList, value: anytype) Error!i64 {
        return self.pushOne("RPUSH", value);
    }

    pub fn pushFront(self: *RedisList, value: anytype) Error!i64 {
        return self.pushOne("LPUSH", value);
    }

    fn pushOne(self: *RedisList, cmd: []const u8, value: anytype) Error!i64 {
        const maybe = encoder.encodeValueAlloc(self.redis.gpa, value) catch return error.Type;
        defer if (maybe) |p| self.redis.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else "";
        var v = try self.redis.execute(&.{ cmd, self.key, payload });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn pushBackMany(self: *RedisList, values: []const []const u8) Error!i64 {
        return self.pushMany("RPUSH", values);
    }

    pub fn pushFrontMany(self: *RedisList, values: []const []const u8) Error!i64 {
        return self.pushMany("LPUSH", values);
    }

    fn pushMany(self: *RedisList, cmd: []const u8, values: []const []const u8) Error!i64 {
        if (values.len == 0) return 0;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, cmd);
        try args.append(self.redis.gpa, self.key);
        try args.appendSlice(self.redis.gpa, values);
        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn popBack(self: *RedisList, gpa: std.mem.Allocator) Error!?[]u8 {
        return self.popOne(gpa, "RPOP", null);
    }

    pub fn popFront(self: *RedisList, gpa: std.mem.Allocator) Error!?[]u8 {
        return self.popOne(gpa, "LPOP", null);
    }

    /// 阻塞出栈（`BRPOP`/`BLPOP`）；`timeout_seconds <= 0` 表示永久阻塞（由客户端兜底 60s）。
    pub fn popBackBlocking(self: *RedisList, gpa: std.mem.Allocator, timeout_seconds: i64) Error!?[]u8 {
        return self.popOne(gpa, "BRPOP", timeout_seconds);
    }

    pub fn popFrontBlocking(self: *RedisList, gpa: std.mem.Allocator, timeout_seconds: i64) Error!?[]u8 {
        return self.popOne(gpa, "BLPOP", timeout_seconds);
    }

    fn popOne(self: *RedisList, gpa: std.mem.Allocator, cmd: []const u8, block: ?i64) Error!?[]u8 {
        var tb: [24]u8 = undefined;
        var v = if (block) |seconds| blk: {
            const t = std.fmt.bufPrint(&tb, "{d}", .{seconds}) catch return error.Config;
            break :blk try self.redis.executeBlocking(&.{ cmd, self.key, t }, seconds);
        } else try self.redis.execute(&.{ cmd, self.key });
        defer v.deinit(self.redis.gpa);

        if (v.isNull()) return null;
        // 阻塞命令返回 [key, value]
        if (block != null) {
            const items = v.asArray() orelse return null;
            if (items.len < 2) return null;
            return items[1].asBytesAlloc(gpa) catch return error.IoError;
        }
        return v.asBytesAlloc(gpa) catch return error.IoError;
    }

    pub fn get(self: *RedisList, comptime T: type, gpa: std.mem.Allocator, index: i64) Error!?T {
        var ib: [24]u8 = undefined;
        const i = std.fmt.bufPrint(&ib, "{d}", .{index}) catch return error.Config;
        var v = try self.redis.execute(&.{ "LINDEX", self.key, i });
        defer v.deinit(self.redis.gpa);
        if (v.isNull()) return null;
        const raw = (v.asBytesAlloc(gpa) catch return error.IoError) orelse return null;
        if (T == []u8 or T == []const u8) return raw;
        defer gpa.free(raw);
        return encoder.decodeValueAlloc(T, gpa, raw) catch null;
    }

    pub fn set(self: *RedisList, index: i64, value: anytype) Error!void {
        var ib: [24]u8 = undefined;
        const i = std.fmt.bufPrint(&ib, "{d}", .{index}) catch return error.Config;
        const maybe = encoder.encodeValueAlloc(self.redis.gpa, value) catch return error.Type;
        defer if (maybe) |p| self.redis.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else "";
        var v = try self.redis.execute(&.{ "LSET", self.key, i, payload });
        defer v.deinit(self.redis.gpa);
    }

    pub fn range(self: *RedisList, gpa: std.mem.Allocator, start: i64, end: i64) Error![][]u8 {
        var sb: [24]u8 = undefined;
        var eb: [24]u8 = undefined;
        const s = std.fmt.bufPrint(&sb, "{d}", .{start}) catch return error.Config;
        const e = std.fmt.bufPrint(&eb, "{d}", .{end}) catch return error.Config;
        var v = try self.redis.execute(&.{ "LRANGE", self.key, s, e });
        defer v.deinit(self.redis.gpa);
        return dupArray(self.redis, gpa, &v);
    }

    pub fn getAll(self: *RedisList, gpa: std.mem.Allocator) Error![][]u8 {
        return self.range(gpa, 0, -1);
    }

    pub fn remove(self: *RedisList, count: i64, value: []const u8) Error!i64 {
        var cb: [24]u8 = undefined;
        const c = std.fmt.bufPrint(&cb, "{d}", .{count}) catch return error.Config;
        var v = try self.redis.execute(&.{ "LREM", self.key, c, value });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn trim(self: *RedisList, start: i64, end: i64) Error!void {
        var sb: [24]u8 = undefined;
        var eb: [24]u8 = undefined;
        const s = std.fmt.bufPrint(&sb, "{d}", .{start}) catch return error.Config;
        const e = std.fmt.bufPrint(&eb, "{d}", .{end}) catch return error.Config;
        try self.redis.executeIgnore(&.{ "LTRIM", self.key, s, e });
    }

    pub fn position(self: *RedisList, value: []const u8) Error!?i64 {
        var v = try self.redis.execute(&.{ "LPOS", self.key, value });
        defer v.deinit(self.redis.gpa);
        if (v.isNull()) return null;
        return v.asInt();
    }

    pub fn contains(self: *RedisList, value: []const u8) Error!bool {
        return (try self.position(value)) != null;
    }

    pub fn clear(self: *RedisList) Error!void {
        _ = try self.redis.remove(self.key);
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
