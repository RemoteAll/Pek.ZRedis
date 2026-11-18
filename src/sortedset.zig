//! 有序集合（对齐 DH.NRedis `RedisSortedSet` / Rust `RedisSortedSet`）。

const std = @import("std");
const resp = @import("resp.zig");
const encoder = @import("encoder.zig");
const redis_mod = @import("redis.zig");

pub const Redis = redis_mod.Redis;
pub const Error = redis_mod.Error;

pub const Scored = struct { member: []u8, score: f64 };

pub const RedisSortedSet = struct {
    redis: *Redis,
    key: []u8,

    pub fn init(redis: *Redis, key: []const u8) !RedisSortedSet {
        return .{ .redis = redis, .key = try redis.gpa.dupe(u8, key) };
    }

    pub fn deinit(self: *RedisSortedSet) void {
        self.redis.gpa.free(self.key);
    }

    pub fn len(self: *RedisSortedSet) Error!i64 {
        var v = try self.redis.execute(&.{ "ZCARD", self.key });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn isEmpty(self: *RedisSortedSet) Error!bool {
        return (try self.len()) == 0;
    }

    pub fn add(self: *RedisSortedSet, member: anytype, score_value: f64) Error!i64 {
        const sb = resp.formatF64Alloc(self.redis.gpa, score_value) catch return error.IoError;
        defer self.redis.gpa.free(sb);
        const maybe = encoder.encodeValueAlloc(self.redis.gpa, member) catch return error.Type;
        defer if (maybe) |p| self.redis.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else "";
        var v = try self.redis.execute(&.{ "ZADD", self.key, sb, payload });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn addMany(self: *RedisSortedSet, members: []const []const u8, score_value: f64) Error!i64 {
        if (members.len == 0) return 0;
        const sb = resp.formatF64Alloc(self.redis.gpa, score_value) catch return error.IoError;
        defer self.redis.gpa.free(sb);

        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, "ZADD");
        try args.append(self.redis.gpa, self.key);
        for (members) |m| {
            try args.append(self.redis.gpa, sb);
            try args.append(self.redis.gpa, m);
        }
        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn remove(self: *RedisSortedSet, members: []const []const u8) Error!i64 {
        if (members.len == 0) return 0;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, "ZREM");
        try args.append(self.redis.gpa, self.key);
        try args.appendSlice(self.redis.gpa, members);
        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn score(self: *RedisSortedSet, member: []const u8) Error!?f64 {
        var v = try self.redis.execute(&.{ "ZSCORE", self.key, member });
        defer v.deinit(self.redis.gpa);
        if (v.isNull()) return null;
        return v.asFloat();
    }

    pub fn increment(self: *RedisSortedSet, member: []const u8, delta: f64) Error!f64 {
        const sb = resp.formatF64Alloc(self.redis.gpa, delta) catch return error.IoError;
        defer self.redis.gpa.free(sb);
        var v = try self.redis.execute(&.{ "ZINCRBY", self.key, sb, member });
        defer v.deinit(self.redis.gpa);
        return v.asFloat() orelse 0;
    }

    pub fn count(self: *RedisSortedSet, min: f64, max: f64) Error!i64 {
        const minb = resp.formatF64Alloc(self.redis.gpa, min) catch return error.IoError;
        const maxb = resp.formatF64Alloc(self.redis.gpa, max) catch return error.IoError;
        defer self.redis.gpa.free(minb);
        defer self.redis.gpa.free(maxb);
        var v = try self.redis.execute(&.{ "ZCOUNT", self.key, minb, maxb });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn range(self: *RedisSortedSet, gpa: std.mem.Allocator, start: i64, stop: i64) Error![][]u8 {
        var sb: [24]u8 = undefined;
        var eb: [24]u8 = undefined;
        const s = std.fmt.bufPrint(&sb, "{d}", .{start}) catch return error.Config;
        const e = std.fmt.bufPrint(&eb, "{d}", .{stop}) catch return error.Config;
        var v = try self.redis.execute(&.{ "ZRANGE", self.key, s, e });
        defer v.deinit(self.redis.gpa);
        return dupArray(self.redis, gpa, &v);
    }

    pub fn rangeWithScores(self: *RedisSortedSet, gpa: std.mem.Allocator, start: i64, stop: i64) Error![]Scored {
        var sb: [24]u8 = undefined;
        var eb: [24]u8 = undefined;
        const s = std.fmt.bufPrint(&sb, "{d}", .{start}) catch return error.Config;
        const e = std.fmt.bufPrint(&eb, "{d}", .{stop}) catch return error.Config;
        var v = try self.redis.execute(&.{ "ZRANGE", self.key, s, e, "WITHSCORES" });
        defer v.deinit(self.redis.gpa);
        return dupScored(self.redis, gpa, &v);
    }

    pub fn rangeByScore(self: *RedisSortedSet, gpa: std.mem.Allocator, min: f64, max: f64, offset: i64, limit: i64) Error![][]u8 {
        const minb = resp.formatF64Alloc(self.redis.gpa, min) catch return error.IoError;
        const maxb = resp.formatF64Alloc(self.redis.gpa, max) catch return error.IoError;
        defer self.redis.gpa.free(minb);
        defer self.redis.gpa.free(maxb);

        var ob: [24]u8 = undefined;
        var cb: [24]u8 = undefined;
        const o = std.fmt.bufPrint(&ob, "{d}", .{offset}) catch return error.Config;
        const c = std.fmt.bufPrint(&cb, "{d}", .{limit}) catch return error.Config;
        var v = try self.redis.execute(&.{ "ZRANGEBYSCORE", self.key, minb, maxb, "LIMIT", o, c });
        defer v.deinit(self.redis.gpa);
        return dupArray(self.redis, gpa, &v);
    }

    pub fn rank(self: *RedisSortedSet, member: []const u8) Error!?i64 {
        var v = try self.redis.execute(&.{ "ZRANK", self.key, member });
        defer v.deinit(self.redis.gpa);
        if (v.isNull()) return null;
        return v.asInt();
    }

    pub fn revRank(self: *RedisSortedSet, member: []const u8) Error!?i64 {
        var v = try self.redis.execute(&.{ "ZREVRANK", self.key, member });
        defer v.deinit(self.redis.gpa);
        if (v.isNull()) return null;
        return v.asInt();
    }

    pub fn popMin(self: *RedisSortedSet, gpa: std.mem.Allocator, limit: i64) Error![]Scored {
        return self.popOne(gpa, "ZPOPMIN", limit);
    }

    pub fn popMax(self: *RedisSortedSet, gpa: std.mem.Allocator, limit: i64) Error![]Scored {
        return self.popOne(gpa, "ZPOPMAX", limit);
    }

    fn popOne(self: *RedisSortedSet, gpa: std.mem.Allocator, cmd: []const u8, limit: i64) Error![]Scored {
        var cb: [24]u8 = undefined;
        const c = std.fmt.bufPrint(&cb, "{d}", .{limit}) catch return error.Config;
        var v = try self.redis.execute(&.{ cmd, self.key, c });
        defer v.deinit(self.redis.gpa);
        return dupScored(self.redis, gpa, &v);
    }

    pub fn removeRangeByScore(self: *RedisSortedSet, min: f64, max: f64) Error!i64 {
        const minb = resp.formatF64Alloc(self.redis.gpa, min) catch return error.IoError;
        const maxb = resp.formatF64Alloc(self.redis.gpa, max) catch return error.IoError;
        defer self.redis.gpa.free(minb);
        defer self.redis.gpa.free(maxb);
        var v = try self.redis.execute(&.{ "ZREMRANGEBYSCORE", self.key, minb, maxb });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn scanAll(self: *RedisSortedSet, gpa: std.mem.Allocator, pattern: []const u8, limit: usize) Error![]Scored {
        var out: std.ArrayList(Scored) = .empty;
        errdefer {
            for (out.items) |s| gpa.free(s.member);
            out.deinit(gpa);
        }
        var cursor: u64 = 0;
        while (true) {
            var cb: [32]u8 = undefined;
            var nb: [32]u8 = undefined;
            const c = std.fmt.bufPrint(&cb, "{d}", .{cursor}) catch return error.Config;
            const n = std.fmt.bufPrint(&nb, "{d}", .{if (limit == 0) 100 else limit}) catch return error.Config;

            var v = try self.redis.execute(&.{ "ZSCAN", self.key, c, "MATCH", pattern, "COUNT", n });
            defer v.deinit(self.redis.gpa);
            const arr = try self.redis.expectArray(&v);
            if (arr.len != 2) return error.Protocol;

            cursor = @intCast(arr[0].asInt() orelse 0);
            if (arr[1].asArray()) |items| {
                var i: usize = 0;
                while (i + 1 < items.len) : (i += 2) {
                    try out.append(gpa, .{
                        .member = try gpa.dupe(u8, items[i].asString() orelse ""),
                        .score = items[i + 1].asFloat() orelse 0,
                    });
                }
            }
            if (cursor == 0) break;
        }
        return out.toOwnedSlice(gpa);
    }

    pub fn clear(self: *RedisSortedSet) Error!void {
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

fn dupScored(redis: *Redis, gpa: std.mem.Allocator, v: *resp.Value) Error![]Scored {
    const items = try redis.expectArray(v);

    var out: std.ArrayList(Scored) = .empty;
    errdefer {
        for (out.items) |s| gpa.free(s.member);
        out.deinit(gpa);
    }
    var i: usize = 0;
    while (i + 1 < items.len) : (i += 2) {
        try out.append(gpa, .{
            .member = try gpa.dupe(u8, items[i].asString() orelse ""),
            .score = items[i + 1].asFloat() orelse 0,
        });
    }
    return out.toOwnedSlice(gpa);
}
