//! 哈希结构（对齐 DH.NRedis `RedisHash` / Rust `RedisHash`）。

const std = @import("std");
const resp = @import("resp.zig");
const encoder = @import("encoder.zig");
const redis_mod = @import("redis.zig");

pub const Redis = redis_mod.Redis;
pub const Error = redis_mod.Error;

pub const RedisHash = struct {
    redis: *Redis,
    key: []u8,

    pub fn init(redis: *Redis, key: []const u8) !RedisHash {
        return .{ .redis = redis, .key = try redis.gpa.dupe(u8, key) };
    }

    pub fn deinit(self: *RedisHash) void {
        self.redis.gpa.free(self.key);
    }

    pub fn count(self: *RedisHash) Error!i64 {
        var v = try self.redis.execute(&.{ "HLEN", self.key });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn isEmpty(self: *RedisHash) Error!bool {
        return (try self.count()) == 0;
    }

    pub fn set(self: *RedisHash, field: []const u8, value: anytype) Error!i64 {
        const maybe = encoder.encodeValueAlloc(self.redis.gpa, value) catch return error.Type;
        defer if (maybe) |p| self.redis.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else "";
        var v = try self.redis.execute(&.{ "HSET", self.key, field, payload });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn setNx(self: *RedisHash, field: []const u8, value: anytype) Error!i64 {
        const maybe = encoder.encodeValueAlloc(self.redis.gpa, value) catch return error.Type;
        defer if (maybe) |p| self.redis.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else "";
        var v = try self.redis.execute(&.{ "HSETNX", self.key, field, payload });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn getRaw(self: *RedisHash, gpa: std.mem.Allocator, field: []const u8) Error!?[]u8 {
        var v = try self.redis.execute(&.{ "HGET", self.key, field });
        defer v.deinit(self.redis.gpa);
        if (v.isNull()) return null;
        return (v.asBytesAlloc(gpa) catch return error.IoError);
    }

    pub fn get(self: *RedisHash, comptime T: type, gpa: std.mem.Allocator, field: []const u8) Error!?T {
        const raw = (try self.getRaw(gpa, field)) orelse return null;
        if (T == []u8 or T == []const u8) return raw;
        defer gpa.free(raw);
        return encoder.decodeValueAlloc(T, gpa, raw) catch null;
    }

    pub fn containsKey(self: *RedisHash, field: []const u8) Error!bool {
        var v = try self.redis.execute(&.{ "HEXISTS", self.key, field });
        defer v.deinit(self.redis.gpa);
        return (v.asInt() orelse 0) == 1;
    }

    /// 删除字段（可多个）。
    pub fn remove(self: *RedisHash, fields: []const []const u8) Error!i64 {
        if (fields.len == 0) return 0;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, "HDEL");
        try args.append(self.redis.gpa, self.key);
        try args.appendSlice(self.redis.gpa, fields);
        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn incrBy(self: *RedisHash, field: []const u8, delta: i64) Error!i64 {
        var buf: [24]u8 = undefined;
        const d = std.fmt.bufPrint(&buf, "{d}", .{delta}) catch return error.Config;
        var v = try self.redis.execute(&.{ "HINCRBY", self.key, field, d });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn incrByFloat(self: *RedisHash, field: []const u8, delta: f64) Error!f64 {
        const d = resp.formatF64Alloc(self.redis.gpa, delta) catch return error.IoError;
        defer self.redis.gpa.free(d);
        var v = try self.redis.execute(&.{ "HINCRBYFLOAT", self.key, field, d });
        defer v.deinit(self.redis.gpa);
        return v.asFloat() orelse 0;
    }

    pub fn keys(self: *RedisHash, gpa: std.mem.Allocator) Error![][]u8 {
        var v = try self.redis.execute(&.{ "HKEYS", self.key });
        defer v.deinit(self.redis.gpa);
        return dupStringArray(self.redis, gpa, &v);
    }

    pub fn values(self: *RedisHash, gpa: std.mem.Allocator) Error![][]u8 {
        var v = try self.redis.execute(&.{ "HVALS", self.key });
        defer v.deinit(self.redis.gpa);
        return dupStringArray(self.redis, gpa, &v);
    }

    pub const FieldValue = struct { field: []u8, value: []u8 };

    /// `HGETALL`（按服务端顺序返回字段/值对）。
    pub fn getAll(self: *RedisHash, gpa: std.mem.Allocator) Error![]FieldValue {
        var v = try self.redis.execute(&.{ "HGETALL", self.key });
        defer v.deinit(self.redis.gpa);
        const items = try self.redis.expectArray(&v);

        var out: std.ArrayList(FieldValue) = .empty;
        errdefer {
            for (out.items) |p| {
                gpa.free(p.field);
                gpa.free(p.value);
            }
            out.deinit(gpa);
        }
        var i: usize = 0;
        while (i + 1 < items.len) : (i += 2) {
            try out.append(gpa, .{
                .field = try gpa.dupe(u8, items[i].asString() orelse ""),
                .value = try gpa.dupe(u8, items[i + 1].asString() orelse ""),
            });
        }
        return out.toOwnedSlice(gpa);
    }

    /// 批量取多个字段（缺失处为 `null`）。
    pub fn hmget(self: *RedisHash, gpa: std.mem.Allocator, fields: []const []const u8) Error![]?[]u8 {
        if (fields.len == 0) return gpa.alloc(?[]u8, 0) catch error.IoError;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, "HMGET");
        try args.append(self.redis.gpa, self.key);
        try args.appendSlice(self.redis.gpa, fields);

        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        const items = try self.redis.expectArray(&v);
        if (items.len != fields.len) return error.Protocol;

        const out = gpa.alloc(?[]u8, fields.len) catch return error.IoError;
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |slot| if (slot) |s| gpa.free(s);
            gpa.free(out);
        }
        for (items, 0..) |item, i| {
            out[i] = if (item.isNull()) null else (item.asBytesAlloc(gpa) catch return error.IoError);
            filled = i + 1;
        }
        return out;
    }

    pub fn clear(self: *RedisHash) Error!void {
        _ = try self.redis.remove(self.key);
    }

    pub const ScanResult = struct { cursor: u64, items: []FieldValue };

    pub fn scan(self: *RedisHash, gpa: std.mem.Allocator, cursor: u64, pattern: []const u8, limit: usize) Error!ScanResult {
        const n = if (limit == 0) 100 else limit;
        var cb: [32]u8 = undefined;
        var nb: [32]u8 = undefined;
        const c = std.fmt.bufPrint(&cb, "{d}", .{cursor}) catch return error.Config;
        const cnt = std.fmt.bufPrint(&nb, "{d}", .{n}) catch return error.Config;

        var v = try self.redis.execute(&.{ "HSCAN", self.key, c, "MATCH", pattern, "COUNT", cnt });
        defer v.deinit(self.redis.gpa);
        const arr = try self.redis.expectArray(&v);
        if (arr.len != 2) return error.Protocol;

        const next: u64 = @intCast(arr[0].asInt() orelse 0);
        return .{ .cursor = next, .items = try dupPairs(self.redis, gpa, arr[1]) };
    }
};

fn dupStringArray(redis: *Redis, gpa: std.mem.Allocator, v: *resp.Value) Error![][]u8 {
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

fn dupPairs(redis: *Redis, gpa: std.mem.Allocator, v: resp.Value) Error![]RedisHash.FieldValue {
    const items = v.asArray() orelse return error.Protocol;
    var out: std.ArrayList(RedisHash.FieldValue) = .empty;
    errdefer {
        for (out.items) |p| {
            gpa.free(p.field);
            gpa.free(p.value);
        }
        out.deinit(gpa);
    }
    var i: usize = 0;
    while (i + 1 < items.len) : (i += 2) {
        try out.append(gpa, .{
            .field = try gpa.dupe(u8, items[i].asString() orelse ""),
            .value = try gpa.dupe(u8, items[i + 1].asString() orelse ""),
        });
    }
    _ = redis;
    return out.toOwnedSlice(gpa);
}
