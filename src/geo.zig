//! 地理位置结构（对齐 DH.NRedis `RedisGeo` / Rust `RedisGeo`）。
//!
//! 坐标顺序与 Redis 一致：`longitude`（经度）在前、`latitude`（纬度）在后。

const std = @import("std");
const resp = @import("resp.zig");
const redis_mod = @import("redis.zig");

pub const Redis = redis_mod.Redis;
pub const Error = redis_mod.Error;

pub const GeoMember = struct {
    name: []const u8,
    longitude: f64,
    latitude: f64,
};

pub const GeoResult = struct {
    member: []u8,
    /// 与中心点的距离（`with_dist=false` 时为 null）
    distance: ?f64 = null,
    longitude: ?f64 = null,
    latitude: ?f64 = null,
};

pub const RadiusOptions = struct {
    unit: []const u8 = "m",
    with_dist: bool = true,
    with_coord: bool = false,
    count: i64 = 0,
    asc: bool = true,
};

pub const RedisGeo = struct {
    redis: *Redis,
    key: []u8,

    pub fn init(redis: *Redis, key: []const u8) !RedisGeo {
        return .{ .redis = redis, .key = try redis.gpa.dupe(u8, key) };
    }

    pub fn deinit(self: *RedisGeo) void {
        self.redis.gpa.free(self.key);
    }

    pub fn add(self: *RedisGeo, name: []const u8, longitude: f64, latitude: f64) Error!i64 {
        const lon = resp.formatF64Alloc(self.redis.gpa, longitude) catch return error.IoError;
        const lat = resp.formatF64Alloc(self.redis.gpa, latitude) catch return error.IoError;
        defer self.redis.gpa.free(lon);
        defer self.redis.gpa.free(lat);
        var v = try self.redis.execute(&.{ "GEOADD", self.key, lon, lat, name });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn addItems(self: *RedisGeo, items: []const GeoMember) Error!i64 {
        if (items.len == 0) return 0;
        const gpa = self.redis.gpa;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(gpa);
        var owned: std.ArrayList([]u8) = .empty;
        defer {
            for (owned.items) |s| gpa.free(s);
            owned.deinit(gpa);
        }

        try args.append(gpa, "GEOADD");
        try args.append(gpa, self.key);
        for (items) |item| {
            const lon = try resp.formatF64Alloc(gpa, item.longitude);
            try owned.append(gpa, lon);
            try args.append(gpa, lon);
            const lat = try resp.formatF64Alloc(gpa, item.latitude);
            try owned.append(gpa, lat);
            try args.append(gpa, lat);
            try args.append(gpa, item.name);
        }

        var v = try self.redis.execute(args.items);
        defer v.deinit(gpa);
        return v.asInt() orelse 0;
    }

    pub fn distance(self: *RedisGeo, from: []const u8, to: []const u8, unit: []const u8) Error!?f64 {
        var v = try self.redis.execute(&.{ "GEODIST", self.key, from, to, unit });
        defer v.deinit(self.redis.gpa);
        if (v.isNull()) return null;
        return v.asFloat();
    }

    pub const Position = struct { longitude: f64, latitude: f64 };

    pub fn position(self: *RedisGeo, gpa: std.mem.Allocator, members: []const []const u8) Error![]?Position {
        if (members.len == 0) return gpa.alloc(?Position, 0) catch error.IoError;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, "GEOPOS");
        try args.append(self.redis.gpa, self.key);
        try args.appendSlice(self.redis.gpa, members);

        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        const items = try self.redis.expectArray(&v);
        if (items.len != members.len) return error.Protocol;

        const out = gpa.alloc(?Position, members.len) catch return error.IoError;
        for (items, 0..) |item, i| {
            if (item.asArray()) |pair| {
                if (pair.len >= 2) {
                    out[i] = .{
                        .longitude = pair[0].asFloat() orelse 0,
                        .latitude = pair[1].asFloat() orelse 0,
                    };
                    continue;
                }
            }
            out[i] = null;
        }
        return out;
    }

    pub fn geohash(self: *RedisGeo, gpa: std.mem.Allocator, members: []const []const u8) Error![]?[]u8 {
        if (members.len == 0) return gpa.alloc(?[]u8, 0) catch error.IoError;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, "GEOHASH");
        try args.append(self.redis.gpa, self.key);
        try args.appendSlice(self.redis.gpa, members);

        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        const items = try self.redis.expectArray(&v);
        if (items.len != members.len) return error.Protocol;

        const out = gpa.alloc(?[]u8, members.len) catch return error.IoError;
        for (items, 0..) |item, i| {
            out[i] = if (item.asString()) |s| (gpa.dupe(u8, s) catch return error.IoError) else null;
        }
        return out;
    }

    pub fn radiusByCoord(self: *RedisGeo, gpa: std.mem.Allocator, longitude: f64, latitude: f64, radius: f64, opts: RadiusOptions) Error![]GeoResult {
        const lon = resp.formatF64Alloc(self.redis.gpa, longitude) catch return error.IoError;
        const lat = resp.formatF64Alloc(self.redis.gpa, latitude) catch return error.IoError;
        const rad = resp.formatF64Alloc(self.redis.gpa, radius) catch return error.IoError;
        defer self.redis.gpa.free(lon);
        defer self.redis.gpa.free(lat);
        defer self.redis.gpa.free(rad);

        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, "GEORADIUS");
        try args.append(self.redis.gpa, self.key);
        try args.append(self.redis.gpa, lon);
        try args.append(self.redis.gpa, lat);
        try args.append(self.redis.gpa, rad);
        try args.append(self.redis.gpa, opts.unit);
        var cb: [24]u8 = undefined;
        const count_text: ?[]const u8 = if (opts.count > 0)
            (std.fmt.bufPrint(&cb, "{d}", .{opts.count}) catch return error.Config)
        else
            null;
        try appendRadiusOptions(self.redis.gpa, &args, opts, count_text);

        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        return parseRadiusReply(self.redis, gpa, &v, opts);
    }

    pub fn radiusByMember(self: *RedisGeo, gpa: std.mem.Allocator, member: []const u8, radius: f64, opts: RadiusOptions) Error![]GeoResult {
        const rad = resp.formatF64Alloc(self.redis.gpa, radius) catch return error.IoError;
        defer self.redis.gpa.free(rad);

        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.redis.gpa);
        try args.append(self.redis.gpa, "GEORADIUSBYMEMBER");
        try args.append(self.redis.gpa, self.key);
        try args.append(self.redis.gpa, member);
        try args.append(self.redis.gpa, rad);
        try args.append(self.redis.gpa, opts.unit);
        var cb: [24]u8 = undefined;
        const count_text: ?[]const u8 = if (opts.count > 0)
            (std.fmt.bufPrint(&cb, "{d}", .{opts.count}) catch return error.Config)
        else
            null;
        try appendRadiusOptions(self.redis.gpa, &args, opts, count_text);

        var v = try self.redis.execute(args.items);
        defer v.deinit(self.redis.gpa);
        return parseRadiusReply(self.redis, gpa, &v, opts);
    }
};

fn appendRadiusOptions(gpa: std.mem.Allocator, args: *std.ArrayList([]const u8), opts: RadiusOptions, count_text: ?[]const u8) !void {
    if (opts.with_dist) try args.append(gpa, "WITHDIST");
    if (opts.with_coord) try args.append(gpa, "WITHCOORD");
    if (opts.asc) try args.append(gpa, "ASC") else try args.append(gpa, "DESC");
    if (count_text) |text| {
        try args.append(gpa, "COUNT");
        try args.append(gpa, text);
    }
}

fn parseRadiusReply(redis: *Redis, gpa: std.mem.Allocator, v: *resp.Value, opts: RadiusOptions) Error![]GeoResult {
    const items = try redis.expectArray(v);

    var out: std.ArrayList(GeoResult) = .empty;
    errdefer {
        for (out.items) |r| gpa.free(r.member);
        out.deinit(gpa);
    }

    for (items) |item| {
        if (item.asArray()) |parts| {
            const member = if (parts.len > 0 and parts[0].asString() != null)
                try gpa.dupe(u8, parts[0].asString().?)
            else
                try gpa.dupe(u8, "");
            var result = GeoResult{ .member = member };
            var idx: usize = 1;
            if (opts.with_dist and idx < parts.len) {
                result.distance = parts[idx].asFloat();
                idx += 1;
            }
            if (opts.with_coord and idx < parts.len) {
                if (parts[idx].asArray()) |pair| {
                    if (pair.len >= 2) {
                        result.longitude = pair[0].asFloat();
                        result.latitude = pair[1].asFloat();
                    }
                }
            }
            try out.append(gpa, result);
        } else if (item.asString()) |s| {
            try out.append(gpa, .{ .member = try gpa.dupe(u8, s) });
        }
    }
    return out.toOwnedSlice(gpa);
}
