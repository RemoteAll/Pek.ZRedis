//! RedLock（对齐 DH.NRedis `Services.RedisRedLock` / Rust `services::acquire_red_lock`）。
//!
//! 算法与 C# 完全一致：
//! - 令牌为 22 位随机字符串；
//! - 依次在全部实例上写入令牌（**普通 SET**，与 C# 一致，非 NX）；
//! - `quorum = n / 2 + 1` 且 `ms_expire - 耗时 - ms_expire * 1% > 0` 视为成功；
//! - 失败回滚已写实例，按 `200ms + rand(0..50ms)` 重试直到 `ms_timeout`；
//! - 释放使用比较令牌的 Lua 脚本（与 C# `TryUnlock` 相同）。

const std = @import("std");
const zzig = @import("zzig");
const full_mod = @import("../full.zig");
const redis_mod = @import("../redis.zig");

pub const FullRedis = full_mod.FullRedis;
pub const Error = redis_mod.Error;

pub const UNLOCK_SCRIPT =
    "if redis.call('get', KEYS[1]) == ARGV[1] then\n    return redis.call('del', KEYS[1])\nelse\n    return 0\nend";

pub const RedLock = struct {
    gpa: std.mem.Allocator,
    key: []u8,
    token: [22]u8,
    instances: std.ArrayList(*FullRedis),

    pub fn keyName(self: *const RedLock) []const u8 {
        return self.key;
    }

    pub fn tokenText(self: *const RedLock) []const u8 {
        return &self.token;
    }

    pub fn lockedCount(self: *const RedLock) usize {
        return self.instances.items.len;
    }

    /// 主动释放（比较令牌后删除）。
    pub fn release(self: *RedLock) void {
        for (self.instances.items) |rds| unlockInstance(rds, self.key, &self.token);
        self.instances.clearRetainingCapacity();
    }

    pub fn deinit(self: *RedLock) void {
        self.release();
        self.instances.deinit(self.gpa);
        self.gpa.free(self.key);
    }
};

/// 在多个独立实例上获取 RedLock；超时返回 `null`。
pub fn acquireRedLock(
    gpa: std.mem.Allocator,
    instances: []const *FullRedis,
    key: []const u8,
    ms_timeout: i64,
    ms_expire: i64,
) !?RedLock {
    if (instances.len == 0) return error.NoInstances;
    if (key.len == 0) return error.EmptyKey;
    if (ms_timeout <= 0 or ms_expire <= 0) return error.InvalidTimeout;

    const io = instances[0].redis.io;
    var token: [22]u8 = undefined;
    randomToken(io, &token);

    const quorum = instances.len / 2 + 1;
    const clock_drift_factor = 0.01;
    const retry_delay_ms: i64 = 200;
    const expire_seconds: i64 = @divTrunc(ms_expire, 1000);
    const start = zzigNowMs();

    while (true) {
        const lock_start = zzigNowMs();
        var locked: std.ArrayList(*FullRedis) = .empty;
        defer locked.deinit(gpa);

        for (instances) |rds| {
            const ok = rds.set(key, &token, expire_seconds) catch false;
            if (ok) try locked.append(gpa, rds);
        }

        const elapsed = zzigNowMs() - lock_start;
        const drift: i64 = @intFromFloat(@as(f64, @floatFromInt(ms_expire)) * clock_drift_factor);
        const validity = ms_expire - elapsed - drift;
        if (locked.items.len >= quorum and validity > 0) {
            var owned_instances: std.ArrayList(*FullRedis) = .empty;
            try owned_instances.appendSlice(gpa, locked.items);
            return RedLock{
                .gpa = gpa,
                .key = try gpa.dupe(u8, key),
                .token = token,
                .instances = owned_instances,
            };
        }

        for (locked.items) |rds| unlockInstance(rds, key, &token);

        const total_elapsed = zzigNowMs() - start;
        if (total_elapsed >= ms_timeout) return null;

        var jitter_raw: [1]u8 = undefined;
        io.random(&jitter_raw);
        const jitter: i64 = @intCast(jitter_raw[0] % 50);
        const remaining = ms_timeout - total_elapsed;
        const delay = @max(@min(retry_delay_ms + jitter, remaining), 1);
        zzig.compat.sleep(@as(u64, @intCast(delay)) * std.time.ns_per_ms);
    }
}

fn zzigNowMs() i64 {
    return @intCast(@divTrunc(zzig.compat.nanoTimestamp(), std.time.ns_per_ms));
}

fn unlockInstance(rds: *FullRedis, key: []const u8, token: []const u8) void {
    // 锁是按 `FullRedis` 前缀写入的，释放脚本必须用同一个完整键
    const full = rds.keyAlloc(key) catch return;
    defer rds.gpa.free(full);
    var v = rds.redis.evalRaw(UNLOCK_SCRIPT, &.{full}, &.{token}) catch return;
    v.deinit(rds.gpa);
}

/// 22 位随机令牌（对应 C# `Rand.NextString(22)`）。
pub fn randomToken(io: std.Io, out: *[22]u8) void {
    const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    var raw: [22]u8 = undefined;
    io.random(&raw);
    for (raw, 0..) |b, i| out[i] = chars[b % chars.len];
}

const testing = std.testing;

test "令牌为 22 位字母数字" {
    var threaded = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    var token: [22]u8 = undefined;
    randomToken(threaded.io(), &token);
    for (token) |c| {
        try testing.expect(std.ascii.isAlphanumeric(c));
    }
}

test "解锁脚本与 C# 一致" {
    try testing.expect(std.mem.indexOf(u8, UNLOCK_SCRIPT, "redis.call('get', KEYS[1]) == ARGV[1]") != null);
    try testing.expect(std.mem.indexOf(u8, UNLOCK_SCRIPT, "redis.call('del', KEYS[1])") != null);
}
