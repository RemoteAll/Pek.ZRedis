//! 跨语言严格对拍（Zig ↔ C# / Rust）。
//!
//! 三者连**同一个 Redis 实例**、用**同一套键与编码**；因此「对齐」的判据是
//! **键名、值字节、score 文本、队列/Stream 布局**逐字节一致。本模块把这份规范写成代码：
//!
//! - `write`：按规范写入（走封装 API：`RedisHash` / `RedisList` / `RedisQueue` …）；
//! - `verify`：按规范校验（**走裸命令** GET/HGETALL/LRANGE/…，避免「自己写自己读」自证）；
//! - `clean`：按前缀清空（SCAN 找键再 DEL）；
//! - `report`：打印版本 / 服务器类型 / 当前库键数。
//!
//! 外部口径由 `tools/interop-strict.ps1` 用 `redis-cli` 交叉校验：
//! Zig 写 → redis-cli 读（字节一致）、redis-cli 写 → Zig 读（字节一致）。

const std = @import("std");
const zzig = @import("zzig");
const redis_mod = @import("redis.zig");
const encoder = @import("encoder.zig");
const full_mod = @import("full.zig");
const hash_mod = @import("hash.zig");
const list_mod = @import("list.zig");
const set_mod = @import("set.zig");
const sortedset_mod = @import("sortedset.zig");
const queue_mod = @import("queues/queue.zig");
const delay_mod = @import("queues/delay.zig");
const reliable_mod = @import("queues/reliable.zig");
const stream_mod = @import("queues/stream.zig");

pub const Redis = redis_mod.Redis;
pub const Error = redis_mod.Error;

pub const Mode = enum { write, verify, clean, report };

/// 默认键前缀（脚本会用 `--prefix` 覆盖；末尾冒号由调用方给全）
pub const default_prefix = "pezredis:interop:";

/// 交叉对拍的**规范数据**：写与校验共用同一份常量，校验侧不复用写侧结果。
const spec = struct {
    const plain_text = "hello-Zig";
    const utf8_text = "中文-对齐-🚀";
    const binary = [_]u8{ 0x00, 0x01, 0x0d, 0x0a, 0x22, 0xff };
    const ttl_value = "ttl-value";
    const ttl_seconds: i64 = 120;
    const counter_target: i64 = 3;
    const bool_text = "True";
    const hash_fields = [_]Pair{ .{ .name = "f1", .value = "1" }, .{ .name = "f2", .value = "中文" } };
    const list_items = [_][]const u8{ "a", "b", "c" };
    const set_members = [_][]const u8{ "x", "y" };
    const zset_pairs = [_]Scored{
        .{ .member = "m1", .score = 1.5, .text = "1.5" },
        .{ .member = "m2", .score = -2.25, .text = "-2.25" },
    };
    const queue_item = "q1";
    const delay_item = "d1";
    const delay_seconds: i64 = 60;
    const reliable_item = "r1";
    const stream_field = stream_mod.Field{ .name = "f1", .value = "v1" };
    const lock_token = "0123456789abcdef0123456789abcdef";
    const lock_offset_ms: u64 = 30_000;
};

const Pair = struct { name: []const u8, value: []const u8 };
const Scored = struct { member: []const u8, score: f64, text: []const u8 };

// ───────────────────────────── 报告 ─────────────────────────────

pub const Report = struct {
    checks: usize = 0,
    failures: usize = 0,

    fn pass(self: *Report, name: []const u8) void {
        self.checks += 1;
        std.debug.print("CHECK={s} OK\n", .{name});
    }

    fn fail(self: *Report, name: []const u8, comptime fmt: []const u8, args: anytype) void {
        self.checks += 1;
        self.failures += 1;
        std.debug.print("CHECK={s} FAIL ", .{name});
        std.debug.print(fmt ++ "\n", args);
    }

    fn expectBytes(self: *Report, name: []const u8, actual: ?[]const u8, expected: []const u8) void {
        if (actual) |a| {
            if (std.mem.eql(u8, a, expected)) return self.pass(name);
            var got: [128]u8 = undefined;
            self.fail(name, "期望 [{s}]（{d} 字节），实际 [{s}]（{d} 字节）", .{
                expected,
                expected.len,
                shorten(&got, a),
                a.len,
            });
            return;
        }
        self.fail(name, "键不存在或类型不符（期望 [{s}]）", .{expected});
    }

    fn expectInt(self: *Report, name: []const u8, actual: anytype, expected: i64) void {
        const v: i64 = @intCast(actual);
        if (v == expected) return self.pass(name);
        self.fail(name, "期望 {d}，实际 {d}", .{ expected, v });
    }

    fn expectTrue(self: *Report, name: []const u8, actual: bool, comptime why: []const u8) void {
        if (actual) return self.pass(name);
        self.fail(name, why, .{});
    }
};

/// 失败信息里截断过长/含控制字符的字节，避免刷屏（日志规范：截断而非省略）。
fn shorten(buf: []u8, bytes: []const u8) []const u8 {
    const n = @min(bytes.len, buf.len);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const b = bytes[i];
        buf[i] = if (b < 0x20 or b == 0x7f) '.' else b;
    }
    return buf[0..n];
}

// ───────────────────────────── 键名 ─────────────────────────────

const Keys = struct {
    str_plain: []const u8,
    str_utf8: []const u8,
    str_bin: []const u8,
    str_ttl: []const u8,
    counter: []const u8,
    bool_key: []const u8,
    time_key: []const u8,
    hash_key: []const u8,
    list_key: []const u8,
    set_key: []const u8,
    zset_key: []const u8,
    queue_key: []const u8,
    delay_key: []const u8,
    reliable_key: []const u8,
    stream_key: []const u8,
    lock_key: []const u8,

    fn build(a: std.mem.Allocator, prefix: []const u8) !Keys {
        return .{
            .str_plain = try std.fmt.allocPrint(a, "{s}str:plain", .{prefix}),
            .str_utf8 = try std.fmt.allocPrint(a, "{s}str:utf8", .{prefix}),
            .str_bin = try std.fmt.allocPrint(a, "{s}str:bin", .{prefix}),
            .str_ttl = try std.fmt.allocPrint(a, "{s}str:ttl", .{prefix}),
            .counter = try std.fmt.allocPrint(a, "{s}num:counter", .{prefix}),
            .bool_key = try std.fmt.allocPrint(a, "{s}val:bool", .{prefix}),
            .time_key = try std.fmt.allocPrint(a, "{s}val:time", .{prefix}),
            .hash_key = try std.fmt.allocPrint(a, "{s}hash", .{prefix}),
            .list_key = try std.fmt.allocPrint(a, "{s}list", .{prefix}),
            .set_key = try std.fmt.allocPrint(a, "{s}set", .{prefix}),
            .zset_key = try std.fmt.allocPrint(a, "{s}zset", .{prefix}),
            .queue_key = try std.fmt.allocPrint(a, "{s}queue", .{prefix}),
            .delay_key = try std.fmt.allocPrint(a, "{s}delay", .{prefix}),
            .reliable_key = try std.fmt.allocPrint(a, "{s}reliable", .{prefix}),
            .stream_key = try std.fmt.allocPrint(a, "{s}stream", .{prefix}),
            .lock_key = try std.fmt.allocPrint(a, "{s}str:plain:lock", .{prefix}),
        };
    }
};

// ───────────────────────────── 入口 ─────────────────────────────

/// 执行对拍动作，返回失败检查项数（0 = 全部通过）。
pub fn run(
    gpa: std.mem.Allocator,
    io: std.Io,
    mode: Mode,
    config: []const u8,
    prefix: []const u8,
) !usize {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    var r = try Redis.fromConfig(gpa, io, config);
    defer r.deinit();

    const keys = try Keys.build(a, if (prefix.len == 0) default_prefix else prefix);
    var rep = Report{};

    const result = switch (mode) {
        .write => writeAll(&r, a, keys, &rep),
        .verify => verifyAll(&r, a, keys, &rep),
        .clean => cleanAll(&r, a, prefix, &rep),
        .report => report(&r),
    };
    result catch |e| {
        std.debug.print("MESSAGE={s}（{s}）\n", .{ @errorName(e), r.lastError() });
        return e;
    };

    std.debug.print("CHECKS={d}\nFAILED={d}\n", .{ rep.checks, rep.failures });
    return rep.failures;
}

// ───────────────────────────── 写入 ─────────────────────────────

fn writeAll(r: *Redis, a: std.mem.Allocator, keys: Keys, rep: *Report) !void {
    _ = rep; // 写入阶段不判定；判定统一在 verify（含外部校验）

    // 字符串（含 UTF-8 / 二进制 / TTL）
    _ = try r.set(keys.str_plain, spec.plain_text, 0);
    _ = try r.set(keys.str_utf8, spec.utf8_text, 0);
    _ = try r.set(keys.str_bin, spec.binary[0..], 0);
    _ = try r.set(keys.str_ttl, spec.ttl_value, spec.ttl_seconds);

    // 计数器（INCR 3 次）
    _ = try r.remove(keys.counter);
    var i: i64 = 0;
    while (i < spec.counter_target) : (i += 1) _ = try r.increment(keys.counter, 1);

    // 布尔 / 时间（编码器口径：`True`、`yyyy-MM-dd HH:mm:ss.fff`）
    const bool_text = try encoder.encodeValueAlloc(a, true);
    _ = try r.set(keys.bool_key, bool_text.?, 0);
    const time_text = try encoder.encodeValueAlloc(a, zzig.DateTime.fromUnixSeconds(1_700_000_000));
    _ = try r.set(keys.time_key, time_text.?, 0);

    // 结构体
    var h = try hash_mod.RedisHash.init(r, keys.hash_key);
    defer h.deinit();
    for (spec.hash_fields) |f| _ = try h.set(f.name, f.value);

    var l = try list_mod.RedisList.init(r, keys.list_key);
    defer l.deinit();
    _ = try l.pushBackMany(&spec.list_items);

    var s = try set_mod.RedisSet.init(r, keys.set_key);
    defer s.deinit();
    _ = try s.addMany(&spec.set_members);

    var z = try sortedset_mod.RedisSortedSet.init(r, keys.zset_key);
    defer z.deinit();
    for (spec.zset_pairs) |p| _ = try z.add(p.member, p.score);

    // 队列族
    var q = try queue_mod.RedisQueue.init(r, keys.queue_key);
    defer q.deinit();
    _ = try q.add(spec.queue_item);

    var d = try delay_mod.RedisDelayQueue.init(r, keys.delay_key);
    defer d.deinit();
    _ = try d.addRaw(spec.delay_item, spec.delay_seconds);

    var rel = try reliable_mod.RedisReliableQueue.init(r, keys.reliable_key, keys.reliable_key);
    defer rel.deinit();
    _ = try rel.add(spec.reliable_item);

    var st = try stream_mod.RedisStream.init(r, keys.stream_key);
    defer st.deinit();
    _ = try st.addFields(&.{spec.stream_field}, null, false);

    // 分布式锁值格式（竖线分隔的 token 与绝对毫秒）：与 C#/Rust `parseLockExpire` 同口径
    const lock_value = try std.fmt.allocPrint(a, "{s}|{d}", .{ spec.lock_token, spec.lock_offset_ms });
    _ = try r.set(keys.lock_key, lock_value, 0);
}

// ───────────────────────────── 校验 ─────────────────────────────

fn verifyAll(r: *Redis, a: std.mem.Allocator, keys: Keys, rep: *Report) !void {
    // 1) 字符串逐字节
    rep.expectBytes("str.plain", try r.getRaw(a, keys.str_plain), spec.plain_text);
    rep.expectBytes("str.utf8", try r.getRaw(a, keys.str_utf8), spec.utf8_text);
    rep.expectBytes("str.binary", try r.getRaw(a, keys.str_bin), spec.binary[0..]);

    // 2) TTL 落在 (0, 规范值]
    const ttl = try r.getExpire(keys.str_ttl);
    rep.expectTrue("str.ttl", ttl > 0 and ttl <= spec.ttl_seconds, "TTL 不在 (0,120] 区间（可能未设置或已过期）");

    // 3) 计数器
    rep.expectBytes("num.counter", try r.getRaw(a, keys.counter), "3");

    // 4) 布尔 / 时间编码口径
    rep.expectBytes("val.bool", try r.getRaw(a, keys.bool_key), spec.bool_text);
    const time_value = try r.getRaw(a, keys.time_key);
    rep.expectTrue("val.time", isDateTimeMs(time_value orelse ""), "时间不是 `yyyy-MM-dd HH:mm:ss.fff` 口径");

    // 5) 哈希（HGETALL 逐对比较，顺序无关）
    {
        var v = try r.execute(&.{ "HGETALL", keys.hash_key });
        defer v.deinit(a);
        const items = try r.expectArray(&v);
        var matched: usize = 0;
        if (items.len == spec.hash_fields.len * 2) {
            var i: usize = 0;
            while (i + 1 < items.len) : (i += 2) {
                const field = items[i].asString() orelse "";
                const value = items[i + 1].asString() orelse "";
                for (spec.hash_fields) |f| {
                    if (std.mem.eql(u8, f.name, field) and std.mem.eql(u8, f.value, value)) matched += 1;
                }
            }
        }
        rep.expectTrue("hash.hset", matched == spec.hash_fields.len, "HGETALL 字段/值与规范不一致");
    }

    // 6) 列表（LRANGE 保序）
    {
        var v = try r.execute(&.{ "LRANGE", keys.list_key, "0", "-1" });
        defer v.deinit(a);
        const items = try r.expectArray(&v);
        var same = items.len == spec.list_items.len;
        if (same) {
            for (items, spec.list_items) |got, want| {
                if (!std.mem.eql(u8, got.asString() orelse "", want)) same = false;
            }
        }
        rep.expectTrue("list.rpush", same, "LRANGE 顺序或内容与规范不一致");
    }

    // 7) 集合（SMEMBERS 排序后比较）
    {
        var v = try r.execute(&.{ "SMEMBERS", keys.set_key });
        defer v.deinit(a);
        const items = try r.expectArray(&v);
        var same = items.len == spec.set_members.len;
        if (same) {
            var want = spec.set_members;
            for (items) |item| {
                const got = item.asString() orelse "";
                var hit = false;
                for (want, 0..) |w, idx| {
                    if (std.mem.eql(u8, w, got)) {
                        want[idx] = "";
                        hit = true;
                        break;
                    }
                }
                if (!hit) same = false;
            }
        }
        rep.expectTrue("set.sadd", same, "SMEMBERS 成员与规范不一致");
    }

    // 8) 有序集合（ZSCORE **文本**逐字比较：`1.5` / `-2.25`）
    for (spec.zset_pairs) |p| {
        var v = try r.execute(&.{ "ZSCORE", keys.zset_key, p.member });
        defer v.deinit(a);
        const name = try std.fmt.allocPrint(a, "zset.score.{s}", .{p.member});
        rep.expectBytes(name, v.asString(), p.text);
    }

    // 9) 普通队列（列表键，FIFO）
    {
        var v = try r.execute(&.{ "LINDEX", keys.queue_key, "0" });
        defer v.deinit(a);
        rep.expectBytes("queue.add", v.asString(), spec.queue_item);
    }

    // 10) 延迟队列（ZSET，score = Unix 秒 + 延迟）
    {
        var v = try r.execute(&.{ "ZRANGE", keys.delay_key, "0", "-1", "WITHSCORES" });
        defer v.deinit(a);
        const items = try r.expectArray(&v);
        var ok = items.len == 2 and std.mem.eql(u8, items[0].asString() orelse "", spec.delay_item);
        if (ok) {
            const text = items[1].asString() orelse "";
            const score = std.fmt.parseFloat(f64, text) catch 0;
            const now = @as(f64, @floatFromInt(zzig.compat.timestamp()));
            ok = @abs(score - (now + @as(f64, @floatFromInt(spec.delay_seconds)))) <= 300;
        }
        rep.expectTrue("queue.delay", ok, "延迟队列成员或 score（now+delay 秒）与规范不一致");
    }

    // 11) 可靠队列（主列表键）
    {
        var v = try r.execute(&.{ "LINDEX", keys.reliable_key, "0" });
        defer v.deinit(a);
        rep.expectBytes("queue.reliable", v.asString(), spec.reliable_item);
    }

    // 12) Stream（XRANGE 取首条：id 形如 `毫秒-序号`，字段逐个比较）
    {
        var v = try r.execute(&.{ "XRANGE", keys.stream_key, "-", "+", "COUNT", "1" });
        defer v.deinit(a);
        const entries = try r.expectArray(&v);
        var ok = entries.len == 1;
        if (ok) {
            const entry = try r.expectArray(&entries[0]);
            ok = entry.len == 2 and isStreamId(entry[0].asString() orelse "");
            if (ok) {
                const fields = try r.expectArray(&entry[1]);
                ok = fields.len == 2 and
                    std.mem.eql(u8, fields[0].asString() orelse "", spec.stream_field.name) and
                    std.mem.eql(u8, fields[1].asString() orelse "", spec.stream_field.value);
            }
        }
        rep.expectTrue("stream.xadd", ok, "Stream 消息 id 或字段与规范不一致");
    }

    // 13) 锁值格式（`{token}|{绝对过期毫秒}`，与 C#/Rust 同为 `parseLockExpire` 口径）
    {
        const raw = try r.getRaw(a, keys.lock_key);
        var ok = false;
        if (raw) |text| {
            const ms = full_mod.parseLockExpire(text);
            ok = ms == spec.lock_offset_ms;
        }
        rep.expectTrue("lock.value", ok, "锁值不是 token|绝对过期毫秒 口径（C#/Rust 同为竖线分隔）");
    }
}

// ───────────────────────────── 清理 / 报告 ─────────────────────────────

fn cleanAll(r: *Redis, a: std.mem.Allocator, prefix: []const u8, rep: *Report) !void {
    const pattern = try std.fmt.allocPrint(a, "{s}*", .{if (prefix.len == 0) default_prefix else prefix});
    const found = try r.keysRaw(pattern);
    defer r.freeStrings(found);
    var removed: i64 = 0;
    for (found) |key| removed += try r.remove(key);
    std.debug.print("CLEANED={d} PATTERN={s}\n", .{ removed, pattern });
    rep.pass("clean");
}

fn report(r: *Redis) !void {
    const version = try r.version();
    defer if (version) |v| r.gpa.free(v);
    const server = r.serverType() catch .redis;
    const dbsize = r.dbsize() catch 0;
    std.debug.print("VERSION={s}\nSERVER={s}\nDBSIZE={d}\n", .{
        version orelse "",
        @tagName(server),
        dbsize,
    });
}

// ───────────────────────────── 纯函数（可单测） ─────────────────────────────

/// `yyyy-MM-dd HH:mm:ss.fff`（C# 编码器口径）
fn isDateTimeMs(text: []const u8) bool {
    if (text.len != 23) return false;
    const shape = "dddd-dd-dd dd:dd:dd.ddd";
    for (text, shape) |c, want| {
        if (want == 'd') {
            if (!std.ascii.isDigit(c)) return false;
        } else if (c != want) return false;
    }
    return true;
}

/// Stream id：`<毫秒>-<序号>`
fn isStreamId(text: []const u8) bool {
    const dash = std.mem.indexOfScalar(u8, text, '-') orelse return false;
    if (dash == 0 or dash + 1 >= text.len) return false;
    for (text[0..dash]) |c| if (!std.ascii.isDigit(c)) return false;
    for (text[dash + 1 ..]) |c| if (!std.ascii.isDigit(c)) return false;
    return true;
}

const testing = std.testing;

test "时间口径判定" {
    try testing.expect(isDateTimeMs("2026-11-18 09:05:07.123"));
    try testing.expect(!isDateTimeMs("2026-11-18T09:05:07.123"));
    try testing.expect(!isDateTimeMs("2026-11-18 09:05:07"));
    try testing.expect(!isDateTimeMs(""));
}

test "Stream id 判定" {
    try testing.expect(isStreamId("1700000000000-0"));
    try testing.expect(!isStreamId("-1"));
    try testing.expect(!isStreamId("1700000000000-"));
    try testing.expect(!isStreamId("1700000000000"));
    try testing.expect(!isStreamId("abc-0"));
}
