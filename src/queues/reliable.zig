//! 可靠队列（对齐 DH.NRedis `RedisReliableQueue<T>` / Rust `RedisReliableQueue`）。
//!
//! 键布局（与 C#/Rust 完全一致，双方可混部消费）：
//! - 主队列 `{key}`：`LPUSH` 生产、`RPOPLPUSH` 消费（弹出同时备份到确认队列）；
//! - 确认队列 `{key}:Ack:{ukey}`：确认用 `LREM 1`；
//! - 状态键 `{key}:Status:{ukey}`：JSON（PascalCase、ISO 时间），7 天过期；
//! - 全局清理权 `{key}:AllStatus`：`SET NX EX RetryInterval` 抢占；
//! - 死信：`LastActive + RetryInterval * 10 < now` 时回滚该消费者的确认队列；
//! - 延迟消息键：`{key}:Delay`（ZSet，`score = Unix 秒 + 延迟`）。

const std = @import("std");
const zzig = @import("zzig");
const redis_mod = @import("../redis.zig");
const encoder = @import("../encoder.zig");
const base = @import("base.zig");
const status_mod = @import("status.zig");
const queue_mod = @import("queue.zig");
const delay_mod = @import("delay.zig");

pub const Redis = redis_mod.Redis;
pub const Error = redis_mod.Error;
pub const QueueStatus = status_mod.QueueStatus;

pub const RedisReliableQueue = struct {
    gpa: std.mem.Allocator,
    redis: *Redis,
    /// 主队列键（含前缀）
    key: []u8,
    /// 键前缀（Publish/Consume 的消息键需要）
    prefix: []u8,
    consumer_key: [8]u8,
    ack_key: []u8,
    status_key: []u8,
    all_status_key: []u8,
    settings: base.Settings = .{},
    /// 死信超时步长（秒，对应 C# `RetryInterval`）
    retry_interval_seconds: i64 = 60,
    /// 延迟转移周期（秒）
    delay_transfer_interval_seconds: u64 = 10,

    mutex: std.Io.Mutex = .init,
    status: QueueStatus,
    next_retry_ms: i64 = 0,

    pub fn init(redis: *Redis, key: []const u8, prefix: []const u8) !RedisReliableQueue {
        const gpa = redis.gpa;
        const now = zzig.DateTime.nowLocal();
        const consumer = status_mod.newConsumerKey(redis.io);

        const key_copy = try gpa.dupe(u8, key);
        errdefer gpa.free(key_copy);

        const prefix_copy = try gpa.dupe(u8, prefix);
        errdefer gpa.free(prefix_copy);

        const ack_key = try std.fmt.allocPrint(gpa, "{s}:Ack:{s}", .{ key, &consumer });
        errdefer gpa.free(ack_key);
        const status_key = try std.fmt.allocPrint(gpa, "{s}:Status:{s}", .{ key, &consumer });
        errdefer gpa.free(status_key);
        const all_status_key = try std.fmt.allocPrint(gpa, "{s}:AllStatus", .{key});
        errdefer gpa.free(all_status_key);

        var st = try QueueStatus.init(gpa, &consumer, now);
        errdefer st.deinit();

        return .{
            .gpa = gpa,
            .redis = redis,
            .key = key_copy,
            .prefix = prefix_copy,
            .consumer_key = consumer,
            .ack_key = ack_key,
            .status_key = status_key,
            .all_status_key = all_status_key,
            .status = st,
        };
    }

    pub fn deinit(self: *RedisReliableQueue) void {
        self.settings.deinit(self.gpa);
        self.status.deinit();
        self.gpa.free(self.key);
        self.gpa.free(self.prefix);
        self.gpa.free(self.ack_key);
        self.gpa.free(self.status_key);
        self.gpa.free(self.all_status_key);
    }

    pub fn consumerKey(self: *const RedisReliableQueue) []const u8 {
        return &self.consumer_key;
    }

    pub fn ackKey(self: *const RedisReliableQueue) []const u8 {
        return self.ack_key;
    }

    pub fn statusKey(self: *const RedisReliableQueue) []const u8 {
        return self.status_key;
    }

    /// 当前状态 JSON（调用方释放）。
    pub fn statusJsonAlloc(self: *RedisReliableQueue) ![]u8 {
        self.mutex.lockUncancelable(self.redis.io);
        defer self.mutex.unlock(self.redis.io);
        return self.status.toJsonAlloc();
    }

    pub fn count(self: *RedisReliableQueue) Error!i64 {
        var v = try self.redis.execute(&.{ "LLEN", self.key });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn isEmpty(self: *RedisReliableQueue) Error!bool {
        return (try self.count()) == 0;
    }

    // ───────────────────────── 生产 ─────────────────────────

    pub fn add(self: *RedisReliableQueue, value: anytype) Error!i64 {
        const maybe = encoder.encodeValueAlloc(self.gpa, value) catch return error.Type;
        defer if (maybe) |p| self.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else "";
        return self.pushRaw(&.{payload});
    }

    pub fn addMany(self: *RedisReliableQueue, values: []const []const u8) Error!i64 {
        if (values.len == 0) return 0;
        return self.pushRaw(values);
    }

    fn pushRaw(self: *RedisReliableQueue, values: []const []const u8) Error!i64 {
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        var owned: std.ArrayList([]u8) = .empty;
        defer {
            for (owned.items) |s| self.gpa.free(s);
            owned.deinit(self.gpa);
        }

        try args.append(self.gpa, "LPUSH");
        try args.append(self.gpa, self.key);
        for (values) |v| {
            const traced = try self.settings.attachTraceAlloc(self.gpa, v);
            try owned.append(self.gpa, traced);
            try args.append(self.gpa, traced);
        }

        var last: i64 = 0;
        var attempt: usize = 0;
        while (attempt <= self.settings.retry_times_when_send_failed) : (attempt += 1) {
            var rv = try self.redis.execute(args.items);
            defer rv.deinit(self.gpa);
            last = rv.asInt() orelse 0;
            if (last > 0) return last;
            if (attempt < self.settings.retry_times_when_send_failed) {
                zzig.compat.sleep(self.settings.retry_interval_ms * std.time.ns_per_ms);
            }
        }
        if (self.settings.throw_on_failure) {
            self.redis.setErrorDiag("发布到可靠队列[{s}]失败！", .{self.key});
            return error.Server;
        }
        return last;
    }

    /// 高级生产：消息体写 KV（带过期），消息键写入队列（与 `consume` 配对）。
    pub fn publish(self: *RedisReliableQueue, messages: []const Pair, expire_seconds: i64) Error!i64 {
        if (messages.len == 0) return 0;

        var pl = self.redis.pipeline();
        defer pl.deinit();
        var expire_buf: [24]u8 = undefined;
        const expire_text = std.fmt.bufPrint(&expire_buf, "{d}", .{expire_seconds}) catch return error.Config;

        var owned: std.ArrayList([]u8) = .empty;
        defer {
            for (owned.items) |s| self.gpa.free(s);
            owned.deinit(self.gpa);
        }

        for (messages) |m| {
            const full_key = try self.fullKeyAlloc(m.key);
            try owned.append(self.gpa, full_key);
            try pl.cmd(&.{ "SET", full_key, m.value });
            if (expire_seconds > 0) try pl.cmd(&.{ "EXPIRE", full_key, expire_text });
        }
        try pl.executeIgnore();

        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        try args.append(self.gpa, "LPUSH");
        try args.append(self.gpa, self.key);
        for (messages) |m| try args.append(self.gpa, m.key);
        var v = try self.redis.execute(args.items);
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub const Pair = struct { key: []const u8, value: []const u8 };

    // ───────────────────────── 消费 ─────────────────────────

    /// 消费获取：`RPOPLPUSH` 主队列 → 确认队列。`timeout_seconds < 0` 不阻塞。
    pub fn takeOne(self: *RedisReliableQueue, gpa: std.mem.Allocator, timeout_seconds: i64) Error!?[]u8 {
        const raw = try self.takeOneString(gpa, timeout_seconds);
        return raw;
    }

    fn takeOneString(self: *RedisReliableQueue, gpa: std.mem.Allocator, timeout_seconds: i64) Error!?[]u8 {
        _ = try self.retryAck();

        var v = if (timeout_seconds >= 0) blk: {
            var tb: [24]u8 = undefined;
            const t = std.fmt.bufPrint(&tb, "{d}", .{timeout_seconds}) catch return error.Config;
            break :blk try self.redis.executeBlocking(&.{ "BRPOPLPUSH", self.key, self.ack_key, t }, timeout_seconds);
        } else try self.redis.execute(&.{ "RPOPLPUSH", self.key, self.ack_key });
        defer v.deinit(self.gpa);

        if (v.isNull()) return null;
        self.bumpConsumes(1);
        return v.asBytesAlloc(gpa) catch return error.IoError;
    }

    /// 批量消费（管道 `RPOPLPUSH`）。
    pub fn take(self: *RedisReliableQueue, gpa: std.mem.Allocator, limit: usize) Error![][]u8 {
        if (limit == 0) return gpa.alloc([]u8, 0) catch error.IoError;
        _ = try self.retryAck();

        var pl = self.redis.pipeline();
        defer pl.deinit();
        for (0..limit) |_| try pl.cmd(&.{ "RPOPLPUSH", self.key, self.ack_key });
        const values = try pl.execute();
        defer {
            for (values) |*v| v.deinit(self.gpa);
            self.gpa.free(values);
        }

        var out: std.ArrayList([]u8) = .empty;
        errdefer {
            for (out.items) |s| gpa.free(s);
            out.deinit(gpa);
        }
        for (values) |v| {
            if (v.isNull()) break;
            const bytes = (v.asBytesAlloc(gpa) catch return error.IoError) orelse break;
            try out.append(gpa, bytes);
            self.bumpConsumes(1);
        }
        return out.toOwnedSlice(gpa);
    }

    /// 确认消费（`LREM ack 1 msg`），返回删除数量。
    pub fn acknowledge(self: *RedisReliableQueue, keys: []const []const u8) Error!i64 {
        if (keys.len == 0) return 0;
        var pl = self.redis.pipeline();
        defer pl.deinit();
        for (keys) |k| try pl.cmd(&.{ "LREM", self.ack_key, "1", k });
        const values = try pl.execute();
        defer {
            for (values) |*v| v.deinit(self.gpa);
            self.gpa.free(values);
        }
        var removed: i64 = 0;
        for (values) |v| removed += v.asInt() orelse 0;
        self.bumpAcks(@intCast(keys.len));
        return removed;
    }

    /// 从确认队列弹出消息（不确认），用于恢复现场。
    pub fn takeAck(self: *RedisReliableQueue, gpa: std.mem.Allocator, limit: usize) Error![][]u8 {
        var out: std.ArrayList([]u8) = .empty;
        errdefer {
            for (out.items) |s| gpa.free(s);
            out.deinit(gpa);
        }
        for (0..limit) |_| {
            var v = try self.redis.execute(&.{ "RPOP", self.ack_key });
            defer v.deinit(self.gpa);
            if (v.isNull()) break;
            const bytes = (v.asBytesAlloc(gpa) catch return error.IoError) orelse break;
            try out.append(gpa, bytes);
        }
        return out.toOwnedSlice(gpa);
    }

    /// 高级消费：与 `publish` 配对；处理成功后删除消息体并确认。
    pub fn consume(self: *RedisReliableQueue, timeout_seconds: i64, ctx: anytype) Error!bool {
        const msg_id = (try self.takeOneString(self.gpa, timeout_seconds)) orelse return false;
        defer self.gpa.free(msg_id);

        const full = try self.fullKeyAlloc(msg_id);
        defer self.gpa.free(full);
        const body = (try self.redis.getRaw(self.gpa, full)) orelse {
            _ = try self.acknowledge(&.{msg_id});
            return false;
        };
        defer self.gpa.free(body);

        try ctx.handle(msg_id, body);
        _ = try self.redis.remove(full);
        _ = try self.acknowledge(&.{msg_id});
        return true;
    }

    /// 大循环消费原始字符串（失败时错误计数，累计 10 次丢弃）。
    pub fn consumeRaw(self: *RedisReliableQueue, timeout_seconds: i64, poll_ms: u64, cancel: *std.atomic.Value(bool), ctx: anytype) Error!void {
        var bak = try self.redis.createSub(self.backupDb());
        defer bak.deinit();

        while (!cancel.load(.monotonic)) {
            const raw = (try self.takeOneString(self.gpa, timeout_seconds)) orelse {
                zzig.compat.sleep(poll_ms * std.time.ns_per_ms);
                continue;
            };
            defer self.gpa.free(raw);

            const ok = ctx.handle(raw) catch false;
            if (ok) {
                _ = try self.redis.remove(raw);
                _ = try self.acknowledge(&.{raw});
                continue;
            }

            var digest: [16]u8 = undefined;
            std.crypto.hash.Md5.hash(raw, &digest, .{});
            const hex = std.fmt.bytesToHex(digest, .lower);
            const error_key = try std.fmt.allocPrint(self.gpa, "{s}:Error:{s}", .{ self.key, hex[0..] });
            defer self.gpa.free(error_key);
            const count_value = try bak.increment(error_key, 1);
            if (count_value < 10) {
                _ = try bak.setExpire(error_key, 30 * 24 * 3600);
            } else {
                _ = try self.redis.remove(raw);
                _ = try self.acknowledge(&.{raw});
            }
        }
    }

    /// 大循环消费 JSON：解析为 `T` 后交给 `ctx.onMessage(parsed, raw)`。
    pub fn consumeJson(self: *RedisReliableQueue, comptime T: type, timeout_seconds: i64, poll_ms: u64, cancel: *std.atomic.Value(bool), ctx: anytype) Error!void {
        var bak = try self.redis.createSub(self.backupDb());
        defer bak.deinit();

        while (!cancel.load(.monotonic)) {
            const raw = (try self.takeOneString(self.gpa, timeout_seconds)) orelse {
                zzig.compat.sleep(poll_ms * std.time.ns_per_ms);
                continue;
            };
            defer self.gpa.free(raw);

            var parsed = encoder.parseJsonAlloc(T, self.gpa, raw) catch {
                try self.recordFailure(&bak, raw);
                continue;
            };
            defer parsed.deinit();

            const ok = ctx.onMessage(&parsed.value, raw) catch false;
            if (ok) {
                _ = try self.redis.remove(raw);
                _ = try self.acknowledge(&.{raw});
            } else {
                try self.recordFailure(&bak, raw);
            }
        }
    }

    fn recordFailure(self: *RedisReliableQueue, bak: *Redis, raw: []const u8) Error!void {
        var digest: [16]u8 = undefined;
        std.crypto.hash.Md5.hash(raw, &digest, .{});
        const hex = std.fmt.bytesToHex(digest, .lower);
        const error_key = try std.fmt.allocPrint(self.gpa, "{s}:Error:{s}", .{ self.key, hex[0..] });
        defer self.gpa.free(error_key);
        const count_value = try bak.increment(error_key, 1);
        if (count_value < 10) {
            _ = try bak.setExpire(error_key, 30 * 24 * 3600);
        } else {
            _ = try self.redis.remove(raw);
            _ = try self.acknowledge(&.{raw});
        }
    }

    fn backupDb(self: *const RedisReliableQueue) i32 {
        return if (self.redis.opts.db == 15) 0 else self.redis.opts.db + 1;
    }

    fn bumpConsumes(self: *RedisReliableQueue, delta: i64) void {
        self.mutex.lockUncancelable(self.redis.io);
        defer self.mutex.unlock(self.redis.io);
        self.status.consumes += delta;
    }

    fn bumpAcks(self: *RedisReliableQueue, delta: i64) void {
        self.mutex.lockUncancelable(self.redis.io);
        defer self.mutex.unlock(self.redis.io);
        self.status.acks += delta;
    }

    fn fullKeyAlloc(self: *const RedisReliableQueue, key: []const u8) ![]u8 {
        if (self.prefix.len == 0) return self.gpa.dupe(u8, key);
        if (key.len >= self.prefix.len and std.ascii.eqlIgnoreCase(key[0..self.prefix.len], self.prefix)) {
            return self.gpa.dupe(u8, key);
        }
        return std.fmt.allocPrint(self.gpa, "{s}{s}", .{ self.prefix, key });
    }

    // ───────────────────────── 死信 ─────────────────────────

    /// 定时回滚本消费者确认队列，并周期性执行全局清理。
    pub fn retryAck(self: *RedisReliableQueue) Error!usize {
        const now_ms: i64 = @intCast(@divTrunc(zzig.compat.nanoTimestamp(), std.time.ns_per_ms));
        self.mutex.lockUncancelable(self.redis.io);
        if (self.next_retry_ms > now_ms) {
            self.mutex.unlock(self.redis.io);
            return 0;
        }
        self.next_retry_ms = now_ms + self.retry_interval_seconds * 1000;
        self.mutex.unlock(self.redis.io);

        const rolled = try self.rollbackAck(self.ack_key, self.key);
        defer {
            for (rolled) |s| self.gpa.free(s);
            self.gpa.free(rolled);
        }

        try self.updateStatus();

        const json = try self.statusJsonAlloc();
        defer self.gpa.free(json);
        if (try self.redis.add(self.all_status_key, json, self.retry_interval_seconds)) {
            _ = try self.rollbackAllAck();
        }
        return rolled.len;
    }

    /// 回滚指定确认队列到主队列，返回消息（调用方释放）。
    pub fn rollbackAck(self: *RedisReliableQueue, from_ack_key: []const u8, to_key: []const u8) Error![][]u8 {
        var out: std.ArrayList([]u8) = .empty;
        errdefer {
            for (out.items) |s| self.gpa.free(s);
            out.deinit(self.gpa);
        }
        while (true) {
            var v = try self.redis.execute(&.{ "RPOPLPUSH", from_ack_key, to_key });
            defer v.deinit(self.gpa);
            if (v.isNull()) break;
            const bytes = (v.asBytesAlloc(self.gpa) catch return error.IoError) orelse break;
            try out.append(self.gpa, bytes);
        }
        return out.toOwnedSlice(self.gpa);
    }

    /// 全局回滚死信（由抢到 `AllStatus` 的实例执行）。
    pub fn rollbackAllAck(self: *RedisReliableQueue) Error!i64 {
        var total: i64 = 0;
        const status_pattern = try std.fmt.allocPrint(self.gpa, "{s}:Status:*", .{self.key});
        defer self.gpa.free(status_pattern);
        const status_keys = try self.redis.keysRaw(status_pattern);
        defer self.redis.freeStrings(status_keys);

        var known_acks: std.ArrayList([]u8) = .empty;
        defer {
            for (known_acks.items) |s| self.gpa.free(s);
            known_acks.deinit(self.gpa);
        }

        for (status_keys) |full_status| {
            const consumer = if (std.mem.lastIndexOfScalar(u8, full_status, ':')) |idx|
                full_status[idx + 1 ..]
            else
                continue;
            const ack_key = try std.fmt.allocPrint(self.gpa, "{s}:Ack:{s}", .{ self.key, consumer });
            defer self.gpa.free(ack_key);
            try known_acks.append(self.gpa, try self.gpa.dupe(u8, ack_key));

            const text = (try self.redis.getRaw(self.gpa, full_status)) orelse continue;
            defer self.gpa.free(text);
            var st = QueueStatus.fromJsonAlloc(self.gpa, text) catch continue;
            defer st.deinit();

            const deadline_secs = st.last_active.toUnixSeconds() + self.retry_interval_seconds * 10;
            const now_secs = zzig.compat.timestamp();
            if (deadline_secs < now_secs) {
                if (try self.redis.containsKey(ack_key)) {
                    const list = try self.rollbackAck(ack_key, self.key);
                    total += @intCast(list.len);
                    for (list) |s| self.gpa.free(s);
                    self.gpa.free(list);
                }
                _ = try self.redis.remove(full_status);
            }
        }

        const ack_pattern = try std.fmt.allocPrint(self.gpa, "{s}:Ack:*", .{self.key});
        defer self.gpa.free(ack_pattern);
        const ack_keys = try self.redis.keysRaw(ack_pattern);
        defer self.redis.freeStrings(ack_keys);
        for (ack_keys) |k| {
            var known = false;
            for (known_acks.items) |kk| {
                if (std.mem.eql(u8, kk, k)) {
                    known = true;
                    break;
                }
            }
            if (!known) _ = try self.redis.remove(k);
        }
        return total;
    }

    /// 清空所有 Ack 队列（危险操作，需所有消费者停止）。
    pub fn clearAllAck(self: *RedisReliableQueue) Error!i64 {
        const pattern = try std.fmt.allocPrint(self.gpa, "{s}:Ack:*", .{self.key});
        defer self.gpa.free(pattern);
        // 通过 SCAN + DEL（复用 FullRedis 语义的简化实现）
        var total: i64 = 0;
        var cursor: u64 = 0;
        while (true) {
            const page = try self.redis.scan(cursor, pattern, 100);
            defer self.redis.freeStrings(page.keys);
            for (page.keys) |k| total += try self.redis.remove(k);
            if (page.cursor == 0) break;
            cursor = page.cursor;
        }
        return total;
    }

    fn updateStatus(self: *RedisReliableQueue) Error!void {
        self.mutex.lockUncancelable(self.redis.io);
        self.status.last_active = zzig.DateTime.nowLocal();
        const json = self.status.toJsonAlloc() catch {
            self.mutex.unlock(self.redis.io);
            return error.IoError;
        };
        self.mutex.unlock(self.redis.io);
        defer self.gpa.free(json);
        _ = try self.redis.set(self.status_key, json, 7 * 24 * 3600);
    }

    // ───────────────────────── 延迟队列 ─────────────────────────

    /// 延迟队列（键 `{key}:Delay`）。
    pub fn delayQueue(self: *RedisReliableQueue) !delay_mod.RedisDelayQueue {
        const delay_key = try std.fmt.allocPrint(self.gpa, "{s}:Delay", .{self.key});
        defer self.gpa.free(delay_key);
        var q = try delay_mod.RedisDelayQueue.init(self.redis, delay_key);
        q.transfer_interval_seconds = @max(self.delay_transfer_interval_seconds, 1);
        return q;
    }

    /// 添加延迟消息（转移由调用方驱动 `delayQueue().transferDue`）。
    pub fn addDelay(self: *RedisReliableQueue, value: anytype, delay_seconds: i64) Error!i64 {
        var q = self.delayQueue() catch return error.IoError;
        defer q.deinit();
        return q.add(value, delay_seconds);
    }

    /// 把到期延迟消息转移到主队列一次，返回条数。
    pub fn transferDelayDue(self: *RedisReliableQueue) Error!usize {
        var delay = self.delayQueue() catch return error.IoError;
        defer delay.deinit();
        var target = queue_mod.RedisQueue.init(self.redis, self.key) catch return error.IoError;
        defer target.deinit();
        return delay.transferDue(&target);
    }
};
