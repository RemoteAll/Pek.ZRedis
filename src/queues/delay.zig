//! 延迟队列（对齐 DH.NRedis `RedisDelayQueue<T>` / Rust `RedisDelayQueue`）。
//!
//! 基于有序集合：`score = Unix 秒 + 延迟秒数`；消费时 `ZRANGEBYSCORE 0 now LIMIT 0 n`
//! 找到到期消息，再以 `ZREM` 抢占（同一消息只有一个消费者能删除成功）。
//!
//! Zig 侧不内置定时器线程：提供 [`RedisDelayQueue.transferDue`]（立即转移）与
//! [`RedisDelayQueue.transferLoop`]（调用方线程循环，带取消标记），由宿主决定调度方式。

const std = @import("std");
const zzig = @import("zzig");
const resp = @import("../resp.zig");
const redis_mod = @import("../redis.zig");
const encoder = @import("../encoder.zig");
const base = @import("base.zig");
const queue_mod = @import("queue.zig");

pub const Redis = redis_mod.Redis;
pub const Error = redis_mod.Error;
pub const RedisQueue = queue_mod.RedisQueue;

pub const RedisDelayQueue = struct {
    gpa: std.mem.Allocator,
    redis: *Redis,
    key: []u8,
    settings: base.Settings = .{},
    /// 默认延迟秒数（对应 C# `Delay`）
    default_delay_seconds: i64 = 60,
    /// 转移轮询间隔秒数（对应 C# `TransferInterval`）
    transfer_interval_seconds: u64 = 10,

    pub fn init(redis: *Redis, key: []const u8) !RedisDelayQueue {
        return .{ .gpa = redis.gpa, .redis = redis, .key = try redis.gpa.dupe(u8, key) };
    }

    pub fn deinit(self: *RedisDelayQueue) void {
        self.settings.deinit(self.gpa);
        self.gpa.free(self.key);
    }

    pub fn count(self: *RedisDelayQueue) Error!i64 {
        var v = try self.redis.execute(&.{ "ZCARD", self.key });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    pub fn isEmpty(self: *RedisDelayQueue) Error!bool {
        return (try self.count()) == 0;
    }

    /// 添加延迟消息（`ZADD score=now+delay`）。
    pub fn add(self: *RedisDelayQueue, value: anytype, delay_seconds: i64) Error!i64 {
        const maybe = encoder.encodeValueAlloc(self.gpa, value) catch return error.Type;
        defer if (maybe) |p| self.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else "";
        return self.addRaw(payload, delay_seconds);
    }

    pub fn addRaw(self: *RedisDelayQueue, payload: []const u8, delay_seconds: i64) Error!i64 {
        const traced = try self.settings.attachTraceAlloc(self.gpa, payload);
        defer self.gpa.free(traced);
        const score = zzig.compat.timestamp() + delay_seconds;
        const score_text = resp.formatF64Alloc(self.gpa, @floatFromInt(score)) catch return error.IoError;
        defer self.gpa.free(score_text);

        var last: i64 = 0;
        var attempt: usize = 0;
        while (attempt <= self.settings.retry_times_when_send_failed) : (attempt += 1) {
            var v = try self.redis.execute(&.{ "ZADD", self.key, score_text, traced });
            defer v.deinit(self.redis.gpa);
            last = v.asInt() orelse 0;
            // ZADD 返回新增成员数；>=0 即视为成功
            if (last >= 0) return last;
            if (attempt < self.settings.retry_times_when_send_failed) {
                zzig.compat.sleep(self.settings.retry_interval_ms * std.time.ns_per_ms);
            }
        }
        if (self.settings.throw_on_failure) {
            self.redis.setErrorDiag("发布到延迟队列[{s}]失败！", .{self.key});
            return error.Server;
        }
        return last;
    }

    /// 批量添加（默认延迟）。
    pub fn addMany(self: *RedisDelayQueue, values: []const []const u8) Error!i64 {
        if (values.len == 0) return 0;
        const score = zzig.compat.timestamp() + self.default_delay_seconds;
        const score_text = resp.formatF64Alloc(self.gpa, @floatFromInt(score)) catch return error.IoError;
        defer self.gpa.free(score_text);

        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        var owned: std.ArrayList([]u8) = .empty;
        defer {
            for (owned.items) |s| self.gpa.free(s);
            owned.deinit(self.gpa);
        }

        try args.append(self.gpa, "ZADD");
        try args.append(self.gpa, self.key);
        for (values) |v| {
            const traced = try self.settings.attachTraceAlloc(self.gpa, v);
            try owned.append(self.gpa, traced);
            try args.append(self.gpa, score_text);
            try args.append(self.gpa, traced);
        }
        var rv = try self.redis.execute(args.items);
        defer rv.deinit(self.redis.gpa);
        return rv.asInt() orelse 0;
    }

    pub fn remove(self: *RedisDelayQueue, value: anytype) Error!i64 {
        const maybe = encoder.encodeValueAlloc(self.gpa, value) catch return error.Type;
        defer if (maybe) |p| self.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else "";
        var v = try self.redis.execute(&.{ "ZREM", self.key, payload });
        defer v.deinit(self.redis.gpa);
        return v.asInt() orelse 0;
    }

    /// 取出最多 `limit` 条已到期消息（`ZRANGEBYSCORE` + `ZREM` 抢占）。
    pub fn takeDue(self: *RedisDelayQueue, gpa: std.mem.Allocator, limit: usize) Error![][]u8 {
        if (limit == 0) return gpa.alloc([]u8, 0) catch error.IoError;
        const now = zzig.compat.timestamp();
        const now_text = resp.formatF64Alloc(self.gpa, @floatFromInt(now)) catch return error.IoError;
        defer self.gpa.free(now_text);
        var lb: [24]u8 = undefined;
        const limit_text = std.fmt.bufPrint(&lb, "{d}", .{limit}) catch return error.Config;

        var candidates = try self.redis.execute(&.{ "ZRANGEBYSCORE", self.key, "0", now_text, "LIMIT", "0", limit_text });
        defer candidates.deinit(self.redis.gpa);
        const items = try self.redis.expectArray(&candidates);

        var out: std.ArrayList([]u8) = .empty;
        errdefer {
            for (out.items) |s| gpa.free(s);
            out.deinit(gpa);
        }
        for (items) |item| {
            const raw = item.asString() orelse continue;
            var removed = try self.redis.execute(&.{ "ZREM", self.key, raw });
            defer removed.deinit(self.redis.gpa);
            if ((removed.asInt() orelse 0) > 0) {
                try out.append(gpa, try gpa.dupe(u8, raw));
            }
        }
        return out.toOwnedSlice(gpa);
    }

    /// 取一条到期消息。`timeout_seconds == 0` 时最多等 60 秒；负数只尝试一次。
    pub fn takeOne(self: *RedisDelayQueue, gpa: std.mem.Allocator, timeout_seconds: i64) Error!?[]u8 {
        var timeout: i64 = if (timeout_seconds == 0) 60 else timeout_seconds;
        while (true) {
            const due = try self.takeDue(gpa, 1);
            if (due.len > 0) {
                const first = due[0];
                if (due.len > 1) {
                    for (due[1..]) |extra| gpa.free(extra);
                }
                gpa.free(due);
                return first;
            }
            gpa.free(due);
            if (timeout <= 0) return null;
            zzig.compat.sleep(std.time.ns_per_s);
            timeout -= 1;
        }
    }

    /// 立即把当前所有到期消息转移到目标队列（不睡眠），返回转移条数。
    pub fn transferDue(self: *RedisDelayQueue, target: *RedisQueue) Error!usize {
        var total: usize = 0;
        while (true) {
            const messages = try self.takeDue(self.gpa, 10);
            defer {
                for (messages) |m| self.gpa.free(m);
                self.gpa.free(messages);
            }
            if (messages.len == 0) return total;
            total += messages.len;
            _ = try target.addMany(messages);
        }
    }

    /// 阻塞式转移循环（直到 `cancel` 置位）；建议由调用方线程运行。
    pub fn transferLoop(self: *RedisDelayQueue, target: *RedisQueue, cancel: *std.atomic.Value(bool)) Error!void {
        while (!cancel.load(.monotonic)) {
            if (try self.transferDue(target) == 0) {
                var i: u64 = 0;
                while (i < self.transfer_interval_seconds * 10) : (i += 1) {
                    if (cancel.load(.monotonic)) return;
                    zzig.compat.sleep(100 * std.time.ns_per_ms);
                }
            }
        }
    }
};
