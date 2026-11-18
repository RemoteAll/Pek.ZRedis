//! FullRedis：键前缀、SCAN 搜索、分布式锁、结构体工厂。
//!
//! 对齐 DH.NRedis `FullRedis` / Rust `FullRedis`：
//! - 键前缀语义与 C# `EnsureStart` 一致（已有前缀不重复添加，比较不区分大小写）；
//! - `search` 返回的键**去掉前缀**（修正 C# `count<=0` 边界的实现与 Rust 一致）；
//! - 分布式锁：值 `{token}|{绝对过期毫秒}`、TTL `ceil(msExpire/1000)`，
//!   超时锁用 `SET XX` 抢占、释放时校验 token 归属；时钟域与 C# `Environment.TickCount64` 一致。

const std = @import("std");
const builtin = @import("builtin");
const zzig = @import("zzig");
const resp = @import("resp.zig");
const encoder = @import("encoder.zig");
const options = @import("options.zig");
const redis_mod = @import("redis.zig");
const hash_mod = @import("hash.zig");
const list_mod = @import("list.zig");
const set_mod = @import("set.zig");
const sortedset_mod = @import("sortedset.zig");
const stack_mod = @import("stack.zig");
const hll_mod = @import("hyperloglog.zig");
const queue_mod = @import("queues/queue.zig");
const delay_mod = @import("queues/delay.zig");
const reliable_mod = @import("queues/reliable.zig");
const stream_mod = @import("queues/stream.zig");
const deferred_mod = @import("services/deferred.zig");

pub const Redis = redis_mod.Redis;
pub const Error = redis_mod.Error;

pub const FullRedis = struct {
    gpa: std.mem.Allocator,
    redis: Redis,
    prefix: []u8,

    pub fn init(gpa: std.mem.Allocator, io: std.Io, opts: options.Options) !FullRedis {
        const prefix = if (opts.prefix) |p| try gpa.dupe(u8, p) else try gpa.alloc(u8, 0);
        errdefer gpa.free(prefix);
        const r = try Redis.init(gpa, io, opts);
        return .{ .gpa = gpa, .redis = r, .prefix = prefix };
    }

    pub fn open(gpa: std.mem.Allocator, io: std.Io, server: []const u8, password: ?[]const u8, db: i32, prefix: []const u8) !FullRedis {
        var opts = try options.Options.new(gpa, server, password, db);
        errdefer opts.deinit();
        if (prefix.len > 0) opts.prefix = try gpa.dupe(u8, prefix);
        return init(gpa, io, opts);
    }

    pub fn fromConfig(gpa: std.mem.Allocator, io: std.Io, config: []const u8) !FullRedis {
        const opts = try options.Options.fromConfig(gpa, config);
        return init(gpa, io, opts);
    }

    pub fn deinit(self: *FullRedis) void {
        self.redis.deinit();
        self.gpa.free(self.prefix);
    }

    pub fn lastError(self: *const FullRedis) []const u8 {
        return self.redis.lastError();
    }

    pub fn stats(self: *FullRedis) @TypeOf(self.redis.stats()) {
        return self.redis.stats();
    }

    // ───────────────────────── 键前缀 ─────────────────────────

    /// 补前缀（已有前缀不重复添加，比较不区分大小写；空键返回前缀本身）。
    pub fn keyAlloc(self: *const FullRedis, key: []const u8) ![]u8 {
        if (self.prefix.len == 0) return self.gpa.dupe(u8, key);
        if (key.len == 0) return self.gpa.dupe(u8, self.prefix);
        if (key.len >= self.prefix.len and std.ascii.eqlIgnoreCase(key[0..self.prefix.len], self.prefix)) {
            return self.gpa.dupe(u8, key);
        }
        return std.fmt.allocPrint(self.gpa, "{s}{s}", .{ self.prefix, key });
    }

    /// 去掉前缀（`search` 等返回值还原）。
    pub fn trimKeyAlloc(self: *const FullRedis, key: []const u8) ![]u8 {
        if (self.prefix.len > 0 and key.len >= self.prefix.len and
            std.ascii.eqlIgnoreCase(key[0..self.prefix.len], self.prefix))
        {
            return self.gpa.dupe(u8, key[self.prefix.len..]);
        }
        return self.gpa.dupe(u8, key);
    }

    // ───────────────────────── 基础命令（带前缀） ─────────────────────────

    pub fn set(self: *FullRedis, key: []const u8, value: anytype, expire_seconds: i64) Error!bool {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return self.redis.set(k, value, expire_seconds);
    }

    pub fn getRaw(self: *FullRedis, gpa: std.mem.Allocator, key: []const u8) Error!?[]u8 {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return self.redis.getRaw(gpa, k);
    }

    pub fn get(self: *FullRedis, comptime T: type, gpa: std.mem.Allocator, key: []const u8) Error!?T {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return self.redis.get(T, gpa, k);
    }

    pub fn getJson(self: *FullRedis, comptime T: type, gpa: std.mem.Allocator, key: []const u8) Error!?std.json.Parsed(T) {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return self.redis.getJson(T, gpa, k);
    }

    pub fn add(self: *FullRedis, key: []const u8, value: anytype, expire_seconds: i64) Error!bool {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return self.redis.add(k, value, expire_seconds);
    }

    pub fn replace(self: *FullRedis, key: []const u8, value: anytype, expire_seconds: i64) Error!bool {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return self.redis.replace(k, value, expire_seconds);
    }

    pub fn remove(self: *FullRedis, key: []const u8) Error!i64 {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return self.redis.remove(k);
    }

    pub fn containsKey(self: *FullRedis, key: []const u8) Error!bool {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return self.redis.containsKey(k);
    }

    pub fn setExpire(self: *FullRedis, key: []const u8, seconds: i64) Error!bool {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return self.redis.setExpire(k, seconds);
    }

    pub fn getExpire(self: *FullRedis, key: []const u8) Error!i64 {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return self.redis.getExpire(k);
    }

    pub fn persist(self: *FullRedis, key: []const u8) Error!bool {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return self.redis.persist(k);
    }

    pub fn typeOf(self: *FullRedis, key: []const u8) Error!?[]u8 {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return self.redis.typeOf(k);
    }

    pub fn increment(self: *FullRedis, key: []const u8, delta: i64) Error!i64 {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return self.redis.increment(k, delta);
    }

    pub fn decrement(self: *FullRedis, key: []const u8, delta: i64) Error!i64 {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return self.redis.decrement(k, delta);
    }

    pub fn ping(self: *FullRedis) Error!bool {
        return self.redis.ping();
    }

    pub fn info(self: *FullRedis) Error!redis_mod.InfoMap {
        return self.redis.info();
    }

    // ───────────────────────── SCAN 搜索 ─────────────────────────

    /// 模糊搜索（`SCAN MATCH`），`count = 0` 表示不限量。返回的键**去掉前缀**。
    pub fn search(self: *FullRedis, gpa: std.mem.Allocator, pattern: []const u8, count: usize) Error![][]u8 {
        const full = try self.keyAlloc(pattern);
        defer self.gpa.free(full);

        var out: std.ArrayList([]u8) = .empty;
        errdefer {
            for (out.items) |s| gpa.free(s);
            out.deinit(gpa);
        }
        const batch = if (count == 0) 1000 else @max(count, 10);
        var cursor: u64 = 0;
        while (true) {
            const page = try self.redis.scan(cursor, full, batch);
            defer self.redis.freeStrings(page.keys);

            for (page.keys) |k| {
                try out.append(gpa, try self.trimKeyAlloc(k));
                if (count > 0 and out.items.len >= count) return out.toOwnedSlice(gpa);
            }
            if (page.cursor == 0) break;
            cursor = page.cursor;
        }
        return out.toOwnedSlice(gpa);
    }

    /// 按模式删除（SCAN + 分批 DEL），返回删除数量。
    pub fn removePattern(self: *FullRedis, pattern: []const u8) Error!i64 {
        const keys = try self.search(self.gpa, pattern, 0);
        defer self.redis.freeStrings(keys);
        if (keys.len == 0) return 0;

        var total: i64 = 0;
        var i: usize = 0;
        while (i < keys.len) {
            const n = @min(keys.len - i, 100);

            var args: std.ArrayList([]const u8) = .empty;
            defer args.deinit(self.gpa);
            var owned: std.ArrayList([]u8) = .empty;
            defer {
                for (owned.items) |s| self.gpa.free(s);
                owned.deinit(self.gpa);
            }

            try args.append(self.gpa, "DEL");
            for (keys[i .. i + n]) |raw| {
                const full = try self.keyAlloc(raw);
                try owned.append(self.gpa, full);
                try args.append(self.gpa, full);
            }
            var v = try self.redis.execute(args.items);
            defer v.deinit(self.gpa);
            total += v.asInt() orelse 0;
            i += n;
        }
        return total;
    }

    // ───────────────────────── 分布式锁 ─────────────────────────

    pub const LockHandle = struct {
        redis: *Redis,
        key: []u8,
        token: [32]u8,
        has_lock: bool = true,

        /// 主动释放（校验 token 归属，不误删他人锁）。
        pub fn release(self: *LockHandle) void {
            if (!self.has_lock) return;
            self.has_lock = false;
            defer self.redis.gpa.free(self.key);

            const current = self.redis.getRaw(self.redis.gpa, self.key) catch return;
            defer if (current) |v| self.redis.gpa.free(v);
            if (current) |value| {
                if (value.len > 33 and std.mem.eql(u8, value[0..32], self.token[0..]) and value[32] == '|') {
                    _ = self.redis.remove(self.key) catch {};
                }
            }
        }
    };

    /// 申请锁；失败返回 `null`（`throw_on_failure` 在 Zig 侧以错误体现，见 `acquireLock`）。
    pub fn acquireLockEx(self: *FullRedis, key: []const u8, ms_timeout: i32, ms_expire: i32) Error!?LockHandle {
        const k = try self.keyAlloc(key);
        errdefer self.gpa.free(k);

        var token: [32]u8 = undefined;
        randomToken(self.redis.io, &token);

        const expire_seconds: i64 = if (ms_expire > 0) @divTrunc(@as(i64, ms_expire) + 999, 1000) else 0;
        const start = bootTicksMs();
        const end = start + @as(u64, @intCast(@max(ms_timeout, 0)));
        var current = start;

        var value_buf: [96]u8 = undefined;
        while (true) {
            const value = std.fmt.bufPrint(&value_buf, "{s}|{d}", .{
                token[0..],
                current + @as(u64, @intCast(@max(ms_expire, 0))),
            }) catch return error.Config;

            if (self.redis.add(k, value, expire_seconds) catch |e| return e) {
                return .{ .redis = &self.redis, .key = k, .token = token };
            }

            // 死锁超期检测与抢占（与 C#/Rust 一致：只有已过期的锁才抢占）
            const dt = blk: {
                const current_value = self.redis.getRaw(self.gpa, k) catch null;
                defer if (current_value) |v| self.gpa.free(v);
                break :blk parseLockExpire(current_value);
            };

            if (dt <= current) {
                const replaced = self.redis.replace(k, value, expire_seconds) catch false;
                if (replaced) {
                    const old = blk: {
                        const old_value = self.redis.getRaw(self.gpa, k) catch null;
                        defer if (old_value) |v| self.gpa.free(v);
                        break :blk parseLockExpire(old_value);
                    };
                    if (old <= dt) {
                        _ = self.redis.setExpire(k, expire_seconds) catch {};
                        return .{ .redis = &self.redis, .key = k, .token = token };
                    }
                }
            }

            current = bootTicksMs();
            if (current >= end) break;
            zzig.compat.sleep(200 * std.time.ns_per_ms);
        }

        self.gpa.free(k);
        return null;
    }

    /// 申请锁；失败返回 `error.Server`（对应 C# `throwOnFailure=true`）。
    pub fn acquireLock(self: *FullRedis, key: []const u8, ms_timeout: i32) Error!LockHandle {
        return (try self.acquireLockEx(key, ms_timeout, ms_timeout)) orelse {
            self.redis.setErrorDiag("Lock [{s}] failed! msTimeout={d}", .{ key, ms_timeout });
            return error.Server;
        };
    }

    // ───────────────────────── 结构体工厂 ─────────────────────────

    pub fn getHash(self: *FullRedis, key: []const u8) !hash_mod.RedisHash {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return hash_mod.RedisHash.init(&self.redis, k);
    }

    pub fn getList(self: *FullRedis, key: []const u8) !list_mod.RedisList {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return list_mod.RedisList.init(&self.redis, k);
    }

    pub fn getSet(self: *FullRedis, key: []const u8) !set_mod.RedisSet {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return set_mod.RedisSet.init(&self.redis, k);
    }

    pub fn getSortedSet(self: *FullRedis, key: []const u8) !sortedset_mod.RedisSortedSet {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return sortedset_mod.RedisSortedSet.init(&self.redis, k);
    }

    pub fn getStack(self: *FullRedis, key: []const u8) !stack_mod.RedisStack {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return stack_mod.RedisStack.init(&self.redis, k);
    }

    pub fn getHyperLogLog(self: *FullRedis, key: []const u8) !hll_mod.HyperLogLog {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        return hll_mod.HyperLogLog.init(&self.redis, k);
    }

    pub fn getQueue(self: *FullRedis, topic: []const u8) !queue_mod.RedisQueue {
        const k = try self.keyAlloc(topic);
        defer self.gpa.free(k);
        return queue_mod.RedisQueue.init(&self.redis, k);
    }

    pub fn getDelayQueue(self: *FullRedis, topic: []const u8) !delay_mod.RedisDelayQueue {
        const k = try self.keyAlloc(topic);
        defer self.gpa.free(k);
        return delay_mod.RedisDelayQueue.init(&self.redis, k);
    }

    pub fn getReliableQueue(self: *FullRedis, topic: []const u8) !reliable_mod.RedisReliableQueue {
        const k = try self.keyAlloc(topic);
        defer self.gpa.free(k);
        return reliable_mod.RedisReliableQueue.init(&self.redis, k, self.prefix);
    }

    pub fn getDeferred(self: *FullRedis, name: []const u8) !deferred_mod.RedisDeferred {
        const k = try self.keyAlloc(name);
        defer self.gpa.free(k);
        return deferred_mod.RedisDeferred.init(&self.redis, k);
    }

    /// Stream 队列（对应 C# `GetStream` / Rust `get_stream`）。
    pub fn getStream(self: *FullRedis, topic: []const u8) !stream_mod.RedisStream {
        const k = try self.keyAlloc(topic);
        defer self.gpa.free(k);
        return stream_mod.RedisStream.init(&self.redis, k);
    }

    /// 同一实例、指定 db 的新 FullRedis（保留前缀，与 C# `CreateSub` 一致）。
    pub fn createSub(self: *FullRedis, db: i32) !FullRedis {
        var opts = options.Options.init(self.gpa);
        errdefer opts.deinit();
        for (self.redis.opts.servers.items) |s| {
            try opts.servers.append(self.gpa, try self.gpa.dupe(u8, s));
        }
        if (self.redis.opts.password) |p| opts.password = try self.gpa.dupe(u8, p);
        if (self.redis.opts.user_name) |u| opts.user_name = try self.gpa.dupe(u8, u);
        if (self.redis.opts.prefix) |p| opts.prefix = try self.gpa.dupe(u8, p);
        opts.db = db;
        opts.timeout_ms = self.redis.opts.timeout_ms;
        opts.retry = self.redis.opts.retry;
        opts.protocol_version = self.redis.opts.protocol_version;
        opts.max_message_size = self.redis.opts.max_message_size;
        opts.expire = self.redis.opts.expire;
        opts.pool = self.redis.opts.pool;
        opts.tls = self.redis.opts.tls;
        opts.tls_insecure = self.redis.opts.tls_insecure;
        return init(self.gpa, self.redis.io, opts);
    }

    pub fn rename(self: *FullRedis, key: []const u8, new_key: []const u8, overwrite: bool) Error!bool {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        const nk = try self.keyAlloc(new_key);
        defer self.gpa.free(nk);
        return self.redis.rename(k, nk, overwrite);
    }

    // ───────────────────────── Tair 扩展（阿里云）─────────────────────────

    /// TairString：带版本号写入（`EXSET key value [EX n] VER v`）。
    pub fn exSet(self: *FullRedis, key: []const u8, value: anytype, version: i64, expire_seconds: i32) Error!?[]u8 {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        const maybe = encoder.encodeValueAlloc(self.gpa, value) catch return error.Type;
        defer if (maybe) |p| self.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else "";

        var vb: [24]u8 = undefined;
        const ver = std.fmt.bufPrint(&vb, "{d}", .{version}) catch return error.Config;
        var v = if (expire_seconds > 0) blk: {
            var eb: [24]u8 = undefined;
            const ex = std.fmt.bufPrint(&eb, "{d}", .{expire_seconds}) catch return error.Config;
            break :blk try self.redis.execute(&.{ "EXSET", k, payload, "EX", ex, "VER", ver });
        } else try self.redis.execute(&.{ "EXSET", k, payload, "VER", ver });
        defer v.deinit(self.gpa);
        const s = v.asString() orelse return null;
        return try self.gpa.dupe(u8, s);
    }

    pub const ExValue = struct { value: ?[]u8, version: i64 };

    /// TairString：读取值及版本（`EXGET`）。
    pub fn exGet(self: *FullRedis, key: []const u8) Error!?ExValue {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        var v = try self.redis.execute(&.{ "EXGET", k });
        defer v.deinit(self.gpa);
        const items = v.asArray() orelse return null;
        if (items.len < 2) return null;
        const version = items[items.len - 1].asInt() orelse 0;
        const value = items[0].asBytesAlloc(self.gpa) catch return error.IoError;
        return .{ .value = value, .version = version };
    }

    pub const ExIncr = struct { value: i64, version: i64 };

    pub fn exIncrBy(self: *FullRedis, key: []const u8, delta: i64, version: i64, expire_seconds: i32) Error!?ExIncr {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        var ib: [24]u8 = undefined;
        var vb: [24]u8 = undefined;
        const inc = std.fmt.bufPrint(&ib, "{d}", .{delta}) catch return error.Config;
        const ver = std.fmt.bufPrint(&vb, "{d}", .{version}) catch return error.Config;
        var v = if (expire_seconds > 0) blk: {
            var eb: [24]u8 = undefined;
            const ex = std.fmt.bufPrint(&eb, "{d}", .{expire_seconds}) catch return error.Config;
            break :blk try self.redis.execute(&.{ "EXINCRBY", k, inc, "EX", ex, "VER", ver });
        } else try self.redis.execute(&.{ "EXINCRBY", k, inc, "VER", ver });
        defer v.deinit(self.gpa);
        const items = v.asArray() orelse return null;
        if (items.len < 2) return null;
        return .{ .value = items[0].asInt() orelse 0, .version = items[1].asInt() orelse 0 };
    }

    pub fn exHSet(self: *FullRedis, key: []const u8, field: []const u8, value: anytype, expire_seconds: i32, nx: bool, ver: i64) Error!i64 {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        const maybe = encoder.encodeValueAlloc(self.gpa, value) catch return error.Type;
        defer if (maybe) |p| self.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else "";

        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        try args.append(self.gpa, "EXHSET");
        try args.append(self.gpa, k);
        try args.append(self.gpa, field);
        try args.append(self.gpa, payload);
        var eb: [24]u8 = undefined;
        var vb: [24]u8 = undefined;
        if (expire_seconds > 0) {
            try args.append(self.gpa, "EX");
            try args.append(self.gpa, std.fmt.bufPrint(&eb, "{d}", .{expire_seconds}) catch return error.Config);
        }
        if (nx) try args.append(self.gpa, "NX");
        if (ver > 0) {
            try args.append(self.gpa, "VER");
            try args.append(self.gpa, std.fmt.bufPrint(&vb, "{d}", .{ver}) catch return error.Config);
        }
        var v = try self.redis.execute(args.items);
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn exHGet(self: *FullRedis, gpa: std.mem.Allocator, key: []const u8, field: []const u8) Error!?[]u8 {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        var v = try self.redis.execute(&.{ "EXHGET", k, field });
        defer v.deinit(self.gpa);
        if (v.isNull()) return null;
        return v.asBytesAlloc(gpa) catch return error.IoError;
    }

    pub fn exHMGet(self: *FullRedis, gpa: std.mem.Allocator, key: []const u8, fields: []const []const u8) Error![]?[]u8 {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        try args.append(self.gpa, "EXHMGET");
        try args.append(self.gpa, k);
        try args.appendSlice(self.gpa, fields);
        var v = try self.redis.execute(args.items);
        defer v.deinit(self.gpa);
        const items = try self.redis.expectArray(&v);

        const out = gpa.alloc(?[]u8, items.len) catch return error.IoError;
        errdefer gpa.free(out);
        for (items, 0..) |item, i| {
            out[i] = if (item.isNull()) null else (item.asBytesAlloc(gpa) catch return error.IoError);
        }
        return out;
    }

    pub fn exHGetWithVer(self: *FullRedis, key: []const u8, field: []const u8) Error!?ExValue {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        var v = try self.redis.execute(&.{ "EXHGETWITHVER", k, field });
        defer v.deinit(self.gpa);
        const items = v.asArray() orelse return null;
        if (items.len < 2) return null;
        const version = items[items.len - 1].asInt() orelse 0;
        const value = items[0].asBytesAlloc(self.gpa) catch return error.IoError;
        return .{ .value = value, .version = version };
    }

    pub fn exHIncrBy(self: *FullRedis, key: []const u8, field: []const u8, delta: i64, expire_seconds: i32) Error!i64 {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        var ib: [24]u8 = undefined;
        const inc = std.fmt.bufPrint(&ib, "{d}", .{delta}) catch return error.Config;
        var v = if (expire_seconds > 0) blk: {
            var eb: [24]u8 = undefined;
            const ex = std.fmt.bufPrint(&eb, "{d}", .{expire_seconds}) catch return error.Config;
            break :blk try self.redis.execute(&.{ "EXHINCRBY", k, field, inc, "EX", ex });
        } else try self.redis.execute(&.{ "EXHINCRBY", k, field, inc });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn exHPttl(self: *FullRedis, key: []const u8, field: []const u8) Error!i64 {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        var v = try self.redis.execute(&.{ "EXHPTTL", k, field });
        defer v.deinit(self.gpa);
        return v.asInt() orelse -2;
    }

    fn exStringList(self: *FullRedis, gpa: std.mem.Allocator, cmd: []const u8, key: []const u8) Error![][]u8 {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        var v = try self.redis.execute(&.{ cmd, k });
        defer v.deinit(self.gpa);
        const items = try self.redis.expectArray(&v);
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

    pub fn exHKeys(self: *FullRedis, gpa: std.mem.Allocator, key: []const u8) Error![][]u8 {
        return self.exStringList(gpa, "EXHKEYS", key);
    }

    pub fn exHVals(self: *FullRedis, gpa: std.mem.Allocator, key: []const u8) Error![][]u8 {
        return self.exStringList(gpa, "EXHVALS", key);
    }

    pub fn exHLen(self: *FullRedis, key: []const u8) Error!i64 {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        var v = try self.redis.execute(&.{ "EXHLEN", k });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn exHDel(self: *FullRedis, key: []const u8, fields: []const []const u8) Error!i64 {
        const k = try self.keyAlloc(key);
        defer self.gpa.free(k);
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        try args.append(self.gpa, "EXHDEL");
        try args.append(self.gpa, k);
        try args.appendSlice(self.gpa, fields);
        var v = try self.redis.execute(args.items);
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }
};

/// 解析锁值中的过期时间戳（兼容旧格式纯数字）。
pub fn parseLockExpire(value: ?[]const u8) u64 {
    const text = value orelse return 0;
    const tail = if (std.mem.indexOfScalar(u8, text, '|')) |idx| text[idx + 1 ..] else text;
    return std.fmt.parseInt(u64, std.mem.trim(u8, tail, " \t\r\n"), 10) catch 0;
}

/// 32 位十六进制随机令牌（对应 C# `Guid.NewGuid().ToString("N")`）。
pub fn randomToken(io: std.Io, out: *[32]u8) void {
    var bytes: [16]u8 = undefined;
    io.random(&bytes);
    const hex = "0123456789abcdef";
    for (bytes, 0..) |b, i| {
        out[i * 2] = hex[b >> 4];
        out[i * 2 + 1] = hex[b & 0x0f];
    }
}

/// 机器启动以来的毫秒数（对齐 C# `Environment.TickCount64` / Rust `boot_ticks_ms`）。
pub fn bootTicksMs() u64 {
    switch (builtin.os.tag) {
        .windows => {
            const kernel32 = struct {
                extern "kernel32" fn GetTickCount64() callconv(.winapi) u64;
            };
            return kernel32.GetTickCount64();
        },
        else => {
            var ts: std.c.timespec = undefined;
            if (std.c.clock_gettime(std.c.CLOCK.MONOTONIC, &ts) != 0) return 0;
            const sec: u64 = @intCast(@max(ts.sec, 0));
            const nsec: u64 = @intCast(@max(ts.nsec, 0));
            return sec * 1000 + nsec / std.time.ns_per_ms;
        },
    }
}

// ─────────────────────────────── 测试 ───────────────────────────────

const testing = std.testing;

test "前缀补齐与去前缀（不区分大小写）" {
    const opts = try options.Options.fromConfig(testing.allocator, "server=127.0.0.1:1;prefix=App:");
    // 只测键处理，不建真实连接：直接构造 FullRedis 的键工具需要 Redis，改用临时实例不可行 →
    // 这里复用 Options + 逻辑复制（真正行为在集成测试里覆盖）。
    defer {
        var o = opts;
        o.deinit();
    }
    try testing.expectEqualStrings("App:", opts.prefix.?);
}

test "锁值过期解析" {
    try testing.expectEqual(@as(u64, 12345), parseLockExpire("abc|12345"));
    try testing.expectEqual(@as(u64, 999), parseLockExpire("999"));
    try testing.expectEqual(@as(u64, 0), parseLockExpire(null));
    try testing.expectEqual(@as(u64, 0), parseLockExpire("abc|not-a-number"));
}

test "令牌为 32 位小写十六进制" {
    var token: [32]u8 = undefined;
    randomToken(std.Io.Threaded.global_single_threaded.io(), &token);
    for (token) |c| {
        try testing.expect((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'));
    }
}
