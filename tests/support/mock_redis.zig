//! 回环假 Redis（测试用最小实现）。
//!
//! 只实现本库测试覆盖到的命令子集；协议面按 RESP2 回复（客户端同时兼容 RESP3）。
//! 作用与 Rust `tests/support/mod.rs` 的 MockRedis 相同：没有真实 Redis 的机器也能跑全绿。

const std = @import("std");
const zzig = @import("zzig");
const pek_zredis = @import("pek_zredis");
const resp = pek_zredis.resp;

pub const MockServer = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    shared: *Shared,
    listener: std.Io.net.Server,
    port: u16,
    thread: std.Thread,
    stop_flag: std.atomic.Value(bool) = .init(false),

    pub fn start(gpa: std.mem.Allocator, io: std.Io, start_port: u16) !*MockServer {
        var port = start_port;
        while (port < start_port + 30) : (port += 1) {
            const addr = std.Io.net.IpAddress.parse("127.0.0.1", port) catch continue;
            const listener = addr.listen(io, .{ .reuse_address = true }) catch continue;

            const shared = try gpa.create(Shared);
            shared.* = .{ .gpa = gpa, .io = io, .port = port };
            const self = try gpa.create(MockServer);
            self.* = .{
                .gpa = gpa,
                .io = io,
                .shared = shared,
                .listener = listener,
                .port = port,
                .thread = undefined,
            };
            self.thread = try std.Thread.spawn(.{}, acceptLoop, .{self});
            return self;
        }
        return error.NoFreePort;
    }

    pub fn stop(self: *MockServer) void {
        self.stop_flag.store(true, .monotonic);
        // 唤醒 accept：主动连一次
        if (std.Io.net.IpAddress.parse("127.0.0.1", self.port)) |addr| {
            if (std.Io.net.IpAddress.connect(&addr, self.io, .{ .mode = .stream, .protocol = .tcp })) |s| {
                s.close(self.io);
            } else |_| {}
        } else |_| {}
        self.thread.join();

        // 等连接线程退出（客户端 deinit 后对端关闭，最多等 2 秒）
        var waited_ms: usize = 0;
        while (self.shared.active_conns.load(.monotonic) > 0 and waited_ms < 2000) : (waited_ms += 5) {
            zzig.compat.sleep(5 * std.time.ns_per_ms);
        }

        self.listener.deinit(self.io);
        self.shared.deinit();
        self.gpa.destroy(self.shared);
        self.gpa.destroy(self);
    }

    pub fn configAlloc(self: *MockServer, gpa: std.mem.Allocator) ![]u8 {
        return std.fmt.allocPrint(gpa, "server=127.0.0.1:{d};db=0", .{self.port});
    }

    pub fn configPasswordAlloc(self: *MockServer, gpa: std.mem.Allocator, password: []const u8) ![]u8 {
        return std.fmt.allocPrint(gpa, "server=127.0.0.1:{d};password={s};db=2", .{ self.port, password });
    }

    fn acceptLoop(self: *MockServer) void {
        while (!self.stop_flag.load(.monotonic)) {
            const stream = self.listener.accept(self.io) catch return;
            if (self.stop_flag.load(.monotonic)) {
                stream.close(self.io);
                return;
            }
            const ctx = self.gpa.create(ConnCtx) catch {
                stream.close(self.io);
                continue;
            };
            _ = self.shared.active_conns.fetchAdd(1, .monotonic);
            ctx.* = .{
                .gpa = self.gpa,
                .io = self.io,
                .shared = self.shared,
                .stream = stream,
            };
            const th = std.Thread.spawn(.{}, connLoop, .{ctx}) catch {
                stream.close(self.io);
                self.gpa.destroy(ctx);
                continue;
            };
            th.detach();
        }
    }
};

const HPair = struct { field: []u8, value: []u8 };
const ZItem = struct { member: []u8, score: f64 };
const StreamEntry = struct { id: []u8, fields: [][]u8 };
const StreamPending = struct { id: []u8, consumer: []u8, delivery: i64, last_ms: i64 };
const StreamConsumer = struct { name: []u8, seen_ms: i64 };
const StreamGroup = struct {
    name: []u8,
    last_ms: u64,
    last_seq: u64,
    consumers: std.ArrayList(StreamConsumer) = .empty,
    pending: std.ArrayList(StreamPending) = .empty,
};

const Kind = enum { string, list, hash, set, zset, stream };

const Entry = struct {
    kind: Kind,
    str: std.ArrayList(u8) = .empty,
    list: std.ArrayList([]u8) = .empty,
    hash: std.ArrayList(HPair) = .empty,
    set: std.ArrayList([]u8) = .empty,
    zset: std.ArrayList(ZItem) = .empty,
    entries: std.ArrayList(StreamEntry) = .empty,
    groups: std.ArrayList(StreamGroup) = .empty,
    last_ms: u64 = 0,
    last_seq: u64 = 0,
    entries_added: i64 = 0,
    expire_at_ms: i64 = 0,
    ver: i64 = 0,

    fn deinit(self: *Entry, gpa: std.mem.Allocator) void {
        self.str.deinit(gpa);
        for (self.list.items) |s| gpa.free(s);
        self.list.deinit(gpa);
        for (self.hash.items) |p| {
            gpa.free(p.field);
            gpa.free(p.value);
        }
        self.hash.deinit(gpa);
        for (self.set.items) |s| gpa.free(s);
        self.set.deinit(gpa);
        for (self.zset.items) |z| gpa.free(z.member);
        self.zset.deinit(gpa);
        for (self.entries.items) |e| {
            gpa.free(e.id);
            for (e.fields) |f| gpa.free(f);
            if (e.fields.len > 0) gpa.free(e.fields);
        }
        self.entries.deinit(gpa);
        for (self.groups.items) |*g| {
            gpa.free(g.name);
            for (g.consumers.items) |c| gpa.free(c.name);
            g.consumers.deinit(gpa);
            for (g.pending.items) |p| {
                gpa.free(p.id);
                gpa.free(p.consumer);
            }
            g.pending.deinit(gpa);
        }
        self.groups.deinit(gpa);
    }

    fn clearValues(self: *Entry, gpa: std.mem.Allocator, kind: Kind) void {
        self.deinit(gpa);
        self.* = .{ .kind = kind };
    }
};

const Sub = struct {
    conn: *ConnCtx,
    channel: []u8,
    pattern: bool,
};

const Shared = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    port: u16 = 0,
    mutex: std.Io.Mutex = .init,
    map: std.StringHashMapUnmanaged(*Entry) = .empty,
    subs: std.ArrayList(Sub) = .empty,
    active_conns: std.atomic.Value(usize) = .init(0),

    fn deinit(self: *Shared) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.gpa.free(e.key_ptr.*);
            e.value_ptr.*.deinit(self.gpa);
            self.gpa.destroy(e.value_ptr.*);
        }
        self.map.deinit(self.gpa);
        for (self.subs.items) |s| self.gpa.free(s.channel);
        self.subs.deinit(self.gpa);
    }

    fn nowMs(self: *Shared) i64 {
        _ = self;
        return @intCast(@divTrunc(zzig.compat.nanoTimestamp(), std.time.ns_per_ms));
    }

    /// 取条目（已按过期清理）。调用方必须持有锁。
    fn get(self: *Shared, key: []const u8) ?*Entry {
        const entry = self.map.get(key) orelse return null;
        if (entry.expire_at_ms != 0 and self.nowMs() >= entry.expire_at_ms) {
            self.removeLocked(key);
            return null;
        }
        return entry;
    }

    fn removeLocked(self: *Shared, key: []const u8) void {
        if (self.map.fetchRemove(key)) |kv| {
            self.gpa.free(kv.key);
            kv.value.deinit(self.gpa);
            self.gpa.destroy(kv.value);
        }
    }

    /// 取或建（kind 不同则清空重建，与 Redis 语义一致）。
    fn getOrCreate(self: *Shared, key: []const u8, kind: Kind) !*Entry {
        if (self.get(key)) |entry| {
            if (entry.kind != kind) {
                entry.clearValues(self.gpa, kind);
                entry.expire_at_ms = 0;
            }
            return entry;
        }
        const entry = try self.gpa.create(Entry);
        entry.* = .{ .kind = kind };
        const key_copy = try self.gpa.dupe(u8, key);
        errdefer {
            entry.deinit(self.gpa);
            self.gpa.destroy(entry);
            self.gpa.free(key_copy);
        }
        try self.map.put(self.gpa, key_copy, entry);
        return entry;
    }
};

const ConnCtx = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    shared: *Shared,
    stream: std.Io.net.Stream,
    write_mutex: std.Io.Mutex = .init,
    active: std.atomic.Value(bool) = .init(true),

    fn sendAll(self: *ConnCtx, bytes: []const u8) void {
        self.write_mutex.lockUncancelable(self.io);
        defer self.write_mutex.unlock(self.io);
        if (!self.active.load(.monotonic)) return;
        var sent: usize = 0;
        while (sent < bytes.len) {
            const r = self.io.operate(.{ .net_write = .{
                .socket_handle = self.stream.socket.handle,
                .data = &.{bytes[sent..]},
            } }) catch {
                self.active.store(false, .monotonic);
                return;
            };
            const n = r.net_write catch {
                self.active.store(false, .monotonic);
                return;
            };
            if (n == 0) {
                self.active.store(false, .monotonic);
                return;
            }
            sent += n;
        }
    }

    fn simple(self: *ConnCtx, text: []const u8) !void {
        var buf: [512]u8 = undefined;
        const out = try std.fmt.bufPrint(&buf, "+{s}\r\n", .{text});
        self.sendAll(out);
    }

    fn err(self: *ConnCtx, text: []const u8) !void {
        var buf: [512]u8 = undefined;
        const out = try std.fmt.bufPrint(&buf, "-ERR {s}\r\n", .{text});
        self.sendAll(out);
    }

    fn int(self: *ConnCtx, value: i64) !void {
        var buf: [48]u8 = undefined;
        const out = try std.fmt.bufPrint(&buf, ":{d}\r\n", .{value});
        self.sendAll(out);
    }

    fn bulk(self: *ConnCtx, data: ?[]const u8) !void {
        if (data == null) {
            self.sendAll("$-1\r\n");
            return;
        }
        var head: [48]u8 = undefined;
        self.sendAll(try std.fmt.bufPrint(&head, "${d}\r\n", .{data.?.len}));
        self.sendAll(data.?);
        self.sendAll("\r\n");
    }

    fn arrayHeader(self: *ConnCtx, n: usize) !void {
        var buf: [48]u8 = undefined;
        self.sendAll(try std.fmt.bufPrint(&buf, "*{d}\r\n", .{n}));
    }

    fn bulkArray(self: *ConnCtx, items: []const []const u8) !void {
        try self.arrayHeader(items.len);
        for (items) |item| try self.bulk(item);
    }
};

fn connLoop(ctx: *ConnCtx) void {
    defer {
        ctx.active.store(false, .monotonic);
        // 移除订阅登记
        ctx.shared.mutex.lockUncancelable(ctx.io);
        var i: usize = 0;
        while (i < ctx.shared.subs.items.len) {
            if (ctx.shared.subs.items[i].conn == ctx) {
                const s = ctx.shared.subs.orderedRemove(i);
                ctx.gpa.free(s.channel);
            } else i += 1;
        }
        ctx.shared.mutex.unlock(ctx.io);

        ctx.stream.close(ctx.io);
        _ = ctx.shared.active_conns.fetchSub(1, .monotonic);
        ctx.gpa.destroy(ctx);
    }

    var reader = resp.Reader.init(ctx.gpa, ctx.io, ctx.stream, 16 * 1024, 8 * 1024 * 1024) catch return;
    defer reader.deinit();

    while (true) {
        var value = reader.readReply() catch return;
        defer value.deinit(ctx.gpa);

        const items = value.asArray() orelse {
            ctx.err("仅支持 RESP 数组命令") catch {};
            continue;
        };
        if (items.len == 0) continue;

        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(ctx.gpa);
        for (items) |item| {
            args.append(ctx.gpa, item.asString() orelse "") catch return;
        }

        ctx.shared.mutex.lockUncancelable(ctx.io);
        handle(ctx, args.items) catch |e| {
            ctx.err(@errorName(e)) catch {};
        };
        ctx.shared.mutex.unlock(ctx.io);

        if (args.items.len > 0 and std.ascii.eqlIgnoreCase(args.items[0], "QUIT")) return;
    }
}

fn eq(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

fn handle(ctx: *ConnCtx, args: []const []const u8) !void {
    const cmd = args[0];
    const s = ctx.shared;

    if (eq(cmd, "PING")) return ctx.simple("PONG");
    if (eq(cmd, "ECHO")) return ctx.bulk(if (args.len > 1) args[1] else "");
    if (eq(cmd, "AUTH") or eq(cmd, "SELECT")) return ctx.simple("OK");
    if (eq(cmd, "HELLO")) {
        // 最小 map：%1 server -> mock
        ctx.sendAll("%1\r\n$6\r\nserver\r\n$4\r\nmock\r\n");
        return;
    }
    if (eq(cmd, "QUIT")) return ctx.simple("OK");
    if (eq(cmd, "INFO")) {
        return ctx.bulk("# Server\r\nredis_version:7.2.4\r\nserver:mock\r\nredis_mode:standalone\r\nrole:master\r\nconnected_slaves:0\r\n");
    }
    if (eq(cmd, "CLUSTER")) {
        if (args.len >= 2 and eq(args[1], "NODES")) {
            var buf: [256]u8 = undefined;
            const line = try std.fmt.bufPrint(
                &buf,
                "0123456789abcdef0123456789abcdef01234567 127.0.0.1:{d}@{d} myself,master - 0 0 1 connected 0-16383\n",
                .{ s.port, @as(u32, s.port) + 10000 },
            );
            return ctx.bulk(line);
        }
        return ctx.err("ERR unsupported CLUSTER subcommand");
    }
    if (eq(cmd, "ASKING")) return ctx.simple("OK");
    if (eq(cmd, "TIME")) {
        const secs = @divTrunc(zzig.compat.nanoTimestamp(), std.time.ns_per_s);
        var sb: [32]u8 = undefined;
        const text = try std.fmt.bufPrint(&sb, "{d}", .{secs});
        return ctx.bulkArray(&.{ text, "0" });
    }
    if (eq(cmd, "DBSIZE")) return ctx.int(@intCast(s.map.count()));
    if (eq(cmd, "FLUSHDB")) {
        var it = s.map.iterator();
        while (it.next()) |e| {
            s.gpa.free(e.key_ptr.*);
            e.value_ptr.*.deinit(s.gpa);
            s.gpa.destroy(e.value_ptr.*);
        }
        s.map.clearRetainingCapacity();
        return ctx.simple("OK");
    }

    // ── 键空间 ──
    if (eq(cmd, "GET")) {
        if (args.len < 2) return ctx.err("GET 参数不足");
        const entry = s.get(args[1]) orelse return ctx.bulk(null);
        if (entry.kind != .string) return ctx.err("WRONGTYPE");
        return ctx.bulk(entry.str.items);
    }
    if (eq(cmd, "SET")) {
        if (args.len < 3) return ctx.err("SET 参数不足");
        var expire_at: i64 = 0;
        var nx = false;
        var xx = false;
        var i: usize = 3;
        while (i < args.len) : (i += 1) {
            if (eq(args[i], "EX") and i + 1 < args.len) {
                const secs = std.fmt.parseInt(i64, args[i + 1], 10) catch 0;
                expire_at = s.nowMs() + secs * 1000;
                i += 1;
            } else if (eq(args[i], "NX")) {
                nx = true;
            } else if (eq(args[i], "XX")) {
                xx = true;
            }
        }
        const existing = s.get(args[1]);
        if (nx and existing != null) return ctx.bulk(null);
        if (xx and existing == null) return ctx.bulk(null);

        const entry = try s.getOrCreate(args[1], .string);
        entry.str.clearRetainingCapacity();
        try entry.str.appendSlice(s.gpa, args[2]);
        entry.expire_at_ms = expire_at;
        return ctx.simple("OK");
    }
    if (eq(cmd, "SETEX")) {
        if (args.len < 4) return ctx.err("SETEX 参数不足");
        const secs = std.fmt.parseInt(i64, args[2], 10) catch 0;
        const entry = try s.getOrCreate(args[1], .string);
        entry.str.clearRetainingCapacity();
        try entry.str.appendSlice(s.gpa, args[3]);
        entry.expire_at_ms = s.nowMs() + secs * 1000;
        return ctx.simple("OK");
    }
    if (eq(cmd, "DEL") or eq(cmd, "UNLINK")) {
        var n: i64 = 0;
        for (args[1..]) |key| {
            if (s.map.get(key) != null) {
                s.removeLocked(key);
                n += 1;
            }
        }
        return ctx.int(n);
    }
    if (eq(cmd, "EXISTS")) {
        var n: i64 = 0;
        for (args[1..]) |key| {
            if (s.get(key) != null) n += 1;
        }
        return ctx.int(n);
    }
    if (eq(cmd, "TYPE")) {
        const entry = s.get(args[1]) orelse return ctx.simple("none");
        return ctx.simple(switch (entry.kind) {
            .string => "string",
            .list => "list",
            .hash => "hash",
            .set => "set",
            .zset => "zset",
            .stream => "stream",
        });
    }
    if (eq(cmd, "TTL") or eq(cmd, "PTTL")) {
        const entry = s.get(args[1]) orelse return ctx.int(-2);
        if (entry.expire_at_ms == 0) return ctx.int(-1);
        const remain = entry.expire_at_ms - s.nowMs();
        return ctx.int(if (eq(cmd, "TTL")) @divTrunc(remain, 1000) else remain);
    }
    if (eq(cmd, "EXPIRE") or eq(cmd, "PEXPIRE")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        const v = std.fmt.parseInt(i64, args[2], 10) catch 0;
        entry.expire_at_ms = s.nowMs() + if (eq(cmd, "EXPIRE")) v * 1000 else v;
        return ctx.int(1);
    }
    if (eq(cmd, "PERSIST")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        entry.expire_at_ms = 0;
        return ctx.int(1);
    }
    if (eq(cmd, "KEYS")) return keysCmd(ctx, args[1]);
    if (eq(cmd, "SCAN")) return scanCmd(ctx, args);
    if (eq(cmd, "RANDOMKEY")) {
        var it = s.map.iterator();
        if (it.next()) |e| return ctx.bulk(e.key_ptr.*);
        return ctx.bulk(null);
    }
    if (eq(cmd, "RENAME")) {
        const entry = s.get(args[1]) orelse return ctx.err("no such key");
        const new_key = try s.gpa.dupe(u8, args[2]);
        try s.map.put(s.gpa, new_key, entry);
        if (s.map.fetchRemove(args[1])) |kv| s.gpa.free(kv.key);
        return ctx.simple("OK");
    }

    // ── 字符串扩展 ──
    if (eq(cmd, "MGET")) {
        try ctx.arrayHeader(args.len - 1);
        for (args[1..]) |key| {
            if (s.get(key)) |entry| {
                if (entry.kind == .string) try ctx.bulk(entry.str.items) else try ctx.bulk(null);
            } else try ctx.bulk(null);
        }
        return;
    }
    if (eq(cmd, "MSET")) {
        var i: usize = 1;
        while (i + 1 < args.len) : (i += 2) {
            const entry = try s.getOrCreate(args[i], .string);
            entry.str.clearRetainingCapacity();
            try entry.str.appendSlice(s.gpa, args[i + 1]);
        }
        return ctx.simple("OK");
    }
    if (eq(cmd, "INCR") or eq(cmd, "INCRBY") or eq(cmd, "DECR") or eq(cmd, "DECRBY")) {
        const delta: i64 = if (eq(cmd, "INCR")) 1 else if (eq(cmd, "DECR")) -1 else if (eq(cmd, "INCRBY")) std.fmt.parseInt(i64, args[2], 10) catch 0 else -(std.fmt.parseInt(i64, args[2], 10) catch 0);
        const entry = try s.getOrCreate(args[1], .string);
        const current = std.fmt.parseInt(i64, entry.str.items, 10) catch 0;
        const next = current + delta;
        entry.str.clearRetainingCapacity();
        var buf: [32]u8 = undefined;
        try entry.str.appendSlice(s.gpa, try std.fmt.bufPrint(&buf, "{d}", .{next}));
        return ctx.int(next);
    }
    if (eq(cmd, "INCRBYFLOAT")) {
        const delta = std.fmt.parseFloat(f64, args[2]) catch 0;
        const entry = try s.getOrCreate(args[1], .string);
        const current = std.fmt.parseFloat(f64, entry.str.items) catch 0;
        const next = current + delta;
        const text = try pek_zredis.resp.formatF64Alloc(s.gpa, next);
        defer s.gpa.free(text);
        entry.str.clearRetainingCapacity();
        try entry.str.appendSlice(s.gpa, text);
        return ctx.bulk(entry.str.items);
    }
    if (eq(cmd, "APPEND")) {
        const entry = try s.getOrCreate(args[1], .string);
        try entry.str.appendSlice(s.gpa, args[2]);
        return ctx.int(@intCast(entry.str.items.len));
    }
    if (eq(cmd, "STRLEN")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        return ctx.int(@intCast(entry.str.items.len));
    }
    if (eq(cmd, "GETRANGE")) {
        const entry = s.get(args[1]) orelse return ctx.bulk("");
        const start = std.fmt.parseInt(i64, args[2], 10) catch 0;
        const end = std.fmt.parseInt(i64, args[3], 10) catch -1;
        const slice = sliceRange(entry.str.items.len, start, end);
        return ctx.bulk(entry.str.items[slice.start..slice.end]);
    }
    if (eq(cmd, "SETRANGE")) {
        const offset = std.fmt.parseInt(usize, args[2], 10) catch 0;
        const entry = try s.getOrCreate(args[1], .string);
        if (entry.str.items.len < offset) try entry.str.resize(s.gpa, offset);
        if (entry.str.items.len < offset + args[3].len) try entry.str.resize(s.gpa, offset + args[3].len);
        @memcpy(entry.str.items[offset .. offset + args[3].len], args[3]);
        return ctx.int(@intCast(entry.str.items.len));
    }
    if (eq(cmd, "SETBIT") or eq(cmd, "GETBIT") or eq(cmd, "BITCOUNT") or eq(cmd, "BITPOS")) {
        return ctx.int(0); // 位图语义不在本轮测试范围（真实 Redis 已覆盖命令正确性）
    }

    // ── 哈希 ──
    if (eq(cmd, "HSET") or eq(cmd, "HMSET")) {
        const entry = try s.getOrCreate(args[1], .hash);
        var added: i64 = 0;
        var i: usize = 2;
        while (i + 1 < args.len) : (i += 2) {
            if (hashFind(entry, args[i])) |idx| {
                s.gpa.free(entry.hash.items[idx].value);
                entry.hash.items[idx].value = try s.gpa.dupe(u8, args[i + 1]);
            } else {
                try entry.hash.append(s.gpa, .{
                    .field = try s.gpa.dupe(u8, args[i]),
                    .value = try s.gpa.dupe(u8, args[i + 1]),
                });
                added += 1;
            }
        }
        if (eq(cmd, "HMSET")) return ctx.simple("OK");
        return ctx.int(added);
    }
    if (eq(cmd, "HSETNX")) {
        const entry = try s.getOrCreate(args[1], .hash);
        if (hashFind(entry, args[2]) != null) return ctx.int(0);
        try entry.hash.append(s.gpa, .{
            .field = try s.gpa.dupe(u8, args[2]),
            .value = try s.gpa.dupe(u8, args[3]),
        });
        return ctx.int(1);
    }
    if (eq(cmd, "HGET")) {
        const entry = s.get(args[1]) orelse return ctx.bulk(null);
        if (entry.kind != .hash) return ctx.bulk(null);
        const idx = hashFind(entry, args[2]) orelse return ctx.bulk(null);
        return ctx.bulk(entry.hash.items[idx].value);
    }
    if (eq(cmd, "HEXISTS")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        if (entry.kind != .hash) return ctx.int(0);
        return ctx.int(if (hashFind(entry, args[2]) != null) 1 else 0);
    }
    if (eq(cmd, "HDEL")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        if (entry.kind != .hash) return ctx.int(0);
        var n: i64 = 0;
        for (args[2..]) |field| {
            if (hashFind(entry, field)) |idx| {
                const removed = entry.hash.orderedRemove(idx);
                s.gpa.free(removed.field);
                s.gpa.free(removed.value);
                n += 1;
            }
        }
        return ctx.int(n);
    }
    if (eq(cmd, "HLEN")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        if (entry.kind != .hash) return ctx.int(0);
        return ctx.int(@intCast(entry.hash.items.len));
    }
    if (eq(cmd, "HKEYS") or eq(cmd, "HVALS") or eq(cmd, "HGETALL")) {
        const entry = s.get(args[1]) orelse {
            try ctx.arrayHeader(0);
            return;
        };
        if (entry.kind != .hash) {
            try ctx.arrayHeader(0);
            return;
        }
        if (eq(cmd, "HGETALL")) {
            try ctx.arrayHeader(entry.hash.items.len * 2);
            for (entry.hash.items) |p| {
                try ctx.bulk(p.field);
                try ctx.bulk(p.value);
            }
            return;
        }
        try ctx.arrayHeader(entry.hash.items.len);
        for (entry.hash.items) |p| try ctx.bulk(if (eq(cmd, "HKEYS")) p.field else p.value);
        return;
    }
    if (eq(cmd, "HMGET")) {
        try ctx.arrayHeader(args.len - 2);
        const entry = s.get(args[1]);
        for (args[2..]) |field| {
            if (entry) |e| {
                if (e.kind == .hash) {
                    if (hashFind(e, field)) |idx| {
                        try ctx.bulk(e.hash.items[idx].value);
                        continue;
                    }
                }
            }
            try ctx.bulk(null);
        }
        return;
    }
    if (eq(cmd, "HINCRBY")) {
        const delta = std.fmt.parseInt(i64, args[3], 10) catch 0;
        const entry = try s.getOrCreate(args[1], .hash);
        const idx = hashFind(entry, args[2]);
        const current: i64 = if (idx) |i| (std.fmt.parseInt(i64, entry.hash.items[i].value, 10) catch 0) else 0;
        const next = current + delta;
        var buf: [32]u8 = undefined;
        const text = try s.gpa.dupe(u8, try std.fmt.bufPrint(&buf, "{d}", .{next}));
        if (idx) |i| {
            s.gpa.free(entry.hash.items[i].value);
            entry.hash.items[i].value = text;
        } else {
            try entry.hash.append(s.gpa, .{ .field = try s.gpa.dupe(u8, args[2]), .value = text });
        }
        return ctx.int(next);
    }
    if (eq(cmd, "HINCRBYFLOAT")) {
        const delta = std.fmt.parseFloat(f64, args[3]) catch 0;
        const entry = try s.getOrCreate(args[1], .hash);
        const idx = hashFind(entry, args[2]);
        const current: f64 = if (idx) |i| (std.fmt.parseFloat(f64, entry.hash.items[i].value) catch 0) else 0;
        const next = current + delta;
        const text = try pek_zredis.resp.formatF64Alloc(s.gpa, next);
        if (idx) |i| {
            s.gpa.free(entry.hash.items[i].value);
            entry.hash.items[i].value = text;
        } else {
            try entry.hash.append(s.gpa, .{ .field = try s.gpa.dupe(u8, args[2]), .value = text });
        }
        return ctx.bulk(text);
    }
    if (eq(cmd, "HSCAN")) return hscanCmd(ctx, args);

    // ── Tair 扩展（EX*，版本号按 key 记录）──
    if (eq(cmd, "EXSET")) {
        var previous: ?[]u8 = null;
        if (s.get(args[1])) |prev| {
            if (prev.kind == .string) previous = try s.gpa.dupe(u8, prev.str.items);
        }
        defer if (previous) |p| s.gpa.free(p);
        const entry = try s.getOrCreate(args[1], .string);
        if (entry.kind != .string) return ctx.err("WRONGTYPE");
        entry.str.clearRetainingCapacity();
        try entry.str.appendSlice(s.gpa, args[2]);
        var i: usize = 3;
        while (i < args.len) : (i += 1) {
            if (eq(args[i], "EX") and i + 1 < args.len) {
                const secs = std.fmt.parseInt(i64, args[i + 1], 10) catch 0;
                entry.expire_at_ms = if (secs > 0) s.nowMs() + secs * 1000 else 0;
                i += 1;
            } else if (eq(args[i], "VER") and i + 1 < args.len) {
                entry.ver = std.fmt.parseInt(i64, args[i + 1], 10) catch 0;
                i += 1;
            }
        }
        return ctx.bulk(previous);
    }
    if (eq(cmd, "EXGET")) {
        const entry = s.get(args[1]) orelse {
            try ctx.arrayHeader(0);
            return;
        };
        if (entry.kind != .string) {
            try ctx.arrayHeader(0);
            return;
        }
        try ctx.arrayHeader(2);
        try ctx.bulk(entry.str.items);
        return ctx.int(entry.ver);
    }
    if (eq(cmd, "EXINCRBY")) {
        const delta = std.fmt.parseInt(i64, args[2], 10) catch 0;
        const entry = try s.getOrCreate(args[1], .string);
        if (entry.kind != .string) return ctx.err("WRONGTYPE");
        const current = std.fmt.parseInt(i64, entry.str.items, 10) catch 0;
        const next = current + delta;
        var buf: [32]u8 = undefined;
        const text = std.fmt.bufPrint(&buf, "{d}", .{next}) catch "0";
        entry.str.clearRetainingCapacity();
        try entry.str.appendSlice(s.gpa, text);
        entry.ver += 1;
        var i: usize = 3;
        while (i < args.len) : (i += 1) {
            if (eq(args[i], "EX") and i + 1 < args.len) {
                const secs = std.fmt.parseInt(i64, args[i + 1], 10) catch 0;
                entry.expire_at_ms = if (secs > 0) s.nowMs() + secs * 1000 else 0;
                i += 1;
            } else if (eq(args[i], "VER") and i + 1 < args.len) {
                // VER 是「期望版本」条件；0 表示不校验
                i += 1;
            }
        }
        try ctx.arrayHeader(2);
        try ctx.int(next);
        return ctx.int(entry.ver);
    }
    if (eq(cmd, "EXHSET")) {
        const entry = try s.getOrCreate(args[1], .hash);
        if (entry.kind != .hash) return ctx.err("WRONGTYPE");
        var nx = false;
        var ver: i64 = -1;
        var ex: i64 = 0;
        var i: usize = 4;
        while (i < args.len) : (i += 1) {
            if (eq(args[i], "NX")) {
                nx = true;
            } else if (eq(args[i], "VER") and i + 1 < args.len) {
                ver = std.fmt.parseInt(i64, args[i + 1], 10) catch -1;
                i += 1;
            } else if (eq(args[i], "EX") and i + 1 < args.len) {
                ex = std.fmt.parseInt(i64, args[i + 1], 10) catch 0;
                i += 1;
            }
        }
        const idx = hashFind(entry, args[2]);
        if (nx and idx != null) return ctx.int(0);
        const copy = try s.gpa.dupe(u8, args[3]);
        if (idx) |k| {
            s.gpa.free(entry.hash.items[k].value);
            entry.hash.items[k].value = copy;
        } else {
            try entry.hash.append(s.gpa, .{ .field = try s.gpa.dupe(u8, args[2]), .value = copy });
        }
        if (ver >= 0) entry.ver = ver;
        if (ex > 0) entry.expire_at_ms = s.nowMs() + ex * 1000;
        return ctx.int(if (idx == null) 1 else 0);
    }
    if (eq(cmd, "EXHGET")) {
        const entry = s.get(args[1]) orelse return ctx.bulk(null);
        if (entry.kind != .hash) return ctx.bulk(null);
        const idx = hashFind(entry, args[2]) orelse return ctx.bulk(null);
        return ctx.bulk(entry.hash.items[idx].value);
    }
    if (eq(cmd, "EXHMGET")) {
        try ctx.arrayHeader(args.len - 2);
        const entry = s.get(args[1]);
        for (args[2..]) |field| {
            if (entry) |e| {
                if (e.kind == .hash) {
                    if (hashFind(e, field)) |idx| {
                        try ctx.bulk(e.hash.items[idx].value);
                        continue;
                    }
                }
            }
            try ctx.bulk(null);
        }
        return;
    }
    if (eq(cmd, "EXHGETWITHVER")) {
        const entry = s.get(args[1]) orelse {
            try ctx.arrayHeader(0);
            return;
        };
        if (entry.kind != .hash) {
            try ctx.arrayHeader(0);
            return;
        }
        const idx = hashFind(entry, args[2]) orelse {
            try ctx.arrayHeader(0);
            return;
        };
        try ctx.arrayHeader(2);
        try ctx.bulk(entry.hash.items[idx].value);
        return ctx.int(entry.ver);
    }
    if (eq(cmd, "EXHINCRBY")) {
        const delta = std.fmt.parseInt(i64, args[3], 10) catch 0;
        const entry = try s.getOrCreate(args[1], .hash);
        if (entry.kind != .hash) return ctx.err("WRONGTYPE");
        const idx = hashFind(entry, args[2]);
        const current: i64 = if (idx) |k| (std.fmt.parseInt(i64, entry.hash.items[k].value, 10) catch 0) else 0;
        const next = current + delta;
        var buf: [32]u8 = undefined;
        const text = try s.gpa.dupe(u8, std.fmt.bufPrint(&buf, "{d}", .{next}) catch "0");
        if (idx) |k| {
            s.gpa.free(entry.hash.items[k].value);
            entry.hash.items[k].value = text;
        } else {
            try entry.hash.append(s.gpa, .{ .field = try s.gpa.dupe(u8, args[2]), .value = text });
        }
        entry.ver += 1;
        var i: usize = 4;
        while (i < args.len) : (i += 1) {
            if (eq(args[i], "EX") and i + 1 < args.len) {
                const secs = std.fmt.parseInt(i64, args[i + 1], 10) catch 0;
                entry.expire_at_ms = if (secs > 0) s.nowMs() + secs * 1000 else 0;
                i += 1;
            }
        }
        return ctx.int(next);
    }
    if (eq(cmd, "EXHPTTL")) {
        const entry = s.get(args[1]) orelse return ctx.int(-2);
        if (entry.kind != .hash or hashFind(entry, args[2]) == null) return ctx.int(-2);
        if (entry.expire_at_ms == 0) return ctx.int(-1);
        const remain = entry.expire_at_ms - s.nowMs();
        return ctx.int(if (remain > 0) remain else -2);
    }
    if (eq(cmd, "EXHKEYS") or eq(cmd, "EXHVALS")) {
        const entry = s.get(args[1]) orelse {
            try ctx.arrayHeader(0);
            return;
        };
        if (entry.kind != .hash) {
            try ctx.arrayHeader(0);
            return;
        }
        try ctx.arrayHeader(entry.hash.items.len);
        for (entry.hash.items) |p| try ctx.bulk(if (eq(cmd, "EXHKEYS")) p.field else p.value);
        return;
    }
    if (eq(cmd, "EXHLEN")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        if (entry.kind != .hash) return ctx.int(0);
        return ctx.int(@intCast(entry.hash.items.len));
    }
    if (eq(cmd, "EXHDEL")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        if (entry.kind != .hash) return ctx.int(0);
        var n: i64 = 0;
        for (args[2..]) |field| {
            if (hashFind(entry, field)) |idx| {
                const removed = entry.hash.orderedRemove(idx);
                s.gpa.free(removed.field);
                s.gpa.free(removed.value);
                n += 1;
            }
        }
        return ctx.int(n);
    }

    // ── 列表 ──
    if (eq(cmd, "LPUSH") or eq(cmd, "RPUSH")) {
        const entry = try s.getOrCreate(args[1], .list);
        for (args[2..]) |v| {
            const copy = try s.gpa.dupe(u8, v);
            if (eq(cmd, "LPUSH")) try entry.list.insert(s.gpa, 0, copy) else try entry.list.append(s.gpa, copy);
        }
        return ctx.int(@intCast(entry.list.items.len));
    }
    if (eq(cmd, "LPOP") or eq(cmd, "RPOP")) {
        const entry = s.get(args[1]) orelse return ctx.bulk(null);
        if (entry.kind != .list or entry.list.items.len == 0) return ctx.bulk(null);
        const item = if (eq(cmd, "LPOP")) entry.list.orderedRemove(0) else entry.list.pop().?;
        defer s.gpa.free(item);
        return ctx.bulk(item);
    }
    if (eq(cmd, "RPOPLPUSH") or eq(cmd, "BRPOPLPUSH")) {
        const src = s.get(args[1]) orelse return ctx.bulk(null);
        if (src.kind != .list or src.list.items.len == 0) return ctx.bulk(null);
        const item = src.list.pop().?;
        const dst = try s.getOrCreate(args[2], .list);
        if (dst.kind != .list) {
            s.gpa.free(item);
            return ctx.err("WRONGTYPE");
        }
        try dst.list.insert(s.gpa, 0, item);
        return ctx.bulk(item);
    }
    if (eq(cmd, "BLPOP") or eq(cmd, "BRPOP")) {
        // 测试只用「已有元素」场景；空列表直接返回 nil（不模拟阻塞）
        const entry = s.get(args[1]) orelse {
            try ctx.bulk(null);
            return;
        };
        if (entry.kind != .list or entry.list.items.len == 0) {
            try ctx.bulk(null);
            return;
        }
        const item = if (eq(cmd, "BLPOP")) entry.list.orderedRemove(0) else entry.list.pop().?;
        try ctx.arrayHeader(2);
        try ctx.bulk(args[1]);
        try ctx.bulk(item);
        s.gpa.free(item);
        return;
    }
    if (eq(cmd, "LLEN")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        return ctx.int(if (entry.kind == .list) @intCast(entry.list.items.len) else 0);
    }
    if (eq(cmd, "LRANGE")) {
        const entry = s.get(args[1]) orelse {
            try ctx.arrayHeader(0);
            return;
        };
        if (entry.kind != .list) {
            try ctx.arrayHeader(0);
            return;
        }
        const start = std.fmt.parseInt(i64, args[2], 10) catch 0;
        const stop = std.fmt.parseInt(i64, args[3], 10) catch -1;
        const slice = sliceRange(entry.list.items.len, start, stop);
        try ctx.arrayHeader(slice.end - slice.start);
        for (entry.list.items[slice.start..slice.end]) |item| try ctx.bulk(item);
        return;
    }
    if (eq(cmd, "LINDEX")) {
        const entry = s.get(args[1]) orelse return ctx.bulk(null);
        if (entry.kind != .list) return ctx.bulk(null);
        const idx = std.fmt.parseInt(i64, args[2], 10) catch 0;
        const real = if (idx < 0) @as(i64, @intCast(entry.list.items.len)) + idx else idx;
        if (real < 0 or real >= entry.list.items.len) return ctx.bulk(null);
        return ctx.bulk(entry.list.items[@intCast(real)]);
    }
    if (eq(cmd, "LSET")) {
        const entry = s.get(args[1]) orelse return ctx.err("no such key");
        const idx = std.fmt.parseInt(usize, args[2], 10) catch 0;
        if (entry.kind != .list or idx >= entry.list.items.len) return ctx.err("index out of range");
        s.gpa.free(entry.list.items[idx]);
        entry.list.items[idx] = try s.gpa.dupe(u8, args[3]);
        return ctx.simple("OK");
    }
    if (eq(cmd, "LREM")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        if (entry.kind != .list) return ctx.int(0);
        const count = std.fmt.parseInt(i64, args[2], 10) catch 0;
        var removed: i64 = 0;
        var i: usize = 0;
        while (i < entry.list.items.len) {
            if (std.mem.eql(u8, entry.list.items[i], args[3])) {
                const item = entry.list.orderedRemove(i);
                s.gpa.free(item);
                removed += 1;
                if (count == 1) break;
            } else i += 1;
        }
        return ctx.int(removed);
    }
    if (eq(cmd, "LTRIM")) {
        const entry = s.get(args[1]) orelse return ctx.simple("OK");
        if (entry.kind != .list) return ctx.simple("OK");
        const start = std.fmt.parseInt(i64, args[2], 10) catch 0;
        const stop = std.fmt.parseInt(i64, args[3], 10) catch -1;
        const slice = sliceRange(entry.list.items.len, start, stop);
        // 先释放范围外的元素
        var i: usize = entry.list.items.len;
        while (i > slice.end) {
            i -= 1;
            if (i >= slice.end) s.gpa.free(entry.list.orderedRemove(i));
        }
        i = 0;
        while (i < slice.start) : (i += 1) {
            s.gpa.free(entry.list.orderedRemove(0));
        }
        return ctx.simple("OK");
    }
    if (eq(cmd, "LPOS")) {
        const entry = s.get(args[1]) orelse return ctx.bulk(null);
        if (entry.kind != .list) return ctx.bulk(null);
        for (entry.list.items, 0..) |item, i| {
            if (std.mem.eql(u8, item, args[2])) return ctx.int(@intCast(i));
        }
        return ctx.bulk(null);
    }

    // ── 集合 ──
    if (eq(cmd, "SADD")) {
        const entry = try s.getOrCreate(args[1], .set);
        var added: i64 = 0;
        for (args[2..]) |m| {
            if (setFind(entry, m) == null) {
                try entry.set.append(s.gpa, try s.gpa.dupe(u8, m));
                added += 1;
            }
        }
        return ctx.int(added);
    }
    if (eq(cmd, "SREM")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        if (entry.kind != .set) return ctx.int(0);
        var n: i64 = 0;
        for (args[2..]) |m| {
            if (setFind(entry, m)) |idx| {
                s.gpa.free(entry.set.orderedRemove(idx));
                n += 1;
            }
        }
        return ctx.int(n);
    }
    if (eq(cmd, "SISMEMBER")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        if (entry.kind != .set) return ctx.int(0);
        return ctx.int(if (setFind(entry, args[2]) != null) 1 else 0);
    }
    if (eq(cmd, "SMISMEMBER")) {
        try ctx.arrayHeader(args.len - 2);
        const entry = s.get(args[1]);
        for (args[2..]) |m| {
            if (entry) |e| {
                if (e.kind == .set and setFind(e, m) != null) {
                    try ctx.int(1);
                    continue;
                }
            }
            try ctx.int(0);
        }
        return;
    }
    if (eq(cmd, "SCARD")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        return ctx.int(if (entry.kind == .set) @intCast(entry.set.items.len) else 0);
    }
    if (eq(cmd, "SMEMBERS")) {
        const entry = s.get(args[1]) orelse {
            try ctx.arrayHeader(0);
            return;
        };
        if (entry.kind != .set) {
            try ctx.arrayHeader(0);
            return;
        }
        try ctx.arrayHeader(entry.set.items.len);
        for (entry.set.items) |m| try ctx.bulk(m);
        return;
    }
    if (eq(cmd, "SPOP")) {
        const entry = s.get(args[1]) orelse {
            try ctx.arrayHeader(0);
            return;
        };
        if (entry.kind != .set) {
            try ctx.arrayHeader(0);
            return;
        }
        const n: usize = if (args.len > 2) @intCast(std.fmt.parseInt(i64, args[2], 10) catch 1) else 1;
        const want = @min(n, entry.set.items.len);
        try ctx.arrayHeader(want);
        for (0..want) |_| {
            const m = entry.set.pop().?;
            try ctx.bulk(m);
            s.gpa.free(m);
        }
        return;
    }
    if (eq(cmd, "SSCAN")) return sscanCmd(ctx, args);
    if (eq(cmd, "SUNION") or eq(cmd, "SINTER") or eq(cmd, "SDIFF")) {
        return setOpCmd(ctx, args);
    }

    // ── 有序集合 ──
    if (eq(cmd, "ZADD")) {
        const entry = try s.getOrCreate(args[1], .zset);
        var added: i64 = 0;
        var i: usize = 2;
        while (i + 1 < args.len) : (i += 2) {
            const score = std.fmt.parseFloat(f64, args[i]) catch 0;
            const member = args[i + 1];
            if (zsetFind(entry, member)) |idx| {
                entry.zset.items[idx].score = score;
            } else {
                try entry.zset.append(s.gpa, .{ .member = try s.gpa.dupe(u8, member), .score = score });
                added += 1;
            }
        }
        return ctx.int(added);
    }
    if (eq(cmd, "ZINCRBY")) {
        const delta = std.fmt.parseFloat(f64, args[2]) catch 0;
        const entry = try s.getOrCreate(args[1], .zset);
        if (zsetFind(entry, args[3])) |idx| {
            entry.zset.items[idx].score += delta;
            const sb = try pek_zredis.resp.formatF64Alloc(s.gpa, entry.zset.items[idx].score);
            try ctx.bulk(sb);
            s.gpa.free(sb);
        } else {
            try entry.zset.append(s.gpa, .{ .member = try s.gpa.dupe(u8, args[3]), .score = delta });
            const sb = try pek_zredis.resp.formatF64Alloc(s.gpa, delta);
            try ctx.bulk(sb);
            s.gpa.free(sb);
        }
        return;
    }
    if (eq(cmd, "ZSCORE")) {
        const entry = s.get(args[1]) orelse return ctx.bulk(null);
        if (entry.kind != .zset) return ctx.bulk(null);
        const idx = zsetFind(entry, args[2]) orelse return ctx.bulk(null);
        const sb = try pek_zredis.resp.formatF64Alloc(s.gpa, entry.zset.items[idx].score);
        try ctx.bulk(sb);
        s.gpa.free(sb);
        return;
    }
    if (eq(cmd, "ZREM")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        if (entry.kind != .zset) return ctx.int(0);
        var n: i64 = 0;
        for (args[2..]) |m| {
            if (zsetFind(entry, m)) |idx| {
                s.gpa.free(entry.zset.orderedRemove(idx).member);
                n += 1;
            }
        }
        return ctx.int(n);
    }
    if (eq(cmd, "ZCARD")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        return ctx.int(if (entry.kind == .zset) @intCast(entry.zset.items.len) else 0);
    }
    if (eq(cmd, "ZCOUNT")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        if (entry.kind != .zset) return ctx.int(0);
        const min = std.fmt.parseFloat(f64, args[2]) catch 0;
        const max = std.fmt.parseFloat(f64, args[3]) catch 0;
        var n: i64 = 0;
        for (entry.zset.items) |z| {
            if (z.score >= min and z.score <= max) n += 1;
        }
        return ctx.int(n);
    }
    if (eq(cmd, "ZRANGE") or eq(cmd, "ZREVRANGE")) {
        const entry = s.get(args[1]) orelse {
            try ctx.arrayHeader(0);
            return;
        };
        if (entry.kind != .zset) {
            try ctx.arrayHeader(0);
            return;
        }
        sortZset(entry, !eq(cmd, "ZREVRANGE"));
        const start = std.fmt.parseInt(i64, args[2], 10) catch 0;
        const stop = std.fmt.parseInt(i64, args[3], 10) catch -1;
        const with_scores = args.len > 4 and eq(args[4], "WITHSCORES");
        const slice = sliceRange(entry.zset.items.len, start, stop);
        try ctx.arrayHeader((slice.end - slice.start) * (if (with_scores) @as(usize, 2) else 1));
        for (entry.zset.items[slice.start..slice.end]) |z| {
            try ctx.bulk(z.member);
            if (with_scores) {
                const sb = try pek_zredis.resp.formatF64Alloc(s.gpa, z.score);
                try ctx.bulk(sb);
                s.gpa.free(sb);
            }
        }
        return;
    }
    if (eq(cmd, "ZRANGEBYSCORE")) {
        const entry = s.get(args[1]) orelse {
            try ctx.arrayHeader(0);
            return;
        };
        if (entry.kind != .zset) {
            try ctx.arrayHeader(0);
            return;
        }
        sortZset(entry, true);
        const min = std.fmt.parseFloat(f64, args[2]) catch 0;
        const max = std.fmt.parseFloat(f64, args[3]) catch 0;
        var offset: usize = 0;
        var limit: usize = std.math.maxInt(usize);
        var i: usize = 4;
        while (i + 1 < args.len) : (i += 1) {
            if (eq(args[i], "LIMIT")) {
                offset = @intCast(std.fmt.parseInt(i64, args[i + 1], 10) catch 0);
                if (i + 2 < args.len) limit = @intCast(std.fmt.parseInt(i64, args[i + 2], 10) catch -1);
                break;
            }
        }
        var count: usize = 0;
        var matched: usize = 0;
        for (entry.zset.items) |z| {
            if (z.score >= min and z.score <= max) {
                if (matched >= offset and count < limit) count += 1;
                matched += 1;
            }
        }
        try ctx.arrayHeader(count);
        var emitted: usize = 0;
        var seen: usize = 0;
        for (entry.zset.items) |z| {
            if (z.score < min or z.score > max) continue;
            if (seen < offset) {
                seen += 1;
                continue;
            }
            if (emitted >= count) break;
            try ctx.bulk(z.member);
            emitted += 1;
        }
        return;
    }
    if (eq(cmd, "ZRANK") or eq(cmd, "ZREVRANK")) {
        const entry = s.get(args[1]) orelse return ctx.bulk(null);
        if (entry.kind != .zset) return ctx.bulk(null);
        sortZset(entry, true);
        for (entry.zset.items, 0..) |z, i| {
            if (std.mem.eql(u8, z.member, args[2])) {
                const rank: i64 = if (eq(cmd, "ZRANK")) @intCast(i) else @intCast(entry.zset.items.len - 1 - i);
                return ctx.int(rank);
            }
        }
        return ctx.bulk(null);
    }
    if (eq(cmd, "ZPOPMIN") or eq(cmd, "ZPOPMAX")) {
        const entry = s.get(args[1]) orelse {
            try ctx.arrayHeader(0);
            return;
        };
        if (entry.kind != .zset) {
            try ctx.arrayHeader(0);
            return;
        }
        sortZset(entry, true);
        const n: usize = if (args.len > 2) @intCast(std.fmt.parseInt(i64, args[2], 10) catch 1) else 1;
        const want = @min(n, entry.zset.items.len);
        try ctx.arrayHeader(want * 2);
        for (0..want) |_| {
            const z = if (eq(cmd, "ZPOPMIN")) entry.zset.orderedRemove(0) else entry.zset.pop().?;
            try ctx.bulk(z.member);
            const sb = try pek_zredis.resp.formatF64Alloc(s.gpa, z.score);
            try ctx.bulk(sb);
            s.gpa.free(sb);
            s.gpa.free(z.member);
        }
        return;
    }
    if (eq(cmd, "ZREMRANGEBYSCORE")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        if (entry.kind != .zset) return ctx.int(0);
        const min = std.fmt.parseFloat(f64, args[2]) catch 0;
        const max = std.fmt.parseFloat(f64, args[3]) catch 0;
        var n: i64 = 0;
        var i: usize = 0;
        while (i < entry.zset.items.len) {
            if (entry.zset.items[i].score >= min and entry.zset.items[i].score <= max) {
                s.gpa.free(entry.zset.orderedRemove(i).member);
                n += 1;
            } else i += 1;
        }
        return ctx.int(n);
    }
    if (eq(cmd, "ZSCAN")) return zscanCmd(ctx, args);

    // ── 发布订阅 ──
    if (eq(cmd, "PUBLISH")) return publishCmd(ctx, args);
    if (eq(cmd, "SUBSCRIBE") or eq(cmd, "PSUBSCRIBE")) {
        const is_pattern = eq(cmd, "PSUBSCRIBE");
        for (args[1..]) |name| {
            try s.subs.append(s.gpa, .{
                .conn = ctx,
                .channel = try s.gpa.dupe(u8, name),
                .pattern = is_pattern,
            });
            try ctx.arrayHeader(3);
            try ctx.bulk(if (is_pattern) "psubscribe" else "subscribe");
            try ctx.bulk(name);
            try ctx.int(@intCast(s.subs.items.len));
        }
        return;
    }
    if (eq(cmd, "UNSUBSCRIBE") or eq(cmd, "PUNSUBSCRIBE")) {
        var i: usize = 0;
        while (i < s.subs.items.len) {
            if (s.subs.items[i].conn == ctx) {
                const sub = s.subs.orderedRemove(i);
                s.gpa.free(sub.channel);
            } else i += 1;
        }
        try ctx.arrayHeader(3);
        try ctx.bulk("unsubscribe");
        try ctx.bulk("");
        try ctx.int(0);
        return;
    }

    // ── 脚本 / 管理命令（最小占位）──
    if (eq(cmd, "SCRIPT")) {
        if (args.len > 1 and eq(args[1], "LOAD")) return ctx.bulk("0000000000000000000000000000000000000000");
        if (args.len > 1 and eq(args[1], "EXISTS")) return ctx.int(1);
        return ctx.simple("OK");
    }
    if (eq(cmd, "EVAL") or eq(cmd, "EVALSHA")) {
        // 最小实现：RedLock 解锁脚本（GET 比较后 DEL）
        // 参数：[EVAL, script, numkeys, key, argv1]
        if (args.len >= 5 and std.mem.indexOf(u8, args[1], "redis.call('del'") != null) {
            const key = args[3];
            const expect = args[4];
            if (s.get(key)) |entry| {
                if (entry.kind == .string and std.mem.eql(u8, entry.str.items, expect)) {
                    s.removeLocked(key);
                    return ctx.int(1);
                }
            }
            return ctx.int(0);
        }
        return ctx.int(1);
    }
    // ── Stream ──
    if (eq(cmd, "XADD")) {
        const entry = try s.getOrCreate(args[1], .stream);
        if (entry.kind != .stream) return ctx.err("WRONGTYPE");
        var i: usize = 2;
        var trim_max: ?usize = null;
        if (i < args.len and eq(args[i], "MAXLEN")) {
            i += 1;
            if (i < args.len and eq(args[i], "~")) i += 1;
            if (i < args.len) {
                trim_max = @intCast(std.fmt.parseInt(i64, args[i], 10) catch 0);
                i += 1;
            }
        }
        if (i >= args.len) return ctx.err("ERR wrong number of arguments");
        const id_arg = args[i];
        i += 1;

        var ms: u64 = 0;
        var seq: u64 = 0;
        if (eq(id_arg, "*")) {
            ms = @intCast(s.nowMs());
            seq = if (entry.last_ms == ms) entry.last_seq + 1 else 0;
        } else {
            const parsed = parseStreamId(id_arg) orelse return ctx.err("ERR Invalid stream ID");
            ms = parsed.ms;
            seq = parsed.seq;
        }
        entry.last_ms = ms;
        entry.last_seq = seq;
        entry.entries_added += 1;

        const id_text = try std.fmt.allocPrint(s.gpa, "{d}-{d}", .{ ms, seq });
        var fields = try s.gpa.alloc([]u8, args.len - i);
        var filled: usize = 0;
        errdefer {
            for (fields[0..filled]) |f| s.gpa.free(f);
            s.gpa.free(fields);
        }
        while (i < args.len) : (i += 1) {
            fields[filled] = try s.gpa.dupe(u8, args[i]);
            filled += 1;
        }
        try entry.entries.append(s.gpa, .{ .id = id_text, .fields = fields });

        if (trim_max) |max| {
            while (entry.entries.items.len > max) {
                const removed = entry.entries.orderedRemove(0);
                s.gpa.free(removed.id);
                for (removed.fields) |f| s.gpa.free(f);
                if (removed.fields.len > 0) s.gpa.free(removed.fields);
            }
        }
        return ctx.bulk(id_text);
    }
    if (eq(cmd, "XLEN")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        if (entry.kind != .stream) return ctx.int(0);
        return ctx.int(@intCast(entry.entries.items.len));
    }
    if (eq(cmd, "XDEL")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        if (entry.kind != .stream) return ctx.int(0);
        var n: i64 = 0;
        for (args[2..]) |id| {
            var k: usize = 0;
            while (k < entry.entries.items.len) : (k += 1) {
                if (std.mem.eql(u8, entry.entries.items[k].id, id)) {
                    const removed = entry.entries.orderedRemove(k);
                    s.gpa.free(removed.id);
                    for (removed.fields) |f| s.gpa.free(f);
                    if (removed.fields.len > 0) s.gpa.free(removed.fields);
                    n += 1;
                    break;
                }
            }
        }
        return ctx.int(n);
    }
    if (eq(cmd, "XTRIM")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        if (entry.kind != .stream) return ctx.int(0);
        if (args.len >= 4 and eq(args[2], "MAXLEN")) {
            var idx: usize = 3;
            if (eq(args[idx], "~")) idx += 1;
            const max: usize = @intCast(std.fmt.parseInt(i64, args[idx], 10) catch 0);
            var n: i64 = 0;
            while (entry.entries.items.len > max) {
                const removed = entry.entries.orderedRemove(0);
                s.gpa.free(removed.id);
                for (removed.fields) |f| s.gpa.free(f);
                if (removed.fields.len > 0) s.gpa.free(removed.fields);
                n += 1;
            }
            return ctx.int(n);
        }
        if (args.len >= 4 and eq(args[2], "MINID")) {
            const min = parseStreamId(args[3]) orelse return ctx.int(0);
            var n: i64 = 0;
            while (entry.entries.items.len > 0) {
                const first = entry.entries.items[0];
                const fid = parseStreamId(first.id) orelse break;
                if (streamOrder(fid.ms, fid.seq, min.ms, min.seq) != .lt) break;
                const removed = entry.entries.orderedRemove(0);
                s.gpa.free(removed.id);
                for (removed.fields) |f| s.gpa.free(f);
                if (removed.fields.len > 0) s.gpa.free(removed.fields);
                n += 1;
            }
            return ctx.int(n);
        }
        return ctx.int(0);
    }
    if (eq(cmd, "XRANGE")) {
        const entry = s.get(args[1]) orelse {
            try ctx.arrayHeader(0);
            return;
        };
        if (entry.kind != .stream) {
            try ctx.arrayHeader(0);
            return;
        }
        const start = parseStreamRange(args[2], true);
        const end = parseStreamRange(args[3], false);
        var limit: usize = std.math.maxInt(usize);
        var i: usize = 4;
        while (i + 1 < args.len) : (i += 1) {
            if (eq(args[i], "COUNT")) {
                limit = @intCast(std.fmt.parseInt(i64, args[i + 1], 10) catch 0);
                break;
            }
        }
        var count: usize = 0;
        for (entry.entries.items) |e| {
            const id = parseStreamId(e.id) orelse continue;
            if (!streamInRange(id.ms, id.seq, start, end)) continue;
            if (count >= limit) break;
            count += 1;
        }
        try ctx.arrayHeader(count);
        var emitted: usize = 0;
        for (entry.entries.items) |*e| {
            const id = parseStreamId(e.id) orelse continue;
            if (!streamInRange(id.ms, id.seq, start, end)) continue;
            if (emitted >= count) break;
            try streamEntryOut(ctx, e);
            emitted += 1;
        }
        return;
    }
    if (eq(cmd, "XREAD") or eq(cmd, "XREADGROUP")) {
        const is_group = eq(cmd, "XREADGROUP");
        var i: usize = 1;
        var group: ?[]const u8 = null;
        var consumer: ?[]const u8 = null;
        var count: usize = std.math.maxInt(usize);
        if (is_group) {
            if (!eq(args[i], "GROUP")) return ctx.err("ERR syntax error");
            group = args[i + 1];
            consumer = args[i + 2];
            i += 3;
        }
        while (i < args.len) : (i += 1) {
            if (eq(args[i], "BLOCK")) {
                i += 1;
            } else if (eq(args[i], "COUNT")) {
                count = @intCast(std.fmt.parseInt(i64, args[i + 1], 10) catch 0);
                i += 1;
            } else if (eq(args[i], "STREAMS")) {
                break;
            }
        }
        if (i + 2 >= args.len) return ctx.err("ERR syntax error");
        const key = args[i + 1];
        const start_arg = args[i + 2];

        if (!is_group) {
            return xreadOut(ctx, s, key, start_arg, count, null);
        }
        return xreadGroupOut(ctx, s, key, group.?, consumer.?, start_arg, count);
    }
    if (eq(cmd, "XACK")) {
        const entry = s.get(args[1]) orelse return ctx.int(0);
        if (entry.kind != .stream) return ctx.int(0);
        const g = findGroup(entry, args[2]) orelse return ctx.int(0);
        var n: i64 = 0;
        for (args[3..]) |id| {
            var k: usize = 0;
            while (k < g.pending.items.len) : (k += 1) {
                if (std.mem.eql(u8, g.pending.items[k].id, id)) {
                    const removed = g.pending.orderedRemove(k);
                    s.gpa.free(removed.id);
                    s.gpa.free(removed.consumer);
                    n += 1;
                    break;
                }
            }
        }
        return ctx.int(n);
    }
    if (eq(cmd, "XPENDING")) {
        const entry = s.get(args[1]) orelse {
            try ctx.arrayHeader(0);
            return;
        };
        if (entry.kind != .stream) {
            try ctx.arrayHeader(0);
            return;
        }
        const g = findGroup(entry, args[2]) orelse return ctx.err("NOGROUP No such group");
        if (args.len == 3) {
            // 汇总：count / min / max / 各消费者计数
            var min_id: ?[]const u8 = null;
            var max_id: ?[]const u8 = null;
            var names: std.ArrayList([]const u8) = .empty;
            var counts: std.ArrayList(i64) = .empty;
            defer names.deinit(s.gpa);
            defer counts.deinit(s.gpa);
            for (g.pending.items) |p| {
                if (min_id == null) min_id = p.id;
                max_id = p.id;
                var found = false;
                for (names.items, 0..) |name, idx| {
                    if (std.mem.eql(u8, name, p.consumer)) {
                        counts.items[idx] += 1;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    try names.append(s.gpa, p.consumer);
                    try counts.append(s.gpa, 1);
                }
            }
            try ctx.arrayHeader(4);
            try ctx.int(@intCast(g.pending.items.len));
            try ctx.bulk(min_id);
            try ctx.bulk(max_id);
            try ctx.arrayHeader(names.items.len);
            for (names.items, 0..) |name, idx| {
                try ctx.arrayHeader(2);
                try ctx.bulk(name);
                try ctx.int(counts.items[idx]);
            }
            return;
        }
        // 明细
        var limit: usize = std.math.maxInt(usize);
        if (args.len >= 6) limit = @intCast(std.fmt.parseInt(i64, args[5], 10) catch 0);
        const start = parseStreamRange(args[3], true);
        const end = parseStreamRange(args[4], false);
        var count: usize = 0;
        for (g.pending.items) |p| {
            const id = parseStreamId(p.id) orelse continue;
            if (!streamInRange(id.ms, id.seq, start, end)) continue;
            if (count >= limit) break;
            count += 1;
        }
        try ctx.arrayHeader(count);
        var emitted: usize = 0;
        for (g.pending.items) |p| {
            const id = parseStreamId(p.id) orelse continue;
            if (!streamInRange(id.ms, id.seq, start, end)) continue;
            if (emitted >= count) break;
            try ctx.arrayHeader(4);
            try ctx.bulk(p.id);
            try ctx.bulk(p.consumer);
            try ctx.int(s.nowMs() - p.last_ms);
            try ctx.int(p.delivery);
            emitted += 1;
        }
        return;
    }
    if (eq(cmd, "XCLAIM")) {
        const entry = s.get(args[1]) orelse {
            try ctx.arrayHeader(0);
            return;
        };
        if (entry.kind != .stream) {
            try ctx.arrayHeader(0);
            return;
        }
        const g = findGroup(entry, args[2]) orelse return ctx.err("NOGROUP No such group");
        const consumer = args[3];
        const min_idle = std.fmt.parseInt(i64, args[4], 10) catch 0;
        var claimed: usize = 0;
        for (g.pending.items) |*p| {
            if (claimed >= args.len - 5) break;
            var matched = false;
            for (args[5..]) |id| {
                if (std.mem.eql(u8, p.id, id)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) continue;
            if (s.nowMs() - p.last_ms < min_idle) continue;
            s.gpa.free(p.consumer);
            p.consumer = try s.gpa.dupe(u8, consumer);
            p.delivery += 1;
            p.last_ms = s.nowMs();
            claimed += 1;
        }
        try ctx.arrayHeader(claimed);
        var emitted: usize = 0;
        for (g.pending.items) |p| {
            if (emitted >= claimed) break;
            if (!std.mem.eql(u8, p.consumer, consumer)) continue;
            for (entry.entries.items) |*e| {
                if (std.mem.eql(u8, e.id, p.id)) {
                    try streamEntryOut(ctx, e);
                    emitted += 1;
                    break;
                }
            }
        }
        return;
    }
    if (eq(cmd, "XGROUP")) {
        if (args.len < 2) return ctx.err("ERR syntax error");
        const sub = args[1];
        if (eq(sub, "CREATE")) {
            if (args.len < 5) return ctx.err("ERR syntax error");
            const entry = try s.getOrCreate(args[2], .stream);
            if (entry.kind != .stream) return ctx.err("WRONGTYPE");
            if (findGroup(entry, args[3]) != null) return ctx.err("BUSYGROUP Consumer Group name already exists");
            const start = if (eq(args[4], "$"))
                IdPair{ .ms = entry.last_ms, .seq = entry.last_seq }
            else
                (parseStreamId(args[4]) orelse IdPair{ .ms = 0, .seq = 0 });
            var group = StreamGroup{ .name = try s.gpa.dupe(u8, args[3]), .last_ms = start.ms, .last_seq = start.seq };
            errdefer {
                s.gpa.free(group.name);
                group.consumers.deinit(s.gpa);
                group.pending.deinit(s.gpa);
            }
            try entry.groups.append(s.gpa, group);
            return ctx.simple("OK");
        }
        if (eq(sub, "DESTROY")) {
            const entry = s.get(args[2]) orelse return ctx.int(0);
            if (entry.kind != .stream) return ctx.int(0);
            var k: usize = 0;
            while (k < entry.groups.items.len) : (k += 1) {
                if (std.mem.eql(u8, entry.groups.items[k].name, args[3])) {
                    var removed = entry.groups.orderedRemove(k);
                    s.gpa.free(removed.name);
                    for (removed.consumers.items) |c| s.gpa.free(c.name);
                    removed.consumers.deinit(s.gpa);
                    for (removed.pending.items) |p| {
                        s.gpa.free(p.id);
                        s.gpa.free(p.consumer);
                    }
                    removed.pending.deinit(s.gpa);
                    return ctx.int(1);
                }
            }
            return ctx.int(0);
        }
        if (eq(sub, "DELCONSUMER")) {
            const entry = s.get(args[2]) orelse return ctx.int(0);
            if (entry.kind != .stream) return ctx.int(0);
            const g = findGroup(entry, args[3]) orelse return ctx.err("NOGROUP No such group");
            var k: usize = 0;
            while (k < g.consumers.items.len) : (k += 1) {
                if (std.mem.eql(u8, g.consumers.items[k].name, args[4])) {
                    s.gpa.free(g.consumers.items[k].name);
                    _ = g.consumers.orderedRemove(k);
                    break;
                }
            }
            var removed_count: i64 = 0;
            k = 0;
            while (k < g.pending.items.len) {
                if (std.mem.eql(u8, g.pending.items[k].consumer, args[4])) {
                    const removed = g.pending.orderedRemove(k);
                    s.gpa.free(removed.id);
                    s.gpa.free(removed.consumer);
                    removed_count += 1;
                } else k += 1;
            }
            return ctx.int(removed_count);
        }
        if (eq(sub, "SETID")) {
            const entry = s.get(args[2]) orelse return ctx.err("NOGROUP No such key");
            if (entry.kind != .stream) return ctx.err("WRONGTYPE");
            const g = findGroup(entry, args[3]) orelse return ctx.err("NOGROUP No such group");
            const start = if (eq(args[4], "$"))
                IdPair{ .ms = entry.last_ms, .seq = entry.last_seq }
            else
                (parseStreamId(args[4]) orelse IdPair{ .ms = 0, .seq = 0 });
            g.last_ms = start.ms;
            g.last_seq = start.seq;
            return ctx.simple("OK");
        }
        if (eq(sub, "CREATECONSUMER")) {
            const entry = s.get(args[2]) orelse return ctx.err("NOGROUP No such key");
            if (entry.kind != .stream) return ctx.err("WRONGTYPE");
            const g = findGroup(entry, args[3]) orelse return ctx.err("NOGROUP No such group");
            if (findConsumer(g, args[4]) != null) return ctx.int(0);
            try g.consumers.append(s.gpa, .{ .name = try s.gpa.dupe(u8, args[4]), .seen_ms = s.nowMs() });
            return ctx.int(1);
        }
        return ctx.err("ERR unknown XGROUP subcommand");
    }
    if (eq(cmd, "XINFO")) {
        if (args.len < 3) return ctx.err("ERR syntax error");
        const sub = args[1];
        if (eq(sub, "STREAM")) {
            const entry = s.get(args[2]) orelse return ctx.err("ERR no such key");
            if (entry.kind != .stream) return ctx.err("WRONGTYPE");
            var last_id_buf: [48]u8 = undefined;
            const last_id = std.fmt.bufPrint(&last_id_buf, "{d}-{d}", .{ entry.last_ms, entry.last_seq }) catch "0-0";
            try ctx.arrayHeader(18);
            try ctx.bulk("length");
            try ctx.int(@intCast(entry.entries.items.len));
            try ctx.bulk("radix-tree-keys");
            try ctx.int(@intCast(entry.entries.items.len));
            try ctx.bulk("radix-tree-nodes");
            try ctx.int(@intCast(entry.entries.items.len + 1));
            try ctx.bulk("groups");
            try ctx.int(@intCast(entry.groups.items.len));
            try ctx.bulk("last-generated-id");
            try ctx.bulk(last_id);
            try ctx.bulk("first-entry");
            if (entry.entries.items.len > 0) try streamEntryOut(ctx, &entry.entries.items[0]) else try ctx.bulk(null);
            try ctx.bulk("last-entry");
            if (entry.entries.items.len > 0) try streamEntryOut(ctx, &entry.entries.items[entry.entries.items.len - 1]) else try ctx.bulk(null);
            try ctx.bulk("entries-added");
            try ctx.int(entry.entries_added);
            return;
        }
        if (eq(sub, "GROUPS")) {
            const entry = s.get(args[2]) orelse {
                try ctx.arrayHeader(0);
                return;
            };
            if (entry.kind != .stream) {
                try ctx.arrayHeader(0);
                return;
            }
            try ctx.arrayHeader(entry.groups.items.len);
            for (entry.groups.items) |g| {
                var buf: [48]u8 = undefined;
                const last = std.fmt.bufPrint(&buf, "{d}-{d}", .{ g.last_ms, g.last_seq }) catch "0-0";
                try ctx.arrayHeader(8);
                try ctx.bulk("name");
                try ctx.bulk(g.name);
                try ctx.bulk("consumers");
                try ctx.int(@intCast(g.consumers.items.len));
                try ctx.bulk("pending");
                try ctx.int(@intCast(g.pending.items.len));
                try ctx.bulk("last-delivered-id");
                try ctx.bulk(last);
            }
            return;
        }
        if (eq(sub, "CONSUMERS")) {
            const entry = s.get(args[2]) orelse {
                try ctx.arrayHeader(0);
                return;
            };
            if (entry.kind != .stream) {
                try ctx.arrayHeader(0);
                return;
            }
            const g = findGroup(entry, args[3]) orelse {
                try ctx.arrayHeader(0);
                return;
            };
            try ctx.arrayHeader(g.consumers.items.len);
            for (g.consumers.items) |c| {
                var pending: i64 = 0;
                for (g.pending.items) |p| {
                    if (std.mem.eql(u8, p.consumer, c.name)) pending += 1;
                }
                try ctx.arrayHeader(6);
                try ctx.bulk("name");
                try ctx.bulk(c.name);
                try ctx.bulk("pending");
                try ctx.int(pending);
                try ctx.bulk("idle");
                try ctx.int(s.nowMs() - c.seen_ms);
            }
            return;
        }
        return ctx.err("ERR unknown XINFO subcommand");
    }
    if (eq(cmd, "OBJECT")) return ctx.bulk(null);
    if (eq(cmd, "MEMORY")) return ctx.bulk(null);
    if (eq(cmd, "FLUSHALL")) return ctx.simple("OK");

    return ctx.err("unsupported command");
}

fn hashFind(entry: *Entry, field: []const u8) ?usize {
    for (entry.hash.items, 0..) |p, i| {
        if (std.mem.eql(u8, p.field, field)) return i;
    }
    return null;
}

fn setFind(entry: *Entry, member: []const u8) ?usize {
    for (entry.set.items, 0..) |m, i| {
        if (std.mem.eql(u8, m, member)) return i;
    }
    return null;
}

fn zsetFind(entry: *Entry, member: []const u8) ?usize {
    for (entry.zset.items, 0..) |z, i| {
        if (std.mem.eql(u8, z.member, member)) return i;
    }
    return null;
}

fn sortZset(entry: *Entry, asc: bool) void {
    const Ctx = struct {
        asc: bool,
        fn less(ctx: @This(), a: ZItem, b: ZItem) bool {
            if (a.score != b.score) return if (ctx.asc) a.score < b.score else a.score > b.score;
            return if (ctx.asc) std.mem.order(u8, a.member, b.member) == .lt else std.mem.order(u8, a.member, b.member) == .gt;
        }
    };
    std.mem.sort(ZItem, entry.zset.items, Ctx{ .asc = asc }, Ctx.less);
}

const Slice = struct { start: usize, end: usize };

fn sliceRange(len: usize, start: i64, stop: i64) Slice {
    const n: i64 = @intCast(len);
    var s = if (start < 0) n + start else start;
    var e = if (stop < 0) n + stop else stop;
    if (s < 0) s = 0;
    if (e >= n) e = n - 1;
    if (n == 0 or s > e or s >= n) return .{ .start = 0, .end = 0 };
    return .{ .start = @intCast(s), .end = @intCast(e + 1) };
}

fn globMatch(pattern: []const u8, text: []const u8) bool {
    return globAt(pattern, text);
}

fn globAt(pattern: []const u8, text: []const u8) bool {
    if (pattern.len == 0) return text.len == 0;
    switch (pattern[0]) {
        '*' => {
            var i: usize = 0;
            while (true) {
                if (globAt(pattern[1..], text[i..])) return true;
                if (i >= text.len) return false;
                i += 1;
            }
        },
        '?' => return text.len > 0 and globAt(pattern[1..], text[1..]),
        else => return text.len > 0 and pattern[0] == text[0] and globAt(pattern[1..], text[1..]),
    }
}

fn keysCmd(ctx: *ConnCtx, pattern: []const u8) !void {
    const s = ctx.shared;
    var matches: std.ArrayList([]const u8) = .empty;
    defer matches.deinit(ctx.gpa);

    var it = s.map.iterator();
    while (it.next()) |e| {
        if (s.get(e.key_ptr.*) == null) continue;
        if (globMatch(pattern, e.key_ptr.*)) try matches.append(ctx.gpa, e.key_ptr.*);
    }
    return ctx.bulkArray(matches.items);
}

fn scanCmd(ctx: *ConnCtx, args: []const []const u8) !void {
    const s = ctx.shared;
    var pattern: []const u8 = "*";
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        if (eq(args[i], "MATCH") and i + 1 < args.len) {
            pattern = args[i + 1];
            i += 1;
        }
    }
    // 简化：一次返回全部匹配键（cursor 恒 0）
    var matches: std.ArrayList([]const u8) = .empty;
    defer matches.deinit(ctx.gpa);
    var it = s.map.iterator();
    while (it.next()) |e| {
        if (s.get(e.key_ptr.*) == null) continue;
        if (globMatch(pattern, e.key_ptr.*)) try matches.append(ctx.gpa, e.key_ptr.*);
    }

    try ctx.arrayHeader(2);
    try ctx.bulk("0");
    try ctx.bulkArray(matches.items);
}

fn hscanCmd(ctx: *ConnCtx, args: []const []const u8) !void {
    const s = ctx.shared;
    var pattern: []const u8 = "*";
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        if (eq(args[i], "MATCH") and i + 1 < args.len) {
            pattern = args[i + 1];
            i += 1;
        }
    }
    const entry = s.get(args[1]);
    var pairs: std.ArrayList([]const u8) = .empty;
    defer pairs.deinit(ctx.gpa);
    if (entry) |e| {
        if (e.kind == .hash) {
            for (e.hash.items) |p| {
                if (globMatch(pattern, p.field)) {
                    try pairs.append(ctx.gpa, p.field);
                    try pairs.append(ctx.gpa, p.value);
                }
            }
        }
    }
    try ctx.arrayHeader(2);
    try ctx.bulk("0");
    try ctx.bulkArray(pairs.items);
}

fn sscanCmd(ctx: *ConnCtx, args: []const []const u8) !void {
    const s = ctx.shared;
    var pattern: []const u8 = "*";
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        if (eq(args[i], "MATCH") and i + 1 < args.len) {
            pattern = args[i + 1];
            i += 1;
        }
    }
    const entry = s.get(args[1]);
    var matches: std.ArrayList([]const u8) = .empty;
    defer matches.deinit(ctx.gpa);
    if (entry) |e| {
        if (e.kind == .set) {
            for (e.set.items) |m| {
                if (globMatch(pattern, m)) try matches.append(ctx.gpa, m);
            }
        }
    }
    try ctx.arrayHeader(2);
    try ctx.bulk("0");
    try ctx.bulkArray(matches.items);
}

fn zscanCmd(ctx: *ConnCtx, args: []const []const u8) !void {
    const s = ctx.shared;
    var pattern: []const u8 = "*";
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        if (eq(args[i], "MATCH") and i + 1 < args.len) {
            pattern = args[i + 1];
            i += 1;
        }
    }
    const entry = s.get(args[1]);
    var pairs: std.ArrayList([]const u8) = .empty;
    defer pairs.deinit(ctx.gpa);
    var owned: std.ArrayList([]u8) = .empty;
    defer {
        for (owned.items) |text| ctx.gpa.free(text);
        owned.deinit(ctx.gpa);
    }
    if (entry) |e| {
        if (e.kind == .zset) {
            for (e.zset.items) |z| {
                if (globMatch(pattern, z.member)) {
                    try pairs.append(ctx.gpa, z.member);
                    const score_text = try pek_zredis.resp.formatF64Alloc(ctx.gpa, z.score);
                    try owned.append(ctx.gpa, score_text);
                    try pairs.append(ctx.gpa, score_text);
                }
            }
        }
    }
    try ctx.arrayHeader(2);
    try ctx.bulk("0");
    try ctx.bulkArray(pairs.items);
}

fn setOpCmd(ctx: *ConnCtx, args: []const []const u8) !void {
    const s = ctx.shared;
    const cmd = args[0];
    var result: std.ArrayList([]const u8) = .empty;
    defer result.deinit(ctx.gpa);

    // 以第一个集合为基准，依次合并/相交/差集
    const first = s.get(args[1]);
    if (first) |e| {
        if (e.kind == .set) {
            for (e.set.items) |m| try result.append(ctx.gpa, m);
        }
    }
    for (args[2..]) |key| {
        const other = s.get(key);
        if (eq(cmd, "SUNION")) {
            if (other) |e| {
                if (e.kind == .set) {
                    for (e.set.items) |m| {
                        var exists = false;
                        for (result.items) |r| {
                            if (std.mem.eql(u8, r, m)) {
                                exists = true;
                                break;
                            }
                        }
                        if (!exists) try result.append(ctx.gpa, m);
                    }
                }
            }
        } else if (eq(cmd, "SINTER")) {
            var i: usize = 0;
            while (i < result.items.len) {
                var found = false;
                if (other) |e| {
                    if (e.kind == .set) {
                        for (e.set.items) |m| {
                            if (std.mem.eql(u8, m, result.items[i])) {
                                found = true;
                                break;
                            }
                        }
                    }
                }
                if (!found) {
                    _ = result.orderedRemove(i);
                } else i += 1;
            }
        } else { // SDIFF
            var i: usize = 0;
            while (i < result.items.len) {
                var found = false;
                if (other) |e| {
                    if (e.kind == .set) {
                        for (e.set.items) |m| {
                            if (std.mem.eql(u8, m, result.items[i])) {
                                found = true;
                                break;
                            }
                        }
                    }
                }
                if (found) {
                    _ = result.orderedRemove(i);
                } else i += 1;
            }
        }
    }
    return ctx.bulkArray(result.items);
}

fn publishCmd(ctx: *ConnCtx, args: []const []const u8) !void {
    const s = ctx.shared;
    const channel = args[1];
    const payload = args[2];
    var receivers: i64 = 0;

    for (s.subs.items) |sub| {
        const matched = if (sub.pattern) globMatch(sub.channel, channel) else std.mem.eql(u8, sub.channel, channel);
        if (!matched) continue;
        receivers += 1;

        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(ctx.gpa);
        if (sub.pattern) {
            try buf.appendSlice(ctx.gpa, "*4\r\n$8\r\npmessage\r\n");
            try appendBulk(&buf, ctx.gpa, sub.channel);
            try appendBulk(&buf, ctx.gpa, channel);
            try appendBulk(&buf, ctx.gpa, payload);
        } else {
            try buf.appendSlice(ctx.gpa, "*3\r\n$7\r\nmessage\r\n");
            try appendBulk(&buf, ctx.gpa, channel);
            try appendBulk(&buf, ctx.gpa, payload);
        }
        sub.conn.sendAll(buf.items);
    }
    return ctx.int(receivers);
}

fn appendBulk(out: *std.ArrayList(u8), gpa: std.mem.Allocator, data: []const u8) !void {
    var head: [32]u8 = undefined;
    try out.appendSlice(gpa, try std.fmt.bufPrint(&head, "${d}\r\n", .{data.len}));
    try out.appendSlice(gpa, data);
    try out.appendSlice(gpa, "\r\n");
}

// ───────────────────────── Stream 辅助 ─────────────────────────

const IdPair = struct { ms: u64, seq: u64 };

fn parseStreamId(text: []const u8) ?IdPair {
    if (std.mem.eql(u8, text, "-")) return .{ .ms = 0, .seq = 0 };
    if (std.mem.eql(u8, text, "+")) return .{ .ms = std.math.maxInt(u64), .seq = std.math.maxInt(u64) };
    if (std.mem.indexOfScalar(u8, text, '-')) |dash| {
        const ms = std.fmt.parseInt(u64, text[0..dash], 10) catch return null;
        const seq = std.fmt.parseInt(u64, text[dash + 1 ..], 10) catch 0;
        return .{ .ms = ms, .seq = seq };
    }
    const ms = std.fmt.parseInt(u64, text, 10) catch return null;
    return .{ .ms = ms, .seq = 0 };
}

fn parseStreamRange(text: []const u8, is_start: bool) IdPair {
    return parseStreamId(text) orelse (if (is_start)
        IdPair{ .ms = 0, .seq = 0 }
    else
        IdPair{ .ms = std.math.maxInt(u64), .seq = std.math.maxInt(u64) });
}

fn streamOrder(a_ms: u64, a_seq: u64, b_ms: u64, b_seq: u64) std.math.Order {
    if (a_ms != b_ms) return std.math.order(a_ms, b_ms);
    return std.math.order(a_seq, b_seq);
}

fn streamInRange(ms: u64, seq: u64, start: IdPair, end: IdPair) bool {
    return streamOrder(ms, seq, start.ms, start.seq) != .lt and
        streamOrder(ms, seq, end.ms, end.seq) != .gt;
}

fn findGroup(entry: *Entry, name: []const u8) ?*StreamGroup {
    for (entry.groups.items) |*g| {
        if (std.mem.eql(u8, g.name, name)) return g;
    }
    return null;
}

fn findConsumer(g: *StreamGroup, name: []const u8) ?*StreamConsumer {
    for (g.consumers.items) |*c| {
        if (std.mem.eql(u8, c.name, name)) return c;
    }
    return null;
}

fn streamEntryOut(ctx: *ConnCtx, e: *const StreamEntry) !void {
    try ctx.arrayHeader(2);
    try ctx.bulk(e.id);
    try ctx.arrayHeader(e.fields.len);
    for (e.fields) |f| try ctx.bulk(f);
}

fn xreadOut(ctx: *ConnCtx, s: *Shared, key: []const u8, start_arg: []const u8, count: usize, _: ?void) !void {
    const entry = s.get(key) orelse return ctx.bulk(null);
    if (entry.kind != .stream) return ctx.bulk(null);

    const start = if (std.mem.eql(u8, start_arg, "$"))
        IdPair{ .ms = entry.last_ms, .seq = entry.last_seq }
    else
        (parseStreamId(start_arg) orelse return ctx.bulk(null));

    var n: usize = 0;
    for (entry.entries.items) |e| {
        const id = parseStreamId(e.id) orelse continue;
        if (streamOrder(id.ms, id.seq, start.ms, start.seq) != .gt) continue;
        if (n >= count) break;
        n += 1;
    }
    if (n == 0) return ctx.bulk(null);

    try ctx.arrayHeader(1);
    try ctx.arrayHeader(2);
    try ctx.bulk(key);
    try ctx.arrayHeader(n);
    var emitted: usize = 0;
    for (entry.entries.items) |*e| {
        const id = parseStreamId(e.id) orelse continue;
        if (streamOrder(id.ms, id.seq, start.ms, start.seq) != .gt) continue;
        if (emitted >= n) break;
        try streamEntryOut(ctx, e);
        emitted += 1;
    }
}

fn xreadGroupOut(
    ctx: *ConnCtx,
    s: *Shared,
    key: []const u8,
    group_name: []const u8,
    consumer_name: []const u8,
    start_arg: []const u8,
    count: usize,
) !void {
    const entry = s.get(key) orelse return ctx.err("NOGROUP No such key");
    if (entry.kind != .stream) return ctx.err("WRONGTYPE");
    const g = findGroup(entry, group_name) orelse return ctx.err("NOGROUP No such group");

    if (findConsumer(g, consumer_name) == null) {
        try g.consumers.append(s.gpa, .{
            .name = try s.gpa.dupe(u8, consumer_name),
            .seen_ms = s.nowMs(),
        });
    }

    if (std.mem.eql(u8, start_arg, ">")) {
        const start = IdPair{ .ms = g.last_ms, .seq = g.last_seq };
        var n: usize = 0;
        for (entry.entries.items) |e| {
            const id = parseStreamId(e.id) orelse continue;
            if (streamOrder(id.ms, id.seq, start.ms, start.seq) != .gt) continue;
            if (n >= count) break;
            n += 1;
        }
        if (n == 0) return ctx.bulk(null);

        try ctx.arrayHeader(1);
        try ctx.arrayHeader(2);
        try ctx.bulk(key);
        try ctx.arrayHeader(n);
        var emitted: usize = 0;
        for (entry.entries.items) |*e| {
            const id = parseStreamId(e.id) orelse continue;
            if (streamOrder(id.ms, id.seq, start.ms, start.seq) != .gt) continue;
            if (emitted >= n) break;
            try g.pending.append(s.gpa, .{
                .id = try s.gpa.dupe(u8, e.id),
                .consumer = try s.gpa.dupe(u8, consumer_name),
                .delivery = 1,
                .last_ms = s.nowMs(),
            });
            g.last_ms = id.ms;
            g.last_seq = id.seq;
            try streamEntryOut(ctx, e);
            emitted += 1;
        }
        return;
    }

    // 0 / 指定 Id：读取自身挂起消息
    const start = parseStreamId(start_arg) orelse IdPair{ .ms = 0, .seq = 0 };
    var n: usize = 0;
    for (g.pending.items) |p| {
        if (!std.mem.eql(u8, p.consumer, consumer_name)) continue;
        const id = parseStreamId(p.id) orelse continue;
        if (streamOrder(id.ms, id.seq, start.ms, start.seq) != .gt) continue;
        if (n >= count) break;
        n += 1;
    }
    if (n == 0) return ctx.bulk(null);

    try ctx.arrayHeader(1);
    try ctx.arrayHeader(2);
    try ctx.bulk(key);
    try ctx.arrayHeader(n);
    var emitted: usize = 0;
    for (g.pending.items) |*p| {
        if (!std.mem.eql(u8, p.consumer, consumer_name)) continue;
        const id = parseStreamId(p.id) orelse continue;
        if (streamOrder(id.ms, id.seq, start.ms, start.seq) != .gt) continue;
        if (emitted >= n) break;
        p.delivery += 1;
        p.last_ms = s.nowMs();
        for (entry.entries.items) |*e| {
            if (std.mem.eql(u8, e.id, p.id)) {
                try streamEntryOut(ctx, e);
                break;
            }
        }
        emitted += 1;
    }
}
