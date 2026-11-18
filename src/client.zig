//! Redis 同步客户端：TCP 连接、认证、协议协商、命令收发。
//!
//! 对齐 DH.NRedis 的 `RedisClient` 与 Rust `pek_rredis::client`：
//! - 一个 `Client` 对应一条 TCP 连接，由连接池管理，**不跨线程共享**；
//! - 握手顺序与 C# 一致：`HELLO 3`（启用 RESP3 时）→ `AUTH` → `SELECT db`；
//! - `-ERR` 回复转为 `error.Server` 并写入 `lastError()`（与 C# 抛 `RedisException` 等价）；
//! - 阻塞命令（BRPOP 等）临时放宽读超时。

const std = @import("std");
const builtin = @import("builtin");
const resp = @import("resp.zig");
const options = @import("options.zig");

/// 库统一错误集（细节在 `lastError()` 里，满足「失败必须能定位」的日志规范）。
pub const Error = error{
    /// 配置错误（连接串、地址、TLS 参数）
    Config,
    /// 建连失败（DNS / TCP / 握手）
    Connect,
    /// 读写 socket 失败
    IoError,
    /// 读写超时（超时**不**标记连接损坏，调用方可重试）
    Timeout,
    /// 协议错误（对端关闭 / 解析失败 / 消息超限）
    Protocol,
    /// 服务端 `-ERR` 回复
    Server,
    /// 连接池已满 / 借出超时
    Pool,
    /// 连接已关闭
    Closed,
    /// 当前构建不支持（例如 TLS 尚未接入）
    Unsupported,
    /// 值编码/解码失败
    Type,
    /// 内存不足
    OutOfMemory,
};

/// 诊断缓冲：出错时写入「做什么 + 为什么 + 关键上下文」。
pub const Diag = struct {
    buf: [1024]u8 = undefined,
    len: usize = 0,

    pub fn set(self: *Diag, comptime fmt: []const u8, args: anytype) void {
        const out = std.fmt.bufPrint(&self.buf, fmt, args) catch blk: {
            break :blk self.buf[0..@min(self.buf.len, 200)];
        };
        self.len = out.len;
    }

    pub fn text(self: *const Diag) []const u8 {
        return self.buf[0..self.len];
    }

    pub fn clear(self: *Diag) void {
        self.len = 0;
    }
};

/// 建立一条连接所需的配置快照（由连接池持有，客户端只借用）。
pub const ConnConfig = struct {
    gpa: std.mem.Allocator,
    endpoint: []u8,
    user_name: ?[]u8 = null,
    password: ?[]u8 = null,
    db: i32 = 0,
    timeout_ms: u64 = 3000,
    protocol_version: i32 = 0,
    max_message_size: usize = 1024 * 1024,
    tls: bool = false,
    tls_server_name: ?[]u8 = null,
    tls_insecure: bool = false,

    pub fn initFromOptions(gpa: std.mem.Allocator, opt: *const options.Options, index: usize) !ConnConfig {
        var cfg = ConnConfig{
            .gpa = gpa,
            .endpoint = try opt.endpointAlloc(gpa, index),
            .db = opt.db,
            .timeout_ms = opt.timeout_ms,
            .protocol_version = opt.protocol_version,
            .max_message_size = opt.max_message_size,
            .tls = opt.tls,
            .tls_insecure = opt.tls_insecure,
        };
        errdefer cfg.deinit();
        if (opt.user_name) |s| cfg.user_name = try gpa.dupe(u8, s);
        if (opt.password) |s| cfg.password = try gpa.dupe(u8, s);
        if (opt.tls_server_name) |s| cfg.tls_server_name = try gpa.dupe(u8, s);
        return cfg;
    }

    pub fn deinit(self: *ConnConfig) void {
        self.gpa.free(self.endpoint);
        if (self.user_name) |s| self.gpa.free(s);
        if (self.password) |s| self.gpa.free(s);
        if (self.tls_server_name) |s| self.gpa.free(s);
        self.* = undefined;
    }

    fn timeoutMs(self: *const ConnConfig) u32 {
        return @intCast(if (self.timeout_ms > 0) self.timeout_ms else 3000);
    }
};

/// 单条 TCP 连接上的 Redis 客户端。
pub const Client = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    /// 按值快照（不存指针：`Client` 会按值返回/移动，指针会悬空）
    cfg: ConnConfig,
    stream: std.Io.net.Stream,
    reader: resp.Reader,
    diag: Diag = .{},
    protocol_version: i32 = 2,
    logged_in: bool = false,
    selected_db: i32 = -1,
    broken: bool = false,
    write_buf: std.ArrayList(u8) = .empty,

    /// 建立连接并完成握手（HELLO / AUTH / SELECT）。
    pub fn connect(gpa: std.mem.Allocator, io: std.Io, cfg: *const ConnConfig) Error!Client {
        if (cfg.tls) {
            // 明确报错而不是静默降级为明文（TLS 在后续里程碑接入 zzig 的 TLS 客户端）
            var c = Client{
                .gpa = gpa,
                .io = io,
                .cfg = cfg.*,
                .stream = undefined,
                .reader = undefined,
            };
            c.diag.set("连接 {s} 失败：当前构建尚未接入 TLS（Tls=true / rediss://），拒绝明文降级", .{cfg.endpoint});
            return error.Unsupported;
        }

        const hp = parseHostPort(cfg.endpoint) catch |e| {
            var c = Client{ .gpa = gpa, .io = io, .cfg = cfg.*, .stream = undefined, .reader = undefined };
            c.diag.set("解析地址 {s} 失败：{s}", .{ cfg.endpoint, @errorName(e) });
            return error.Config;
        };

        const addr = std.Io.net.IpAddress.resolve(io, hp.host, hp.port) catch |e| {
            var c = Client{ .gpa = gpa, .io = io, .cfg = cfg.*, .stream = undefined, .reader = undefined };
            c.diag.set("DNS 解析 {s}:{d} 失败：{s}", .{ hp.host, hp.port, @errorName(e) });
            return error.Connect;
        };
        // 注意：Zig 0.17 的 Windows 后端 `netConnectIpWindows` 对非 none 的 timeout 是
        // `@panic("TODO implement ...")`（zzig 各驱动同样不传 timeout）。因此连接阶段不设超时，
        // 读写超时由套接字 SO_RCVTIMEO / SO_SNDTIMEO 保证（见 setSocketTimeouts）；
        // 域名/IP 解析失败会立即返回错误。
        const stream = std.Io.net.IpAddress.connect(&addr, io, .{
            .mode = .stream,
            .protocol = .tcp,
            .timeout = .none,
        }) catch |e| {
            var c = Client{ .gpa = gpa, .io = io, .cfg = cfg.*, .stream = undefined, .reader = undefined };
            c.diag.set("TCP 连接 {s}:{d} 失败（超时 {d}ms）：{s}", .{ hp.host, hp.port, cfg.timeout_ms, @errorName(e) });
            return error.Connect;
        };

        var self = Client{
            .gpa = gpa,
            .io = io,
            .cfg = cfg.*,
            .stream = stream,
            .reader = undefined,
            .protocol_version = if (cfg.protocol_version >= 3) 3 else 2,
        };
        self.reader = resp.Reader.init(gpa, io, self.stream, 16 * 1024, cfg.max_message_size) catch {
            self.diag.set("初始化读缓冲失败（{s}）", .{cfg.endpoint});
            stream.close(io);
            return error.IoError;
        };
        setSocketTimeouts(&self.stream, cfg.timeoutMs());
        errdefer {
            self.reader.deinit();
            self.stream.close(io);
            self.write_buf.deinit(gpa);
        }

        self.handshake() catch |e| {
            if (self.diag.len == 0) {
                self.diag.set("握手失败（{s} db={d} 用户 {s}）：{s}", .{
                    cfg.endpoint,
                    cfg.db,
                    cfg.user_name orelse "default",
                    @errorName(e),
                });
            }
            return e;
        };
        return self;
    }

    pub fn deinit(self: *Client) void {
        self.stream.close(self.io);
        self.reader.deinit();
        self.write_buf.deinit(self.gpa);
        self.* = undefined;
    }

    pub fn lastError(self: *const Client) []const u8 {
        return self.diag.text();
    }

    pub fn endpoint(self: *const Client) []const u8 {
        return self.cfg.endpoint;
    }

    pub fn isBroken(self: *const Client) bool {
        return self.broken;
    }

    pub fn markBroken(self: *Client) void {
        self.broken = true;
    }

    // ───────────────────────── 基础读写 ─────────────────────────

    /// 执行一条命令，返回原始应答。服务端错误转为 `error.Server`。
    pub fn command(self: *Client, args: []const []const u8) Error!resp.Value {
        try self.ensureReady(args);
        try self.writeFrame(args);
        return self.readReply();
    }

    /// 执行阻塞命令（BRPOP / BRPOPLPUSH / BLMOVE 等），临时放宽读超时。
    pub fn commandBlocking(self: *Client, args: []const []const u8, block_seconds: i64) Error!resp.Value {
        const extra_s: u64 = if (block_seconds <= 0) 60 else @as(u64, @intCast(block_seconds)) + 2;
        setSocketTimeouts(&self.stream, @intCast(extra_s * 1000));
        defer setSocketTimeouts(&self.stream, self.cfg.timeoutMs());
        return self.command(args);
    }

    /// 读取一条推送/回复（不发送命令，用于订阅循环）。
    pub fn readMessage(self: *Client) Error!resp.Value {
        return self.readReply();
    }

    /// 批量执行命令（管道）：一次性写入全部请求，再顺序读取全部应答。
    pub fn commandMany(self: *Client, cmds: []const []const []const u8) Error![]resp.Value {
        self.write_buf.clearRetainingCapacity();
        for (cmds) |cmd| {
            resp.appendCommand(&self.write_buf, self.gpa, cmd) catch {
                self.diag.set("组帧失败（命令数 {d}）", .{cmds.len});
                return error.IoError;
            };
        }
        try self.checkSize(self.write_buf.items.len, "PIPELINE");
        try self.writeAll(self.write_buf.items);

        const results = self.gpa.alloc(resp.Value, cmds.len) catch return error.IoError;
        var got: usize = 0;
        errdefer {
            for (results[0..got]) |*v| v.deinit(self.gpa);
            self.gpa.free(results);
        }
        while (got < cmds.len) : (got += 1) {
            results[got] = try self.readReply();
        }
        return results;
    }

    /// 发送 `PING` 验证连接存活。
    pub fn ping(self: *Client) Error!void {
        var v = try self.command(&.{"PING"});
        defer v.deinit(self.gpa);
        if (v.asString()) |s| {
            if (std.ascii.eqlIgnoreCase(s, "PONG")) return;
        }
        self.diag.set("PING 未返回 PONG（{s}）", .{cfgName(self.cfg)});
        return error.Protocol;
    }

    /// 丢弃读缓冲（连接归还池前清理；订阅类连接应直接销毁而不是复用）。
    pub fn drain(self: *Client) void {
        self.reader.clearBuffered();
    }

    /// 主动退出（QUIT），并标记连接不可再用。
    pub fn quit(self: *Client) Error!void {
        _ = self.command(&.{"QUIT"}) catch {};
        self.logged_in = false;
        self.broken = true;
    }

    /// 调整读超时（订阅循环 / 阻塞命令用）。
    pub fn setReadTimeoutMs(self: *Client, timeout_ms: u32) void {
        setSocketTimeouts(&self.stream, timeout_ms);
    }

    /// 发送帧（不做读写配对；供订阅类命令使用）。
    pub fn writeFramePublic(self: *Client, args: []const []const u8) Error!void {
        try self.writeFrame(args);
    }

    /// 切换数据库。
    pub fn select(self: *Client, db: i32) Error!void {
        var db_buf: [16]u8 = undefined;
        const text = std.fmt.bufPrint(&db_buf, "{d}", .{db}) catch return error.Config;
        var v = try self.rawCommand(&.{ "SELECT", text });
        defer v.deinit(self.gpa);
        if (v.asString()) |s| {
            if (std.ascii.eqlIgnoreCase(s, "OK")) {
                self.selected_db = db;
                return;
            }
        }
        self.diag.set("SELECT {d} 失败（{s}）：服务端返回 {s}", .{ db, cfgName(self.cfg), describe(&v) });
        return error.Server;
    }

    // ───────────────────────── 握手与状态 ─────────────────────────

    fn handshake(self: *Client) Error!void {
        if (self.protocol_version >= 3) {
            self.hello(3) catch |e| switch (e) {
                // 与 C# CheckLogin 相同：HELLO 失败（老版本 Redis）降级 RESP2
                error.Server => self.protocol_version = 2,
                else => return e,
            };
            if (self.protocol_version >= 3) self.logged_in = true;
        }

        if (!self.logged_in) {
            if (self.cfg.password != null) try self.auth();
            self.logged_in = true;
        }

        if (self.cfg.db != 0) {
            try self.select(self.cfg.db);
        } else {
            self.selected_db = 0;
        }
    }

    fn hello(self: *Client, protover: i32) Error!void {
        var proto_buf: [8]u8 = undefined;
        const proto = std.fmt.bufPrint(&proto_buf, "{d}", .{protover}) catch return error.Config;

        if (self.cfg.password) |pwd| {
            const user = self.cfg.user_name orelse "default";
            var v = try self.rawCommand(&.{ "HELLO", proto, "AUTH", user, pwd });
            defer v.deinit(self.gpa);
            if (std.meta.activeTag(v) == .map) return;
            if (v.asString()) |s| {
                if (std.ascii.eqlIgnoreCase(s, "OK")) return;
            }
            // HELLO 返回值必须是 map；其它形态按服务端错误处理
            self.diag.set("HELLO {d} AUTH 返回非 map（{s}）：{s}", .{ protover, cfgName(self.cfg), describe(&v) });
            return error.Server;
        }

        var v = try self.rawCommand(&.{ "HELLO", proto });
        defer v.deinit(self.gpa);
        if (std.meta.activeTag(v) == .map) return;
        self.diag.set("HELLO {d} 返回非 map（{s}）：{s}", .{ protover, cfgName(self.cfg), describe(&v) });
        return error.Server;
    }

    fn auth(self: *Client) Error!void {
        const pwd = self.cfg.password orelse return;
        var v = if (self.cfg.user_name) |user|
            try self.rawCommand(&.{ "AUTH", user, pwd })
        else
            try self.rawCommand(&.{ "AUTH", pwd });
        defer v.deinit(self.gpa);
        if (v.asString()) |s| {
            if (std.ascii.eqlIgnoreCase(s, "OK")) return;
        }
        self.diag.set("AUTH 失败（{s} 用户 {s}）：服务端返回 {s}", .{ cfgName(self.cfg), self.cfg.user_name orelse "default", describe(&v) });
        return error.Server;
    }

    /// 与 C# `CheckLogin` / `CheckSelect` 对应的惰性握手。
    fn ensureReady(self: *Client, args: []const []const u8) Error!void {
        const cmd = if (args.len > 0) args[0] else "";
        const skip_login = eqAny(cmd, &.{ "AUTH", "HELLO" });

        if (!self.logged_in and !skip_login) {
            if (self.protocol_version >= 3) {
                self.hello(3) catch |e| switch (e) {
                    error.Server => {
                        self.protocol_version = 2;
                        if (self.cfg.password != null) try self.auth();
                        self.logged_in = true;
                    },
                    else => return e,
                };
                if (self.protocol_version >= 3) self.logged_in = true;
            } else {
                if (self.cfg.password != null) try self.auth();
                self.logged_in = true;
            }
        }

        if (self.selected_db != self.cfg.db and
            !eqAny(cmd, &.{ "AUTH", "SELECT", "INFO", "HELLO", "QUIT" }))
        {
            if (self.cfg.db > 0) {
                try self.select(self.cfg.db);
            } else {
                self.selected_db = self.cfg.db;
            }
        }
    }

    /// 不触发握手检查的底层命令（仅握手内部使用）。
    fn rawCommand(self: *Client, args: []const []const u8) Error!resp.Value {
        try self.writeFrame(args);
        return self.readReply();
    }

    // ───────────────────────── 帧读写 ─────────────────────────

    fn writeFrame(self: *Client, args: []const []const u8) Error!void {
        self.write_buf.clearRetainingCapacity();
        resp.appendCommand(&self.write_buf, self.gpa, args) catch {
            self.diag.set("组帧失败（{d} 个参数）", .{args.len});
            return error.IoError;
        };
        try self.checkSize(self.write_buf.items.len, if (args.len > 0) args[0] else "");
        try self.writeAll(self.write_buf.items);
    }

    fn checkSize(self: *Client, size: usize, cmd: []const u8) Error!void {
        if (self.cfg.max_message_size != 0 and size >= self.cfg.max_message_size) {
            self.diag.set(
                "命令[{s}]的数据包大小[{d}]超过最大限制[{d}]，大 key 会拖累整个 Redis 实例，可通过 MaxMessageSize 调节",
                .{ cmd, size, self.cfg.max_message_size },
            );
            return error.Protocol;
        }
    }

    fn writeAll(self: *Client, bytes: []const u8) Error!void {
        var sent: usize = 0;
        while (sent < bytes.len) {
            const r = self.io.operate(.{ .net_write = .{
                .socket_handle = self.stream.socket.handle,
                .data = &.{bytes[sent..]},
            } }) catch {
                self.broken = true;
                self.diag.set("写 socket 被取消（{s}）", .{cfgName(self.cfg)});
                return error.IoError;
            };
            const n = r.net_write catch |e| switch (e) {
                error.ConnectionTimedOut => {
                    // 写超时后连接状态不可知，直接废弃（重试会重新建连）
                    self.broken = true;
                    self.diag.set("写 socket 超时（{s}，已发 {d}/{d} 字节，{d}ms）", .{ cfgName(self.cfg), sent, bytes.len, self.cfg.timeoutMs() });
                    return error.Timeout;
                },
                else => {
                    self.broken = true;
                    self.diag.set("写 socket 失败（{s}，已发 {d}/{d} 字节）：{s}", .{ cfgName(self.cfg), sent, bytes.len, @errorName(e) });
                    return error.IoError;
                },
            };
            if (n == 0) {
                self.broken = true;
                self.diag.set("写 socket 返回 0 字节（{s}，连接可能已断开）", .{cfgName(self.cfg)});
                return error.Protocol;
            }
            sent += n;
        }
    }

    fn readReply(self: *Client) Error!resp.Value {
        const value = self.reader.readReply() catch |e| switch (e) {
            error.Timeout => {
                self.diag.set("读超时（{s}，{d}ms）", .{ cfgName(self.cfg), self.cfg.timeout_ms });
                return error.Timeout;
            },
            error.ConnectionClosed => {
                self.broken = true;
                self.diag.set("连接被对端关闭（{s}）", .{cfgName(self.cfg)});
                return error.Protocol;
            },
            error.MessageTooLarge => {
                self.broken = true;
                self.diag.set("应答超过 MaxMessageSize[{d}]（{s}）", .{ self.cfg.max_message_size, cfgName(self.cfg) });
                return error.Protocol;
            },
            error.Protocol => {
                self.broken = true;
                self.diag.set("RESP 协议错误（{s}）", .{cfgName(self.cfg)});
                return error.Protocol;
            },
            error.Io => {
                self.broken = true;
                self.diag.set("读 socket 失败（{s}）", .{cfgName(self.cfg)});
                return error.IoError;
            },
        };

        if (std.meta.activeTag(value) == .err) {
            self.diag.set("服务端错误（{s}）：{s}", .{ cfgName(self.cfg), value.err });
            var owned = value;
            owned.deinit(self.gpa);
            return error.Server;
        }
        return value;
    }
};

fn cfgName(cfg: ConnConfig) []const u8 {
    return cfg.endpoint;
}

/// 在套接字上设置读写超时（毫秒）。
///
/// 为什么不用 `io.operateTimeout`：Zig 0.17 的 Windows Threaded 后端对带超时的 I/O
/// 会返回 `error.ConcurrencyUnavailable`（与 `netConnectIpWindows` 的 TODO 同源），
/// 因此超时只能落到 OS 的 SO_RCVTIMEO / SO_SNDTIMEO 上。
fn setSocketTimeouts(stream: *const std.Io.net.Stream, timeout_ms: u32) void {
    if (timeout_ms == 0) return;
    switch (builtin.os.tag) {
        .windows => {
            const ws2_32 = std.os.windows.ws2_32;
            const win = struct {
                extern "ws2_32" fn setsockopt(
                    s: std.posix.socket_t,
                    level: i32,
                    optname: i32,
                    optval: [*]const u8,
                    optlen: i32,
                ) callconv(.winapi) c_int;
            };
            var ms: u32 = timeout_ms;
            _ = win.setsockopt(stream.socket.handle, @intCast(ws2_32.SOL.SOCKET), @intCast(ws2_32.SO.RCVTIMEO), @ptrCast(&ms), @sizeOf(u32));
            _ = win.setsockopt(stream.socket.handle, @intCast(ws2_32.SOL.SOCKET), @intCast(ws2_32.SO.SNDTIMEO), @ptrCast(&ms), @sizeOf(u32));
        },
        else => {
            var tv: std.posix.timeval = .{
                .sec = @intCast(timeout_ms / 1000),
                .usec = @intCast((timeout_ms % 1000) * 1000),
            };
            std.posix.setsockopt(stream.socket.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};
            std.posix.setsockopt(stream.socket.handle, std.posix.SOL.SOCKET, std.posix.SO.SNDTIMEO, std.mem.asBytes(&tv)) catch {};
            _ = &tv;
        },
    }
}

fn eqAny(cmd: []const u8, names: []const []const u8) bool {
    for (names) |n| {
        if (std.ascii.eqlIgnoreCase(cmd, n)) return true;
    }
    return false;
}

/// 服务端返回值的可读摘要（写入诊断，截断而非省略）。
fn describe(v: *const resp.Value) []const u8 {
    return switch (v.*) {
        .simple, .err, .bulk, .verbatim, .big_number => |s| s,
        .integer => "整数",
        .null_ => "空",
        .double => "浮点",
        .boolean => "布尔",
        .array, .set, .push => "数组",
        .map, .attribute => "映射",
    };
}

/// 解析 `host:port` / `[::1]:6379` / 裸 IPv6（默认 6379）。
pub const HostPort = struct { host: []const u8, port: u16 };

pub fn parseHostPort(endpoint: []const u8) !HostPort {
    var s = std.mem.trim(u8, endpoint, " \t\r\n");
    if (s.len == 0) return error.EmptyEndpoint;
    if (s.len > 6 and std.ascii.eqlIgnoreCase(s[0..6], "tcp://")) s = s[6..];
    if (s.len > 9 and std.ascii.eqlIgnoreCase(s[0..9], "rediss://")) s = s[9..];

    if (s[0] == '[') {
        const end = std.mem.indexOfScalar(u8, s, ']') orelse return error.InvalidEndpoint;
        const host = s[1..end];
        if (end + 1 < s.len) {
            if (s[end + 1] != ':') return error.InvalidEndpoint;
            const port = std.fmt.parseInt(u16, s[end + 2 ..], 10) catch return error.InvalidEndpoint;
            return .{ .host = host, .port = port };
        }
        return .{ .host = host, .port = 6379 };
    }

    var colons: usize = 0;
    for (s) |c| {
        if (c == ':') colons += 1;
    }
    if (colons > 1) return .{ .host = s, .port = 6379 }; // 裸 IPv6
    if (colons == 1) {
        const idx = std.mem.lastIndexOfScalar(u8, s, ':').?;
        const port = std.fmt.parseInt(u16, s[idx + 1 ..], 10) catch return error.InvalidEndpoint;
        return .{ .host = s[0..idx], .port = port };
    }
    return .{ .host = s, .port = 6379 };
}

// ─────────────────────────────── 测试 ───────────────────────────────

const testing = std.testing;

test "解析 host:port / IPv6 / 裸主机" {
    {
        const hp = try parseHostPort("127.0.0.1:6380");
        try testing.expectEqualStrings("127.0.0.1", hp.host);
        try testing.expectEqual(@as(u16, 6380), hp.port);
    }
    {
        const hp = try parseHostPort("redis.local");
        try testing.expectEqualStrings("redis.local", hp.host);
        try testing.expectEqual(@as(u16, 6379), hp.port);
    }
    {
        const hp = try parseHostPort("[::1]:6379");
        try testing.expectEqualStrings("::1", hp.host);
        try testing.expectEqual(@as(u16, 6379), hp.port);
    }
    {
        const hp = try parseHostPort("rediss://h:7000");
        try testing.expectEqualStrings("h", hp.host);
        try testing.expectEqual(@as(u16, 7000), hp.port);
    }
    {
        const hp = try parseHostPort("::1");
        try testing.expectEqualStrings("::1", hp.host);
        try testing.expectEqual(@as(u16, 6379), hp.port);
    }
}
