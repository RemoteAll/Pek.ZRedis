//! 连接配置：连接字符串解析、选项与连接池参数。
//!
//! 与 DH.NRedis 的 `Redis.Init` / `RedisOptions` / `RedisPoolConfig`、以及 Rust
//! `pek_rredis::options` 对齐（默认值完全一致）：
//!
//! | DH.NRedis | Pek.ZRedis |
//! |-----------|------------|
//! | `new FullRedis(server, pwd, db)` | [`Options.new`] |
//! | `rds.Init("server=...;password=...;db=3")` | [`Options.fromConfig`] |
//! | `RedisOptions` / `RedisPoolConfig` | [`Options`] / [`PoolConfig`] |
//!
//! 支持的键（不区分大小写）：`Server`、`Port`、`UserName`、`Password`、`Db`、
//! `Timeout` / `responseTimeout` / `connectTimeout`、`Prefix`、`ProtocolVersion`、
//! `MaxMessageSize`、`Expire`、`PoolMin`、`PoolMax`、`PoolIdleTime`、`MaxLifetime`、
//! `WaitTimeout`、`Ssl`/`Tls`、`TlsServerName`、`TlsInsecure`，以及拓扑预留字段：
//! `Mode`、`AutoDetect`、`TopologyRefreshSeconds`、`ReadFromReplicas`、`SentinelMasterName`。
//!
//! 分隔符规则与 C# `Redis.ParseConfig` 一致：含 `;` 或 `=` 不超过两段时用 `;`，
//! 否则按旧版 `,` 分隔；无等号的段按位置参数 `[0]`、`[1]`… 处理。

const std = @import("std");

pub const Mode = enum {
    auto,
    standalone,
    cluster,
    sentinel,
    replication,

    pub fn parse(text: []const u8) !Mode {
        const s = std.mem.trim(u8, text, " \t\r\n");
        const eql = std.ascii.eqlIgnoreCase;
        if (s.len == 0 or eql(s, "auto")) return .auto;
        if (eql(s, "standalone") or eql(s, "single")) return .standalone;
        if (eql(s, "cluster")) return .cluster;
        if (eql(s, "sentinel")) return .sentinel;
        if (eql(s, "replication") or eql(s, "masterslave") or eql(s, "master-slave") or eql(s, "replica")) return .replication;
        return error.InvalidMode;
    }
};

/// TLS 协议版本约束（对齐 C# `SslProtocol` / `TlsProtocol`）。
///
/// C# 用 `SslProtocols` 按位或表达（如 `Tls12, Tls13`）；这里只保留本实现能表达的三档：
/// 不约束 / 仅 1.2 / 仅 1.3 / 两者都允许。**旧协议（Ssl2/Ssl3/Tls/Tls11）明确报错**，
/// 不静默降级——C# 解析失败会退化成 `None`（等于不启用 TLS），那等于把配错变成明文，不能跟。
pub const TlsProtocol = enum {
    none,
    tls12,
    tls13,
    both,

    pub fn parse(text: []const u8) !TlsProtocol {
        const s = std.mem.trim(u8, text, " \t\r\n");
        if (s.len == 0) return .none;
        const eql = std.ascii.eqlIgnoreCase;
        if (eql(s, "none")) return .none;

        var has12 = false;
        var has13 = false;
        var it = std.mem.tokenizeAny(u8, s, ",|;+ ");
        while (it.next()) |part| {
            if (eql(part, "tls12") or eql(part, "tls1.2")) {
                has12 = true;
            } else if (eql(part, "tls13") or eql(part, "tls1.3")) {
                has13 = true;
            } else if (eql(part, "ssl2") or eql(part, "ssl3") or eql(part, "tls") or
                eql(part, "tls10") or eql(part, "tls1.0") or eql(part, "tls11") or eql(part, "tls1.1"))
            {
                return error.UnsupportedTlsProtocol;
            } else {
                return error.InvalidTlsProtocol;
            }
        }
        if (has12 and has13) return .both;
        if (has12) return .tls12;
        if (has13) return .tls13;
        return error.InvalidTlsProtocol;
    }
};

/// 连接池参数（默认值与 C# `RedisPoolConfig` 一致）。
pub const PoolConfig = struct {
    /// 池回收时保留的最小空闲数
    min: usize = 10,
    /// 池最大容量
    max: usize = 100_000,
    /// 空闲清理时间（秒）
    idle_time: u64 = 30,
    /// 连接最大生命周期（秒）
    max_lifetime: u64 = 300,
    /// 池满时借出等待超时（秒）
    wait_timeout: u64 = 15,
};

pub const Options = struct {
    gpa: std.mem.Allocator,

    /// 实例名（日志/监控用）
    instance_name: ?[]u8 = null,
    /// 服务器地址列表（`host:port`，可多个，网络异常时自动切换）
    servers: std.ArrayList([]u8) = .empty,
    /// 用户名（Redis 6.0+ ACL）
    user_name: ?[]u8 = null,
    /// 密码
    password: ?[]u8 = null,
    /// 数据库索引
    db: i32 = 0,
    /// 读写超时（毫秒）
    timeout_ms: u64 = 3000,
    /// 出错重试次数
    retry: usize = 3,
    /// 键前缀
    prefix: ?[]u8 = null,
    /// 协议版本：0/2 = RESP2，3 = RESP3
    protocol_version: i32 = 0,
    /// 默认过期时间（秒；<=0 不过期）
    expire: i64 = 0,
    /// 最大消息大小（字节；默认 1MB，0 = 不限）
    max_message_size: usize = 1024 * 1024,
    pool: PoolConfig = .{},
    /// 拓扑模式（预留：Cluster/Sentinel/Replication 在后续里程碑实现）
    mode: Mode = .auto,
    auto_detect: bool = false,
    topology_refresh_seconds: u64 = 60,
    read_from_replicas: bool = false,
    sentinel_master_name: ?[]u8 = null,
    /// 是否启用 TLS（`rediss://` 也会置位）
    tls: bool = false,
    tls_server_name: ?[]u8 = null,
    tls_insecure: bool = false,
    /// 固定的服务端证书（对齐 C# `Certificate` / `Cert` / `CertificateFile`）：
    /// 给了就**只信这张**（作为唯一信任锚），不再加载系统根证书
    certificate_path: ?[]u8 = null,
    /// C# `CertificatePassword` / `CertPassword`：PKCS#12 密码（本实现不支持，非空即报错）
    certificate_password: ?[]u8 = null,
    /// 协议版本约束（C# `SslProtocol` / `TlsProtocol`）
    tls_protocol: TlsProtocol = .none,

    pub fn init(gpa: std.mem.Allocator) Options {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *Options) void {
        if (self.instance_name) |s| self.gpa.free(s);
        if (self.user_name) |s| self.gpa.free(s);
        if (self.password) |s| self.gpa.free(s);
        if (self.prefix) |s| self.gpa.free(s);
        if (self.sentinel_master_name) |s| self.gpa.free(s);
        if (self.tls_server_name) |s| self.gpa.free(s);
        if (self.certificate_path) |s| self.gpa.free(s);
        if (self.certificate_password) |s| self.gpa.free(s);
        for (self.servers.items) |s| self.gpa.free(s);
        self.servers.deinit(self.gpa);
        self.* = undefined;
    }

    /// 等价于 C# `new FullRedis(server, password, db)`。
    pub fn new(gpa: std.mem.Allocator, server: []const u8, password: ?[]const u8, db: i32) !Options {
        var opt = Options.init(gpa);
        errdefer opt.deinit();
        try opt.splitServersInto(server);
        if (password) |p| {
            const t = std.mem.trim(u8, p, " \t\r\n");
            if (t.len > 0) opt.password = try gpa.dupe(u8, t);
        }
        opt.db = db;
        return opt;
    }

    /// 等价于 C# `new FullRedis(); rds.Init(config)`。
    pub fn fromConfig(gpa: std.mem.Allocator, config: []const u8) !Options {
        var opt = Options.init(gpa);
        errdefer opt.deinit();
        try opt.applyConfig(config);
        return opt;
    }

    /// 应用连接字符串（可多次调用，后到的配置覆盖先前的）。
    pub fn applyConfig(self: *Options, config: []const u8) !void {
        if (std.mem.trim(u8, config, " \t\r\n").len == 0) return error.EmptyConfig;

        var arena = std.heap.ArenaAllocator.init(self.gpa);
        defer arena.deinit();
        const a = arena.allocator();

        var kv: std.ArrayList(KV) = .empty;
        try parseConfig(a, config, &kv);
        const dic = kv.items;

        if (getStr(dic, &.{"server"})) |v| {
            const t = std.mem.trim(u8, v, " \t\r\n");
            if (t.len > 0) {
                self.clearServers();
                try self.splitServersInto(t);
                for (self.servers.items) |s| {
                    if (hasScheme(s, "rediss://")) self.tls = true;
                }
            }
        }
        // 兼容位置参数形式："127.0.0.1:6379,pass,3" 的第一个参数是地址
        if (self.servers.items.len == 0) {
            if (getStr(dic, &.{"[0]"})) |v| {
                self.clearServers();
                try self.splitServersInto(v);
            }
        }

        // 独立 Port 只补到未写端口的地址
        if (getInt(dic, &.{"port"})) |port| {
            if (port > 0) {
                for (self.servers.items) |*s| {
                    if (!addressHasPort(s.*)) {
                        const merged = if (needsBracket(s.*))
                            try std.fmt.allocPrint(self.gpa, "[{s}]:{d}", .{ s.*, port })
                        else
                            try std.fmt.allocPrint(self.gpa, "{s}:{d}", .{ s.*, port });
                        self.gpa.free(s.*);
                        s.* = merged;
                    }
                }
            }
        }

        if (getStr(dic, &.{"username"})) |v| try self.replace(&self.user_name, v);
        if (getStr(dic, &.{"password"})) |v| try self.replace(&self.password, v);
        if (getInt(dic, &.{"db"})) |v| self.db = @intCast(v);
        if (getInt(dic, &.{ "timeout", "responsetimeout", "connecttimeout" })) |v| {
            if (v > 0) self.timeout_ms = @intCast(v);
        }
        if (getStr(dic, &.{"prefix"})) |v| {
            if (v.len > 0) try self.replace(&self.prefix, v);
        }
        if (getInt(dic, &.{"protocolversion"})) |v| self.protocol_version = @intCast(v);
        if (getInt(dic, &.{"maxmessagesize"})) |v| {
            if (v >= 0) self.max_message_size = @intCast(v);
        }
        if (getInt(dic, &.{"expire"})) |v| {
            if (v >= 0) self.expire = v;
        }
        if (getStr(dic, &.{"mode"})) |v| {
            self.mode = Mode.parse(v) catch return error.InvalidMode;
        }
        if (getBool(dic, &.{"autodetect"})) |v| self.auto_detect = v;
        if (getInt(dic, &.{"topologyrefreshseconds"})) |v| {
            if (v >= 0) self.topology_refresh_seconds = @intCast(v);
        }
        if (getBool(dic, &.{"readfromreplicas"})) |v| self.read_from_replicas = v;
        if (getStr(dic, &.{"sentinelmastername"})) |v| {
            if (v.len > 0) try self.replace(&self.sentinel_master_name, v);
        }
        if (getBool(dic, &.{ "ssl", "tls", "usetls" })) |v| self.tls = v;
        if (getStr(dic, &.{"tlsservername"})) |v| {
            if (v.len > 0) try self.replace(&self.tls_server_name, v);
        }
        if (getBool(dic, &.{ "tlsinsecure", "sslinsecure", "insecureskipverify" })) |v| self.tls_insecure = v;
        if (getStr(dic, &.{ "sslprotocol", "tlsprotocol" })) |v| {
            self.tls_protocol = TlsProtocol.parse(v) catch |e| switch (e) {
                error.UnsupportedTlsProtocol => return error.UnsupportedTlsProtocol,
                else => return error.InvalidTlsProtocol,
            };
            // C#：`SslProtocol != None` 即视为启用 TLS
            if (self.tls_protocol != .none) self.tls = true;
        }
        if (getStr(dic, &.{ "certificate", "cert", "certificatefile" })) |v| {
            const t = std.mem.trim(u8, v, " \t\r\n");
            if (t.len > 0) try self.replace(&self.certificate_path, t);
        }
        if (getStr(dic, &.{ "certificatepassword", "certpassword" })) |v| {
            const t = std.mem.trim(u8, v, " \t\r\n");
            if (t.len > 0) try self.replace(&self.certificate_password, t);
        }

        if (getInt(dic, &.{ "poolmin", "poolmin" })) |v| {
            if (v >= 0) self.pool.min = @intCast(v);
        }
        if (getInt(dic, &.{ "poolmax", "poolmax" })) |v| {
            if (v >= 0) self.pool.max = @intCast(v);
        }
        if (getInt(dic, &.{ "poolidletime", "poolidletime" })) |v| {
            if (v >= 0) self.pool.idle_time = @intCast(v);
        }
        if (getInt(dic, &.{"maxlifetime"})) |v| {
            if (v >= 0) self.pool.max_lifetime = @intCast(v);
        }
        if (getInt(dic, &.{"waittimeout"})) |v| {
            if (v >= 0) self.pool.wait_timeout = @intCast(v);
        }

        if (self.servers.items.len == 0) return error.MissingServer;
    }

    /// 规范化地址：去掉 `tcp://` / `rediss://`，缺端口补 6379。调用方释放。
    pub fn endpointAlloc(self: *const Options, gpa: std.mem.Allocator, index: usize) ![]u8 {
        if (index >= self.servers.items.len) return error.MissingServer;
        var s = std.mem.trim(u8, self.servers.items[index], " \t\r\n");
        if (hasScheme(s, "tcp://")) s = s["tcp://".len..];
        if (hasScheme(s, "rediss://")) s = s["rediss://".len..];
        if (addressHasPort(s)) return gpa.dupe(u8, s);
        if (needsBracket(s)) return std.fmt.allocPrint(gpa, "[{s}]:6379", .{s});
        return std.fmt.allocPrint(gpa, "{s}:6379", .{s});
    }

    pub fn serverCount(self: *const Options) usize {
        return self.servers.items.len;
    }

    fn clearServers(self: *Options) void {
        for (self.servers.items) |s| self.gpa.free(s);
        self.servers.clearRetainingCapacity();
    }

    fn splitServersInto(self: *Options, text: []const u8) !void {
        var it = std.mem.splitScalar(u8, text, ',');
        while (it.next()) |raw| {
            const s = std.mem.trim(u8, raw, " \t\r\n");
            if (s.len == 0) continue;
            try self.servers.append(self.gpa, try self.gpa.dupe(u8, s));
        }
    }

    fn replace(self: *Options, slot: *?[]u8, value: []const u8) !void {
        if (slot.*) |old| self.gpa.free(old);
        slot.* = try self.gpa.dupe(u8, std.mem.trim(u8, value, " \t\r\n"));
    }
};

const KV = struct { key: []u8, value: []const u8 };

/// 解析连接字符串（键统一小写；位置参数保留 `[n]`）。
pub fn parseConfig(arena: std.mem.Allocator, config: []const u8, out: *std.ArrayList(KV)) !void {
    var sep: u8 = ';';
    if (std.mem.indexOfScalar(u8, config, ';') == null) {
        var eq_count: usize = 0;
        for (config) |c| {
            if (c == '=') eq_count += 1;
        }
        if (eq_count > 2) sep = ',';
    }

    var positional: usize = 0;
    var it = std.mem.splitScalar(u8, config, sep);
    while (it.next()) |raw| {
        const part = std.mem.trim(u8, raw, " \t\r\n");
        if (part.len == 0) continue;
        if (std.mem.indexOfScalar(u8, part, '=')) |idx| {
            const key = std.mem.trim(u8, part[0..idx], " \t\r\n");
            const value = std.mem.trim(u8, part[idx + 1 ..], " \t\r\n");
            try out.append(arena, .{ .key = try lowerAlloc(arena, key), .value = value });
        } else {
            const key = try std.fmt.allocPrint(arena, "[{d}]", .{positional});
            positional += 1;
            try out.append(arena, .{ .key = key, .value = part });
        }
    }
}

fn lowerAlloc(arena: std.mem.Allocator, s: []const u8) ![]u8 {
    const out = try arena.alloc(u8, s.len);
    for (s, 0..) |c, i| out[i] = std.ascii.toLower(c);
    return out;
}

fn getStr(dic: []const KV, keys: []const []const u8) ?[]const u8 {
    for (keys) |k| {
        for (dic) |kv| {
            if (std.mem.eql(u8, kv.key, k)) return kv.value;
        }
    }
    return null;
}

fn getInt(dic: []const KV, keys: []const []const u8) ?i64 {
    const v = getStr(dic, keys) orelse return null;
    return std.fmt.parseInt(i64, std.mem.trim(u8, v, " \t\r\n"), 10) catch null;
}

fn getBool(dic: []const KV, keys: []const []const u8) ?bool {
    const v = getStr(dic, keys) orelse return null;
    const s = std.mem.trim(u8, v, " \t\r\n");
    const eql = std.ascii.eqlIgnoreCase;
    if (eql(s, "true") or std.mem.eql(u8, s, "1") or eql(s, "yes")) return true;
    if (eql(s, "false") or std.mem.eql(u8, s, "0") or eql(s, "no")) return false;
    return null;
}

fn hasScheme(s: []const u8, prefix: []const u8) bool {
    return s.len >= prefix.len and std.ascii.eqlIgnoreCase(s[0..prefix.len], prefix);
}

/// 地址是否已带端口：`host:6379` / `[::1]:6379` 为真；裸 `[::1]` / 裸 IPv6 为假。
fn addressHasPort(s: []const u8) bool {
    if (s.len == 0) return false;
    if (s[0] == '[') return std.mem.indexOf(u8, s, "]:") != null;
    var colons: usize = 0;
    for (s) |c| {
        if (c == ':') colons += 1;
    }
    // 裸 IPv6（多个冒号）视为「无端口」
    return colons == 1;
}

/// 裸 IPv6 需要补方括号才能拼端口。
fn needsBracket(s: []const u8) bool {
    if (s.len == 0 or s[0] == '[') return false;
    var colons: usize = 0;
    for (s) |c| {
        if (c == ':') colons += 1;
    }
    return colons > 1;
}

// ─────────────────────────────── 测试 ───────────────────────────────

const testing = std.testing;

test "解析完整连接串" {
    var opt = try Options.fromConfig(testing.allocator, "server=127.0.0.1:6379;password=123456;db=3;timeout=5000;prefix=app:;PoolMax=100");
    defer opt.deinit();

    const ep = try opt.endpointAlloc(testing.allocator, 0);
    defer testing.allocator.free(ep);
    try testing.expectEqualStrings("127.0.0.1:6379", ep);
    try testing.expectEqualStrings("123456", opt.password.?);
    try testing.expectEqual(@as(i32, 3), opt.db);
    try testing.expectEqual(@as(u64, 5000), opt.timeout_ms);
    try testing.expectEqualStrings("app:", opt.prefix.?);
    try testing.expectEqual(@as(usize, 100), opt.pool.max);
}

test "缺端口补 6379，rediss 打开 TLS" {
    var opt = try Options.fromConfig(testing.allocator, "server=rediss://redis.example.com;db=0");
    defer opt.deinit();
    const ep = try opt.endpointAlloc(testing.allocator, 0);
    defer testing.allocator.free(ep);
    try testing.expectEqualStrings("redis.example.com:6379", ep);
    try testing.expect(opt.tls);
}

test "旧版逗号分隔键值对" {
    var opt = try Options.fromConfig(testing.allocator, "server=127.0.0.1:6379,password=pass123,db=3");
    defer opt.deinit();
    try testing.expectEqual(@as(usize, 1), opt.serverCount());
    const ep = try opt.endpointAlloc(testing.allocator, 0);
    defer testing.allocator.free(ep);
    try testing.expectEqualStrings("127.0.0.1:6379", ep);
    try testing.expectEqualStrings("pass123", opt.password.?);
    try testing.expectEqual(@as(i32, 3), opt.db);
}

test "TLS 协议约束按 C# SslProtocol 口径解析" {
    try testing.expectEqual(TlsProtocol.none, try TlsProtocol.parse(""));
    try testing.expectEqual(TlsProtocol.none, try TlsProtocol.parse(" None "));
    try testing.expectEqual(TlsProtocol.tls12, try TlsProtocol.parse("Tls12"));
    try testing.expectEqual(TlsProtocol.tls13, try TlsProtocol.parse("tls13"));
    try testing.expectEqual(TlsProtocol.both, try TlsProtocol.parse("Tls12, Tls13"));
    try testing.expectEqual(TlsProtocol.both, try TlsProtocol.parse("Tls13|Tls12"));
    // 旧协议明确拒绝（不能像 C# 那样解析失败退化成 None = 不启用 TLS）
    try testing.expectError(error.UnsupportedTlsProtocol, TlsProtocol.parse("Ssl3"));
    try testing.expectError(error.UnsupportedTlsProtocol, TlsProtocol.parse("Tls11"));
    try testing.expectError(error.InvalidTlsProtocol, TlsProtocol.parse("Tls99"));

    var opt = try Options.fromConfig(testing.allocator, "server=h:1;TlsProtocol=Tls12");
    defer opt.deinit();
    try testing.expect(opt.tls); // C#：SslProtocol != None 即启用 TLS
    try testing.expectEqual(TlsProtocol.tls12, opt.tls_protocol);

    try testing.expectError(error.UnsupportedTlsProtocol, Options.fromConfig(testing.allocator, "server=h:1;SslProtocol=Ssl3"));
    try testing.expectError(error.InvalidTlsProtocol, Options.fromConfig(testing.allocator, "server=h:1;SslProtocol=Nope"));
}

test "Certificate / CertificatePassword 解析（C# 别名口径）" {
    var opt = try Options.fromConfig(testing.allocator, "server=h:1;Tls=1;Certificate=cert.pem;CertificatePassword=pw");
    defer opt.deinit();
    try testing.expectEqualStrings("cert.pem", opt.certificate_path.?);
    try testing.expectEqualStrings("pw", opt.certificate_password.?);

    // `Cert` / `CertificateFile` 与 `CertPassword` 是 C# 的别名
    var alias = try Options.fromConfig(testing.allocator, "server=h:1;Tls=1;CertificateFile=/tmp/a.pem;CertPassword=x");
    defer alias.deinit();
    try testing.expectEqualStrings("/tmp/a.pem", alias.certificate_path.?);
    try testing.expectEqualStrings("x", alias.certificate_password.?);

    var plain = try Options.fromConfig(testing.allocator, "server=h:1;Tls=1");
    defer plain.deinit();
    try testing.expect(plain.certificate_path == null);
    try testing.expectEqual(TlsProtocol.none, plain.tls_protocol);
}

test "Port 键只补到无端口地址" {
    var opt = try Options.fromConfig(testing.allocator, "server=a;Port=6380;db=1");
    defer opt.deinit();
    try testing.expectEqualStrings("a:6380", opt.servers.items[0]);
}

test "缺 Server 报 MissingServer" {
    try testing.expectError(error.MissingServer, Options.fromConfig(testing.allocator, "password=x;db=1"));
}

test "TLS / 模式 / 池参数" {
    var opt = try Options.fromConfig(testing.allocator, "server=h:1;Tls=true;TlsInsecure=1;Mode=sentinel;SentinelMasterName=mymaster;PoolMin=2;PoolIdleTime=9;WaitTimeout=4;MaxLifetime=77;ReadFromReplicas=yes;AutoDetect=true");
    defer opt.deinit();
    try testing.expect(opt.tls);
    try testing.expect(opt.tls_insecure);
    try testing.expectEqual(Mode.sentinel, opt.mode);
    try testing.expectEqualStrings("mymaster", opt.sentinel_master_name.?);
    try testing.expectEqual(@as(usize, 2), opt.pool.min);
    try testing.expectEqual(@as(u64, 9), opt.pool.idle_time);
    try testing.expectEqual(@as(u64, 4), opt.pool.wait_timeout);
    try testing.expectEqual(@as(u64, 77), opt.pool.max_lifetime);
    try testing.expect(opt.read_from_replicas);
    try testing.expect(opt.auto_detect);
}

test "多地址与 IPv6" {
    var opt = try Options.fromConfig(testing.allocator, "server=a:1,b,[::1]:2");
    defer opt.deinit();
    try testing.expectEqual(@as(usize, 3), opt.serverCount());
    const ep2 = try opt.endpointAlloc(testing.allocator, 2);
    defer testing.allocator.free(ep2);
    try testing.expectEqualStrings("[::1]:2", ep2);
}
