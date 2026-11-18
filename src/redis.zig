//! 基础命令层（对齐 DH.NRedis `Redis` / `FullRedis` 的 public 方法面）。
//!
//! 这一层只做「命令 → RESP 值 → Zig 值」的翻译，不处理键前缀；
//! 前缀、SCAN、分布式锁等便利面在 `full.zig`。
//!
//! 线程安全：内部持有连接池，可多线程共享（与 C# 单例用法一致）。

const std = @import("std");
const zzig = @import("zzig");
const resp = @import("resp.zig");
const encoder = @import("encoder.zig");
const options = @import("options.zig");
const client = @import("client.zig");
const pool = @import("pool.zig");
const cluster = @import("cluster.zig");
const info_mod = @import("info.zig");
const discovery = @import("topology_discovery.zig");

pub const Error = client.Error;

pub const ServerType = enum {
    redis,
    garnet,
    pika,
    dragonfly_db,
    huawei_cloud,
    alibaba_cloud,
    tencent_cloud,
};

pub const InfoMap = info_mod.InfoMap;
pub const parseInfo = info_mod.parseInfo;

pub const Redis = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    opts: options.Options,
    conn_pool: pool.Pool,
    /// 拓扑模式下按端点惰性创建的连接池（仅 `executeOnEndpoint` / `executeOnKey` 使用）
    ep_pools: std.StringHashMapUnmanaged(*pool.Pool) = .empty,
    topo_mutex: std.Io.Mutex = .init,
    topology: ?cluster.Topology = null,
    topology_refreshed_ms: i64 = 0,
    commands: std.atomic.Value(u64) = .init(0),
    errors: std.atomic.Value(u64) = .init(0),
    diag: client.Diag = .{},
    diag_mutex: std.Io.Mutex = .init,

    /// 创建实例（接管 `opts` 的所有权）。连接惰性建立（第一次命令时）。
    pub fn init(gpa: std.mem.Allocator, io: std.Io, opts: options.Options) !Redis {
        if (opts.serverCount() == 0) return error.MissingServer;
        const cfg = try client.ConnConfig.initFromOptions(gpa, &opts, 0);
        return .{
            .gpa = gpa,
            .io = io,
            .opts = opts,
            .conn_pool = pool.Pool.init(gpa, io, cfg, opts.pool),
        };
    }

    /// 等价于 `new FullRedis(server, password, db)`。
    pub fn open(gpa: std.mem.Allocator, io: std.Io, server: []const u8, password: ?[]const u8, db: i32) !Redis {
        const opts = try options.Options.new(gpa, server, password, db);
        return init(gpa, io, opts);
    }

    /// 等价于 `new FullRedis(); Init(config)`。
    pub fn fromConfig(gpa: std.mem.Allocator, io: std.Io, config: []const u8) !Redis {
        const opts = try options.Options.fromConfig(gpa, config);
        return init(gpa, io, opts);
    }

    pub fn deinit(self: *Redis) void {
        if (self.topology) |*t| t.deinit();
        self.topology = null;
        var it = self.ep_pools.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.gpa.destroy(entry.value_ptr.*);
            self.gpa.free(entry.key_ptr.*);
        }
        self.ep_pools.deinit(self.gpa);
        self.conn_pool.deinit();
        self.opts.deinit();
    }

    /// 最近一次失败的完整原因（多线程下为「最近覆盖」，不保证与调用线程一一对应）。
    pub fn lastError(self: *const Redis) []const u8 {
        return self.diag.text();
    }

    pub fn stats(self: *Redis) struct { commands: u64, errors: u64, pool: pool.Pool.Stats } {
        return .{
            .commands = self.commands.load(.monotonic),
            .errors = self.errors.load(.monotonic),
            .pool = self.conn_pool.stats(),
        };
    }

    fn setDiag(self: *Redis, comptime fmt: []const u8, args: anytype) void {
        self.diag_mutex.lockUncancelable(self.io);
        defer self.diag_mutex.unlock(self.io);
        self.diag.set(fmt, args);
    }

    /// 供上层（FullRedis / 结构体）写诊断。
    pub fn setErrorDiag(self: *Redis, comptime fmt: []const u8, args: anytype) void {
        self.setDiag(fmt, args);
    }

    // ───────────────────────── 执行内核 ─────────────────────────

    /// 执行一条命令（非服务端错误按 `Retry` 重试）。
    pub fn execute(self: *Redis, args: []const []const u8) Error!resp.Value {
        return self.executeInner(args, null);
    }

    /// 执行阻塞命令（BRPOP / BLPOP / BLMOVE…）。
    pub fn executeBlocking(self: *Redis, args: []const []const u8, block_seconds: i64) Error!resp.Value {
        return self.executeInner(args, block_seconds);
    }

    /// 执行并丢弃应答。
    pub fn executeIgnore(self: *Redis, args: []const []const u8) Error!void {
        var v = try self.executeInner(args, null);
        v.deinit(self.gpa);
    }

    fn executeInner(self: *Redis, args: []const []const u8, block: ?i64) Error!resp.Value {
        return self.executeInnerPool(&self.conn_pool, null, args, block);
    }

    fn executeInnerPool(
        self: *Redis,
        p: *pool.Pool,
        endpoint: ?[]const u8,
        args: []const []const u8,
        block: ?i64,
    ) Error!resp.Value {
        const attempts = @max(self.opts.retry, 1);
        var last: Error = error.IoError;

        var attempt: usize = 0;
        while (attempt < attempts) : (attempt += 1) {
            var lease = p.acquire() catch |e| {
                _ = self.errors.fetchAdd(1, .monotonic);
                if (endpoint) |ep| {
                    self.setDiag("借出连接失败（{s}）：{s}（{s}）", .{ ep, p.lastError(), @errorName(e) });
                } else {
                    self.setDiag("借出连接失败（{s}）：{s}", .{ p.lastError(), @errorName(e) });
                }
                return e;
            };
            const c = lease.get();

            const result = if (block) |seconds|
                c.commandBlocking(args, seconds)
            else
                c.command(args);

            if (result) |v| {
                _ = self.commands.fetchAdd(1, .monotonic);
                lease.release();
                return v;
            } else |e| {
                last = e;
                const detail = c.lastError();
                self.setDiag("{s}", .{detail});
                lease.release(); // broken 连接由池销毁
                if (e == error.Server) {
                    // 服务端错误不重试（与 C#/Rust 一致）
                    _ = self.errors.fetchAdd(1, .monotonic);
                    return e;
                }
            }

            if (attempt + 1 < attempts) {
                const shift: u6 = @intCast(@min(attempt, 5));
                zzig.compat.sleep((@as(u64, 50) << shift) * std.time.ns_per_ms);
            }
        }

        _ = self.errors.fetchAdd(1, .monotonic);
        return last;
    }

    // ───────────────────────── 拓扑路由 ─────────────────────────

    /// 取（或惰性创建）指定端点的连接池；参数沿用当前选项，仅替换端点。
    fn poolFor(self: *Redis, endpoint: []const u8) Error!*pool.Pool {
        if (self.ep_pools.get(endpoint)) |p| return p;

        var cfg = client.ConnConfig.initFromOptions(self.gpa, &self.opts, 0) catch return error.OutOfMemory;
        self.gpa.free(cfg.endpoint);
        cfg.endpoint = self.gpa.dupe(u8, endpoint) catch return error.OutOfMemory;

        const p = self.gpa.create(pool.Pool) catch return error.OutOfMemory;
        p.* = pool.Pool.init(self.gpa, self.io, cfg, self.opts.pool);
        const key = self.gpa.dupe(u8, endpoint) catch {
            p.deinit();
            self.gpa.destroy(p);
            return error.OutOfMemory;
        };
        self.ep_pools.put(self.gpa, key, p) catch {
            self.gpa.free(key);
            p.deinit();
            self.gpa.destroy(p);
            return error.OutOfMemory;
        };
        return p;
    }

    /// 在指定端点上执行（集群/主从路由与对拍用）。
    pub fn executeOnEndpoint(self: *Redis, endpoint: []const u8, args: []const []const u8) Error!resp.Value {
        const p = try self.poolFor(endpoint);
        return self.executeInnerPool(p, endpoint, args, null);
    }

    /// 按拓扑选节点执行；自动处理 `MOVED`（记忆覆盖）与 `ASK`（先发 `ASKING`），
    /// 连接类失败后按拓扑重选节点。
    pub fn executeOnKey(
        self: *Redis,
        topology: *cluster.Topology,
        key: []const u8,
        write: bool,
        args: []const []const u8,
    ) Error!resp.Value {
        var route = topology.selectRoute(key, write) orelse {
            self.setDiag("拓扑为空，无法路由 key={s}", .{key});
            return error.Config;
        };
        var asking = false;
        var owned_endpoint: ?[]u8 = null;
        defer if (owned_endpoint) |e| self.gpa.free(e);

        var hops: usize = 0;
        while (hops < 8) : (hops += 1) {
            if (asking) {
                var ask = self.executeOnEndpoint(route.endpoint, &.{"ASKING"}) catch |e| {
                    self.setDiag("ASKING 失败（{s}）：{s}", .{ route.endpoint, @errorName(e) });
                    return e;
                };
                ask.deinit(self.gpa);
            }

            if (self.executeOnEndpoint(route.endpoint, args)) |value| {
                topology.resetNode(route.endpoint);
                return value;
            } else |err| {
                if (err == error.Server) {
                    if (try cluster.parseRedirect(self.gpa, self.lastError())) |r0| {
                        var redirect = r0;
                        defer redirect.deinit(self.gpa);
                        topology.rememberRedirect(redirect.slot, redirect.endpoint, redirect.kind == .moved);
                        if (owned_endpoint) |e| self.gpa.free(e);
                        owned_endpoint = try self.gpa.dupe(u8, redirect.endpoint);
                        route = .{
                            .endpoint = owned_endpoint.?,
                            .slot = redirect.slot,
                            .node_index = null,
                        };
                        asking = redirect.kind == .ask;
                        continue;
                    }
                    return err;
                }

                const failed = route.endpoint;
                route = topology.reselectRoute(key, write, failed) orelse {
                    self.setDiag("节点 {s} 失败且无可用替代（key={s}）：{s}", .{ failed, key, @errorName(err) });
                    return err;
                };
                asking = false;
            }
        }

        self.setDiag("重定向次数超限（>{d}）：key={s}", .{ hops, key });
        return error.Server;
    }

    /// 接管一份外部构建的拓扑（调用方不再释放）。
    pub fn setTopology(self: *Redis, topo: cluster.Topology) void {
        self.topo_mutex.lockUncancelable(self.io);
        defer self.topo_mutex.unlock(self.io);
        if (self.topology) |*old| old.deinit();
        self.topology = topo;
        self.topology_refreshed_ms = nowMs();
    }

    pub fn clearTopology(self: *Redis) void {
        self.topo_mutex.lockUncancelable(self.io);
        defer self.topo_mutex.unlock(self.io);
        if (self.topology) |*old| old.deinit();
        self.topology = null;
        self.topology_refreshed_ms = 0;
    }

    pub fn topologyNodeCount(self: *Redis) usize {
        self.topo_mutex.lockUncancelable(self.io);
        defer self.topo_mutex.unlock(self.io);
        if (self.topology) |*t| return t.nodeCount();
        return 0;
    }

    pub fn topologyMode(self: *Redis) ?options.Mode {
        self.topo_mutex.lockUncancelable(self.io);
        defer self.topo_mutex.unlock(self.io);
        if (self.topology) |*t| return t.mode();
        return null;
    }

    /// 拉取并替换当前拓扑：
    /// - `cluster`：`CLUSTER NODES`；
    /// - `sentinel`：`INFO Sentinel` 找主库，再按主库 `INFO` 建复制拓扑；
    /// - `replication`：`INFO` 解析 `master_host` / `slaveN`；
    /// - `standalone`：单节点静态拓扑；
    /// - `auto`：先 `INFO` 识别模式再走上面分支。
    pub fn refreshTopology(self: *Redis) Error!void {
        self.topo_mutex.lockUncancelable(self.io);
        defer self.topo_mutex.unlock(self.io);

        if (self.topology) |*old| old.deinit();
        self.topology = null;

        const seed = self.opts.endpointAlloc(self.gpa, 0) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.Config,
        };
        defer self.gpa.free(seed);

        var mode = self.opts.mode;
        if (mode == .auto) {
            var inf = try self.info();
            defer inf.deinit();
            mode = discovery.detectMode(&inf, self.opts.serverCount()) orelse .standalone;
        }

        switch (mode) {
            .cluster => {
                var v = try self.executeOnEndpoint(seed, &.{ "CLUSTER", "NODES" });
                defer v.deinit(self.gpa);
                const text = v.asString() orelse {
                    self.setDiag("CLUSTER NODES 未返回文本（{s}）", .{seed});
                    return error.Server;
                };
                var topo = try cluster.RedisClusterTopology.init(self.gpa, self.io, text, self.opts.read_from_replicas);
                if (topo.isEmpty()) {
                    topo.deinit();
                    self.setDiag("CLUSTER NODES 解析后为空（{s}）", .{seed});
                    return error.Server;
                }
                self.topology = .{ .cluster = topo };
            },
            .sentinel => try self.loadSentinelTopology(seed),
            .replication => try self.loadReplicationTopology(seed, .replication),
            else => {
                const nodes = try self.gpa.alloc(cluster.ClusterNode, 1);
                nodes[0] = try cluster.ClusterNode.new(self.gpa, seed);
                self.topology = .{
                    .static = cluster.StaticTopology.init(self.gpa, self.io, .standalone, nodes, self.opts.read_from_replicas),
                };
            },
        }

        self.topology_refreshed_ms = nowMs();
    }

    fn infoFromEndpoint(self: *Redis, endpoint: []const u8) Error!InfoMap {
        var v = try self.executeOnEndpoint(endpoint, &.{"INFO"});
        defer v.deinit(self.gpa);
        return parseInfo(self.gpa, v.asString() orelse "");
    }

    fn loadReplicationTopology(self: *Redis, endpoint: []const u8, mode: options.Mode) Error!void {
        var inf = try self.infoFromEndpoint(endpoint);
        defer inf.deinit();
        var disc = try discovery.parseDiscovery(self.gpa, &inf);
        defer disc.deinit();

        const nodes = discovery.buildReplicationNodes(self.gpa, &disc, endpoint) catch return error.OutOfMemory;
        self.topology = .{
            .replication = cluster.RedisReplicationTopology.init(self.gpa, self.io, mode, nodes, self.opts.read_from_replicas),
        };
    }

    fn loadSentinelTopology(self: *Redis, seed: []const u8) Error!void {
        // 先问哨兵主库地址；查不到就按种子节点当主库处理
        var v = self.executeOnEndpoint(seed, &.{ "INFO", "Sentinel" }) catch null;
        if (v) |*value| {
            defer value.deinit(self.gpa);
            if (value.asString()) |text| {
                var inf = try parseInfo(self.gpa, text);
                defer inf.deinit();
                var disc = try discovery.parseDiscovery(self.gpa, &inf);
                defer disc.deinit();
                if (disc.masters.len > 0) {
                    const master = disc.masters[0];
                    return self.loadReplicationTopology(master, .sentinel);
                }
            }
        }
        try self.loadReplicationTopology(seed, .sentinel);
    }

    fn ensureTopology(self: *Redis) Error!void {
        self.topo_mutex.lockUncancelable(self.io);
        const have = self.topology != null;
        const refresh_seconds = self.opts.topology_refresh_seconds;
        const stale = have and refresh_seconds > 0 and
            (nowMs() - self.topology_refreshed_ms) >= @as(i64, @intCast(refresh_seconds)) * 1000;
        self.topo_mutex.unlock(self.io);

        if (have and !stale) return;

        self.refreshTopology() catch |err| {
            if (have) {
                // 刷新失败时保留旧拓扑继续服务（与 C#/Rust 的「尽力而为」一致）
                self.setDiag("拓扑刷新失败，继续使用旧拓扑（{s}）", .{@errorName(err)});
                return;
            }
            return err;
        };
    }

    /// 自动模式执行：确保拓扑新鲜 → 按 key 选节点 → `MOVED`/`ASK`/失败重选。
    pub fn executeAuto(self: *Redis, key: []const u8, write: bool, args: []const []const u8) Error!resp.Value {
        try self.ensureTopology();

        self.topo_mutex.lockUncancelable(self.io);
        defer self.topo_mutex.unlock(self.io);
        if (self.topology) |*topo| return self.executeOnKey(topo, key, write, args);

        self.setDiag("拓扑为空，无法执行 key={s}", .{key});
        return error.Config;
    }

    /// 管道执行（一次性写入、顺序读回）。
    fn executeMany(self: *Redis, cmds: []const []const []const u8) Error![]resp.Value {
        const attempts = @max(self.opts.retry, 1);
        var last: Error = error.IoError;
        var attempt: usize = 0;
        while (attempt < attempts) : (attempt += 1) {
            var lease = self.conn_pool.acquire() catch |e| {
                _ = self.errors.fetchAdd(1, .monotonic);
                self.setDiag("借出连接失败（{s}）：{s}", .{ self.conn_pool.lastError(), @errorName(e) });
                return e;
            };
            const c = lease.get();
            const result = c.commandMany(cmds);
            if (result) |values| {
                _ = self.commands.fetchAdd(values.len, .monotonic);
                lease.release();
                return values;
            } else |e| {
                last = e;
                self.setDiag("{s}", .{c.lastError()});
                lease.release();
                if (e == error.Server) {
                    _ = self.errors.fetchAdd(1, .monotonic);
                    return e;
                }
            }
            if (attempt + 1 < attempts) {
                const shift: u6 = @intCast(@min(attempt, 5));
                zzig.compat.sleep((@as(u64, 50) << shift) * std.time.ns_per_ms);
            }
        }
        _ = self.errors.fetchAdd(1, .monotonic);
        return last;
    }

    /// 取返回值并释放（使用调用方分配器）。
    fn takeStringAlloc(self: *Redis, gpa: std.mem.Allocator, v: *resp.Value) ![]u8 {
        const out = v.asBytesAlloc(gpa) catch return error.IoError;
        return out orelse {
            self.setDiag("服务端返回 {s}，无法转为字符串", .{@tagName(std.meta.activeTag(v.*))});
            return error.Protocol;
        };
    }

    /// 取数组视图（**借用**：元素所有权仍属于 `v`，由 `v.deinit` 统一释放）。
    pub fn expectArray(self: *Redis, v: *resp.Value) ![]resp.Value {
        return switch (v.*) {
            .array => |items| items,
            else => {
                self.setDiag("服务端返回 {s}，期望数组", .{@tagName(std.meta.activeTag(v.*))});
                return error.Protocol;
            },
        };
    }

    fn freeValues(self: *Redis, items: []resp.Value) void {
        for (items) |*item| item.deinit(self.gpa);
        self.gpa.free(items);
    }

    // ───────────────────────── 键空间 ─────────────────────────

    pub fn dbsize(self: *Redis) Error!i64 {
        var v = try self.execute(&.{"DBSIZE"});
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    /// `KEYS pattern`（返回的每个字符串由调用方释放）。
    pub fn keysRaw(self: *Redis, pattern: []const u8) Error![][]u8 {
        var v = try self.execute(&.{ "KEYS", pattern });
        defer v.deinit(self.gpa);
        const items = try self.expectArray(&v);

        var out: std.ArrayList([]u8) = .empty;
        errdefer {
            for (out.items) |s| self.gpa.free(s);
            out.deinit(self.gpa);
        }
        for (items) |item| {
            if (item.asString()) |s| try out.append(self.gpa, try self.gpa.dupe(u8, s));
        }
        return out.toOwnedSlice(self.gpa);
    }

    pub fn keys(self: *Redis) Error![][]u8 {
        return self.keysRaw("*");
    }

    pub fn freeStrings(self: *Redis, list: [][]u8) void {
        for (list) |s| self.gpa.free(s);
        self.gpa.free(list);
    }

    pub fn containsKey(self: *Redis, key: []const u8) Error!bool {
        var v = try self.execute(&.{ "EXISTS", key });
        defer v.deinit(self.gpa);
        return (v.asInt() orelse 0) > 0;
    }

    pub fn remove(self: *Redis, key: []const u8) Error!i64 {
        var v = try self.execute(&.{ "DEL", key });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn removeMany(self: *Redis, key_list: []const []const u8) Error!i64 {
        if (key_list.len == 0) return 0;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        try args.append(self.gpa, "DEL");
        try args.appendSlice(self.gpa, key_list);
        var v = try self.execute(args.items);
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn setExpire(self: *Redis, key: []const u8, seconds: i64) Error!bool {
        var buf: [24]u8 = undefined;
        const s = std.fmt.bufPrint(&buf, "{d}", .{seconds}) catch return error.Config;
        var v = try self.execute(&.{ "EXPIRE", key, s });
        defer v.deinit(self.gpa);
        return (v.asInt() orelse 0) == 1;
    }

    pub fn setExpireMs(self: *Redis, key: []const u8, milliseconds: i64) Error!bool {
        var buf: [24]u8 = undefined;
        const s = std.fmt.bufPrint(&buf, "{d}", .{milliseconds}) catch return error.Config;
        var v = try self.execute(&.{ "PEXPIRE", key, s });
        defer v.deinit(self.gpa);
        return (v.asInt() orelse 0) == 1;
    }

    pub fn getExpire(self: *Redis, key: []const u8) Error!i64 {
        var v = try self.execute(&.{ "TTL", key });
        defer v.deinit(self.gpa);
        return v.asInt() orelse -2;
    }

    pub fn getExpireMs(self: *Redis, key: []const u8) Error!i64 {
        var v = try self.execute(&.{ "PTTL", key });
        defer v.deinit(self.gpa);
        return v.asInt() orelse -2;
    }

    pub fn persist(self: *Redis, key: []const u8) Error!bool {
        var v = try self.execute(&.{ "PERSIST", key });
        defer v.deinit(self.gpa);
        return (v.asInt() orelse 0) == 1;
    }

    /// 键类型（自有副本；键不存在返回 `null`）。调用方释放。
    pub fn typeOf(self: *Redis, key: []const u8) Error!?[]u8 {
        var v = try self.execute(&.{ "TYPE", key });
        defer v.deinit(self.gpa);
        const s = v.asString() orelse return null;
        if (std.ascii.eqlIgnoreCase(s, "none")) return null;
        return try self.gpa.dupe(u8, s);
    }

    pub fn rename(self: *Redis, key: []const u8, new_key: []const u8, overwrite: bool) Error!bool {
        const cmd: []const u8 = if (overwrite) "RENAME" else "RENAMENX";
        var v = try self.execute(&.{ cmd, key, new_key });
        defer v.deinit(self.gpa);
        if (overwrite) {
            const s = v.asString() orelse return false;
            return std.ascii.eqlIgnoreCase(s, "OK");
        }
        return (v.asInt() orelse 0) == 1;
    }

    pub fn unlink(self: *Redis, key_list: []const []const u8) Error!i64 {
        if (key_list.len == 0) return 0;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        try args.append(self.gpa, "UNLINK");
        try args.appendSlice(self.gpa, key_list);
        var v = try self.execute(args.items);
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn touch(self: *Redis, key_list: []const []const u8) Error!i64 {
        if (key_list.len == 0) return 0;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        try args.append(self.gpa, "TOUCH");
        try args.appendSlice(self.gpa, key_list);
        var v = try self.execute(args.items);
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn randomKey(self: *Redis) Error!?[]u8 {
        var v = try self.execute(&.{"RANDOMKEY"});
        defer v.deinit(self.gpa);
        if (v.isNull()) return null;
        const s = v.asString() orelse return null;
        return try self.gpa.dupe(u8, s);
    }

    pub fn memoryUsage(self: *Redis, key: []const u8, samples: i32) Error!?i64 {
        var buf: [16]u8 = undefined;
        const s = std.fmt.bufPrint(&buf, "{d}", .{samples}) catch return error.Config;
        var v = try self.execute(&.{ "MEMORY", "USAGE", key, "SAMPLES", s });
        defer v.deinit(self.gpa);
        if (v.isNull()) return null;
        return v.asInt();
    }

    pub fn objectEncoding(self: *Redis, key: []const u8) Error!?[]u8 {
        var v = try self.execute(&.{ "OBJECT", "ENCODING", key });
        defer v.deinit(self.gpa);
        if (v.isNull()) return null;
        const s = v.asString() orelse return null;
        return try self.gpa.dupe(u8, s);
    }

    pub const ScanResult = struct {
        cursor: u64,
        keys: [][]u8,
    };

    pub fn scan(self: *Redis, cursor: u64, pattern: []const u8, count: usize) Error!ScanResult {
        const n = if (count == 0) 100 else count;
        var cur_buf: [32]u8 = undefined;
        var cnt_buf: [32]u8 = undefined;
        const cur = std.fmt.bufPrint(&cur_buf, "{d}", .{cursor}) catch return error.Config;
        const cnt = std.fmt.bufPrint(&cnt_buf, "{d}", .{n}) catch return error.Config;

        var v = try self.execute(&.{ "SCAN", cur, "MATCH", pattern, "COUNT", cnt });
        defer v.deinit(self.gpa);
        const items = try self.expectArray(&v);
        if (items.len != 2) {
            self.setDiag("SCAN 返回结构非法（元素数 {d}，期望 2）", .{items.len});
            return error.Protocol;
        }

        const next_cursor: u64 = @intCast(items[0].asInt() orelse 0);
        var keys_out: std.ArrayList([]u8) = .empty;
        errdefer {
            for (keys_out.items) |s| self.gpa.free(s);
            keys_out.deinit(self.gpa);
        }
        if (items[1].asArray()) |arr| {
            for (arr) |item| {
                if (item.asString()) |s| try keys_out.append(self.gpa, try self.gpa.dupe(u8, s));
            }
        }
        return .{ .cursor = next_cursor, .keys = try keys_out.toOwnedSlice(self.gpa) };
    }

    // ───────────────────────── 字符串 ─────────────────────────

    /// 设置值（`SETEX` / `SET`）。`expire_seconds < 0` 用 `Expire` 默认值。
    pub fn set(self: *Redis, key: []const u8, value: anytype, expire_seconds: i64) Error!bool {
        var expire = expire_seconds;
        if (expire < 0) expire = self.opts.expire;

        const maybe = encoder.encodeValueAlloc(self.gpa, value) catch return error.Type;
        defer if (maybe) |p| self.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else ""; // 与 Rust `unwrap_or_default` 一致

        var v = if (expire > 0) blk: {
            var buf: [24]u8 = undefined;
            const s = std.fmt.bufPrint(&buf, "{d}", .{expire}) catch return error.Config;
            break :blk try self.execute(&.{ "SETEX", key, s, payload });
        } else try self.execute(&.{ "SET", key, payload });
        defer v.deinit(self.gpa);
        const s = v.asString() orelse return false;
        return std.ascii.eqlIgnoreCase(s, "OK");
    }

    /// 原始字节值；键不存在返回 `null`。调用方释放返回的切片。
    pub fn getRaw(self: *Redis, gpa: std.mem.Allocator, key: []const u8) Error!?[]u8 {
        var v = try self.execute(&.{ "GET", key });
        defer v.deinit(self.gpa);
        if (v.isNull()) return null;
        const bytes = v.asBytesAlloc(gpa) catch return error.IoError;
        return bytes orelse {
            self.setDiag("GET {s} 返回 {s}，无法转为字节", .{ key, @tagName(std.meta.activeTag(v)) });
            return error.Protocol;
        };
    }

    /// 解码为指定类型（解码失败返回 `null`，与 C# 编码器容错一致）。
    pub fn get(self: *Redis, comptime T: type, gpa: std.mem.Allocator, key: []const u8) Error!?T {
        const raw = (try self.getRaw(gpa, key)) orelse return null;
        if (T == []u8) return raw;
        if (T == []const u8) return raw;
        defer gpa.free(raw);
        return encoder.decodeValueAlloc(T, gpa, raw) catch |e| {
            self.setDiag("GET {s} 解码为 {s} 失败：{s}（原文 {s}）", .{ key, @typeName(T), @errorName(e), truncate(raw, 120) });
            return null;
        };
    }

    pub fn getString(self: *Redis, gpa: std.mem.Allocator, key: []const u8) Error!?[]u8 {
        return self.getRaw(gpa, key);
    }

    /// JSON 解码（`Parsed(T).deinit()` 由调用方负责）。
    pub fn getJson(self: *Redis, comptime T: type, gpa: std.mem.Allocator, key: []const u8) Error!?std.json.Parsed(T) {
        const raw = (try self.getRaw(gpa, key)) orelse return null;
        defer gpa.free(raw);
        return encoder.parseJsonAlloc(T, gpa, raw) catch |e| {
            self.setDiag("GET {s} 解析 JSON 为 {s} 失败：{s}（原文 {s}）", .{ key, @typeName(T), @errorName(e), truncate(raw, 120) });
            return null;
        };
    }

    /// 仅当键不存在时写入（`SET NX`）。
    pub fn add(self: *Redis, key: []const u8, value: anytype, expire_seconds: i64) Error!bool {
        var expire = expire_seconds;
        if (expire < 0) expire = self.opts.expire;

        const maybe = encoder.encodeValueAlloc(self.gpa, value) catch return error.Type;
        defer if (maybe) |p| self.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else "";

        var v = if (expire > 0) blk: {
            var buf: [24]u8 = undefined;
            const s = std.fmt.bufPrint(&buf, "{d}", .{expire}) catch return error.Config;
            break :blk try self.execute(&.{ "SET", key, payload, "EX", s, "NX" });
        } else try self.execute(&.{ "SET", key, payload, "NX" });
        defer v.deinit(self.gpa);
        if (v.isNull()) return false;
        const s = v.asString() orelse return false;
        return std.ascii.eqlIgnoreCase(s, "OK");
    }

    /// 仅当键已存在时覆盖（`SET XX`）。
    pub fn replace(self: *Redis, key: []const u8, value: anytype, expire_seconds: i64) Error!bool {
        var expire = expire_seconds;
        if (expire < 0) expire = self.opts.expire;

        const maybe = encoder.encodeValueAlloc(self.gpa, value) catch return error.Type;
        defer if (maybe) |p| self.gpa.free(p);
        const payload: []const u8 = if (maybe) |p| p else "";

        var v = if (expire > 0) blk: {
            var buf: [24]u8 = undefined;
            const s = std.fmt.bufPrint(&buf, "{d}", .{expire}) catch return error.Config;
            break :blk try self.execute(&.{ "SET", key, payload, "EX", s, "XX" });
        } else try self.execute(&.{ "SET", key, payload, "XX" });
        defer v.deinit(self.gpa);
        if (v.isNull()) return false;
        const s = v.asString() orelse return false;
        return std.ascii.eqlIgnoreCase(s, "OK");
    }

    pub fn append(self: *Redis, key: []const u8, value: []const u8) Error!i64 {
        var v = try self.execute(&.{ "APPEND", key, value });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn strlen(self: *Redis, key: []const u8) Error!i64 {
        var v = try self.execute(&.{ "STRLEN", key });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn getRange(self: *Redis, gpa: std.mem.Allocator, key: []const u8, start: i64, end: i64) Error![]u8 {
        var sb: [24]u8 = undefined;
        var eb: [24]u8 = undefined;
        const s = std.fmt.bufPrint(&sb, "{d}", .{start}) catch return error.Config;
        const e = std.fmt.bufPrint(&eb, "{d}", .{end}) catch return error.Config;
        var v = try self.execute(&.{ "GETRANGE", key, s, e });
        defer v.deinit(self.gpa);
        return self.takeStringAlloc(gpa, &v);
    }

    pub fn setRange(self: *Redis, key: []const u8, offset: i64, value: []const u8) Error!i64 {
        var ob: [24]u8 = undefined;
        const o = std.fmt.bufPrint(&ob, "{d}", .{offset}) catch return error.Config;
        var v = try self.execute(&.{ "SETRANGE", key, o, value });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn increment(self: *Redis, key: []const u8, delta: i64) Error!i64 {
        var buf: [24]u8 = undefined;
        const d = std.fmt.bufPrint(&buf, "{d}", .{delta}) catch return error.Config;
        var v = try self.execute(&.{ "INCRBY", key, d });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn incrementFloat(self: *Redis, key: []const u8, delta: f64) Error!f64 {
        const d = resp.formatF64Alloc(self.gpa, delta) catch return error.IoError;
        defer self.gpa.free(d);
        var v = try self.execute(&.{ "INCRBYFLOAT", key, d });
        defer v.deinit(self.gpa);
        return v.asFloat() orelse 0;
    }

    pub fn decrement(self: *Redis, key: []const u8, delta: i64) Error!i64 {
        return self.increment(key, -delta);
    }

    pub fn setBit(self: *Redis, key: []const u8, offset: u64, value: u8) Error!i64 {
        var ob: [24]u8 = undefined;
        const o = std.fmt.bufPrint(&ob, "{d}", .{offset}) catch return error.Config;
        const vb: []const u8 = if (value != 0) "1" else "0";
        var v = try self.execute(&.{ "SETBIT", key, o, vb });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn getBit(self: *Redis, key: []const u8, offset: u64) Error!i64 {
        var ob: [24]u8 = undefined;
        const o = std.fmt.bufPrint(&ob, "{d}", .{offset}) catch return error.Config;
        var v = try self.execute(&.{ "GETBIT", key, o });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn bitCount(self: *Redis, key: []const u8, start: i64, end: i64) Error!i64 {
        var sb: [24]u8 = undefined;
        var eb: [24]u8 = undefined;
        const s = std.fmt.bufPrint(&sb, "{d}", .{start}) catch return error.Config;
        const e = std.fmt.bufPrint(&eb, "{d}", .{end}) catch return error.Config;
        var v = try self.execute(&.{ "BITCOUNT", key, s, e });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn bitPos(self: *Redis, key: []const u8, bit: i32, start: i64, end: i64) Error!i64 {
        var bb: [8]u8 = undefined;
        var sb: [24]u8 = undefined;
        var eb: [24]u8 = undefined;
        const b = std.fmt.bufPrint(&bb, "{d}", .{bit}) catch return error.Config;
        const s = std.fmt.bufPrint(&sb, "{d}", .{start}) catch return error.Config;
        const e = std.fmt.bufPrint(&eb, "{d}", .{end}) catch return error.Config;
        var v = try self.execute(&.{ "BITPOS", key, b, s, e });
        defer v.deinit(self.gpa);
        return v.asInt() orelse -1;
    }

    /// `BITOP <operation> <dest> <keys...>`。
    pub fn bitOp(self: *Redis, operation: []const u8, dest_key: []const u8, key_list: []const []const u8) Error!i64 {
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        try args.append(self.gpa, "BITOP");
        try args.append(self.gpa, operation);
        try args.append(self.gpa, dest_key);
        try args.appendSlice(self.gpa, key_list);
        var v = try self.execute(args.items);
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    /// 批量读：返回与 `keys` 等长的值（无值处为 `null`）。
    pub fn getAllRaw(self: *Redis, gpa: std.mem.Allocator, key_list: []const []const u8) Error![]?[]u8 {
        if (key_list.len == 0) return gpa.alloc(?[]u8, 0) catch error.IoError;

        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        try args.append(self.gpa, "MGET");
        try args.appendSlice(self.gpa, key_list);

        var v = try self.execute(args.items);
        defer v.deinit(self.gpa);
        const items = try self.expectArray(&v);
        if (items.len != key_list.len) {
            self.setDiag("MGET 返回数量 {d} 与请求键数量 {d} 不一致", .{ items.len, key_list.len });
            return error.Protocol;
        }

        const out = gpa.alloc(?[]u8, key_list.len) catch return error.IoError;
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

    /// 批量写（`<=2` 项逐个 SET，其余 MSET + EXPIRE 管道，与 C#/Rust 一致）。
    pub fn setAll(self: *Redis, pairs: []const Pair, expire_seconds: i64) Error!void {
        if (pairs.len == 0) return;
        var expire = expire_seconds;
        if (expire < 0) expire = self.opts.expire;

        if (pairs.len <= 2) {
            for (pairs) |p| _ = try self.set(p.key, p.value, expire);
            return;
        }

        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        try args.append(self.gpa, "MSET");
        for (pairs) |p| {
            try args.append(self.gpa, p.key);
            try args.append(self.gpa, p.value);
        }
        try self.executeIgnore(args.items);

        if (expire > 0) {
            var pl = self.pipeline();
            defer pl.deinit();
            var buf: [24]u8 = undefined;
            const s = std.fmt.bufPrint(&buf, "{d}", .{expire}) catch return error.Config;
            for (pairs) |p| try pl.cmd(&.{ "EXPIRE", p.key, s });
            try pl.executeIgnore();
        }
    }

    pub const Pair = struct { key: []const u8, value: []const u8 };

    // ───────────────────────── 服务器 ─────────────────────────

    pub fn ping(self: *Redis) Error!bool {
        var v = try self.execute(&.{"PING"});
        defer v.deinit(self.gpa);
        const s = v.asString() orelse return false;
        return std.ascii.eqlIgnoreCase(s, "PONG");
    }

    pub fn info(self: *Redis) Error!InfoMap {
        var v = try self.execute(&.{"INFO"});
        defer v.deinit(self.gpa);
        return parseInfo(self.gpa, v.asString() orelse "");
    }

    pub fn version(self: *Redis) Error!?[]u8 {
        var inf = try self.info();
        defer inf.deinit();
        const v = inf.get("redis_version") orelse return null;
        return try self.gpa.dupe(u8, v);
    }

    pub fn versionParts(self: *Redis) Error!struct { major: u32, minor: u32, patch: u32 } {
        const ver = (try self.version()) orelse return .{ .major = 0, .minor = 0, .patch = 0 };
        defer self.gpa.free(ver);
        return parseVersion(ver);
    }

    /// 版本门禁：低于要求时报错（用于需要 6.0+ 的命令）。
    pub fn requireVersion(self: *Redis, required: []const u8, command: []const u8) Error!void {
        const have = try self.versionParts();
        const need = parseVersion(required);
        const ok = have.major > need.major or
            (have.major == need.major and have.minor > need.minor) or
            (have.major == need.major and have.minor == need.minor and have.patch >= need.patch);
        if (!ok) {
            self.setDiag("命令 {s} 需要 Redis {s}+，当前服务端为 {d}.{d}.{d}", .{ command, required, have.major, have.minor, have.patch });
            return error.Server;
        }
    }

    pub fn serverType(self: *Redis) Error!ServerType {
        var inf = try self.info();
        defer inf.deinit();
        return detectServerType(&inf);
    }

    pub fn time(self: *Redis) Error!struct { seconds: i64, microseconds: i64 } {
        var v = try self.execute(&.{"TIME"});
        defer v.deinit(self.gpa);
        const items = try self.expectArray(&v);
        if (items.len != 2) return error.Protocol;
        return .{
            .seconds = items[0].asInt() orelse 0,
            .microseconds = items[1].asInt() orelse 0,
        };
    }

    /// `FLUSHDB`。
    pub fn clear(self: *Redis) Error!void {
        try self.executeIgnore(&.{"FLUSHDB"});
    }

    /// 同一实例、指定 db 的新客户端视图（对齐 C# `CreateSub`）。
    pub fn createSub(self: *Redis, db: i32) !Redis {
        var opts = options.Options.init(self.gpa);
        errdefer opts.deinit();
        for (self.opts.servers.items) |s| try opts.servers.append(self.gpa, try self.gpa.dupe(u8, s));
        if (self.opts.password) |p| opts.password = try self.gpa.dupe(u8, p);
        if (self.opts.user_name) |u| opts.user_name = try self.gpa.dupe(u8, u);
        if (self.opts.prefix) |p| opts.prefix = try self.gpa.dupe(u8, p);
        opts.db = db;
        opts.timeout_ms = self.opts.timeout_ms;
        opts.retry = self.opts.retry;
        opts.protocol_version = self.opts.protocol_version;
        opts.max_message_size = self.opts.max_message_size;
        opts.expire = self.opts.expire;
        opts.pool = self.opts.pool;
        opts.tls = self.opts.tls;
        opts.tls_insecure = self.opts.tls_insecure;
        return init(self.gpa, self.io, opts);
    }

    // ───────────────────────── 脚本 ─────────────────────────

    pub fn scriptLoad(self: *Redis, script: []const u8) Error![]u8 {
        var v = try self.execute(&.{ "SCRIPT", "LOAD", script });
        defer v.deinit(self.gpa);
        return self.takeStringAlloc(self.gpa, &v);
    }

    pub fn scriptExists(self: *Redis, sha1: []const u8) Error!bool {
        var v = try self.execute(&.{ "SCRIPT", "EXISTS", sha1 });
        defer v.deinit(self.gpa);
        const items = try self.expectArray(&v);
        if (items.len == 0) return false;
        return (items[0].asInt() orelse 0) == 1;
    }

    pub fn scriptFlush(self: *Redis) Error!void {
        try self.executeIgnore(&.{ "SCRIPT", "FLUSH" });
    }

    pub fn evalRaw(self: *Redis, script: []const u8, key_list: []const []const u8, args: []const []const u8) Error!resp.Value {
        var parts: std.ArrayList([]const u8) = .empty;
        defer parts.deinit(self.gpa);
        try parts.append(self.gpa, "EVAL");
        try parts.append(self.gpa, script);
        var kb: [24]u8 = undefined;
        const ks = std.fmt.bufPrint(&kb, "{d}", .{key_list.len}) catch return error.Config;
        try parts.append(self.gpa, ks);
        try parts.appendSlice(self.gpa, key_list);
        try parts.appendSlice(self.gpa, args);
        return self.execute(parts.items);
    }

    /// 脚本求值并按 `T` 解码（null 返回 null）。
    pub fn eval(self: *Redis, comptime T: type, gpa: std.mem.Allocator, script: []const u8, key_list: []const []const u8, args: []const []const u8) Error!?T {
        var v = try self.evalRaw(script, key_list, args);
        defer v.deinit(self.gpa);
        if (v.isNull()) return null;
        const bytes = (v.asBytesAlloc(gpa) catch return error.IoError) orelse return null;
        if (T == []u8) return bytes;
        defer gpa.free(bytes);
        return encoder.decodeValueAlloc(T, gpa, bytes) catch null;
    }

    // ───────────────────────── 管道 ─────────────────────────

    pub fn pipeline(self: *Redis) Pipeline {
        return .{
            .redis = self,
            .arena = std.heap.ArenaAllocator.init(self.gpa),
        };
    }

    pub const Pipeline = struct {
        redis: *Redis,
        arena: std.heap.ArenaAllocator,
        cmds: std.ArrayList([]const []const u8) = .empty,

        pub fn deinit(self: *Pipeline) void {
            self.cmds.deinit(self.redis.gpa);
            self.arena.deinit();
        }

        /// 追加命令（参数会被复制，调用方的缓冲可复用）。
        pub fn cmd(self: *Pipeline, args: []const []const u8) !void {
            const a = self.arena.allocator();
            const copy = try a.alloc([]const u8, args.len);
            for (args, 0..) |item, i| copy[i] = try a.dupe(u8, item);
            try self.cmds.append(self.redis.gpa, copy);
        }

        /// 执行并返回全部应答（调用方逐个 `deinit` 后释放数组）。
        pub fn execute(self: *Pipeline) Error![]resp.Value {
            if (self.cmds.items.len == 0) return self.redis.gpa.alloc(resp.Value, 0) catch return error.IoError;
            return self.redis.executeMany(self.cmds.items);
        }

        pub fn executeIgnore(self: *Pipeline) Error!void {
            if (self.cmds.items.len == 0) return;
            const values = try self.redis.executeMany(self.cmds.items);
            for (values) |*v| v.deinit(self.redis.gpa);
            self.redis.gpa.free(values);
        }
    };
};

/// 版本号解析（容忍 `7.2.4` / `7.2` / 带后缀）。
pub const Version = struct { major: u32, minor: u32, patch: u32 };

pub fn parseVersion(text: []const u8) Version {
    var out = Version{ .major = 0, .minor = 0, .patch = 0 };
    var it = std.mem.splitScalar(u8, std.mem.trim(u8, text, " \t\r\n"), '.');
    if (it.next()) |s| out.major = parseVersionPart(s);
    if (it.next()) |s| out.minor = parseVersionPart(s);
    if (it.next()) |s| out.patch = parseVersionPart(s);
    return out;
}

fn parseVersionPart(s: []const u8) u32 {
    var end: usize = 0;
    while (end < s.len and s[end] >= '0' and s[end] <= '9') : (end += 1) {}
    return std.fmt.parseInt(u32, s[0..end], 10) catch 0;
}

pub fn detectServerType(inf: *const InfoMap) ServerType {
    const redis_version = inf.get("redis_version") orelse "";
    const server = inf.get("server") orelse "";
    const os = inf.get("os") orelse "";

    if (containsIgnoreCase(redis_version, "Garnet") or containsIgnoreCase(server, "Garnet")) return .garnet;
    if (containsIgnoreCase(redis_version, "pika") or containsIgnoreCase(server, "Pika")) return .pika;
    if (containsIgnoreCase(server, "dragonfly")) return .dragonfly_db;
    if (containsIgnoreCase(os, "Huawei")) return .huawei_cloud;
    if (containsIgnoreCase(server, "Tair") or containsIgnoreCase(server, "Alibaba")) return .alibaba_cloud;
    if (containsIgnoreCase(server, "Tencent")) return .tencent_cloud;
    return .redis;
}

fn containsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0) return true;
    if (needle.len > haystack.len) return false;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return true;
    }
    return false;
}

fn truncate(s: []const u8, max: usize) []const u8 {
    return s[0..@min(s.len, max)];
}

// ─────────────────────────────── 测试 ───────────────────────────────

const testing = std.testing;

test "版本号解析" {
    const v = parseVersion("7.2.4");
    try testing.expectEqual(@as(u32, 7), v.major);
    try testing.expectEqual(@as(u32, 2), v.minor);
    try testing.expectEqual(@as(u32, 4), v.patch);
    const v2 = parseVersion("6.0");
    try testing.expectEqual(@as(u32, 6), v2.major);
    try testing.expectEqual(@as(u32, 0), v2.patch);
    const v3 = parseVersion("7.4.1-garnet");
    try testing.expectEqual(@as(u32, 4), v3.minor);
}

test "服务器类型识别" {
    var inf = InfoMap{ .gpa = testing.allocator };
    defer inf.deinit();
    try inf.map.put(testing.allocator, try testing.allocator.dupe(u8, "redis_version"), try testing.allocator.dupe(u8, "1.0-Garnet"));
    try testing.expectEqual(ServerType.garnet, detectServerType(&inf));
}

fn nowMs() i64 {
    return @intCast(@divTrunc(zzig.compat.nanoTimestamp(), std.time.ns_per_ms));
}
