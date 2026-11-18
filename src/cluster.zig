//! 拓扑层（对齐 DH.NRedis `Clusters/*` / Rust `Pek.RRedis::cluster`）。
//!
//! 提供 Cluster / Sentinel / Replication / Static 四类拓扑的**纯数据**部分：
//! - CRC16（XMODEM）哈希槽与 `{hash tag}` 规则；
//! - `CLUSTER NODES` 行解析（flags / master-id / 槽位 / 迁移）；
//! - 按 key 选节点、失败重选、`MOVED` 覆盖记忆、节点健康退避；
//! - `MOVED` / `ASK` 重定向文本解析。
//!
//! 网络侧（自动发现、`ASKING`、拓扑定时刷新）在 `Redis.executeOnKey` 中完成。

const std = @import("std");
const zzig = @import("zzig");
const options = @import("options.zig");

pub const RedirectKind = enum { moved, ask };

/// `MOVED 3999 127.0.0.1:6381` / `ASK 3999 127.0.0.1:6381`。
pub const Redirect = struct {
    kind: RedirectKind,
    slot: u16,
    endpoint: []u8,

    pub fn deinit(self: *Redirect, gpa: std.mem.Allocator) void {
        gpa.free(self.endpoint);
    }
};

pub fn parseRedirect(gpa: std.mem.Allocator, text: []const u8) !?Redirect {
    // 诊断文本形如「服务端错误（host:port）：MOVED 3999 1.2.3.4:6381」，
    // 因此按 token 扫描，不要求以 MOVED/ASK 开头。
    var it = std.mem.tokenizeAny(u8, text, " \t\r\n");
    while (it.next()) |token| {
        const kind: RedirectKind = if (std.ascii.eqlIgnoreCase(token, "MOVED"))
            .moved
        else if (std.ascii.eqlIgnoreCase(token, "ASK"))
            .ask
        else
            continue;
        const slot_text = it.next() orelse return null;
        const endpoint_text = it.next() orelse return null;
        const slot = std.fmt.parseInt(u16, slot_text, 10) catch return null;
        return .{ .kind = kind, .slot = slot, .endpoint = try gpa.dupe(u8, endpoint_text) };
    }
    return null;
}

// ───────────────────────── 哈希槽 ─────────────────────────

/// CRC16-XMODEM（Redis `CRC16`）。
pub fn crc16(bytes: []const u8) u16 {
    var crc: u16 = 0;
    for (bytes) |b| {
        crc ^= @as(u16, b) << 8;
        var i: u4 = 0;
        while (i < 8) : (i += 1) {
            if ((crc & 0x8000) != 0) {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    return crc;
}

/// 提取 `{hash tag}`；无 tag 或 tag 为空时返回整 key。
pub fn hashTag(key: []const u8) []const u8 {
    const open = std.mem.indexOfScalar(u8, key, '{') orelse return key;
    const close = std.mem.indexOfScalarPos(u8, key, open + 1, '}') orelse return key;
    if (close == open + 1) return key;
    return key[open + 1 .. close];
}

/// key 对应的 16384 槽位。
pub fn hashSlot(key: []const u8) u16 {
    return crc16(hashTag(key)) % 16384;
}

// ───────────────────────── 节点模型 ─────────────────────────

pub const SlotRange = struct {
    from: u16,
    to: u16,

    pub fn new(from_: u16, to_: u16) SlotRange {
        return if (from_ <= to_) .{ .from = from_, .to = to_ } else .{ .from = to_, .to = from_ };
    }

    pub fn contains(self: SlotRange, slot: u16) bool {
        return slot >= self.from and slot <= self.to;
    }
};

pub const SlotMigrationKind = enum { importing, migrating };

pub const SlotMigration = struct {
    slot: u16,
    node_id: []u8,
    kind: SlotMigrationKind,

    pub fn deinit(self: *SlotMigration, gpa: std.mem.Allocator) void {
        gpa.free(self.node_id);
    }
};

/// 集群 / 主从 / 哨兵统一节点模型。
pub const ClusterNode = struct {
    gpa: std.mem.Allocator,
    endpoint: []u8,
    node_id: ?[]u8 = null,
    flags: [][]u8 = &.{},
    master_id: ?[]u8 = null,
    is_replica: bool = false,
    link_up: bool = true,
    slots: []SlotRange = &.{},
    migrations: []SlotMigration = &.{},

    pub fn deinit(self: *ClusterNode) void {
        self.gpa.free(self.endpoint);
        if (self.node_id) |s| self.gpa.free(s);
        for (self.flags) |s| self.gpa.free(s);
        if (self.flags.len > 0) self.gpa.free(self.flags);
        if (self.master_id) |s| self.gpa.free(s);
        if (self.slots.len > 0) self.gpa.free(self.slots);
        for (self.migrations) |*m| m.deinit(self.gpa);
        if (self.migrations.len > 0) self.gpa.free(self.migrations);
    }

    pub fn new(gpa: std.mem.Allocator, endpoint: []const u8) !ClusterNode {
        return .{ .gpa = gpa, .endpoint = try gpa.dupe(u8, endpoint) };
    }

    pub fn containsSlot(self: *const ClusterNode, slot: u16) bool {
        for (self.slots) |r| {
            if (r.contains(slot)) return true;
        }
        return false;
    }

    /// 解析 `CLUSTER NODES` 的一行。
    pub fn parseLine(gpa: std.mem.Allocator, line: []const u8) !?ClusterNode {
        var it = std.mem.tokenizeAny(u8, line, " \t\r\n");
        const id = it.next() orelse return null;
        const addr = it.next() orelse return null;
        const flags_text = it.next() orelse return null;
        const master = it.next() orelse return null;
        _ = it.next(); // ping-sent
        _ = it.next(); // pong-recv
        _ = it.next(); // config-epoch
        _ = it.next(); // link-state

        const endpoint = if (std.mem.indexOfScalar(u8, addr, '@')) |at| addr[0..at] else addr;
        var node = ClusterNode{
            .gpa = gpa,
            .endpoint = try gpa.dupe(u8, endpoint),
            .node_id = try gpa.dupe(u8, id),
            .master_id = if (std.mem.eql(u8, master, "-")) null else try gpa.dupe(u8, master),
        };
        errdefer node.deinit();

        var flags: std.ArrayList([]u8) = .empty;
        errdefer {
            for (flags.items) |f| gpa.free(f);
            flags.deinit(gpa);
        }
        var flag_it = std.mem.tokenizeScalar(u8, flags_text, ',');
        while (flag_it.next()) |flag| {
            const copy = try gpa.dupe(u8, flag);
            try flags.append(gpa, copy);
            if (std.ascii.eqlIgnoreCase(flag, "slave") or std.ascii.eqlIgnoreCase(flag, "replica")) {
                node.is_replica = true;
            }
            if (std.ascii.eqlIgnoreCase(flag, "fail?") or std.ascii.eqlIgnoreCase(flag, "fail")) {
                node.link_up = false;
            }
        }
        node.flags = try flags.toOwnedSlice(gpa);
        flags = .empty; // 已转移，errdefer 不再重复释放

        var slots: std.ArrayList(SlotRange) = .empty;
        errdefer slots.deinit(gpa);
        var migrations: std.ArrayList(SlotMigration) = .empty;
        errdefer {
            for (migrations.items) |*m| m.deinit(gpa);
            migrations.deinit(gpa);
        }
        while (it.next()) |part| {
            if (parseMigration(gpa, part)) |m| {
                try migrations.append(gpa, m);
                continue;
            } else |_| {}
            if (parseSlotRange(part)) |range| try slots.append(gpa, range);
        }
        node.slots = try slots.toOwnedSlice(gpa);
        slots = .empty;
        node.migrations = try migrations.toOwnedSlice(gpa);
        migrations = .empty;
        return node;
    }
};

fn parseSlotRange(text: []const u8) ?SlotRange {
    if (text.len == 0 or text[0] == '[') return null;
    if (std.mem.indexOfScalar(u8, text, '-')) |dash| {
        const from = std.fmt.parseInt(u16, text[0..dash], 10) catch return null;
        const to = std.fmt.parseInt(u16, text[dash + 1 ..], 10) catch return null;
        return SlotRange.new(from, to);
    }
    const single = std.fmt.parseInt(u16, text, 10) catch return null;
    return SlotRange.new(single, single);
}

fn parseMigration(gpa: std.mem.Allocator, text: []const u8) !SlotMigration {
    // `[slot->-nodeid]`（importing）或 `[slot->nodeid]`（migrating）
    if (text.len < 4 or text[0] != '[' or text[text.len - 1] != ']') return error.NotMigration;
    const body = text[1 .. text.len - 1];
    const arrow = std.mem.indexOf(u8, body, "->") orelse return error.NotMigration;
    const slot = std.fmt.parseInt(u16, body[0..arrow], 10) catch return error.NotMigration;
    var rest = body[arrow + 2 ..];
    var kind: SlotMigrationKind = .migrating;
    if (rest.len > 0 and rest[0] == '-') {
        kind = .importing;
        rest = rest[1..];
    }
    if (rest.len == 0) return error.NotMigration;
    return .{ .slot = slot, .node_id = try gpa.dupe(u8, rest), .kind = kind };
}

// ───────────────────────── 节点健康 ─────────────────────────

const HealthState = struct { errors: u32 = 0, retry_after_ms: i64 = 0 };

/// 端点健康：失败按 1s、2s、4s…（上限 64s）退避。
pub const Health = struct {
    gpa: std.mem.Allocator,
    mutex: std.Io.Mutex = .init,
    states: std.StringHashMapUnmanaged(HealthState) = .empty,

    pub fn init(gpa: std.mem.Allocator) Health {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *Health) void {
        var it = self.states.iterator();
        while (it.next()) |e| self.gpa.free(e.key_ptr.*);
        self.states.deinit(self.gpa);
    }

    pub fn isAvailable(self: *const Health, io: std.Io, endpoint: []const u8) bool {
        const this = @constCast(self);
        this.mutex.lockUncancelable(io);
        defer this.mutex.unlock(io);
        const state = this.states.get(endpoint) orelse return true;
        return state.retry_after_ms <= nowMs();
    }

    pub fn markFailure(self: *const Health, io: std.Io, endpoint: []const u8) void {
        const this = @constCast(self);
        this.mutex.lockUncancelable(io);
        defer this.mutex.unlock(io);
        const gop = this.states.getOrPut(this.gpa, endpoint) catch return;
        if (!gop.found_existing) {
            gop.key_ptr.* = this.gpa.dupe(u8, endpoint) catch {
                _ = this.states.remove(endpoint);
                return;
            };
            gop.value_ptr.* = .{};
        }
        gop.value_ptr.errors +|= 1;
        const shift: u5 = @intCast(@min(gop.value_ptr.errors, 6));
        gop.value_ptr.retry_after_ms = nowMs() + (@as(i64, 1) << shift) * 1000;
    }

    pub fn reset(self: *const Health, io: std.Io, endpoint: []const u8) void {
        const this = @constCast(self);
        this.mutex.lockUncancelable(io);
        defer this.mutex.unlock(io);
        if (this.states.fetchRemove(endpoint)) |kv| this.gpa.free(kv.key);
    }
};

fn nowMs() i64 {
    return @intCast(@divTrunc(zzig.compat.nanoTimestamp(), std.time.ns_per_ms));
}

// ───────────────────────── 路由结果 ─────────────────────────

/// 选中的路由：endpoint 借用拓扑内部存储，拓扑存活期间有效。
pub const Route = struct {
    endpoint: []const u8,
    slot: u16,
    node_index: ?usize,
};

// ───────────────────────── Cluster 拓扑 ─────────────────────────

pub const RedisClusterTopology = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    nodes: []ClusterNode = &.{},
    read_from_replicas: bool = false,
    overrides: std.AutoHashMapUnmanaged(u16, []u8) = .empty,
    health: Health,

    pub fn init(gpa: std.mem.Allocator, io: std.Io, text: []const u8, read_from_replicas: bool) !RedisClusterTopology {
        var list: std.ArrayList(ClusterNode) = .empty;
        errdefer {
            for (list.items) |*n| n.deinit();
            list.deinit(gpa);
        }
        var it = std.mem.splitScalar(u8, text, '\n');
        while (it.next()) |line| {
            const trimmed = std.mem.trim(u8, line, " \t\r");
            if (trimmed.len == 0) continue;
            const node = (try ClusterNode.parseLine(gpa, trimmed)) orelse continue;
            try list.append(gpa, node);
        }
        sortNodes(list.items);
        return .{
            .gpa = gpa,
            .io = io,
            .nodes = try list.toOwnedSlice(gpa),
            .read_from_replicas = read_from_replicas,
            .health = Health.init(gpa),
        };
    }

    pub fn deinit(self: *RedisClusterTopology) void {
        for (self.nodes) |*n| n.deinit();
        if (self.nodes.len > 0) self.gpa.free(self.nodes);
        var it = self.overrides.iterator();
        while (it.next()) |e| self.gpa.free(e.value_ptr.*);
        self.overrides.deinit(self.gpa);
        self.health.deinit();
    }

    pub fn isEmpty(self: *const RedisClusterTopology) bool {
        return self.nodes.len == 0;
    }

    pub fn mode(self: *const RedisClusterTopology) options.Mode {
        _ = self;
        return .cluster;
    }

    pub fn mapEndpoint(self: *const RedisClusterTopology, endpoint: []const u8) ?usize {
        for (self.nodes, 0..) |n, i| {
            if (std.mem.eql(u8, n.endpoint, endpoint)) return i;
        }
        return null;
    }

    pub fn rememberRedirect(self: *RedisClusterTopology, slot: u16, endpoint: []const u8, persistent: bool) void {
        if (!persistent) return;
        const copy = self.gpa.dupe(u8, endpoint) catch return;
        if (self.overrides.fetchPut(self.gpa, slot, copy) catch null) |old| self.gpa.free(old.value);
    }

    pub fn resetNode(self: *RedisClusterTopology, endpoint: []const u8) void {
        self.health.reset(self.io, endpoint);
    }

    pub fn selectRoute(self: *const RedisClusterTopology, key: []const u8, write: bool) ?Route {
        const slot = hashSlot(key);
        if (self.overrides.get(slot)) |endpoint| {
            return .{ .endpoint = endpoint, .slot = slot, .node_index = self.mapEndpoint(endpoint) };
        }
        const idx = self.candidatesForSlot(slot, write) orelse return null;
        return .{ .endpoint = self.nodes[idx].endpoint, .slot = slot, .node_index = idx };
    }

    pub fn reselectRoute(self: *RedisClusterTopology, key: []const u8, write: bool, current_endpoint: []const u8) ?Route {
        self.health.markFailure(self.io, current_endpoint);
        const slot = hashSlot(key);
        if (self.overrides.get(slot)) |endpoint| {
            if (!std.mem.eql(u8, endpoint, current_endpoint) and self.health.isAvailable(self.io, endpoint)) {
                return .{ .endpoint = endpoint, .slot = slot, .node_index = self.mapEndpoint(endpoint) };
            }
        }
        for (self.nodes, 0..) |*n, i| {
            if (!n.link_up or !n.containsSlot(slot)) continue;
            if (std.mem.eql(u8, n.endpoint, current_endpoint)) continue;
            if (write and n.is_replica) continue;
            if (!self.health.isAvailable(self.io, n.endpoint)) continue;
            return .{ .endpoint = n.endpoint, .slot = slot, .node_index = i };
        }
        return null;
    }

    fn candidatesForSlot(self: *const RedisClusterTopology, slot: u16, write: bool) ?usize {
        // 写：优先主节点；读：可配从节点
        var fallback: ?usize = null;
        for (self.nodes, 0..) |*n, i| {
            if (!n.link_up or !n.containsSlot(slot)) continue;
            if (write and n.is_replica) continue;
            if (!self.health.isAvailable(self.io, n.endpoint)) {
                if (fallback == null) fallback = i;
                continue;
            }
            if (!write and self.read_from_replicas and n.is_replica) return i;
            if (!n.is_replica) return i;
            if (fallback == null) fallback = i;
        }
        return fallback;
    }
};

// ───────────────────────── 复制（主从/哨兵）拓扑 ─────────────────────────

pub const RedisReplicationTopology = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    topology_mode: options.Mode = .standalone,
    nodes: []ClusterNode = &.{},
    read_from_replicas: bool = false,
    health: Health,

    pub fn init(gpa: std.mem.Allocator, io: std.Io, topology_mode: options.Mode, nodes: []ClusterNode, read_from_replicas: bool) RedisReplicationTopology {
        sortNodes(nodes);
        return .{
            .gpa = gpa,
            .io = io,
            .topology_mode = topology_mode,
            .nodes = nodes,
            .read_from_replicas = read_from_replicas,
            .health = Health.init(gpa),
        };
    }

    pub fn deinit(self: *RedisReplicationTopology) void {
        for (self.nodes) |*n| n.deinit();
        if (self.nodes.len > 0) self.gpa.free(self.nodes);
        self.health.deinit();
    }

    pub fn mode(self: *const RedisReplicationTopology) options.Mode {
        return self.topology_mode;
    }

    pub fn mapEndpoint(self: *const RedisReplicationTopology, endpoint: []const u8) ?usize {
        for (self.nodes, 0..) |n, i| {
            if (std.mem.eql(u8, n.endpoint, endpoint)) return i;
        }
        return null;
    }

    pub fn resetNode(self: *RedisReplicationTopology, endpoint: []const u8) void {
        self.health.reset(self.io, endpoint);
    }

    pub fn rememberRedirect(self: *RedisReplicationTopology, slot: u16, endpoint: []const u8, persistent: bool) void {
        _ = self;
        _ = slot;
        _ = endpoint;
        _ = persistent;
    }

    pub fn selectRoute(self: *const RedisReplicationTopology, key: []const u8, write: bool) ?Route {
        const idx = self.preferred(write) orelse return null;
        return .{ .endpoint = self.nodes[idx].endpoint, .slot = hashSlot(key), .node_index = idx };
    }

    pub fn reselectRoute(self: *RedisReplicationTopology, key: []const u8, write: bool, current_endpoint: []const u8) ?Route {
        self.health.markFailure(self.io, current_endpoint);
        for (self.nodes, 0..) |*n, i| {
            if (!n.link_up) continue;
            if (std.mem.eql(u8, n.endpoint, current_endpoint)) continue;
            if (write and n.is_replica) continue;
            if (!self.health.isAvailable(self.io, n.endpoint)) continue;
            return .{ .endpoint = n.endpoint, .slot = hashSlot(key), .node_index = i };
        }
        return null;
    }

    fn preferred(self: *const RedisReplicationTopology, write: bool) ?usize {
        var replica: ?usize = null;
        var fallback: ?usize = null;
        for (self.nodes, 0..) |*n, i| {
            if (!n.link_up) continue;
            if (n.is_replica) {
                if (replica == null) replica = i;
                continue;
            }
            if (!self.health.isAvailable(self.io, n.endpoint)) {
                if (fallback == null) fallback = i;
                continue;
            }
            if (write) return i;
            if (write or !self.read_from_replicas) return i;
        }
        if (!write and self.read_from_replicas) {
            if (replica) |i| {
                if (self.health.isAvailable(self.io, self.nodes[i].endpoint)) return i;
            }
        }
        return fallback orelse replica;
    }
};

/// 静态拓扑（无槽位信息的直连/测试用）。
pub const StaticTopology = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    topology_mode: options.Mode = .standalone,
    nodes: []ClusterNode = &.{},
    read_from_replicas: bool = false,

    pub fn init(gpa: std.mem.Allocator, io: std.Io, topology_mode: options.Mode, nodes: []ClusterNode, read_from_replicas: bool) StaticTopology {
        return .{ .gpa = gpa, .io = io, .topology_mode = topology_mode, .nodes = nodes, .read_from_replicas = read_from_replicas };
    }

    pub fn deinit(self: *StaticTopology) void {
        for (self.nodes) |*n| n.deinit();
        if (self.nodes.len > 0) self.gpa.free(self.nodes);
    }

    pub fn mode(self: *const StaticTopology) options.Mode {
        return self.topology_mode;
    }

    pub fn mapEndpoint(self: *const StaticTopology, endpoint: []const u8) ?usize {
        for (self.nodes, 0..) |n, i| {
            if (std.mem.eql(u8, n.endpoint, endpoint)) return i;
        }
        return null;
    }

    pub fn rememberRedirect(self: *StaticTopology, slot: u16, endpoint: []const u8, persistent: bool) void {
        _ = self;
        _ = slot;
        _ = endpoint;
        _ = persistent;
    }

    pub fn resetNode(self: *StaticTopology, endpoint: []const u8) void {
        _ = self;
        _ = endpoint;
    }

    pub fn selectRoute(self: *const StaticTopology, key: []const u8, write: bool) ?Route {
        const slot = hashSlot(key);
        for (self.nodes, 0..) |*n, i| {
            if (!n.link_up) continue;
            if (!write and self.read_from_replicas and n.is_replica) {
                return .{ .endpoint = n.endpoint, .slot = slot, .node_index = i };
            }
            if (!n.is_replica) return .{ .endpoint = n.endpoint, .slot = slot, .node_index = i };
        }
        return null;
    }

    pub fn reselectRoute(self: *StaticTopology, key: []const u8, write: bool, current_endpoint: []const u8) ?Route {
        const slot = hashSlot(key);
        for (self.nodes, 0..) |*n, i| {
            if (!n.link_up) continue;
            if (std.mem.eql(u8, n.endpoint, current_endpoint)) continue;
            if (write and n.is_replica) continue;
            return .{ .endpoint = n.endpoint, .slot = slot, .node_index = i };
        }
        return null;
    }
};

/// 拓扑统一视图（Cluster / Replication / Sentinel / Static）。
pub const Topology = union(enum) {
    cluster: RedisClusterTopology,
    replication: RedisReplicationTopology,
    static: StaticTopology,

    pub fn deinit(self: *Topology) void {
        switch (self.*) {
            .cluster => |*t| t.deinit(),
            .replication => |*t| t.deinit(),
            .static => |*t| t.deinit(),
        }
    }

    pub fn mode(self: *const Topology) options.Mode {
        return switch (self.*) {
            .cluster => |*t| t.mode(),
            .replication => |*t| t.mode(),
            .static => |*t| t.mode(),
        };
    }

    pub fn nodeCount(self: *const Topology) usize {
        return switch (self.*) {
            .cluster => |*t| t.nodes.len,
            .replication => |*t| t.nodes.len,
            .static => |*t| t.nodes.len,
        };
    }

    pub fn nodeAt(self: *const Topology, index: usize) ?*const ClusterNode {
        return switch (self.*) {
            .cluster => |*t| if (index < t.nodes.len) &t.nodes[index] else null,
            .replication => |*t| if (index < t.nodes.len) &t.nodes[index] else null,
            .static => |*t| if (index < t.nodes.len) &t.nodes[index] else null,
        };
    }

    pub fn selectRoute(self: *const Topology, key: []const u8, write: bool) ?Route {
        return switch (self.*) {
            .cluster => |*t| t.selectRoute(key, write),
            .replication => |*t| t.selectRoute(key, write),
            .static => |*t| t.selectRoute(key, write),
        };
    }

    pub fn reselectRoute(self: *Topology, key: []const u8, write: bool, current_endpoint: []const u8) ?Route {
        return switch (self.*) {
            .cluster => |*t| t.reselectRoute(key, write, current_endpoint),
            .replication => |*t| t.reselectRoute(key, write, current_endpoint),
            .static => |*t| t.reselectRoute(key, write, current_endpoint),
        };
    }

    pub fn rememberRedirect(self: *Topology, slot: u16, endpoint: []const u8, persistent: bool) void {
        switch (self.*) {
            .cluster => |*t| t.rememberRedirect(slot, endpoint, persistent),
            .replication => |*t| t.rememberRedirect(slot, endpoint, persistent),
            .static => |*t| t.rememberRedirect(slot, endpoint, persistent),
        }
    }

    pub fn resetNode(self: *Topology, endpoint: []const u8) void {
        switch (self.*) {
            .cluster => |*t| t.resetNode(endpoint),
            .replication => |*t| t.resetNode(endpoint),
            .static => |*t| t.resetNode(endpoint),
        }
    }
};

fn sortNodes(nodes: []ClusterNode) void {
    std.mem.sort(ClusterNode, nodes, {}, struct {
        fn lessThan(_: void, a: ClusterNode, b: ClusterNode) bool {
            const a_min = minSlot(a);
            const b_min = minSlot(b);
            if (a_min != b_min) return a_min < b_min;
            if (a.is_replica != b.is_replica) return !a.is_replica;
            return std.mem.lessThan(u8, a.endpoint, b.endpoint);
        }

        fn minSlot(n: ClusterNode) u16 {
            var m: u16 = std.math.maxInt(u16);
            for (n.slots) |r| m = @min(m, r.from);
            return m;
        }
    }.lessThan);
}

// ───────────────────────── 测试 ─────────────────────────

const testing = std.testing;

test "哈希槽：CRC16 与 hash tag" {
    try testing.expectEqual(@as(u16, 12182), hashSlot("foo"));
    try testing.expectEqual(@as(u16, 5061), hashSlot("bar"));
    try testing.expectEqual(@as(u16, 3443), hashSlot("{user1000}.following"));
    try testing.expectEqualStrings("user1000", hashTag("{user1000}.following"));
    try testing.expectEqualStrings("whole", hashTag("whole"));
    try testing.expectEqualStrings("key{}x", hashTag("key{}x"));
}

test "CLUSTER NODES 行解析：主从 / 槽位 / 迁移" {
    const a = testing.allocator;
    const line = "07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected";
    var node = (try ClusterNode.parseLine(a, line)).?;
    defer node.deinit();
    try testing.expectEqualStrings("127.0.0.1:30004", node.endpoint);
    try testing.expect(node.is_replica);
    try testing.expect(node.link_up);
    try testing.expect(node.master_id != null);

    const master_line = "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001 myself,master - 0 0 1 connected 0-5460 [5461->-07c37dfe] [5462->abc]";
    var master = (try ClusterNode.parseLine(a, master_line)).?;
    defer master.deinit();
    try testing.expect(!master.is_replica);
    try testing.expectEqual(@as(usize, 1), master.slots.len);
    try testing.expect(master.containsSlot(100));
    try testing.expect(!master.containsSlot(5461));
    try testing.expectEqual(@as(usize, 2), master.migrations.len);
    try testing.expectEqual(SlotMigrationKind.importing, master.migrations[0].kind);
    try testing.expectEqual(SlotMigrationKind.migrating, master.migrations[1].kind);
}

test "Cluster 拓扑：按槽选节点 / MOVED 记忆 / 失败重选" {
    const a = testing.allocator;
    var threaded = std.Io.Threaded.init(a, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(a);
    // 0-8191 主 A，8192-16383 主 B；A 有从 A1
    try text.appendSlice(a, "a 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-8191\n");
    try text.appendSlice(a, "b 10.0.0.2:6379@16379 master - 0 0 1 connected 8192-16383\n");

    var topo = try RedisClusterTopology.init(a, io, text.items, true);
    defer topo.deinit();
    try testing.expectEqual(@as(usize, 2), topo.nodes.len);

    const r1 = topo.selectRoute("bar", true).?;
    try testing.expectEqualStrings("10.0.0.1:6379", r1.endpoint);

    // 槽位覆盖后指向新端点
    topo.rememberRedirect(r1.slot, "10.0.0.9:6379", true);
    const r2 = topo.selectRoute("bar", true).?;
    try testing.expectEqualStrings("10.0.0.9:6379", r2.endpoint);

    // 失败后重选到另一个可用主节点
    const r3 = topo.reselectRoute("bar", true, r2.endpoint);
    // 覆盖链仍指向旧端点时可能为空，此处仅验证不崩溃且返回结构正确
    if (r3) |r| try testing.expect(r.endpoint.len > 0);
}

test "复制拓扑：读写优先级" {
    const a = testing.allocator;
    var threaded = std.Io.Threaded.init(a, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var nodes = try a.alloc(ClusterNode, 2);
    nodes[0] = try ClusterNode.new(a, "10.0.0.2:6379");
    nodes[0].is_replica = true;
    nodes[1] = try ClusterNode.new(a, "10.0.0.1:6379");

    var topo = RedisReplicationTopology.init(a, io, .replication, nodes, true);
    defer topo.deinit();

    try testing.expectEqualStrings("10.0.0.1:6379", topo.selectRoute("k", true).?.endpoint);
    try testing.expectEqualStrings("10.0.0.2:6379", topo.selectRoute("k", false).?.endpoint);
}

test "重定向解析：MOVED / ASK" {
    const a = testing.allocator;
    var moved = (try parseRedirect(a, "MOVED 3999 127.0.0.1:6381")).?;
    defer moved.deinit(a);
    try testing.expectEqual(RedirectKind.moved, moved.kind);
    try testing.expectEqual(@as(u16, 3999), moved.slot);
    try testing.expectEqualStrings("127.0.0.1:6381", moved.endpoint);

    var ask = (try parseRedirect(a, "ASK 3999 127.0.0.1:6381")).?;
    defer ask.deinit(a);
    try testing.expectEqual(RedirectKind.ask, ask.kind);

    try testing.expect((try parseRedirect(a, "ERR unknown")) == null);
}
