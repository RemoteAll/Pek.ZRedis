//! 拓扑自动发现（对齐 DH.NRedis `Clusters/*` 与 Rust `redis.rs` 的 `detect_topology_mode_from_info`
//! / `parse_replication_discovery`）。
//!
//! 只做 `INFO` 文本 → 模式 / 节点列表的**纯解析**；网络拉取由调用方完成
//! （`Redis.info()` 或 `INFO Sentinel`）。

const std = @import("std");
const options = @import("options.zig");
const redis_mod = @import("redis.zig");
const cluster = @import("cluster.zig");

/// 复制发现结果。
pub const Discovery = struct {
    gpa: std.mem.Allocator,
    role: ?[]u8 = null,
    master_endpoint: ?[]u8 = null,
    slaves: [][]u8 = &.{},
    /// Sentinel 报出的主节点地址（`master0:...address=ip:port`）
    masters: [][]u8 = &.{},

    pub fn deinit(self: *Discovery) void {
        if (self.role) |s| self.gpa.free(s);
        if (self.master_endpoint) |s| self.gpa.free(s);
        for (self.slaves) |s| self.gpa.free(s);
        if (self.slaves.len > 0) self.gpa.free(self.slaves);
        for (self.masters) |s| self.gpa.free(s);
        if (self.masters.len > 0) self.gpa.free(self.masters);
    }
};

/// 依据 `INFO` 判断拓扑模式（与 Rust 同规则）。
pub fn detectMode(info: *const redis_mod.InfoMap, configured_servers: usize) ?options.Mode {
    if (info.get("redis_mode")) |mode| {
        if (std.ascii.eqlIgnoreCase(mode, "cluster")) return .cluster;
    }
    if (info.get("sentinel_masters") != null) return .sentinel;
    if (info.get("role") != null and configured_servers > 1) {
        const standalone = if (info.get("redis_mode")) |mode|
            std.ascii.eqlIgnoreCase(mode, "standalone")
        else
            true;
        if (standalone) return .replication;
    }
    return null;
}

/// 解析 `ip=1.2.3.4,port=6379,...` 或 `address=1.2.3.4:6379,...` 形式的端点。
fn endpointFromFields(gpa: std.mem.Allocator, text: []const u8) !?[]u8 {
    var ip: ?[]const u8 = null;
    var port: ?[]const u8 = null;
    var address: ?[]const u8 = null;

    var it = std.mem.tokenizeScalar(u8, text, ',');
    while (it.next()) |field| {
        const eq = std.mem.indexOfScalar(u8, field, '=') orelse continue;
        const key = std.mem.trim(u8, field[0..eq], " \t");
        const value = std.mem.trim(u8, field[eq + 1 ..], " \t");
        if (std.mem.eql(u8, key, "ip")) {
            ip = value;
        } else if (std.mem.eql(u8, key, "port")) {
            port = value;
        } else if (std.mem.eql(u8, key, "address")) {
            address = value;
        }
    }
    if (address) |a| {
        if (a.len == 0) return null;
        return try gpa.dupe(u8, a);
    }
    const host = ip orelse return null;
    const p = port orelse return null;
    if (host.len == 0 or p.len == 0) return null;
    return try std.fmt.allocPrint(gpa, "{s}:{s}", .{ host, p });
}

/// 解析主从 / 哨兵发现信息。
pub fn parseDiscovery(gpa: std.mem.Allocator, info: *const redis_mod.InfoMap) !Discovery {
    var out = Discovery{ .gpa = gpa };
    errdefer out.deinit();

    if (info.get("role")) |role| {
        if (role.len > 0) out.role = try gpa.dupe(u8, role);
    }
    if (info.get("master_host")) |host| {
        if (info.get("master_port")) |port| {
            if (host.len > 0 and port.len > 0) {
                out.master_endpoint = try std.fmt.allocPrint(gpa, "{s}:{s}", .{ host, port });
            }
        }
    }

    if (info.get("connected_slaves")) |count_text| {
        const count = std.fmt.parseInt(usize, count_text, 10) catch 0;
        var slaves: std.ArrayList([]u8) = .empty;
        errdefer {
            for (slaves.items) |s| gpa.free(s);
            slaves.deinit(gpa);
        }
        var i: usize = 0;
        while (i < count) : (i += 1) {
            var key_buf: [32]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "slave{d}", .{i}) catch continue;
            const text = info.get(key) orelse continue;
            if (try endpointFromFields(gpa, text)) |endpoint| try slaves.append(gpa, endpoint);
        }
        out.slaves = try slaves.toOwnedSlice(gpa);
        slaves = .empty;
    }

    if (info.get("sentinel_masters")) |count_text| {
        const count = std.fmt.parseInt(usize, count_text, 10) catch 0;
        var masters: std.ArrayList([]u8) = .empty;
        errdefer {
            for (masters.items) |s| gpa.free(s);
            masters.deinit(gpa);
        }
        var i: usize = 0;
        while (i < count) : (i += 1) {
            var key_buf: [32]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "master{d}", .{i}) catch continue;
            const text = info.get(key) orelse continue;
            if (try endpointFromFields(gpa, text)) |endpoint| try masters.append(gpa, endpoint);
        }
        out.masters = try masters.toOwnedSlice(gpa);
        masters = .empty;
    }

    return out;
}

/// 由发现结果构造复制拓扑节点：主节点在前，从节点在后。
///
/// `seed_endpoint` 是多地址配置里的首个端点，仅在 INFO 没给出主节点地址时兜底。
pub fn buildReplicationNodes(
    gpa: std.mem.Allocator,
    discovery: *const Discovery,
    seed_endpoint: []const u8,
) ![]cluster.ClusterNode {
    var nodes: std.ArrayList(cluster.ClusterNode) = .empty;
    errdefer {
        for (nodes.items) |*n| n.deinit();
        nodes.deinit(gpa);
    }

    const master_endpoint = discovery.master_endpoint orelse seed_endpoint;
    try nodes.append(gpa, try cluster.ClusterNode.new(gpa, master_endpoint));
    for (discovery.slaves) |slave| {
        var node = try cluster.ClusterNode.new(gpa, slave);
        node.is_replica = true;
        try nodes.append(gpa, node);
    }
    return nodes.toOwnedSlice(gpa);
}

// ───────────────────────── 测试 ─────────────────────────

const testing = std.testing;

fn infoOf(gpa: std.mem.Allocator, text: []const u8) !redis_mod.InfoMap {
    return redis_mod.parseInfo(gpa, text);
}

test "模式识别：cluster / sentinel / replication / 未知" {
    const a = testing.allocator;

    var cluster_info = try infoOf(a, "# Server\r\nredis_mode:cluster\r\nrole:master\r\n");
    defer cluster_info.deinit();
    try testing.expectEqual(options.Mode.cluster, detectMode(&cluster_info, 1).?);

    var sentinel_info = try infoOf(a, "sentinel_masters:1\r\nmaster0:name=m,status=ok,address=127.0.0.1:6379\r\n");
    defer sentinel_info.deinit();
    try testing.expectEqual(options.Mode.sentinel, detectMode(&sentinel_info, 1).?);

    var repl_info = try infoOf(a, "redis_mode:standalone\r\nrole:slave\r\nmaster_host:h\r\nmaster_port:1\r\n");
    defer repl_info.deinit();
    try testing.expectEqual(options.Mode.replication, detectMode(&repl_info, 2).?);
    try testing.expect(detectMode(&repl_info, 1) == null);

    var plain = try infoOf(a, "role:master\r\n");
    defer plain.deinit();
    try testing.expect(detectMode(&plain, 1) == null);
}

test "复制发现：主节点 / 从节点 / 哨兵主节点" {
    const a = testing.allocator;
    var info = try infoOf(
        a,
        "role:master\r\nconnected_slaves:2\r\nslave0:ip=10.0.0.2,port=6380,state=online\r\n" ++
            "slave1:ip=10.0.0.3,port=6381,state=online\r\nsentinel_masters:1\r\n" ++
            "master0:name=redis-master,status=ok,address=127.0.0.1:6379,slaves=2,sentinels=3\r\n",
    );
    defer info.deinit();

    var discovery = try parseDiscovery(a, &info);
    defer discovery.deinit();

    try testing.expectEqualStrings("master", discovery.role.?);
    try testing.expectEqual(@as(usize, 2), discovery.slaves.len);
    try testing.expectEqualStrings("10.0.0.2:6380", discovery.slaves[0]);
    try testing.expectEqual(@as(usize, 1), discovery.masters.len);
    try testing.expectEqualStrings("127.0.0.1:6379", discovery.masters[0]);

    const nodes = try buildReplicationNodes(a, &discovery, "seed:1");
    var threaded = std.Io.Threaded.init(a, .{});
    defer threaded.deinit();
    var topo = cluster.RedisReplicationTopology.init(a, threaded.io(), .replication, nodes, true);
    defer topo.deinit();
    try testing.expectEqual(@as(usize, 3), topo.nodes.len);
    try testing.expect(topo.nodes[0].is_replica == false);
    try testing.expect(topo.nodes[1].is_replica);
}
