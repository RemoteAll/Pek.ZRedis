//! 拓扑路由集成测试（回环假 Redis）。

const std = @import("std");
const pek_zredis = @import("pek_zredis");
const support = @import("support/mock_redis.zig");

const testing = std.testing;
const MockServer = support.MockServer;

const IoHolder = struct {
    threaded: std.Io.Threaded,
    io: std.Io,
};

fn newIo() *IoHolder {
    const held = std.heap.page_allocator.create(IoHolder) catch unreachable;
    held.threaded = std.Io.Threaded.init(std.heap.page_allocator, .{});
    held.io = held.threaded.io();
    return held;
}

test "拓扑路由：按端点执行 + 按 key 选节点" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16701);
    defer server.stop();
    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var fr = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr.deinit();

    const endpoint = try std.fmt.allocPrint(a, "127.0.0.1:{d}", .{server.port});
    defer a.free(endpoint);

    // 直接指定端点执行
    var direct = try fr.redis.executeOnEndpoint(endpoint, &.{ "SET", "ep:k", "v1" });
    defer direct.deinit(a);
    try testing.expectEqualStrings("OK", direct.asString().?);

    var back = try fr.redis.executeOnEndpoint(endpoint, &.{ "GET", "ep:k" });
    defer back.deinit(a);
    try testing.expectEqualStrings("v1", back.asString().?);

    // 静态拓扑：同一端点，按 key 路由
    var nodes = try a.alloc(pek_zredis.cluster.ClusterNode, 1);
    nodes[0] = try pek_zredis.cluster.ClusterNode.new(a, endpoint);
    var topo = pek_zredis.cluster.Topology{
        .static = pek_zredis.cluster.StaticTopology.init(a, io, .standalone, nodes, false),
    };
    defer topo.deinit();

    const route = topo.selectRoute("route:k", true).?;
    try testing.expectEqualStrings(endpoint, route.endpoint);

    var routed = try fr.redis.executeOnKey(&topo, "route:k", true, &.{ "SET", "route:k", "v2" });
    defer routed.deinit(a);
    try testing.expectEqualStrings("OK", routed.asString().?);

    const got = (try fr.getRaw(a, "route:k")).?;
    defer a.free(got);
    try testing.expectEqualStrings("v2", got);
}
