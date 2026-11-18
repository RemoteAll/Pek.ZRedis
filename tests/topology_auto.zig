//! 拓扑自动发现 / 自动选路集成测试（回环假 Redis）。

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

test "拓扑自动：cluster 模式刷新 + executeAuto 读写" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16711);
    defer server.stop();
    const base = try server.configAlloc(a);
    defer a.free(base);
    const cfg = try std.fmt.allocPrint(a, "{s};Mode=cluster;TopologyRefreshSeconds=0", .{base});
    defer a.free(cfg);

    var r = try pek_zredis.Redis.fromConfig(a, io, cfg);
    defer r.deinit();

    try r.refreshTopology();
    try testing.expectEqual(@as(usize, 1), r.topologyNodeCount());
    try testing.expectEqual(pek_zredis.options.Mode.cluster, r.topologyMode().?);

    var set_v = try r.executeAuto("auto:k", true, &.{ "SET", "auto:k", "v1" });
    defer set_v.deinit(a);
    try testing.expectEqualStrings("OK", set_v.asString().?);

    var get_v = try r.executeAuto("auto:k", false, &.{ "GET", "auto:k" });
    defer get_v.deinit(a);
    try testing.expectEqualStrings("v1", get_v.asString().?);

    // 已有拓扑且刷新区间为 0 时复用，不重复拉取
    var v2 = try r.executeAuto("auto:k2", true, &.{ "SET", "auto:k2", "v2" });
    v2.deinit(a);
    try testing.expectEqual(@as(usize, 1), r.topologyNodeCount());
}

test "拓扑自动：replication 模式按 INFO 发现节点" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16721);
    defer server.stop();
    const base = try server.configAlloc(a);
    defer a.free(base);
    const cfg = try std.fmt.allocPrint(a, "{s};Mode=replication;ReadFromReplicas=1", .{base});
    defer a.free(cfg);

    var r = try pek_zredis.Redis.fromConfig(a, io, cfg);
    defer r.deinit();

    try r.refreshTopology();
    try testing.expectEqual(pek_zredis.options.Mode.replication, r.topologyMode().?);
    // 假服务端上报 connected_slaves:0 → 只有主节点
    try testing.expectEqual(@as(usize, 1), r.topologyNodeCount());

    var v = try r.executeAuto("repl:k", true, &.{ "SET", "repl:k", "v1" });
    defer v.deinit(a);
    try testing.expectEqualStrings("OK", v.asString().?);
}
