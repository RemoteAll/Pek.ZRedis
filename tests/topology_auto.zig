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

test "拓扑自动：单键命令按槽位路由到不同服务端" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const s1 = try MockServer.start(a, io, 16801);
    defer s1.stop();
    const s2 = try MockServer.start(a, io, 16861);
    defer s2.stop();
    const cfg1 = try s1.configAlloc(a);
    defer a.free(cfg1);
    const cfg2 = try s2.configAlloc(a);
    defer a.free(cfg2);

    const ep1 = try std.fmt.allocPrint(a, "127.0.0.1:{d}", .{s1.port});
    defer a.free(ep1);
    const ep2 = try std.fmt.allocPrint(a, "127.0.0.1:{d}", .{s2.port});
    defer a.free(ep2);

    const cfg = try std.fmt.allocPrint(a, "{s};Mode=cluster;TopologyRefreshSeconds=0", .{cfg1});
    defer a.free(cfg);
    var fr = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr.deinit();

    // 槽位 0-8191 → s1，8192-16383 → s2
    const nodes_text = try std.fmt.allocPrint(
        a,
        "a {s}@0 master - 0 0 1 connected 0-8191\nb {s}@0 master - 0 0 1 connected 8192-16383\n",
        .{ ep1, ep2 },
    );
    defer a.free(nodes_text);
    fr.redis.setTopology(.{
        .cluster = try pek_zredis.cluster.RedisClusterTopology.init(a, io, nodes_text, false),
    });

    // bar=5061（s1），foo=12182（s2）
    var v = try fr.redis.execute(&.{ "SET", "bar", "v-bar" });
    v.deinit(a);
    v = try fr.redis.execute(&.{ "SET", "foo", "v-foo" });
    v.deinit(a);

    var got_bar = try fr.redis.execute(&.{ "GET", "bar" });
    defer got_bar.deinit(a);
    try testing.expectEqualStrings("v-bar", got_bar.asString().?);
    var got_foo = try fr.redis.execute(&.{ "GET", "foo" });
    defer got_foo.deinit(a);
    try testing.expectEqualStrings("v-foo", got_foo.asString().?);

    // 结构体命令也走同一套选路（RedisHash → HINCRBY）
    var h = try fr.getHash("h1");
    defer h.deinit();
    _ = try h.incrBy("pv", 1);

    // 直连两侧核对落点
    var c1 = try pek_zredis.Redis.fromConfig(a, io, cfg1);
    defer c1.deinit();
    var c2 = try pek_zredis.Redis.fromConfig(a, io, cfg2);
    defer c2.deinit();

    var x = try c1.execute(&.{ "GET", "bar" });
    defer x.deinit(a);
    try testing.expectEqualStrings("v-bar", x.asString().?);
    var y = try c1.execute(&.{ "GET", "foo" });
    defer y.deinit(a);
    try testing.expect(y.isNull());

    var z = try c2.execute(&.{ "GET", "foo" });
    defer z.deinit(a);
    try testing.expectEqualStrings("v-foo", z.asString().?);
    var w = try c2.execute(&.{ "GET", "bar" });
    defer w.deinit(a);
    try testing.expect(w.isNull());

    // h1 只落在一个节点上
    const h1_slot = pek_zredis.cluster.hashSlot("h1");
    const on_first = h1_slot <= 8191;
    var hlen_a = try c1.execute(&.{ "HLEN", "h1" });
    defer hlen_a.deinit(a);
    var hlen_b = try c2.execute(&.{ "HLEN", "h1" });
    defer hlen_b.deinit(a);
    const expect_a: i64 = if (on_first) 1 else 0;
    const expect_b: i64 = if (on_first) 0 else 1;
    try testing.expectEqual(expect_a, hlen_a.asInt().?);
    try testing.expectEqual(expect_b, hlen_b.asInt().?);

    // 多键命令：MGET 按节点分组后仍按原 key 顺序汇总
    const multi = [_][]const u8{ "bar", "foo" };
    const values = try fr.redis.getAllRaw(a, &multi);
    defer {
        for (values) |slot| if (slot) |s| a.free(s);
        a.free(values);
    }
    try testing.expectEqualStrings("v-bar", values[0].?);
    try testing.expectEqualStrings("v-foo", values[1].?);

    // 多键命令：DEL 跨节点求和
    try testing.expectEqual(@as(i64, 2), try fr.redis.removeMany(&multi));
    var gone = try c1.execute(&.{ "GET", "bar" });
    defer gone.deinit(a);
    try testing.expect(gone.isNull());
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
