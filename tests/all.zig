//! 集成测试：回环假 Redis（不依赖真实服务端）。

const std = @import("std");
const pek_zredis = @import("pek_zredis");
const zzig = @import("zzig");
const support = @import("support/mock_redis.zig");

const testing = std.testing;
const MockServer = support.MockServer;

const IoHolder = struct {
    threaded: std.Io.Threaded,
    io: std.Io,
};

/// `Threaded.io()` 返回的 Io 指向 Threaded 实例本身，**不能按值拷贝**；
/// 这里放到堆上，保证 io 的生命周期与测试一致（page_allocator 不参与泄漏检测）。
fn newIo() *IoHolder {
    const held = std.heap.page_allocator.create(IoHolder) catch unreachable;
    held.threaded = std.Io.Threaded.init(std.heap.page_allocator, .{});
    held.io = held.threaded.io();
    return held;
}

test "客户端：AUTH + SELECT + 基础读写 / 过期 / 类型" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16401);
    defer server.stop();

    const cfg = try server.configPasswordAlloc(a, "p@ss");
    defer a.free(cfg);

    var r = try pek_zredis.Redis.fromConfig(a, io, cfg);
    defer r.deinit();

    try testing.expect(try r.ping());
    try testing.expect(try r.set("k1", "hello", 0));

    const got = (try r.getRaw(a, "k1")).?;
    defer a.free(got);
    try testing.expectEqualStrings("hello", got);

    try testing.expect(try r.containsKey("k1"));
    const type_name = (try r.typeOf("k1")).?;
    defer a.free(type_name);
    try testing.expectEqualStrings("string", type_name);
    try testing.expect(try r.setExpire("k1", 60));
    const ttl = try r.getExpire("k1");
    try testing.expect(ttl > 0 and ttl <= 60);
    try testing.expect(try r.persist("k1"));
    try testing.expectEqual(@as(i64, -1), try r.getExpire("k1"));
    try testing.expectEqual(@as(i64, 1), try r.remove("k1"));
    try testing.expect(!(try r.containsKey("k1")));
}

test "客户端：RESP3 协商（HELLO 3）" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16411);
    defer server.stop();

    const base = try server.configAlloc(a);
    defer a.free(base);
    const cfg = try std.fmt.allocPrint(a, "{s};ProtocolVersion=3", .{base});
    defer a.free(cfg);

    var r = try pek_zredis.Redis.fromConfig(a, io, cfg);
    defer r.deinit();

    try testing.expect(try r.ping());
    try testing.expect(try r.set("p3", "v", 0));
    const got = (try r.getRaw(a, "p3")).?;
    defer a.free(got);
    try testing.expectEqualStrings("v", got);
}

test "批量 / 数值 / 追加 / 范围" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16421);
    defer server.stop();

    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var r = try pek_zredis.Redis.fromConfig(a, io, cfg);
    defer r.deinit();

    try r.setAll(&.{
        .{ .key = "a", .value = "1" },
        .{ .key = "b", .value = "2" },
        .{ .key = "c", .value = "3" },
    }, 0);

    const values = try r.getAllRaw(a, &.{ "a", "b", "c", "missing" });
    defer {
        for (values) |slot| if (slot) |v| a.free(v);
        a.free(values);
    }
    try testing.expectEqualStrings("1", values[0].?);
    try testing.expectEqualStrings("3", values[2].?);
    try testing.expect(values[3] == null);

    try testing.expectEqual(@as(i64, 0), try r.increment("n", 0)); // INCRBY 0 初始化
    try testing.expectEqual(@as(i64, 10), try r.increment("n", 10));
    try testing.expectEqual(@as(i64, 7), try r.decrement("n", 3));

    try testing.expectEqual(@as(i64, 5), try r.append("s", "hello"));
    try testing.expectEqual(@as(i64, 5), try r.strlen("s"));
    const part = try r.getRange(a, "s", 0, 2);
    defer a.free(part);
    try testing.expectEqualStrings("hel", part);
}

test "管道：一次写入多条命令并顺序读回" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16431);
    defer server.stop();

    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var r = try pek_zredis.Redis.fromConfig(a, io, cfg);
    defer r.deinit();

    var pl = r.pipeline();
    defer pl.deinit();
    try pl.cmd(&.{ "SET", "p1", "x" });
    try pl.cmd(&.{ "GET", "p1" });
    try pl.cmd(&.{ "INCR", "pc" });
    const results = try pl.execute();
    defer {
        for (results) |*v| v.deinit(a);
        a.free(results);
    }
    try testing.expectEqual(@as(usize, 3), results.len);
    try testing.expectEqualStrings("OK", results[0].asString().?);
    try testing.expectEqualStrings("x", results[1].asString().?);
    try testing.expectEqual(@as(i64, 1), results[2].asInt().?);
}

test "FullRedis：前缀 / SCAN 搜索 / 按模式删除" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16441);
    defer server.stop();

    const base = try server.configAlloc(a);
    defer a.free(base);
    const cfg = try std.fmt.allocPrint(a, "{s};prefix=App:", .{base});
    defer a.free(cfg);

    var fr = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr.deinit();

    _ = try fr.set("u:1", "alice", 0);
    _ = try fr.set("u:2", "bob", 0);
    _ = try fr.set("o:1", "order", 0);

    // 已有前缀不重复添加
    _ = try fr.set("App:u:3", "carol", 0);

    const keys = try fr.search(a, "u:*", 0);
    defer fr.redis.freeStrings(keys);
    try testing.expectEqual(@as(usize, 3), keys.len);
    for (keys) |k| try testing.expect(std.mem.startsWith(u8, k, "u:"));

    const removed = try fr.removePattern("u:*");
    try testing.expectEqual(@as(i64, 3), removed);

    const left = try fr.search(a, "*", 0);
    defer fr.redis.freeStrings(left);
    try testing.expectEqual(@as(usize, 1), left.len);
    try testing.expectEqualStrings("o:1", left[0]);
}

test "分布式锁：互斥 / 超时 / 释放后可重入" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16451);
    defer server.stop();

    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var fr = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr.deinit();

    var lock = (try fr.acquireLockEx("order:lock", 100, 5000)).?;
    try testing.expect(lock.has_lock);

    // 对方持有时抢锁失败
    const second = try fr.acquireLockEx("order:lock", 100, 5000);
    try testing.expect(second == null);

    lock.release();
    var third = try fr.acquireLockEx("order:lock", 500, 5000);
    try testing.expect(third != null);
    if (third) |*handle| handle.release();
}

test "哈希 / 列表 / 集合 / 有序集合 / 栈" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16461);
    defer server.stop();

    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var fr = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr.deinit();

    // 哈希
    var h = try fr.getHash("h");
    defer h.deinit();
    _ = try h.set("Name", "NewLife");
    _ = try h.set("Age", 18);
    _ = try h.incrBy("Age", 1);
    try testing.expectEqual(@as(i64, 2), try h.count());
    try testing.expectEqual(@as(i64, 19), (try h.get(i64, a, "Age")).?);
    const name = (try h.getRaw(a, "Name")).?;
    defer a.free(name);
    try testing.expectEqualStrings("NewLife", name);

    // 列表
    var l = try fr.getList("l");
    defer l.deinit();
    _ = try l.pushBack("b");
    _ = try l.pushFront("a");
    _ = try l.pushBack("c");
    try testing.expectEqual(@as(i64, 3), try l.len());
    const all = try l.getAll(a);
    defer fr.redis.freeStrings(all);
    try testing.expectEqualStrings("a", all[0]);
    try testing.expectEqualStrings("c", all[2]);
    const front = (try l.popFront(a)).?;
    defer a.free(front);
    try testing.expectEqualStrings("a", front);

    // 集合
    var s = try fr.getSet("s");
    defer s.deinit();
    _ = try s.addMany(&.{ "x", "y", "z" });
    try testing.expectEqual(@as(i64, 3), try s.len());
    try testing.expect(try s.contains("y"));
    const members = try s.members(a);
    defer fr.redis.freeStrings(members);
    try testing.expectEqual(@as(usize, 3), members.len);

    // 有序集合
    var z = try fr.getSortedSet("z");
    defer z.deinit();
    _ = try z.add("one", 1);
    _ = try z.add("two", 2);
    _ = try z.add("three", 3);
    try testing.expectEqual(@as(i64, 3), try z.len());
    try testing.expectEqual(@as(f64, 2), (try z.score("two")).?);
    const range = try z.range(a, 0, -1);
    defer fr.redis.freeStrings(range);
    try testing.expectEqualStrings("one", range[0]);
    try testing.expectEqualStrings("three", range[2]);
    try testing.expectEqual(@as(i64, 2), (try z.rank("three")).?);

    // 栈
    var st = try fr.getStack("stack");
    defer st.deinit();
    _ = try st.push("1");
    _ = try st.push("2");
    const top = (try st.takeOne(a, -1)).?;
    defer a.free(top);
    try testing.expectEqualStrings("2", top);
}

test "PubSub：订阅 / 发布 / 收消息" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16471);
    defer server.stop();

    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var r = try pek_zredis.Redis.fromConfig(a, io, cfg);
    defer r.deinit();

    var sub = try pek_zredis.PubSub.connect(a, io, &r.opts);
    defer sub.deinit();
    try sub.subscribe(&.{"news"});

    var receivers = try r.execute(&.{ "PUBLISH", "news", "hello-sub" });
    defer receivers.deinit(a);
    try testing.expectEqual(@as(i64, 1), receivers.asInt().?);

    const msg = (try sub.nextMessage(1000)).?;
    var m = msg;
    defer m.deinit(a);
    try testing.expectEqualStrings("news", msg.channel);
    try testing.expectEqualStrings("hello-sub", msg.payload);

    // 模式订阅
    var sub2 = try pek_zredis.PubSub.connect(a, io, &r.opts);
    defer sub2.deinit();
    try sub2.psubscribe(&.{"order.*"});

    var sent = try r.execute(&.{ "PUBLISH", "order.created", "o-1" });
    defer sent.deinit(a);
    const msg2 = (try sub2.nextMessage(1000)).?;
    var m2 = msg2;
    defer m2.deinit(a);
    try testing.expectEqualStrings("order.*", msg2.pattern.?);
    try testing.expectEqualStrings("order.created", msg2.channel);
    try testing.expectEqualStrings("o-1", msg2.payload);
}

fn workerIncr(r: *pek_zredis.Redis, iterations: usize, failed: *std.atomic.Value(bool)) void {
    for (0..iterations) |_| {
        _ = r.increment("cc", 1) catch {
            failed.store(true, .monotonic);
            return;
        };
    }
}

test "连接池：多线程并发命令" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16481);
    defer server.stop();

    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var r = try pek_zredis.Redis.fromConfig(a, io, cfg);
    defer r.deinit();

    var failed = std.atomic.Value(bool).init(false);
    const threads = 8;
    const per_thread = 25;
    var handles: [threads]std.Thread = undefined;
    for (0..threads) |i| {
        handles[i] = try std.Thread.spawn(.{}, workerIncr, .{ &r, per_thread, &failed });
    }
    for (handles) |h| h.join();

    try testing.expect(!failed.load(.monotonic));
    const total = (try r.get(i64, a, "cc")).?;
    try testing.expectEqual(@as(i64, threads * per_thread), total);

    const st = r.stats();
    try testing.expect(st.commands >= threads * per_thread);
}
