//! 队列 / 服务 / Tair 集成测试（回环假 Redis）。

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

fn freeStrings(gpa: std.mem.Allocator, list: [][]u8) void {
    for (list) |s| gpa.free(s);
    gpa.free(list);
}

test "普通队列：先进先出 / 批量 / 清空" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16501);
    defer server.stop();
    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var fr = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr.deinit();

    var q = try fr.getQueue("mail");
    defer q.deinit();

    try testing.expectEqual(@as(i64, 1), try q.add("a"));
    try testing.expectEqual(@as(i64, 2), try q.add("b"));
    try testing.expectEqual(@as(i64, 4), try q.addMany(&.{ "c", "d" }));
    try testing.expectEqual(@as(i64, 4), try q.count());
    try testing.expect(!(try q.isEmpty()));

    const first = (try q.takeOne(a, -1)).?;
    defer a.free(first);
    try testing.expectEqualStrings("a", first);

    try q.clear();
    try testing.expect(try q.isEmpty());
    try testing.expect((try q.takeOne(a, -1)) == null);
}

test "延迟队列：到期获取 / 转移主队列" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16511);
    defer server.stop();
    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var fr = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr.deinit();

    var dq = try fr.getDelayQueue("jobs");
    defer dq.deinit();

    _ = try dq.addRaw("j1", 0);
    _ = try dq.addRaw("j2", 0);
    _ = try dq.addRaw("future", 600);
    try testing.expectEqual(@as(i64, 3), try dq.count());

    const due = try dq.takeDue(a, 10);
    defer freeStrings(a, due);
    try testing.expectEqual(@as(usize, 2), due.len);

    // 未到期的仍在队列里
    try testing.expectEqual(@as(i64, 1), try dq.count());

    var q = try fr.getQueue("jobs:ready");
    defer q.deinit();
    _ = try dq.addRaw("j3", 0);
    try testing.expectEqual(@as(usize, 1), try dq.transferDue(&q));
    const moved = (try q.takeOne(a, -1)).?;
    defer a.free(moved);
    try testing.expectEqualStrings("j3", moved);
}

test "可靠队列：确认 / 回滚 / 发布消费 / 延迟补投" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16521);
    defer server.stop();
    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var fr = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr.deinit();

    var rq = try fr.getReliableQueue("orders");
    defer rq.deinit();

    _ = try rq.add("m1");
    const m1 = (try rq.takeOne(a, -1)).?;
    defer a.free(m1);
    try testing.expectEqualStrings("m1", m1);
    try testing.expectEqual(@as(i64, 0), try rq.count());
    try testing.expectEqual(@as(i64, 1), try rq.acknowledge(&.{m1}));

    // 未确认的消息：回滚到主队列
    _ = try rq.add("m2");
    const m2 = (try rq.takeOne(a, -1)).?;
    defer a.free(m2);
    const rolled = try rq.rollbackAck(rq.ackKey(), rq.key);
    defer freeStrings(a, rolled);
    try testing.expectEqual(@as(usize, 1), rolled.len);
    try testing.expectEqual(@as(i64, 1), try rq.count());

    // 清掉回滚的消息
    const drained = (try rq.takeOne(a, -1)).?;
    defer a.free(drained);
    _ = try rq.acknowledge(&.{drained});

    // 发布 + 消费（消息体存 KV）
    _ = try rq.publish(&.{.{ .key = "id1", .value = "body-1" }}, 60);
    const ConsumeCtx = struct {
        seen: bool = false,
        pub fn handle(self: *@This(), msg_id: []const u8, body: []const u8) !void {
            if (std.mem.eql(u8, msg_id, "id1") and std.mem.eql(u8, body, "body-1")) self.seen = true;
        }
    };
    var ctx = ConsumeCtx{};
    try testing.expect(try rq.consume(-1, &ctx));
    try testing.expect(ctx.seen);

    // 延迟消息到期补投
    _ = try rq.addDelay("d1", 0);
    try testing.expectEqual(@as(usize, 1), try rq.transferDelayDue());
    const d1 = (try rq.takeOne(a, -1)).?;
    defer a.free(d1);
    try testing.expectEqualStrings("d1", d1);
    _ = try rq.acknowledge(&.{d1});
}

test "延迟批处理：去重 / 成功清空 / 失败回写" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16531);
    defer server.stop();
    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var fr = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr.deinit();

    var df = try fr.getDeferred("batch");
    defer df.deinit();

    try testing.expectEqual(@as(i64, 2), try df.add(&.{ "k1", "k2" }));
    // 重复加入不增加
    try testing.expectEqual(@as(i64, 0), try df.add(&.{"k1"}));

    const Ctx = struct {
        calls: usize = 0,
        fail: bool = false,
        pub fn handle(self: *@This(), keys: []const []const u8) !void {
            self.calls += 1;
            if (self.fail) return error.Boom;
            try testing.expect(keys.len > 0);
        }
    };

    var ok_ctx = Ctx{};
    try testing.expectEqual(@as(usize, 2), try df.processOnce(&ok_ctx));
    try testing.expectEqual(@as(usize, 1), ok_ctx.calls);

    // 失败时回写集合
    _ = try df.add(&.{"k3"});
    var bad_ctx = Ctx{ .fail = true };
    try testing.expectError(error.Server, df.processOnce(&bad_ctx));
    try testing.expectEqual(@as(usize, 1), bad_ctx.calls);

    var retry_ctx = Ctx{};
    try testing.expectEqual(@as(usize, 1), try df.processOnce(&retry_ctx));
}

test "统计服务：累加 / 延迟转移 / 快照处理" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16541);
    defer server.stop();
    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var fr = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr.deinit();

    var stat = try pek_zredis.RedisStat.init(&fr, "user:stat");
    defer stat.deinit();

    try testing.expectEqual(@as(i64, 3), try stat.increment("user:day", "a", 3));
    try testing.expectEqual(@as(i64, 2), try stat.increment("user:day", "b", 2));

    try testing.expectEqual(@as(i64, 1), try stat.addDelayQueue("user:day", 0));
    // 10 分钟内重复加入被去重
    try testing.expectEqual(@as(i64, 0), try stat.addDelayQueue("user:day", 0));

    try testing.expectEqual(@as(usize, 1), try stat.transferDueOnce(10));

    const SaveCtx = struct {
        got: usize = 0,
        sum: i64 = 0,
        key_ok: bool = false,
        pub fn onSave(self: *@This(), key: []const u8, values: []const pek_zredis.services.stat.FieldPair) !void {
            self.got += 1;
            self.sum = 0;
            for (values) |v| self.sum += v.value;
            self.key_ok = std.mem.eql(u8, key, "user:day");
        }
    };
    var ctx = SaveCtx{};
    try testing.expect(try stat.consumeOnce(-1, &ctx));
    try testing.expectEqual(@as(usize, 1), ctx.got);
    try testing.expect(ctx.key_ok);
    try testing.expectEqual(@as(i64, 5), ctx.sum);
    try testing.expect(!(try fr.redis.containsKey("user:day")));
}

test "RedLock：多实例写入 / 令牌比较释放" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16551);
    defer server.stop();
    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var fr1 = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr1.deinit();
    var fr2 = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr2.deinit();

    var lock1 = (try pek_zredis.acquireRedLock(a, &.{ &fr1, &fr2 }, "lock:red", 100, 10_000)).?;
    defer lock1.deinit();
    try testing.expectEqual(@as(usize, 2), lock1.lockedCount());
    try testing.expectEqual(@as(usize, 22), lock1.tokenText().len);

    const token1 = try a.dupe(u8, lock1.tokenText());
    defer a.free(token1);

    // 与 C# 一致：写入用普通 SET，后到者可覆盖
    var lock2 = (try pek_zredis.acquireRedLock(a, &.{ &fr1, &fr2 }, "lock:red", 100, 10_000)).?;
    defer lock2.deinit();
    try testing.expect(!std.mem.eql(u8, token1, lock2.tokenText()));

    // 旧令牌释放不得删除他人锁
    lock1.release();
    try testing.expect(try fr1.redis.containsKey("lock:red"));

    // 新令牌释放生效
    lock2.release();
    try testing.expect(!(try fr1.redis.containsKey("lock:red")));
}

test "Tair 扩展：字符串版本 / 哈希版本 / 计数" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16561);
    defer server.stop();
    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var fr = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr.deinit();

    // EXSET / EXGET
    const old = try fr.exSet("ts", "v1", 5, 0);
    try testing.expect(old == null);
    const got = (try fr.exGet("ts")).?;
    defer if (got.value) |v| a.free(v);
    try testing.expectEqualStrings("v1", got.value.?);
    try testing.expectEqual(@as(i64, 5), got.version);

    // EXINCRBY 返回 [value, version]
    const inc = (try fr.exIncrBy("tn", 3, 0, 0)).?;
    try testing.expectEqual(@as(i64, 3), inc.value);
    try testing.expectEqual(@as(i64, 1), inc.version);

    // EXHSET / EXHGET / EXHINCRBY / EXHPTTL / EXHKEYS / EXHLEN / EXHDEL
    try testing.expectEqual(@as(i64, 1), try fr.exHSet("th", "f1", "x", 60, false, 2));
    try testing.expectEqual(@as(i64, 0), try fr.exHSet("th", "f1", "y", 0, true, 0)); // NX 拒绝

    const v = (try fr.exHGet(a, "th", "f1")).?;
    defer a.free(v);
    try testing.expectEqualStrings("x", v);

    const with_ver = (try fr.exHGetWithVer("th", "f1")).?;
    defer if (with_ver.value) |vv| a.free(vv);
    try testing.expectEqual(@as(i64, 2), with_ver.version);

    try testing.expectEqual(@as(i64, 7), try fr.exHIncrBy("th", "n", 7, 0));
    try testing.expect(try fr.exHPttl("th", "f1") > 0);

    const keys = try fr.exHKeys(a, "th");
    defer freeStrings(a, keys);
    try testing.expectEqual(@as(usize, 2), keys.len);

    const vals = try fr.exHVals(a, "th");
    defer freeStrings(a, vals);
    try testing.expectEqual(@as(usize, 2), vals.len);

    try testing.expectEqual(@as(i64, 2), try fr.exHLen("th"));
    try testing.expectEqual(@as(i64, 1), try fr.exHDel("th", &.{"f1"}));
    try testing.expectEqual(@as(i64, 1), try fr.exHLen("th"));

    const missing = try fr.exHMGet(a, "th", &.{ "f1", "n" });
    defer {
        for (missing) |slot| if (slot) |s| a.free(s);
        a.free(missing);
    }
    try testing.expect(missing[0] == null);
    try testing.expectEqualStrings("7", missing[1].?);
}
