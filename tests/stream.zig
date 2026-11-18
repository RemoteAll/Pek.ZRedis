//! Stream / EventBus 集成测试（回环假 Redis）。

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

fn freeMessages(gpa: std.mem.Allocator, list: []pek_zredis.StreamMessage) void {
    for (list) |*m| m.deinit();
    if (list.len > 0) gpa.free(list);
}

test "Stream：基元/对象/数组写入、区间读取、裁剪与删除" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16601);
    defer server.stop();
    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var fr = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr.deinit();

    var s = try fr.getStream("evt");
    defer s.deinit();

    // 基元 → `__data`
    const id1 = (try s.add("hello", null)).?;
    defer a.free(id1);
    try testing.expect(id1.len > 0);

    // 对象 → 属性扁平化
    const Obj = struct { Name: []const u8, Age: i64 };
    const id2 = (try s.add(Obj{ .Name = "Alice", .Age = 18 }, null)).?;
    defer a.free(id2);

    // 字段对 → 直接写字段
    const id3 = (try s.addFields(&.{
        .{ .name = "K1", .value = "V1" },
        .{ .name = "K2", .value = "V2" },
    }, null, false)).?;
    defer a.free(id3);

    try testing.expectEqual(@as(i64, 3), try s.countMessages());
    try testing.expect(!(try s.isEmpty()));

    const all = try s.range(null, null, 0);
    defer freeMessages(a, all);
    try testing.expectEqual(@as(usize, 3), all.len);
    try testing.expectEqualStrings("hello", all[0].primitiveValue().?);

    const msg2 = &all[1];
    try testing.expectEqualStrings("Alice", msg2.field("Name").?);
    try testing.expectEqualStrings("18", msg2.field("Age").?);

    // 结构化解析
    const parsed = (try msg2.toStruct(Obj, a)).?;
    defer {
        var p = parsed;
        p.deinit();
    }
    try testing.expectEqualStrings("Alice", parsed.value.Name);
    try testing.expectEqual(@as(i64, 18), parsed.value.Age);

    // 裁剪保留 1 条
    try testing.expectEqual(@as(i64, 2), try s.trimMaxLen(1, true));
    try testing.expectEqual(@as(i64, 1), try s.countMessages());

    // 删除
    const last = (try s.range(null, null, 0));
    defer freeMessages(a, last);
    try testing.expectEqual(@as(usize, 1), last.len);
    try testing.expectEqual(@as(i64, 1), try s.delete(last[0].id));
    try testing.expect(try s.isEmpty());
}

test "Stream：消费组读取 / 确认 / 挂起 / 抢夺" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16611);
    defer server.stop();
    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var fr = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr.deinit();

    var producer = try fr.getStream("orders");
    defer producer.deinit();
    const p1 = (try producer.add("m1", null)).?;
    defer a.free(p1);
    const p2 = (try producer.add("m2", null)).?;
    defer a.free(p2);

    var s = try fr.getStream("orders");
    defer s.deinit();
    try testing.expect(try s.setGroup("g1"));
    // 重复设置不重建
    try testing.expect(!(try s.setGroup("g1")));

    const batch = try s.takeMessages(10, 0);
    defer freeMessages(a, batch);
    try testing.expectEqual(@as(usize, 2), batch.len);
    try testing.expectEqualStrings("m1", batch[0].primitiveValue().?);

    // 未确认 → 挂起（先看明细，再看汇总）
    const pending = try s.pending("g1", null, null, 10);
    defer {
        for (pending) |p| {
            var item = p;
            item.deinit();
        }
        if (pending.len > 0) a.free(pending);
    }
    try testing.expectEqual(@as(usize, 2), pending.len);
    try testing.expectEqual(@as(i64, 1), pending[0].delivery);

    var info = (try s.pendingInfo("g1")).?;
    defer info.deinit(a);
    try testing.expectEqual(@as(i64, 2), info.count);

    // 确认后挂起清零
    try testing.expectEqual(@as(i64, 2), try s.acknowledge(&.{ batch[0].id, batch[1].id }));
    var info2 = (try s.pendingInfo("g1")).?;
    defer info2.deinit(a);
    try testing.expectEqual(@as(i64, 0), info2.count);

    // 消费者列表
    const consumers = try s.getConsumers("g1");
    defer {
        for (consumers) |*c| c.deinit(a);
        if (consumers.len > 0) a.free(consumers);
    }
    try testing.expectEqual(@as(usize, 1), consumers.len);

    // 消费组信息
    const groups = try s.getGroups();
    defer {
        for (groups) |*g| g.deinit(a);
        if (groups.len > 0) a.free(groups);
    }
    try testing.expectEqual(@as(usize, 1), groups.len);
    try testing.expectEqualStrings("g1", groups[0].name.?);
}

test "Stream：独立消费按游标推进 + 强抢未确认消息" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16621);
    defer server.stop();
    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var fr = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr.deinit();

    var s = try fr.getStream("solo");
    defer s.deinit();
    const sid1 = (try s.add("a", null)).?;
    defer a.free(sid1);
    const sid2 = (try s.add("b", null)).?;
    defer a.free(sid2);

    const first = try s.takeMessages(1, 0);
    defer freeMessages(a, first);
    try testing.expectEqual(@as(usize, 1), first.len);
    try testing.expectEqualStrings("a", first[0].primitiveValue().?);

    const second = try s.takeMessages(1, 0);
    defer freeMessages(a, second);
    try testing.expectEqual(@as(usize, 1), second.len);
    try testing.expectEqualStrings("b", second[0].primitiveValue().?);

    // 没有新消息
    const empty = try s.takeMessages(1, 0);
    defer freeMessages(a, empty);
    try testing.expectEqual(@as(usize, 0), empty.len);

    // 抢夺：另一个消费者 XCLAIM 后能看到挂起项
    var g = try fr.getStream("solo");
    defer g.deinit();
    _ = try g.setGroup("gg");
    // 新组先对齐到末尾，避免把历史消息当作新消息
    _ = try g.groupSetId("gg", "$");
    const gid = (try g.add("c", null)).?;
    defer a.free(gid);
    const read = try g.readGroup("gg", "c1", 10, 0, ">");
    defer freeMessages(a, read);
    try testing.expectEqual(@as(usize, 1), read.len);

    var claimed = try g.claim("gg", "c2", read[0].id, 0);
    defer claimed.deinit(a);
    const items = claimed.asArray().?;
    try testing.expectEqual(@as(usize, 1), items.len);

    const pend = try g.pending("gg", null, null, 10);
    defer {
        for (pend) |p| {
            var item = p;
            item.deinit();
        }
        if (pend.len > 0) a.free(pend);
    }
    try testing.expectEqual(@as(usize, 1), pend.len);
    try testing.expectEqualStrings("c2", pend[0].consumer);
    try testing.expectEqual(@as(i64, 2), pend[0].delivery);
}

test "EventBus：发布 / 消费即确认 / 无消息返回 false" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16631);
    defer server.stop();
    const cfg = try server.configAlloc(a);
    defer a.free(cfg);

    var fr = try pek_zredis.FullRedis.fromConfig(a, io, cfg);
    defer fr.deinit();

    const Event = struct { Name: []const u8, Count: i64 };
    const Bus = pek_zredis.RedisEventBus(Event);

    var bus = try Bus.init(&fr, "bus:topic", "workers");
    defer bus.deinit();
    // 关闭按时间裁剪，避免假 Redis 的 XTRIM MINID 干扰断言；从组起始位读，确保能看到发布前入队的消息
    bus.setExpire(null);
    bus.setMaintenanceInterval(0);
    bus.setFromLastOffset(false);

    const id = try bus.publish(Event{ .Name = "created", .Count = 7 });
    defer a.free(id);
    try testing.expect(id.len > 0);

    const Ctx = struct {
        seen: usize = 0,
        name_ok: bool = false,
        count: i64 = 0,
        pub fn handle(self: *@This(), event: *const Event, message: *const pek_zredis.StreamMessage) !void {
            self.seen += 1;
            self.name_ok = std.mem.eql(u8, event.Name, "created");
            self.count = event.Count;
            try testing.expect(message.id.len > 0);
        }
    };
    var ctx = Ctx{};
    try testing.expect(try bus.consumeOnce(&ctx));
    try testing.expectEqual(@as(usize, 1), ctx.seen);
    try testing.expect(ctx.name_ok);
    try testing.expectEqual(@as(i64, 7), ctx.count);

    // 已确认，无新消息
    try testing.expect(!(try bus.consumeOnce(&ctx)));
}
