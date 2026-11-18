//! 队列状态（对齐 DH.NRedis `RedisQueueStatus`）。
//!
//! JSON 字段名与 C# 一致（PascalCase）；时间字段写 ISO 8601、读兼容 ISO 与 FastJson 文本。
//! 注意：Zig 0.17 没有进程级环境变量读取入口，`machine_name` / `user_name` / `ip` 默认留空，
//! `process_id` 为 0；这些字段只用于运维展示，不参与消息/键/字节格式互通。

const std = @import("std");
const zzig = @import("zzig");
const encoder = @import("../encoder.zig");

pub const QueueStatus = struct {
    gpa: std.mem.Allocator,
    key: []u8,
    machine_name: ?[]u8 = null,
    user_name: ?[]u8 = null,
    process_id: i32 = 0,
    ip: ?[]u8 = null,
    create_time: zzig.DateTime,
    last_active: zzig.DateTime,
    consumes: i64 = 0,
    acks: i64 = 0,

    pub fn init(gpa: std.mem.Allocator, key: []const u8, now: zzig.DateTime) !QueueStatus {
        return .{
            .gpa = gpa,
            .key = try gpa.dupe(u8, key),
            .create_time = now,
            .last_active = now,
        };
    }

    pub fn deinit(self: *QueueStatus) void {
        self.gpa.free(self.key);
        if (self.machine_name) |s| self.gpa.free(s);
        if (self.user_name) |s| self.gpa.free(s);
        if (self.ip) |s| self.gpa.free(s);
        self.* = undefined;
    }

    const Wire = struct {
        Key: []const u8,
        MachineName: ?[]const u8 = null,
        UserName: ?[]const u8 = null,
        ProcessId: i32 = 0,
        Ip: ?[]const u8 = null,
        CreateTime: encoder.JsonDateTime,
        LastActive: encoder.JsonDateTime,
        Consumes: i64 = 0,
        Acks: i64 = 0,
    };

    /// 序列化为 JSON（写入 `{key}:Status:{ukey}`）。调用方释放。
    pub fn toJsonAlloc(self: *const QueueStatus) ![]u8 {
        const wire = Wire{
            .Key = self.key,
            .MachineName = self.machine_name,
            .UserName = self.user_name,
            .ProcessId = self.process_id,
            .Ip = self.ip,
            .CreateTime = encoder.JsonDateTime.init(self.create_time),
            .LastActive = encoder.JsonDateTime.init(self.last_active),
            .Consumes = self.consumes,
            .Acks = self.acks,
        };
        return std.json.Stringify.valueAlloc(self.gpa, wire, .{});
    }

    /// 解析 JSON（兼容 C# System.Text.Json 与 FastJson 文本）。返回自有实例。
    pub fn fromJsonAlloc(gpa: std.mem.Allocator, text: []const u8) !QueueStatus {
        const trimmed = std.mem.trim(u8, text, " \t\r\n\x00");
        var parsed = try std.json.parseFromSlice(Wire, gpa, trimmed, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();

        var out = QueueStatus{
            .gpa = gpa,
            .key = try gpa.dupe(u8, parsed.value.Key),
            .create_time = parsed.value.CreateTime.value,
            .last_active = parsed.value.LastActive.value,
            .process_id = parsed.value.ProcessId,
            .consumes = parsed.value.Consumes,
            .acks = parsed.value.Acks,
        };
        errdefer out.deinit();
        if (parsed.value.MachineName) |s| out.machine_name = try gpa.dupe(u8, s);
        if (parsed.value.UserName) |s| out.user_name = try gpa.dupe(u8, s);
        if (parsed.value.Ip) |s| out.ip = try gpa.dupe(u8, s);
        return out;
    }
};

/// 8 位随机消费标识（与 C# `Rand.NextString(8)` 用途一致；字符集不参与互通）。
pub fn newConsumerKey(io: std.Io) [8]u8 {
    const chars = "0123456789abcdefghijklmnopqrstuvwxyz";
    var raw: [8]u8 = undefined;
    io.random(&raw);
    var out: [8]u8 = undefined;
    for (raw, 0..) |b, i| out[i] = chars[b % chars.len];
    return out;
}

// ─────────────────────────────── 测试 ───────────────────────────────

const testing = std.testing;

test "状态 JSON 为 PascalCase 且时间写 ISO" {
    const a = testing.allocator;
    var st = try QueueStatus.init(a, "abc12345", try zzig.DateTime.parse("2026-09-26 10:00:00"));
    defer st.deinit();
    st.process_id = 4321;
    st.consumes = 12;
    st.acks = 11;

    const json = try st.toJsonAlloc();
    defer a.free(json);
    try testing.expect(std.mem.indexOf(u8, json, "\"Key\":\"abc12345\"") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"CreateTime\":\"2026-09-26T10:00:00\"") != null);
    try testing.expect(std.mem.indexOf(u8, json, "\"ProcessId\":4321") != null);
}

test "状态解析兼容 C# ISO 带偏移与 FastJson 文本" {
    const a = testing.allocator;
    const cs = "{\"Key\":\"abc12345\",\"MachineName\":\"DEV-PC\",\"UserName\":\"qcjxb\",\"ProcessId\":4321,\"Ip\":\"192.168.1.10\",\"CreateTime\":\"2026-09-26T10:00:00+08:00\",\"LastActive\":\"2026-09-26T10:05:00+08:00\",\"Consumes\":12,\"Acks\":11}";
    var st = try QueueStatus.fromJsonAlloc(a, cs);
    defer st.deinit();
    try testing.expectEqualStrings("abc12345", st.key);
    try testing.expectEqualStrings("DEV-PC", st.machine_name.?);
    try testing.expectEqual(@as(i32, 4321), st.process_id);
    try testing.expectEqual(@as(u8, 26), st.create_time.day);

    const fast = "{\"Key\":\"k\",\"MachineName\":null,\"UserName\":null,\"ProcessId\":1,\"Ip\":null,\"CreateTime\":\"2026-09-26 10:00:00\",\"LastActive\":\"2026-09-26 10:00:00.123\",\"Consumes\":0,\"Acks\":0}";
    var st2 = try QueueStatus.fromJsonAlloc(a, fast);
    defer st2.deinit();
    try testing.expectEqual(@as(u32, 123_000_000), st2.last_active.nanos);
}

test "消费标识为 8 位小写字母数字" {
    var threaded = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    const key = newConsumerKey(threaded.io());
    for (key) |c| {
        try testing.expect((c >= '0' and c <= '9') or (c >= 'a' and c <= 'z'));
    }
}
