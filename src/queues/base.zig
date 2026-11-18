//! 队列公共设置（对齐 DH.NRedis `QueueBase` / Rust `QueueSettings`）。

const std = @import("std");

pub const Settings = struct {
    /// 追踪标识（对应 C# `Redis.Tracer.AttachParameter` + `DefaultSpan.Current`）
    trace_name: ?[]u8 = null,
    trace_value: ?[]u8 = null,

    /// 发送失败重试次数（失败含被代理吞掉的返回 0）。默认 3
    retry_times_when_send_failed: usize = 3,
    /// 重试间隔（毫秒）。默认 1000
    retry_interval_ms: u64 = 1000,
    /// 最终失败是否报错。默认 false（与 C# 一致，仅记录）
    throw_on_failure: bool = false,

    pub fn deinit(self: *Settings, gpa: std.mem.Allocator) void {
        if (self.trace_name) |s| gpa.free(s);
        if (self.trace_value) |s| gpa.free(s);
        self.* = undefined;
    }

    /// 设置追踪标识（复制字符串）。传 `null` 关闭。
    pub fn setTrace(self: *Settings, gpa: std.mem.Allocator, name: ?[]const u8, value: ?[]const u8) !void {
        if (self.trace_name) |s| gpa.free(s);
        if (self.trace_value) |s| gpa.free(s);
        self.trace_name = null;
        self.trace_value = null;
        if (name) |n| {
            if (n.len > 0) self.trace_name = try gpa.dupe(u8, n);
        }
        if (value) |v| {
            if (v.len > 0) self.trace_value = try gpa.dupe(u8, v);
        }
    }

    /// 向编码后的消息注入追踪标识（复刻 C# `RedisHelper.AttachTraceId` 的字符串分支）。
    ///
    /// 仅当消息是 JSON 对象文本（`{...}`）时生效；返回**自有**缓冲，调用方释放。
    pub fn attachTraceAlloc(self: *const Settings, gpa: std.mem.Allocator, payload: []const u8) ![]u8 {
        const name = self.trace_name orelse return gpa.dupe(u8, payload);
        const value = self.trace_value orelse return gpa.dupe(u8, payload);
        if (payload.len < 2 or payload[0] != '{' or payload[payload.len - 1] != '}') {
            return gpa.dupe(u8, payload);
        }

        const text = payload;
        // 已存在同名字段（忽略大小写）→ 不重复注入
        const needle = try std.fmt.allocPrint(gpa, "\"{s}\":", .{name});
        defer gpa.free(needle);
        if (containsIgnoreCase(text, needle)) {
            // 兼容属性值为 null 的占位：`,"name":null}` → 替换为真实值
            const empty_tail = try std.fmt.allocPrint(gpa, ",\"{s}\":null}}", .{name});
            defer gpa.free(empty_tail);
            if (endsWithIgnoreCase(text, empty_tail)) {
                const keep = text.len - empty_tail.len;
                return std.fmt.allocPrint(gpa, "{s},\"{s}\":\"{s}\"}}", .{ text[0..keep], name, value });
            }
            return gpa.dupe(u8, payload);
        }

        return std.fmt.allocPrint(gpa, "{s},\"{s}\":\"{s}\"}}", .{ text[0 .. text.len - 1], name, value });
    }
};

fn containsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0 or needle.len > haystack.len) return false;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return true;
    }
    return false;
}

fn endsWithIgnoreCase(text: []const u8, suffix: []const u8) bool {
    if (suffix.len > text.len) return false;
    return std.ascii.eqlIgnoreCase(text[text.len - suffix.len ..], suffix);
}

// ─────────────────────────────── 测试 ───────────────────────────────

const testing = std.testing;

test "无追踪标识时原样返回" {
    const a = testing.allocator;
    const s = Settings{};
    const out = try s.attachTraceAlloc(a, "{\"a\":1}");
    defer a.free(out);
    try testing.expectEqualStrings("{\"a\":1}", out);
}

test "追踪标识追加到 JSON 对象" {
    const a = testing.allocator;
    var s = Settings{};
    defer s.deinit(a);
    try s.setTrace(a, "traceId", "abc");
    const out = try s.attachTraceAlloc(a, "{\"Name\":\"NewLife\"}");
    defer a.free(out);
    try testing.expectEqualStrings("{\"Name\":\"NewLife\",\"traceId\":\"abc\"}", out);
}

test "已有同名字段不重复注入，null 占位被替换" {
    const a = testing.allocator;
    var s = Settings{};
    defer s.deinit(a);
    try s.setTrace(a, "traceId", "abc");

    const same = try s.attachTraceAlloc(a, "{\"traceId\":\"old\"}");
    defer a.free(same);
    try testing.expectEqualStrings("{\"traceId\":\"old\"}", same);

    const empty = try s.attachTraceAlloc(a, "{\"Name\":1,\"traceId\":null}");
    defer a.free(empty);
    try testing.expectEqualStrings("{\"Name\":1,\"traceId\":\"abc\"}", empty);
}

test "非 JSON 对象不修改" {
    const a = testing.allocator;
    var s = Settings{};
    defer s.deinit(a);
    try s.setTrace(a, "t", "v");
    const out = try s.attachTraceAlloc(a, "plain-text");
    defer a.free(out);
    try testing.expectEqualStrings("plain-text", out);
}
