//! 与 DH.NRedis 完全对齐的值编码器。
//!
//! C# 端 `DefaultPacketEncoder` / `RedisJsonEncoder` 决定了值在 Redis 中的字节格式，
//! 本模块逐条复刻，保证 **C# 写入的 Zig 能读，Zig 写入的 C# 能读**：
//!
//! | 值类型 | 编码结果（两端一致） |
//! |--------|----------------------|
//! | `null` | 空（不写入） |
//! | 字符串 | 原始 UTF-8 字节（**无引号**） |
//! | 字节数组 | 原始二进制 |
//! | 布尔 | `True` / `False`（读取兼容 `true` / `OK` / `1` / `0`） |
//! | 整数 | 十进制文本 `123` |
//! | 浮点 | 往返最短文本 `1.5`；`NaN` / `Infinity` / `-Infinity` |
//! | 时间 | `yyyy-MM-dd HH:mm:ss.fff`（读取兼容 ISO 8601 与 C# FastJson 文本） |
//! | 复杂对象 | 紧凑 JSON（属性名建议 PascalCase 以对齐 C#） |
//!
//! 与 Rust 的差异：Rust 用 serde 泛型；Zig 用 `encodeValueAlloc` 的编译期分发，
//! 结构体自动走 JSON，标量按上表，`zzig.DateTime` 走编码器时间格式。

const std = @import("std");
const zzig = @import("zzig");
const resp = @import("resp.zig");

/// 显式载荷（需要精确控制「文本 / 二进制 / 时间 / JSON」时使用）。
pub const Payload = union(enum) {
    null_,
    /// 原始 UTF-8（不加引号）
    text: []const u8,
    /// 原始二进制
    bytes: []const u8,
    boolean: bool,
    int: i64,
    uint: u64,
    float: f64,
    /// 编码器时间格式 `yyyy-MM-dd HH:mm:ss.fff`
    datetime: zzig.DateTime,
    /// 已解析 JSON 树（写紧凑 JSON）
    json: std.json.Value,
    /// 已序列化的 JSON 文本（原样写入，用于避免二次解析）
    json_raw: []const u8,
};

/// 编码载荷。返回 `null` 表示空值（对应 C# 编码 `null` 得到空数据包）。
pub fn encodePayloadAlloc(gpa: std.mem.Allocator, payload: Payload) !?[]u8 {
    return switch (payload) {
        .null_ => null,
        .text, .bytes, .json_raw => |s| try gpa.dupe(u8, s),
        .boolean => |v| try gpa.dupe(u8, if (v) "True" else "False"),
        .int => |v| try std.fmt.allocPrint(gpa, "{d}", .{v}),
        .uint => |v| try std.fmt.allocPrint(gpa, "{d}", .{v}),
        .float => |v| try resp.formatF64Alloc(gpa, v),
        .datetime => |v| try formatDateTimeMsAlloc(gpa, v),
        .json => |v| try std.json.Stringify.valueAlloc(gpa, v, .{}),
    };
}

/// 编译期分发编码：标量按 C# 口径，结构体/JSON 树走紧凑 JSON。
pub fn encodeValueAlloc(gpa: std.mem.Allocator, value: anytype) !?[]u8 {
    const T = @TypeOf(value);
    if (T == Payload) return encodePayloadAlloc(gpa, value);

    const info = @typeInfo(T);
    switch (info) {
        .null => return null,
        .optional => {
            if (value) |v| return encodeValueAlloc(gpa, v);
            return null;
        },
        .bool => return try gpa.dupe(u8, if (value) "True" else "False"),
        .int, .comptime_int => return try std.fmt.allocPrint(gpa, "{d}", .{value}),
        .float, .comptime_float => return try resp.formatF64Alloc(gpa, @floatCast(value)),
        .@"enum" => return try gpa.dupe(u8, @tagName(value)),
        .array => |arr| {
            if (arr.child == u8) return try gpa.dupe(u8, &value);
            @compileError("Pek.ZRedis：不支持把 " ++ @typeName(T) ++ " 直接写入 Redis（请用 JSON 包装或显式 Payload）");
        },
        .pointer => |ptr| {
            if (ptr.size == .slice and ptr.child == u8) return try gpa.dupe(u8, value);
            if (ptr.size == .one and @typeInfo(ptr.child) == .array and @typeInfo(ptr.child).array.child == u8) {
                return try gpa.dupe(u8, value);
            }
            @compileError("Pek.ZRedis：不支持把 " ++ @typeName(T) ++ " 直接写入 Redis（请用 JSON 包装或显式 Payload）");
        },
        .@"struct" => {
            if (T == zzig.DateTime) return try formatDateTimeMsAlloc(gpa, value);
            return try std.json.Stringify.valueAlloc(gpa, value, .{});
        },
        .@"union" => {
            if (T == std.json.Value) return try std.json.Stringify.valueAlloc(gpa, value, .{});
            @compileError("Pek.ZRedis：不支持把 " ++ @typeName(T) ++ " 直接写入 Redis（请用 JSON 包装或显式 Payload）");
        },
        else => @compileError("Pek.ZRedis：不支持的类型 " ++ @typeName(T)),
    }
}

/// 解码文本/数字/布尔/时间（`T` 为标量或字符串）。
pub fn decodeValueAlloc(comptime T: type, gpa: std.mem.Allocator, bytes: []const u8) !T {
    const info = @typeInfo(T);
    switch (info) {
        .pointer => |ptr| {
            if (ptr.size == .slice and ptr.child == u8) {
                if (ptr.attrs.@"const") return try gpa.dupe(u8, bytes);
                return try gpa.dupe(u8, bytes);
            }
            @compileError("Pek.ZRedis：decodeValueAlloc 只支持 []u8 / []const u8，其它类型请用 parseJsonAlloc");
        },
        .bool => {
            const s = std.mem.trim(u8, bytes, " \t\r\n");
            if (resp.parseBool(s)) |v| return v;
            return error.InvalidBoolean;
        },
        .int => {
            const s = std.mem.trim(u8, bytes, " \t\r\n");
            const n = std.fmt.parseInt(i64, s, 10) catch return error.InvalidInteger;
            return std.math.cast(T, n) orelse error.IntegerOverflow;
        },
        .float => {
            return @floatCast(try resp.parseF64(bytes));
        },
        .@"struct" => {
            if (T == zzig.DateTime) return parseDateTime(bytes);
            @compileError("Pek.ZRedis：结构体请用 parseJsonAlloc");
        },
        else => @compileError("Pek.ZRedis：不支持的解码类型 " ++ @typeName(T)),
    }
}

/// JSON 解码：返回 `Parsed(T)`，由调用方 `deinit()`。
pub fn parseJsonAlloc(comptime T: type, gpa: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(T) {
    const text = stripBom(bytes);
    return std.json.parseFromSlice(T, gpa, text, .{ .ignore_unknown_fields = true });
}

/// 去掉 UTF-8 BOM（.NET 某些序列化输出可能带）。
pub fn stripBom(bytes: []const u8) []const u8 {
    if (bytes.len >= 3 and bytes[0] == 0xEF and bytes[1] == 0xBB and bytes[2] == 0xBF) return bytes[3..];
    return bytes;
}

/// 编码器时间：`yyyy-MM-dd HH:mm:ss.fff`（毫秒 3 位，与 C# 一致）。
pub fn formatDateTimeMsAlloc(gpa: std.mem.Allocator, dt: zzig.DateTime) ![]u8 {
    var buf: [32]u8 = undefined;
    const text = try formatDateTimeMsBuf(&buf, dt);
    return gpa.dupe(u8, text);
}

pub fn formatDateTimeMsBuf(buf: []u8, dt: zzig.DateTime) ![]const u8 {
    const ms: u32 = dt.nanos / 1_000_000;
    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}", .{
        @as(u32, @intCast(@max(dt.year, 0))),
        dt.month,
        dt.day,
        dt.hour,
        dt.minute,
        dt.second,
        ms,
    });
}

/// 时间解析：接受 C# 编码器文本、ISO 8601（含 `Z` / 偏移，值按本地无时区处理）。
pub fn parseDateTime(bytes: []const u8) !zzig.DateTime {
    const s = std.mem.trim(u8, bytes, " \t\r\n");
    if (s.len == 0) return error.EmptyDateTime;
    return zzig.DateTime.parse(s);
}

/// JSON 内时间字段的包装：写入 ISO 8601（对齐 System.Text.Json），
/// 读取同时兼容 FastJson 文本（`2026-09-26 10:00:00`）。
pub const JsonDateTime = struct {
    value: zzig.DateTime,

    pub fn init(value: zzig.DateTime) JsonDateTime {
        return .{ .value = value };
    }

    pub fn jsonStringify(self: JsonDateTime, jws: anytype) !void {
        var buf: [40]u8 = undefined;
        const text = formatIsoBuf(&buf, self.value) catch unreachable;
        try jws.write(text);
    }

    pub fn jsonParse(allocator: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !JsonDateTime {
        const text = switch (try source.nextAllocMax(allocator, .alloc_if_needed, options.max_value_len.?)) {
            .string => |s| s,
            .allocated_string => |s| s,
            else => return error.UnexpectedToken,
        };
        const dt = parseDateTime(text) catch return error.UnexpectedToken;
        return .{ .value = dt };
    }

    pub fn jsonParseFromValue(allocator: std.mem.Allocator, source: std.json.Value, options: std.json.ParseOptions) !JsonDateTime {
        _ = allocator;
        _ = options;
        const text = switch (source) {
            .string => |s| s,
            else => return error.UnexpectedToken,
        };
        const dt = parseDateTime(text) catch return error.UnexpectedToken;
        return .{ .value = dt };
    }
};

/// Stream/字段路径的编码器文本时间（`yyyy-MM-dd HH:mm:ss.fff`）。
pub const DateTimeText = struct {
    value: zzig.DateTime,

    pub fn init(value: zzig.DateTime) DateTimeText {
        return .{ .value = value };
    }

    pub fn jsonStringify(self: DateTimeText, jws: anytype) !void {
        var buf: [32]u8 = undefined;
        const text = formatDateTimeMsBuf(&buf, self.value) catch unreachable;
        try jws.write(text);
    }

    pub fn jsonParse(allocator: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !DateTimeText {
        const text = switch (try source.nextAllocMax(allocator, .alloc_if_needed, options.max_value_len.?)) {
            .string => |s| s,
            .allocated_string => |s| s,
            else => return error.UnexpectedToken,
        };
        const dt = parseDateTime(text) catch return error.UnexpectedToken;
        return .{ .value = dt };
    }

    pub fn jsonParseFromValue(allocator: std.mem.Allocator, source: std.json.Value, options: std.json.ParseOptions) !DateTimeText {
        _ = allocator;
        _ = options;
        const text = switch (source) {
            .string => |s| s,
            else => return error.UnexpectedToken,
        };
        const dt = parseDateTime(text) catch return error.UnexpectedToken;
        return .{ .value = dt };
    }
};

/// ISO 8601（无时区）：`2026-09-26T10:00:00` / `...T10:00:00.123`（去尾随 0）。
fn formatIsoBuf(buf: []u8, dt: zzig.DateTime) ![]const u8 {
    if (dt.nanos == 0) {
        return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}", .{
            @as(u32, @intCast(@max(dt.year, 0))),
            dt.month,
            dt.day,
            dt.hour,
            dt.minute,
            dt.second,
        });
    }
    // 与 chrono `%Y-%m-%dT%H:%M:%S%.f` 对齐：纳秒转 9 位后去尾随 0
    var frac: [9]u8 = undefined;
    _ = std.fmt.bufPrint(&frac, "{d:0>9}", .{dt.nanos}) catch unreachable;
    var len: usize = 9;
    while (len > 0 and frac[len - 1] == '0') len -= 1;
    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{s}", .{
        @as(u32, @intCast(@max(dt.year, 0))),
        dt.month,
        dt.day,
        dt.hour,
        dt.minute,
        dt.second,
        frac[0..len],
    });
}

// ─────────────────────────────── 测试 ───────────────────────────────

const testing = std.testing;

fn encoded(gpa: std.mem.Allocator, value: anytype) ![]u8 {
    return (try encodeValueAlloc(gpa, value)).?;
}

test "标量编码与 C# 端一致" {
    const a = testing.allocator;
    {
        const s = try encoded(a, "hello");
        defer a.free(s);
        try testing.expectEqualStrings("hello", s); // 字符串无引号
    }
    {
        const s = try encoded(a, true);
        defer a.free(s);
        try testing.expectEqualStrings("True", s);
    }
    {
        const s = try encoded(a, @as(i32, -5));
        defer a.free(s);
        try testing.expectEqualStrings("-5", s);
    }
    {
        const s = try encoded(a, 1.5);
        defer a.free(s);
        try testing.expectEqualStrings("1.5", s);
    }
    {
        const s = try encoded(a, 0.1);
        defer a.free(s);
        try testing.expectEqualStrings("0.1", s);
    }
    {
        const s = try encoded(a, @as(f64, 1.0));
        defer a.free(s);
        try testing.expectEqualStrings("1", s); // 整数值不带 .0
    }
    {
        const s = try encoded(a, [_]u8{ 1, 2, 3 });
        defer a.free(s);
        try testing.expectEqualSlices(u8, &.{ 1, 2, 3 }, s);
    }
}

test "时间编码为 yyyy-MM-dd HH:mm:ss.fff" {
    const a = testing.allocator;
    const dt = try zzig.DateTime.parse("2026-09-26 10:00:00.123456");
    const s = try encoded(a, dt);
    defer a.free(s);
    try testing.expectEqualStrings("2026-09-26 10:00:00.123", s);

    const dt2 = try zzig.DateTime.parse("2026-09-26T10:00:00");
    const s2 = try encoded(a, dt2);
    defer a.free(s2);
    try testing.expectEqualStrings("2026-09-26 10:00:00.000", s2);
}

test "JSON 结构体为紧凑 JSON（PascalCase 原样）" {
    const a = testing.allocator;
    const User = struct { Name: []const u8, Age: i32 };
    const s = try encoded(a, User{ .Name = "NewLife", .Age = 18 });
    defer a.free(s);
    try testing.expectEqualStrings("{\"Name\":\"NewLife\",\"Age\":18}", s);
}

test "解码：字符串 / 布尔 / 整数 / 浮点 / 时间" {
    const a = testing.allocator;
    {
        const v = try decodeValueAlloc([]u8, a, "abc");
        defer a.free(v);
        try testing.expectEqualStrings("abc", v);
    }
    try testing.expect(try decodeValueAlloc(bool, a, "OK"));
    try testing.expect(try decodeValueAlloc(bool, a, "true"));
    try testing.expect(!try decodeValueAlloc(bool, a, "0"));
    try testing.expectEqual(@as(i64, -12), try decodeValueAlloc(i64, a, "-12 "));
    try testing.expectEqual(@as(f64, 2.5), try decodeValueAlloc(f64, a, "2.5"));
    const dt = try decodeValueAlloc(zzig.DateTime, a, "2026-09-26T10:00:00.5");
    try testing.expectEqual(@as(u8, 26), dt.day);
}

test "JSON 解码容忍 BOM 与未知字段" {
    const a = testing.allocator;
    const Bom = "\xEF\xBB\xBF";
    const User = struct { Name: []const u8 };
    var parsed = try parseJsonAlloc(User, a, Bom ++ "{\"Name\":\"x\",\"Extra\":1}");
    defer parsed.deinit();
    try testing.expectEqualStrings("x", parsed.value.Name);
}

test "JsonDateTime 兼容 FastJson 文本与 ISO" {
    const a = testing.allocator;
    const M = struct { CreateTime: JsonDateTime };
    var p1 = try parseJsonAlloc(M, a, "{\"CreateTime\":\"2026-09-26 10:00:00\"}");
    defer p1.deinit();
    try testing.expectEqual(@as(u8, 26), p1.value.CreateTime.value.day);

    var p2 = try parseJsonAlloc(M, a, "{\"CreateTime\":\"2026-09-26T10:00:00.123\"}");
    defer p2.deinit();
    try testing.expectEqual(@as(u32, 123_000_000), p2.value.CreateTime.value.nanos);

    const out = try std.json.Stringify.valueAlloc(a, M{ .CreateTime = JsonDateTime.init(p1.value.CreateTime.value) }, .{});
    defer a.free(out);
    try testing.expectEqualStrings("{\"CreateTime\":\"2026-09-26T10:00:00\"}", out);
}
