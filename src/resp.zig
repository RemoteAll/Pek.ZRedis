//! RESP2 / RESP3 协议编解码。
//!
//! 对齐 Rust `pek_rredis::resp` 与 C# `DH.NRedis.RedisClient` 的组包（GetRequest）与
//! 解析（ParseResponse）逻辑：
//!
//! - 组包：`*<n>\r\n$<len>\r\n<data>\r\n...`（与 C# 逐字节一致）；
//! - 解析：RESP2 全量 + RESP3 的 Map / Set / Double / Bool / Null / Verbatim / Push /
//!   BigNumber / Attribute；`Attribute` 由 `readReply` 自动跳过（与 Rust 一致）。
//!
//! 所有字符串/数组由调用方提供的分配器持有，`Value.deinit` 递归释放。

const std = @import("std");

pub const Value = union(enum) {
    /// 状态回复 `+OK`
    simple: []u8,
    /// 错误回复 `-ERR ...`
    err: []u8,
    /// 整数 `:123`
    integer: i64,
    /// 二进制安全字符串 `$n`
    bulk: []u8,
    /// 空值（RESP2 `$-1`/`*-1`，RESP3 `_`）
    null_,
    /// 数组 `*n`
    array: []Value,
    /// RESP3 映射 `%n`
    map: []Pair,
    /// RESP3 集合 `~n`
    set: []Value,
    /// RESP3 双精度浮点 `,`
    double: f64,
    /// RESP3 布尔 `#`
    boolean: bool,
    /// RESP3 文本 `=`
    verbatim: []u8,
    /// RESP3 推送 `>`
    push: []Value,
    /// RESP3 大数字 `(`
    big_number: []u8,
    /// RESP3 属性 `|`（读取时应跳过）
    attribute: []Pair,

    pub const Pair = struct {
        key: Value,
        value: Value,
    };

    pub fn deinit(self: *Value, gpa: std.mem.Allocator) void {
        switch (self.*) {
            .simple, .err, .bulk, .verbatim, .big_number => |s| gpa.free(s),
            .null_, .integer, .double, .boolean => {},
            .array, .set, .push => |items| {
                for (items) |*item| item.deinit(gpa);
                gpa.free(items);
            },
            .map, .attribute => |pairs| {
                for (pairs) |*pair| {
                    pair.key.deinit(gpa);
                    pair.value.deinit(gpa);
                }
                gpa.free(pairs);
            },
        }
        self.* = .null_;
    }

    pub fn isNull(self: Value) bool {
        return self == .null_;
    }

    pub fn isError(self: Value) bool {
        return self == .err;
    }

    /// 转成字节（标量跟随 C# `IPacket.ToStr()` 语义）。返回值由调用方释放。
    pub fn asBytesAlloc(self: Value, gpa: std.mem.Allocator) !?[]u8 {
        return switch (self) {
            .bulk, .simple, .err, .verbatim, .big_number => |s| try gpa.dupe(u8, s),
            .integer => |v| try std.fmt.allocPrint(gpa, "{d}", .{v}),
            .double => |v| try formatF64Alloc(gpa, v),
            .boolean => |v| try gpa.dupe(u8, if (v) "1" else "0"),
            else => null,
        };
    }

    /// 直接取 UTF-8 视图（不分配；仅 bulk/simple/verbatim/err/big_number 可用）。
    pub fn asString(self: Value) ?[]const u8 {
        return switch (self) {
            .bulk, .simple, .err, .verbatim, .big_number => |s| s,
            else => null,
        };
    }

    /// 转整数：兼容 C# `ToInt()` 对字符串结果的宽松转换。
    pub fn asInt(self: Value) ?i64 {
        return switch (self) {
            .integer => |v| v,
            .boolean => |v| if (v) 1 else 0,
            .double => |v| @intFromFloat(v),
            .bulk, .simple, .verbatim, .big_number => |s| std.fmt.parseInt(i64, std.mem.trim(u8, s, " \t\r\n"), 10) catch null,
            else => null,
        };
    }

    /// 转浮点：支持 RESP3 Double 与字符串形式。
    pub fn asFloat(self: Value) ?f64 {
        return switch (self) {
            .double => |v| v,
            .integer => |v| @floatFromInt(v),
            .bulk, .simple, .verbatim, .big_number => |s| parseF64(s) catch null,
            else => null,
        };
    }

    /// 转布尔：兼容 C# `RedisJsonEncoder` 对 `OK` 的特殊处理。
    pub fn asBool(self: Value) ?bool {
        return switch (self) {
            .boolean => |v| v,
            .integer => |v| v != 0,
            .simple, .bulk, .verbatim, .big_number => |s| parseBool(s),
            .null_ => false,
            else => null,
        };
    }

    /// 数组元素视图（Push/Set 也按数组处理）。
    pub fn asArray(self: *const Value) ?[]const Value {
        return switch (self.*) {
            .array, .set, .push => |items| items,
            else => null,
        };
    }
};

/// 把命令编码为 RESP2 数组帧追加到 `out`（与 C# GetRequest 输出完全一致）。
pub fn appendCommand(out: *std.ArrayList(u8), gpa: std.mem.Allocator, args: []const []const u8) !void {
    try out.append(gpa, '*');
    try appendInt(out, gpa, args.len);
    try out.appendSlice(gpa, "\r\n");
    for (args) |arg| {
        try out.append(gpa, '$');
        try appendInt(out, gpa, arg.len);
        try out.appendSlice(gpa, "\r\n");
        try out.appendSlice(gpa, arg);
        try out.appendSlice(gpa, "\r\n");
    }
}

/// 独立帧（调用方释放）。
pub fn encodeCommandAlloc(gpa: std.mem.Allocator, args: []const []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    try appendCommand(&out, gpa, args);
    return out.toOwnedSlice(gpa);
}

pub fn appendInt(out: *std.ArrayList(u8), gpa: std.mem.Allocator, value: anytype) !void {
    var buf: [24]u8 = undefined;
    const text = try std.fmt.bufPrint(&buf, "{d}", .{value});
    try out.appendSlice(gpa, text);
}

/// 把毫秒转为 `std.Io.Timeout`（单调时钟，避免系统时间跳变影响读写超时）。
pub fn timeoutMs(ms: u64) std.Io.Timeout {
    return .{ .duration = .{
        .raw = .{ .nanoseconds = @as(i96, @intCast(ms)) * std.time.ns_per_ms },
        .clock = .awake,
    } };
}

/// `.NET` 浮点文本：`NaN` / `Infinity` / `-Infinity`，其余为最短往返表示。
pub fn formatF64Alloc(gpa: std.mem.Allocator, v: f64) ![]u8 {
    if (std.math.isNan(v)) return gpa.dupe(u8, "NaN");
    if (std.math.isInf(v)) return gpa.dupe(u8, if (v > 0) "Infinity" else "-Infinity");
    return std.fmt.allocPrint(gpa, "{d}", .{v});
}

/// 容忍空白与 `Infinity` / `NaN` 的浮点解析。
pub fn parseF64(text: []const u8) !f64 {
    const s = std.mem.trim(u8, text, " \t\r\n");
    const eql = std.ascii.eqlIgnoreCase;
    if (eql(s, "Infinity") or eql(s, "inf") or eql(s, "+inf")) return std.math.inf(f64);
    if (eql(s, "-Infinity") or eql(s, "-inf")) return -std.math.inf(f64);
    if (eql(s, "NaN") or eql(s, "nan")) return std.math.nan(f64);
    return std.fmt.parseFloat(f64, s);
}

/// 宽松布尔解析（C# `ChangeType` + `RedisJsonEncoder` 的 OK 规则）。
pub fn parseBool(text: []const u8) ?bool {
    const s = std.mem.trim(u8, text, " \t\r\n");
    if (s.len == 0) return false;
    if (std.mem.eql(u8, s, "1")) return true;
    if (std.mem.eql(u8, s, "0")) return false;
    if (std.ascii.eqlIgnoreCase(s, "OK")) return true;
    if (std.ascii.eqlIgnoreCase(s, "true")) return true;
    if (std.ascii.eqlIgnoreCase(s, "false")) return false;
    return null;
}

/// 流式 RESP 解码器：自带缓冲，可从 socket 分片读取。
pub const Reader = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    /// 按值持有：`Client` 会被按值返回/移动，存指针会悬空（Windows AFD 句柄会失效）
    stream: std.Io.net.Stream,
    /// 非空时从 TLS 明文流读取（`zzig.tls_client.Client.reader`），否则直读 socket
    tls_reader: ?*std.Io.Reader = null,
    /// 最近一次 TLS 读取的错误名（诊断用：对外统一映射成 `Io` 会丢掉 TLS 具体原因）
    tls_read_err: []const u8 = "",
    buf: []u8,
    start: usize = 0,
    end: usize = 0,
    max_message_size: usize = 1024 * 1024,
    line_buf: std.ArrayList(u8) = .empty,

    pub const Error = error{
        Protocol,
        Io,
        Timeout,
        ConnectionClosed,
        MessageTooLarge,
    };

    pub fn init(
        gpa: std.mem.Allocator,
        io: std.Io,
        stream: std.Io.net.Stream,
        buffer_size: usize,
        max_message_size: usize,
    ) !Reader {
        return .{
            .gpa = gpa,
            .io = io,
            .stream = stream,
            .buf = try gpa.alloc(u8, if (buffer_size == 0) 16 * 1024 else buffer_size),
            .max_message_size = max_message_size,
        };
    }

    pub fn deinit(self: *Reader) void {
        self.gpa.free(self.buf);
        self.line_buf.deinit(self.gpa);
    }

    /// 丢弃已缓冲的数据（连接归还池前清理）。
    pub fn clearBuffered(self: *Reader) void {
        self.start = 0;
        self.end = 0;
    }

    pub fn bufferedLen(self: *const Reader) usize {
        return self.end - self.start;
    }

    fn readSome(self: *Reader, dst: []u8) Error!usize {
        if (self.tls_reader) |tls_reader| {
            // ★ TLS 明文流**不能**用 `readVec(&.{dst})`：`std.Io.Reader.readVec` 允许实现
            // 「忽略 data、把明文写进自身缓冲并返回 0」（zzig `tls_client.zig` 正是如此：
            // 应用数据 `r.end += cleartext.len` 后 return 0，NewSessionTicket 也是 return 0）。
            // 之前把「返回 0」当成 EOF，导致握手成功后第一次读就被判成 `ConnectionClosed`
            // → `Protocol`（实测：服务端已收到 PING 并回了 +PONG，客户端却报对端关闭）。
            // 正确姿势是 `peekGreedy(1)`：内部用 vtable.readVec 填充自身缓冲，直到真有
            // 至少 1 字节明文，或真的收到 close_notify（EndOfStream）。
            const avail = tls_reader.peekGreedy(1) catch |e| {
                // TLS 具体原因（TlsAlert / TlsBadRecordMac / TlsConnectionTruncated …）
                // 在这一层还能看到，映射成 Io 后上层就分不清「对端关连接」还是「记录校验失败」。
                self.tls_read_err = @errorName(e);
                return switch (e) {
                    error.EndOfStream => 0,
                    else => error.Io,
                };
            };
            const n = @min(avail.len, dst.len);
            @memcpy(dst[0..n], avail[0..n]);
            tls_reader.toss(n);
            self.tls_read_err = "";
            return n;
        }

        var slices = [_][]u8{dst};
        // 不用 `operateTimeout`：Zig 0.17 的 Windows Threaded 后端对带超时的 I/O
        // 会返回 `error.ConcurrencyUnavailable`（与 netConnectIpWindows 的 TODO 同源）。
        // 读写超时改由客户端在套接字上设置 SO_RCVTIMEO / SO_SNDTIMEO（见 client.zig）。
        const result = self.io.operate(.{ .net_read = .{
            .socket_handle = self.stream.socket.handle,
            .data = &slices,
        } }) catch return error.Io;
        const n = result.net_read catch |e| switch (e) {
            error.ConnectionTimedOut => return error.Timeout,
            else => return error.Io,
        };
        return n.data_len;
    }

    /// 确保缓冲里至少有 1 字节；对端关闭时返回 `ConnectionClosed`。
    fn ensure(self: *Reader) Error!void {
        if (self.start < self.end) return;
        const n = try self.readSome(self.buf);
        if (n == 0) return error.ConnectionClosed;
        self.start = 0;
        self.end = n;
    }

    fn readByte(self: *Reader) Error!u8 {
        try self.ensure();
        const b = self.buf[self.start];
        self.start += 1;
        return b;
    }

    /// 读一行（去掉 CRLF）到 `line_buf`。
    fn readLine(self: *Reader) Error!void {
        self.line_buf.clearRetainingCapacity();
        while (true) {
            const b = try self.readByte();
            if (b == '\n') break;
            if (self.line_buf.items.len >= self.max_message_size) return error.MessageTooLarge;
            self.line_buf.append(self.gpa, b) catch return error.Io;
        }
        if (self.line_buf.items.len > 0 and self.line_buf.items[self.line_buf.items.len - 1] == '\r') {
            self.line_buf.items.len -= 1;
        }
    }

    fn readExactAlloc(self: *Reader, len: usize) Error![]u8 {
        if (self.max_message_size != 0 and len > self.max_message_size) return error.MessageTooLarge;
        const out = self.gpa.alloc(u8, len) catch return error.Io;
        errdefer self.gpa.free(out);

        var got: usize = 0;
        while (got < len) {
            if (self.start < self.end) {
                const n = @min(len - got, self.end - self.start);
                @memcpy(out[got .. got + n], self.buf[self.start .. self.start + n]);
                self.start += n;
                got += n;
                continue;
            }
            const n = try self.readSome(self.buf);
            if (n == 0) return error.ConnectionClosed;
            self.start = 0;
            self.end = n;
        }
        return out;
    }

    fn expectCrlf(self: *Reader) Error!void {
        const a = try self.readByte();
        const b = try self.readByte();
        if (a != '\r' or b != '\n') return error.Protocol;
    }

    /// 读取一个完整值。
    pub fn readValue(self: *Reader) Error!Value {
        try self.readLine();
        if (self.line_buf.items.len == 0) return error.Protocol;
        const kind = self.line_buf.items[0];
        const rest = self.line_buf.items[1..];

        return switch (kind) {
            '+' => .{ .simple = try self.dupeLine(rest) },
            '-' => .{ .err = try self.dupeLine(rest) },
            ':' => .{ .integer = parseI64(rest) orelse return error.Protocol },
            '$' => try self.readBulk(rest),
            '=' => try self.readVerbatim(rest),
            '*' => try self.readAggregate(rest, .array),
            '~' => try self.readAggregate(rest, .set),
            '>' => try self.readAggregate(rest, .push),
            '%' => .{ .map = try self.readPairs(rest) },
            '|' => .{ .attribute = try self.readPairs(rest) },
            ',' => .{ .double = parseF64(rest) catch return error.Protocol },
            '#' => blk: {
                if (std.mem.eql(u8, rest, "t")) break :blk .{ .boolean = true };
                if (std.mem.eql(u8, rest, "f")) break :blk .{ .boolean = false };
                return error.Protocol;
            },
            '_' => .null_,
            '(' => .{ .big_number = try self.dupeLine(rest) },
            else => error.Protocol,
        };
    }

    /// 读取回复：跳过 RESP3 属性帧，返回其后的真实回复。
    pub fn readReply(self: *Reader) Error!Value {
        while (true) {
            var value = try self.readValue();
            if (value == .attribute) {
                value.deinit(self.gpa);
                continue;
            }
            return value;
        }
    }

    fn dupeLine(self: *Reader, rest: []const u8) Error![]u8 {
        return self.gpa.dupe(u8, rest) catch return error.Io;
    }

    fn readBulk(self: *Reader, header: []const u8) Error!Value {
        const len = parseI64(header) orelse return error.Protocol;
        if (len < 0) return .null_;
        const data = try self.readExactAlloc(@intCast(len));
        errdefer self.gpa.free(data);
        try self.expectCrlf();
        return .{ .bulk = data };
    }

    fn readVerbatim(self: *Reader, header: []const u8) Error!Value {
        const len = parseI64(header) orelse return error.Protocol;
        if (len < 0) return .null_;
        const data = try self.readExactAlloc(@intCast(len));
        errdefer self.gpa.free(data);
        try self.expectCrlf();
        // 形如 "txt:some text"，去掉 3 字节格式前缀（与 Rust 一致）
        if (data.len > 4 and data[3] == ':') {
            const text = self.gpa.dupe(u8, data[4..]) catch {
                self.gpa.free(data);
                return error.Io;
            };
            self.gpa.free(data);
            return .{ .verbatim = text };
        }
        return .{ .verbatim = data };
    }

    const AggKind = enum { array, set, push };

    fn readAggregate(self: *Reader, header: []const u8, kind: AggKind) Error!Value {
        const len = parseI64(header) orelse return error.Protocol;
        if (len < 0) return .null_;
        const count: usize = @intCast(len);
        // 防止恶意长度把内存打爆：每元素至少 1 字节
        if (self.max_message_size != 0 and count > self.max_message_size) return error.MessageTooLarge;

        var items: std.ArrayList(Value) = .empty;
        errdefer {
            for (items.items) |*item| item.deinit(self.gpa);
            items.deinit(self.gpa);
        }
        items.ensureTotalCapacity(self.gpa, @min(count, 1024)) catch return error.Io;
        for (0..count) |_| {
            items.append(self.gpa, try self.readValue()) catch return error.Io;
        }
        const owned = items.toOwnedSlice(self.gpa) catch return error.Io;
        return switch (kind) {
            .array => .{ .array = owned },
            .set => .{ .set = owned },
            .push => .{ .push = owned },
        };
    }

    fn readPairs(self: *Reader, header: []const u8) Error![]Value.Pair {
        const len = parseI64(header) orelse return error.Protocol;
        const count: usize = if (len < 0) 0 else @intCast(len);
        if (self.max_message_size != 0 and count > self.max_message_size) return error.MessageTooLarge;

        var pairs: std.ArrayList(Value.Pair) = .empty;
        errdefer {
            for (pairs.items) |*pair| {
                pair.key.deinit(self.gpa);
                pair.value.deinit(self.gpa);
            }
            pairs.deinit(self.gpa);
        }
        pairs.ensureTotalCapacity(self.gpa, @min(count, 1024)) catch return error.Io;
        for (0..count) |_| {
            const k = try self.readValue();
            errdefer {
                var kk = k;
                kk.deinit(self.gpa);
            }
            const v = try self.readValue();
            pairs.append(self.gpa, .{ .key = k, .value = v }) catch return error.Io;
        }
        return pairs.toOwnedSlice(self.gpa) catch return error.Io;
    }
};

fn parseI64(bytes: []const u8) ?i64 {
    const s = std.mem.trim(u8, bytes, " \t\r\n");
    return std.fmt.parseInt(i64, s, 10) catch null;
}

// ─────────────────────────────── 测试 ───────────────────────────────

const testing = std.testing;

fn decodeBytes(gpa: std.mem.Allocator, bytes: []const u8) !Value {
    // 用回环 stream 太笨重：直接把字节塞进 Reader 的缓冲。
    const dummy_stream: std.Io.net.Stream = undefined;
    var r = Reader{
        .gpa = gpa,
        .io = std.Io.Threaded.global_single_threaded.io(),
        .stream = dummy_stream,
        .buf = try gpa.dupe(u8, bytes),
        .start = 0,
        .end = bytes.len,
    };
    defer r.deinit();
    return r.readReply();
}

test "组包与 C# GetRequest 逐字节一致" {
    const frame = try encodeCommandAlloc(testing.allocator, &.{ "SET", "key", "value" });
    defer testing.allocator.free(frame);
    try testing.expectEqualStrings("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", frame);
}

test "组包支持二进制载荷" {
    const payload = [_]u8{ 0, 1, '"', '\r', '\n', 255 };
    const frame = try encodeCommandAlloc(testing.allocator, &.{ "LPUSH", "q", &payload });
    defer testing.allocator.free(frame);
    const expected = "*3\r\n$5\r\nLPUSH\r\n$1\r\nq\r\n$6\r\n" ++ "\x00\x01\"\r\n\xff" ++ "\r\n";
    try testing.expectEqualStrings(expected, frame);
}

test "解析 RESP2 标量" {
    const a = testing.allocator;
    {
        var v = try decodeBytes(a, "+OK\r\n");
        defer v.deinit(a);
        try testing.expectEqualStrings("OK", v.asString().?);
    }
    {
        var v = try decodeBytes(a, ":42\r\n");
        defer v.deinit(a);
        try testing.expectEqual(@as(i64, 42), v.integer);
    }
    {
        var v = try decodeBytes(a, "$5\r\nvalue\r\n");
        defer v.deinit(a);
        try testing.expectEqualStrings("value", v.bulk);
    }
    {
        var v = try decodeBytes(a, "$-1\r\n");
        defer v.deinit(a);
        try testing.expect(v.isNull());
    }
    {
        var v = try decodeBytes(a, "$0\r\n\r\n");
        defer v.deinit(a);
        try testing.expectEqual(@as(usize, 0), v.bulk.len);
    }
}

test "解析嵌套数组与空元素" {
    const a = testing.allocator;
    var v = try decodeBytes(a, "*3\r\n$3\r\nfoo\r\n:7\r\n*2\r\n$1\r\na\r\n$-1\r\n");
    defer v.deinit(a);
    const items = v.asArray().?;
    try testing.expectEqual(@as(usize, 3), items.len);
    try testing.expectEqualStrings("foo", items[0].bulk);
    try testing.expectEqual(@as(i64, 7), items[1].integer);
    try testing.expect(items[2].asArray().?[1].isNull());
}

test "解析 RESP3 扩展类型" {
    const a = testing.allocator;
    {
        var v = try decodeBytes(a, "#t\r\n");
        defer v.deinit(a);
        try testing.expect(v.boolean);
    }
    {
        var v = try decodeBytes(a, ",inf\r\n");
        defer v.deinit(a);
        try testing.expect(std.math.isInf(v.double));
    }
    {
        var v = try decodeBytes(a, "=9\r\ntxt:hello\r\n");
        defer v.deinit(a);
        try testing.expectEqualStrings("hello", v.verbatim);
    }
    {
        var v = try decodeBytes(a, "%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n");
        defer v.deinit(a);
        try testing.expectEqual(@as(usize, 2), v.map.len);
        try testing.expectEqualStrings("b", v.map[1].key.bulk);
    }
    {
        var v = try decodeBytes(a, "~2\r\n:1\r\n:2\r\n");
        defer v.deinit(a);
        try testing.expectEqual(@as(usize, 2), v.set.len);
    }
    {
        var v = try decodeBytes(a, "|1\r\n$3\r\nttl\r\n:60\r\n+OK\r\n");
        defer v.deinit(a);
        try testing.expect(std.meta.activeTag(v) == .simple); // readReply 已跳过属性帧
        try testing.expectEqualStrings("OK", v.simple);
    }
}

test "非法协议标记报 Protocol" {
    try testing.expectError(error.Protocol, decodeBytes(testing.allocator, "?bad\r\n"));
    try testing.expectError(error.Protocol, decodeBytes(testing.allocator, "$\r\n"));
}

/// 模拟 zzig `tls_client.zig` 的 Reader 实现：**忽略** `data`，把明文字节写进自身缓冲，
/// 并且一律 `return 0`（应用数据与 NewSessionTicket 都是这个行为）。
const TlsLikeSource = struct {
    var source: []const u8 = "";
    var pos: usize = 0;

    fn streamImpl(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        _ = r;
        _ = w;
        _ = limit;
        return error.ReadFailed;
    }

    fn readVecImpl(r: *std.Io.Reader, data: [][]u8) std.Io.Reader.Error!usize {
        _ = data;
        if (pos >= source.len) return error.EndOfStream;
        const n = @min(source.len - pos, r.buffer.len - r.end);
        if (n == 0) return 0;
        @memcpy(r.buffer[r.end..][0..n], source[pos..][0..n]);
        r.end += n;
        pos += n;
        return 0;
    }
};

test "TLS 明文读取：实现写自身缓冲并返回 0 时不能当成 EOF" {
    const gpa = testing.allocator;
    TlsLikeSource.source = "+PONG\r\n";
    TlsLikeSource.pos = 0;

    var tls_buf: [32]u8 = undefined;
    var tls_reader = std.Io.Reader{
        .buffer = &tls_buf,
        .vtable = &.{ .stream = TlsLikeSource.streamImpl, .readVec = TlsLikeSource.readVecImpl },
        .seek = 0,
        .end = 0,
    };

    var r = Reader{
        .gpa = gpa,
        .io = std.Io.Threaded.global_single_threaded.io(),
        .stream = undefined,
        .tls_reader = &tls_reader,
        .buf = try gpa.alloc(u8, 64),
    };
    defer r.deinit();

    var v = try r.readReply();
    defer v.deinit(gpa);
    try testing.expectEqualStrings("PONG", v.asString().?);

    // 真正的 close_notify（这里由 EndOfStream 模拟）才算关闭
    try testing.expectError(error.ConnectionClosed, r.readReply());
}
