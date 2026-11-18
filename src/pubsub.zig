//! 发布订阅（对齐 DH.NRedis `PubSub` / Rust `PubSub`）。
//!
//! 订阅占用**独立连接**（不能从连接池借，否则订阅态会污染普通命令）；
//! `nextMessage` 用短读超时轮询，超时返回 `null` 便于循环里响应退出信号。

const std = @import("std");
const resp = @import("resp.zig");
const options = @import("options.zig");
const client = @import("client.zig");

pub const Error = client.Error;

pub const Message = struct {
    /// 模式订阅时的模式串；普通订阅为 null
    pattern: ?[]u8 = null,
    channel: []u8,
    payload: []u8,

    pub fn deinit(self: *Message, gpa: std.mem.Allocator) void {
        if (self.pattern) |p| gpa.free(p);
        gpa.free(self.channel);
        gpa.free(self.payload);
    }
};

pub const PubSub = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    cfg: client.ConnConfig,
    conn: client.Client,
    closed: bool = false,

    pub fn connect(gpa: std.mem.Allocator, io: std.Io, opt: *const options.Options) !PubSub {
        var cfg = try client.ConnConfig.initFromOptions(gpa, opt, 0);
        errdefer cfg.deinit();
        const conn = try client.Client.connect(gpa, io, &cfg);
        return .{ .gpa = gpa, .io = io, .cfg = cfg, .conn = conn };
    }

    pub fn deinit(self: *PubSub) void {
        self.conn.deinit();
        self.cfg.deinit();
        self.closed = true;
    }

    pub fn lastError(self: *const PubSub) []const u8 {
        return self.conn.lastError();
    }

    pub fn subscribe(self: *PubSub, channels: []const []const u8) Error!void {
        return self.subscribeLike("SUBSCRIBE", channels);
    }

    pub fn psubscribe(self: *PubSub, patterns: []const []const u8) Error!void {
        return self.subscribeLike("PSUBSCRIBE", patterns);
    }

    fn subscribeLike(self: *PubSub, cmd: []const u8, items: []const []const u8) Error!void {
        if (items.len == 0) return;
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        try args.append(self.gpa, cmd);
        try args.appendSlice(self.gpa, items);

        self.conn.writeFramePublic(args.items) catch |e| return e;
        // 每个频道一条确认：["subscribe", channel, count]
        for (items) |_| {
            var v = try self.conn.readMessage();
            v.deinit(self.gpa);
        }
    }

    pub fn unsubscribe(self: *PubSub, channels: []const []const u8) Error!void {
        return self.subscribeLike("UNSUBSCRIBE", channels);
    }

    pub fn punsubscribe(self: *PubSub, patterns: []const []const u8) Error!void {
        return self.subscribeLike("PUNSUBSCRIBE", patterns);
    }

    /// 读取一条消息；`timeout_ms` 内无消息返回 `null`。
    pub fn nextMessage(self: *PubSub, timeout_ms: u64) Error!?Message {
        self.conn.setReadTimeoutMs(@intCast(timeout_ms));
        var v = self.conn.readMessage() catch |e| switch (e) {
            error.Timeout => return null,
            else => return e,
        };
        defer v.deinit(self.gpa);

        const items = v.asArray() orelse return null;
        if (items.len < 3) return null;
        const kind = items[0].asString() orelse return null;

        if (std.ascii.eqlIgnoreCase(kind, "message") and items.len >= 3) {
            return .{
                .channel = try self.gpa.dupe(u8, items[1].asString() orelse ""),
                .payload = try self.gpa.dupe(u8, items[2].asString() orelse ""),
            };
        }
        if (std.ascii.eqlIgnoreCase(kind, "pmessage") and items.len >= 4) {
            return .{
                .pattern = try self.gpa.dupe(u8, items[1].asString() orelse ""),
                .channel = try self.gpa.dupe(u8, items[2].asString() orelse ""),
                .payload = try self.gpa.dupe(u8, items[3].asString() orelse ""),
            };
        }
        return null;
    }
};
