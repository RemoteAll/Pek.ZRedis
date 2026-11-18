//! TLS 接入测试（当前仅覆盖「不静默降级」路径）。

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

test "TLS：对明文服务端握手失败且不降级" {
    const a = testing.allocator;
    var t = newIo();
    defer t.threaded.deinit();
    const io = t.io;

    const server = try MockServer.start(a, io, 16901);
    defer server.stop();
    const base = try server.configAlloc(a);
    defer a.free(base);
    const cfg = try std.fmt.allocPrint(a, "{s};Tls=1;TlsInsecure=1;timeout=800", .{base});
    defer a.free(cfg);

    var r = try pek_zredis.Redis.fromConfig(a, io, cfg);
    defer r.deinit();

    // 假服务端是明文：TLS 握手必须失败（error.Tls），绝不能静默降级成明文命令
    try testing.expectError(error.Tls, r.ping());
    try testing.expect(r.lastError().len > 0);
}
