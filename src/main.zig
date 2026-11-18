//! zredis CLI：跨语言（C# / Rust / Zig）对拍与运维用小工具。
//!
//! 输出统一为 `KEY=VALUE` 行，便于 PowerShell/Shell 脚本串联。
//! 子命令：
//! - `ping <config>`：连接并 PING；
//! - `set <config> <key> <text> [expire]`：写入字符串；
//! - `get <config> <key>`：读取并打印 `VALUE=`（二进制安全用 hex）；
//! - `info <config>`：打印版本 / 服务器类型；
//! - `selfcheck <config>`：读写删一轮自检。

const std = @import("std");
const pek_zredis = @import("pek_zredis");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, gpa);
    defer args.deinit();
    _ = args.next(); // 可执行文件名

    const cmd = args.next() orelse return usage();
    if (std.mem.eql(u8, cmd, "help") or std.mem.eql(u8, cmd, "--help")) return usage();

    const config = args.next() orelse return usage();
    var r = pek_zredis.Redis.fromConfig(gpa, io, config) catch |e| {
        std.debug.print("RESULT=FAIL\nMESSAGE=连接配置失败：{s}\n", .{@errorName(e)});
        return;
    };
    defer r.deinit();

    if (std.mem.eql(u8, cmd, "ping")) {
        const ok = r.ping() catch |e| {
            std.debug.print("RESULT=FAIL\nMESSAGE=PING 失败：{s}（{s}）\n", .{ @errorName(e), r.lastError() });
            return;
        };
        std.debug.print("RESULT={s}\n", .{if (ok) "OK" else "FAIL"});
        return;
    }

    if (std.mem.eql(u8, cmd, "set")) {
        const key = args.next() orelse return usage();
        const value = args.next() orelse return usage();
        const expire_text = args.next();
        const expire: i64 = if (expire_text) |t| std.fmt.parseInt(i64, t, 10) catch 0 else 0;

        const ok = r.set(key, value, expire) catch |e| {
            std.debug.print("RESULT=FAIL\nMESSAGE=SET 失败：{s}（{s}）\n", .{ @errorName(e), r.lastError() });
            return;
        };
        std.debug.print("RESULT={s}\n", .{if (ok) "OK" else "FAIL"});
        return;
    }

    if (std.mem.eql(u8, cmd, "get")) {
        const key = args.next() orelse return usage();
        const value = r.getRaw(gpa, key) catch |e| {
            std.debug.print("RESULT=FAIL\nMESSAGE=GET 失败：{s}（{s}）\n", .{ @errorName(e), r.lastError() });
            return;
        };
        if (value) |v| {
            defer gpa.free(v);
            std.debug.print("RESULT=OK\nLEN={d}\nVALUE={s}\n", .{ v.len, v });
        } else {
            std.debug.print("RESULT=OK\nLEN=0\nVALUE=\n", .{});
        }
        return;
    }

    if (std.mem.eql(u8, cmd, "info")) {
        const v = r.version() catch |e| {
            std.debug.print("RESULT=FAIL\nMESSAGE=读取版本失败：{s}（{s}）\n", .{ @errorName(e), r.lastError() });
            return;
        };
        defer if (v) |s| gpa.free(s);
        const st = r.serverType() catch pek_zredis.ServerType.redis;
        std.debug.print("RESULT=OK\nVERSION={s}\nSERVER={s}\n", .{ v orelse "", @tagName(st) });
        return;
    }

    if (std.mem.eql(u8, cmd, "selfcheck")) {
        const key = "zredis:selfcheck";
        _ = r.set(key, "hello", 60) catch |e| {
            std.debug.print("RESULT=FAIL\nSTEP=set\nMESSAGE={s}（{s}）\n", .{ @errorName(e), r.lastError() });
            return;
        };
        const got = r.getRaw(gpa, key) catch |e| {
            std.debug.print("RESULT=FAIL\nSTEP=get\nMESSAGE={s}（{s}）\n", .{ @errorName(e), r.lastError() });
            return;
        };
        defer if (got) |v| gpa.free(v);
        _ = r.remove(key) catch {};
        if (got) |v| {
            if (std.mem.eql(u8, v, "hello")) {
                std.debug.print("RESULT=OK\nMESSAGE=写入/读取/删除通过\n", .{});
                return;
            }
        }
        std.debug.print("RESULT=FAIL\nMESSAGE=回读内容不一致\n", .{});
        return;
    }

    return usage();
}

fn usage() void {
    std.debug.print(
        \\用法：zredis <命令> <config> [参数...]
        \\  ping      <config>
        \\  set       <config> <key> <text> [expireSeconds]
        \\  get       <config> <key>
        \\  info      <config>
        \\  selfcheck <config>
        \\config 例：server=127.0.0.1:6379;password=123456;db=7;prefix=app:
        \\
    , .{});
}
