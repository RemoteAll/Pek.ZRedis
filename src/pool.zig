//! 同步连接池。
//!
//! 对齐 DH.NRedis 的 `ObjectPool<RedisClient>` 与 Rust `pek_rredis::pool`：
//! - 默认参数：`Min=10`、`Max=100000`、`IdleTime=30s`、`MaxLifetime=300s`、`WaitTimeout=15s`；
//! - 空闲过久借出前主动 `PING`（对应 C# `MyPool.OnGet`），坏连接直接销毁；
//! - 池满时等待 `WaitTimeout`；超时返回 `error.Pool` 并带「池上限/等待秒数」。

const std = @import("std");
const zzig = @import("zzig");
const client = @import("client.zig");
const options = @import("options.zig");
const resp = @import("resp.zig");

const IdleEntry = struct {
    client: *client.Client,
    idle_since_ns: i128,
};

pub const Pool = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    cfg: client.ConnConfig,
    pool_cfg: options.PoolConfig,

    mutex: std.Io.Mutex = .init,
    cond: std.Io.Condition = .init,
    idle: std.ArrayList(IdleEntry) = .empty,
    total: usize = 0,
    created: usize = 0,
    closed: bool = false,
    diag: client.Diag = .{},

    pub fn init(gpa: std.mem.Allocator, io: std.Io, cfg: client.ConnConfig, pool_cfg: options.PoolConfig) Pool {
        return .{ .gpa = gpa, .io = io, .cfg = cfg, .pool_cfg = pool_cfg };
    }

    pub fn deinit(self: *Pool) void {
        self.mutex.lockUncancelable(self.io);
        self.closed = true;
        while (self.idle.pop()) |entry| {
            entry.client.deinit();
            self.gpa.destroy(entry.client);
        }
        self.total = 0;
        self.mutex.unlock(self.io);

        self.idle.deinit(self.gpa);
        self.cfg.deinit();
    }

    pub fn lastError(self: *const Pool) []const u8 {
        return self.diag.text();
    }

    pub const Stats = struct { total: usize, idle: usize, created: usize };

    pub fn stats(self: *Pool) Stats {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return .{ .total = self.total, .idle = self.idle.items.len, .created = self.created };
    }

    /// 借出一条连接。池满时最多等待 `WaitTimeout` 秒。
    pub fn acquire(self: *Pool) client.Error!Lease {
        const wait_ns: i128 = @as(i128, @intCast(@max(self.pool_cfg.wait_timeout, 1))) * std.time.ns_per_s;
        const deadline = zzig.compat.nanoTimestamp() + wait_ns;

        while (true) {
            self.mutex.lockUncancelable(self.io);
            if (self.closed) {
                self.mutex.unlock(self.io);
                return error.Closed;
            }

            // 1) 复用空闲连接
            if (self.idle.pop()) |entry| {
                self.mutex.unlock(self.io);
                if (self.checkReusable(entry)) {
                    return .{ .pool = self, .client = entry.client };
                }
                entry.client.deinit();
                self.gpa.destroy(entry.client);
                self.mutex.lockUncancelable(self.io);
                self.total -= 1;
                self.mutex.unlock(self.io);
                continue;
            }

            // 2) 未达上限则新建（连接动作在锁外，避免阻塞其它线程）
            if (self.total < self.pool_cfg.max) {
                self.total += 1;
                self.created += 1;
                self.mutex.unlock(self.io);

                const holder = self.gpa.create(client.Client) catch {
                    self.decTotalAndSignal();
                    return error.Pool;
                };
                holder.* = client.Client.connect(self.gpa, self.io, &self.cfg) catch |e| {
                    self.setDiag("建立连接失败（{s}）：{s}", .{ self.cfg.endpoint, @errorName(e) });
                    self.gpa.destroy(holder);
                    self.decTotalAndSignal();
                    return e;
                };
                return .{ .pool = self, .client = holder };
            }

            // 3) 池满：等待归还
            const now = zzig.compat.nanoTimestamp();
            const remaining = deadline - now;
            if (remaining <= 0) {
                self.mutex.unlock(self.io);
                self.setDiag("连接池已满（上限 {d}），等待 {d} 秒后仍未借到连接（{s}）", .{
                    self.pool_cfg.max,
                    self.pool_cfg.wait_timeout,
                    self.cfg.endpoint,
                });
                return error.Pool;
            }

            self.cond.waitTimeout(self.io, &self.mutex, resp.timeoutMs(@intCast(@divTrunc(remaining, std.time.ns_per_ms)))) catch |e| {
                self.mutex.unlock(self.io);
                if (e == error.Timeout) {
                    // 超时后可能刚好有连接归还，回到循环再判一次
                    self.mutex.lockUncancelable(self.io);
                    const has_idle = self.idle.items.len > 0 or self.total < self.pool_cfg.max;
                    self.mutex.unlock(self.io);
                    if (!has_idle) {
                        self.setDiag("连接池已满（上限 {d}），等待 {d} 秒后仍未借到连接（{s}）", .{
                            self.pool_cfg.max,
                            self.pool_cfg.wait_timeout,
                            self.cfg.endpoint,
                        });
                        return error.Pool;
                    }
                    continue;
                }
                self.setDiag("等待连接时被取消（{s}）", .{self.cfg.endpoint});
                return error.Pool;
            };
            self.mutex.unlock(self.io);
        }
    }

    fn checkReusable(self: *Pool, entry: IdleEntry) bool {
        if (entry.client.isBroken()) return false;
        const idle_ns: i128 = zzig.compat.nanoTimestamp() - entry.idle_since_ns;
        if (self.pool_cfg.max_lifetime > 0) {
            const lifetime_ns: i128 = @as(i128, @intCast(self.pool_cfg.max_lifetime * 2)) * std.time.ns_per_s;
            if (idle_ns >= lifetime_ns) return false;
        }
        if (self.pool_cfg.idle_time > 0) {
            const idle_limit_ns: i128 = @as(i128, @intCast(self.pool_cfg.idle_time)) * std.time.ns_per_s;
            if (idle_ns >= idle_limit_ns) {
                entry.client.ping() catch return false;
            }
        }
        return true;
    }

    fn release(self: *Pool, c: *client.Client) void {
        if (c.isBroken()) {
            c.deinit();
            self.gpa.destroy(c);
            self.decTotalAndSignal();
            return;
        }
        c.drain();

        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.closed) {
            self.gpa.destroy(c);
            self.total -= 1;
            return;
        }

        self.idle.append(self.gpa, .{ .client = c, .idle_since_ns = zzig.compat.nanoTimestamp() }) catch {
            self.gpa.destroy(c);
            self.total -= 1;
            return;
        };

        // 修剪到最小空闲数（与 Rust/C# 一致：保留最近使用的连接）
        const keep = @min(self.pool_cfg.min, self.pool_cfg.max);
        while (self.idle.items.len > keep) {
            const old = self.idle.orderedRemove(0);
            old.client.deinit();
            self.gpa.destroy(old.client);
            self.total -= 1;
        }
        self.cond.signal(self.io);
    }

    fn decTotalAndSignal(self: *Pool) void {
        self.mutex.lockUncancelable(self.io);
        if (self.total > 0) self.total -= 1;
        self.mutex.unlock(self.io);
        self.cond.signal(self.io);
    }

    fn setDiag(self: *Pool, comptime fmt: []const u8, args: anytype) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.diag.set(fmt, args);
    }
};

/// 借出的连接；`release` / `discard` 二选一（`defer` 里调用 release 最稳妥）。
pub const Lease = struct {
    pool: *Pool,
    client: ?*client.Client,

    pub fn get(self: *Lease) *client.Client {
        return self.client.?;
    }

    pub fn release(self: *Lease) void {
        if (self.client) |c| {
            self.pool.release(c);
            self.client = null;
        }
    }

    /// 主动销毁（例如订阅连接、不可恢复错误）。
    pub fn discard(self: *Lease) void {
        if (self.client) |c| {
            c.markBroken();
            self.pool.release(c);
            self.client = null;
        }
    }
};

test "池参数默认值与 C# 一致" {
    const testing = std.testing;
    const cfg = options.PoolConfig{};
    try testing.expectEqual(@as(usize, 10), cfg.min);
    try testing.expectEqual(@as(usize, 100_000), cfg.max);
    try testing.expectEqual(@as(u64, 30), cfg.idle_time);
    try testing.expectEqual(@as(u64, 300), cfg.max_lifetime);
    try testing.expectEqual(@as(u64, 15), cfg.wait_timeout);
}
