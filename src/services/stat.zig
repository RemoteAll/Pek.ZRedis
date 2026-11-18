//! Redis 流式统计服务（对齐 DH.NRedis `Services.RedisStat` / Rust `RedisStat`）。
//!
//! 语义：
//! - 统计值写在主库 HASH（`HINCRBY`）；
//! - 待处理 key 进入**备份库**（`db == 15 ? 0 : db + 1`）的延迟队列 / 可靠队列；
//! - 消费时先 `RENAME` 出快照，再读回 HASH，成功保存后删除快照；
//! - 延迟转移与可靠消费由调用方驱动（`transferDueOnce` / `consumeOnce`）。

const std = @import("std");
const zzig = @import("zzig");
const full_mod = @import("../full.zig");
const hash_mod = @import("../hash.zig");
const queue_mod = @import("../queues/queue.zig");
const reliable_mod = @import("../queues/reliable.zig");

pub const FullRedis = full_mod.FullRedis;
pub const Error = @import("../redis.zig").Error;

pub const FieldPair = struct { field: []const u8, value: i64 };

pub const RedisStat = struct {
    gpa: std.mem.Allocator,
    redis: *FullRedis,
    /// 备份库连接（延迟队列 / 可靠队列落在这里）
    backup: FullRedis,
    name: []u8,

    pub fn init(redis: *FullRedis, name: []const u8) !RedisStat {
        const gpa = redis.gpa;
        const db = redis.redis.opts.db;
        const bak_db: i32 = if (db == 15) 0 else db + 1;
        var backup = try redis.createSub(bak_db);
        errdefer backup.deinit();
        // 备份库队列名与主库一致（含前缀）
        const key = try redis.keyAlloc(name);
        errdefer gpa.free(key);
        return .{ .gpa = gpa, .redis = redis, .backup = backup, .name = key };
    }

    pub fn deinit(self: *RedisStat) void {
        self.backup.deinit();
        self.gpa.free(self.name);
    }

    pub fn nameKey(self: *const RedisStat) []const u8 {
        return self.name;
    }

    /// 统计累加（`HINCRBY`）。
    pub fn increment(self: *RedisStat, key: []const u8, field: []const u8, value: i32) Error!i64 {
        var h = try self.redis.getHash(key);
        defer h.deinit();
        return h.incrBy(field, value);
    }

    /// 把 key 放入延迟队列；`exists:{key}` 做 10 分钟去重。
    pub fn addDelayQueue(self: *RedisStat, key: []const u8, delay_seconds: i64) Error!i64 {
        const exists_key = try std.fmt.allocPrint(self.gpa, "exists:{s}", .{key});
        defer self.gpa.free(exists_key);
        const added = try self.redis.add(exists_key, "1", 600);
        if (!added) return 0;

        var rq = self.backup.getReliableQueue(self.name) catch return error.IoError;
        defer rq.deinit();
        return rq.addDelay(key, delay_seconds);
    }

    /// 立即转移一批已到期延迟消息到主可靠队列，返回转移条数。
    pub fn transferDueOnce(self: *RedisStat, batch_size: usize) Error!usize {
        var rq = self.backup.getReliableQueue(self.name) catch return error.IoError;
        defer rq.deinit();
        var delay = rq.delayQueue() catch return error.IoError;
        defer delay.deinit();

        const messages = try delay.takeDue(self.gpa, batch_size);
        defer {
            for (messages) |m| self.gpa.free(m);
            self.gpa.free(messages);
        }
        if (messages.len == 0) return 0;

        var target = self.backup.getQueue(self.name) catch return error.IoError;
        defer target.deinit();
        _ = try target.addMany(messages);
        return messages.len;
    }

    /// 处理单个统计 key：先重命名快照，再读回 HASH，成功保存后删除快照。
    pub fn processKey(self: *RedisStat, key: []const u8, ctx: anytype) Error!bool {
        const snapshot = try std.fmt.allocPrint(self.gpa, "{s}:__stat_snapshot", .{key});
        defer self.gpa.free(snapshot);

        if (!(try self.redis.containsKey(snapshot))) {
            if (!(try self.redis.rename(key, snapshot, true))) return false;
            const exists_key = try std.fmt.allocPrint(self.gpa, "exists:{s}", .{key});
            defer self.gpa.free(exists_key);
            _ = try self.redis.remove(exists_key);
        }

        var h = try self.redis.getHash(snapshot);
        defer h.deinit();
        const raw = try h.getAll(self.gpa);
        defer {
            for (raw) |p| {
                self.gpa.free(p.field);
                self.gpa.free(p.value);
            }
            self.gpa.free(raw);
        }

        var values: std.ArrayList(FieldPair) = .empty;
        defer values.deinit(self.gpa);
        for (raw) |p| {
            const n = std.fmt.parseInt(i64, p.value, 10) catch {
                self.redis.redis.setErrorDiag("统计值不是整数：field={s} value={s}", .{ p.field, p.value });
                return error.Type;
            };
            if (n < std.math.minInt(i32) or n > std.math.maxInt(i32)) {
                self.redis.redis.setErrorDiag("统计值超出 i32 范围：field={s} value={d}", .{ p.field, n });
                return error.Type;
            }
            try values.append(self.gpa, .{ .field = p.field, .value = n });
        }

        try ctx.onSave(key, values.items);
        _ = try self.redis.remove(snapshot);
        return true;
    }

    /// 处理一条统计 key；无消息返回 false。
    pub fn consumeOnce(self: *RedisStat, timeout_seconds: i64, ctx: anytype) Error!bool {
        var rq = self.backup.getReliableQueue(self.name) catch return error.IoError;
        defer rq.deinit();

        const key = (try rq.takeOne(self.gpa, timeout_seconds)) orelse return false;
        defer self.gpa.free(key);

        _ = try self.processKey(key, ctx);
        _ = try rq.acknowledge(&.{key});
        return true;
    }
};
