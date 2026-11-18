//! Redis Stream 队列（对齐 DH.NRedis `RedisStream` / Rust `Pek.RRedis::queues::stream`）。
//!
//! 语义要点：
//! - 生产：基元 → 单字段 `__data`；对象 → 属性扁平化；数组 → 两两成对的字段名/值；
//!   每 1000 条带一次 `MAXLEN ~` 裁剪；
//! - 消费组：优先消费 `retry_ack` 抢回的挂起消息，再 `>` 读新消息，最后 `0` 补读自身挂起；
//! - 独立消费：`XREAD` 按 `start_id` 顺序推进游标；
//! - 死信：按 `retry_interval_seconds` 节流，`XPENDING` + `XCLAIM` 抢占超时未确认消息，
//!   投递次数达到 `max_retry` 直接确认丢弃；顺带清理空闲超过 1 小时的消费者。

const std = @import("std");
const zzig = @import("zzig");
const resp = @import("../resp.zig");
const encoder = @import("../encoder.zig");
const redis_mod = @import("../redis.zig");
const full_mod = @import("../full.zig");
const base = @import("base.zig");
const status_mod = @import("status.zig");

pub const Redis = redis_mod.Redis;
pub const Error = redis_mod.Error;
pub const FullRedis = full_mod.FullRedis;
pub const Value = resp.Value;

pub const Field = struct { name: []const u8, value: []const u8 };

/// 单条 Stream 消息：Id + 扁平字段数组（`[字段名, 字段值, ...]`）。
pub const Message = struct {
    gpa: std.mem.Allocator,
    id: []u8,
    body: [][]u8,

    pub fn deinit(self: *Message) void {
        self.gpa.free(self.id);
        for (self.body) |s| self.gpa.free(s);
        self.gpa.free(self.body);
    }

    /// 字段值（按字段名查找）。
    pub fn field(self: *const Message, name: []const u8) ?[]const u8 {
        var i: usize = 0;
        while (i + 1 < self.body.len) : (i += 2) {
            if (std.mem.eql(u8, self.body[i], name)) return self.body[i + 1];
        }
        return null;
    }

    /// 基元消息的原始值（与 C# `Message.GetBody` 的 `__data` 分支一致）。
    pub fn primitiveValue(self: *const Message) ?[]const u8 {
        return self.primitiveValueWith("__data");
    }

    /// 指定基元字段名的原始值。
    pub fn primitiveValueWith(self: *const Message, primitive_key: []const u8) ?[]const u8 {
        if (self.body.len == 2 and std.mem.eql(u8, self.body[0], primitive_key)) return self.body[1];
        return null;
    }

    /// 基元消息解码（标量、时间、JSON 等，规则同编码器）。
    pub fn primitive(self: *const Message, comptime T: type, gpa: std.mem.Allocator) !?T {
        const raw = self.primitiveValue() orelse return null;
        return encoder.decodeValueAlloc(T, gpa, raw) catch null;
    }

    /// 字段映射为 JSON 文本（值用启发式类型解析）。
    pub fn jsonTextAlloc(self: *const Message, gpa: std.mem.Allocator) ![]u8 {
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(gpa);
        try buf.append(gpa, '{');
        var i: usize = 0;
        while (i + 1 < self.body.len) : (i += 2) {
            if (i > 0) try buf.append(gpa, ',');
            try appendJsonString(&buf, gpa, self.body[i]);
            try buf.append(gpa, ':');
            try appendHeuristicValue(&buf, gpa, self.body[i + 1]);
        }
        try buf.append(gpa, '}');
        return buf.toOwnedSlice(gpa);
    }

    /// 字段映射为结构体（先全部按字符串，失败再回退启发式类型）。
    ///
    /// 返回 `Parsed(T)`，调用方负责 `deinit()`。
    pub fn toStruct(self: *const Message, comptime T: type, gpa: std.mem.Allocator) !?std.json.Parsed(T) {
        if (self.body.len == 2 and std.mem.eql(u8, self.body[0], "__data")) return null;

        // 全部按字符串（保留 "0012" 这类前导零）
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(gpa);
        try buf.append(gpa, '{');
        var i: usize = 0;
        while (i + 1 < self.body.len) : (i += 2) {
            if (i > 0) try buf.append(gpa, ',');
            try appendJsonString(&buf, gpa, self.body[i]);
            try buf.append(gpa, ':');
            try appendJsonString(&buf, gpa, self.body[i + 1]);
        }
        try buf.append(gpa, '}');

        if (std.json.parseFromSlice(T, gpa, buf.items, .{ .ignore_unknown_fields = true, .allocate = .alloc_always })) |parsed| return parsed else |_| {}

        const heuristic = try self.jsonTextAlloc(gpa);
        defer gpa.free(heuristic);
        return std.json.parseFromSlice(T, gpa, heuristic, .{ .ignore_unknown_fields = true, .allocate = .alloc_always }) catch null;
    }
};

fn appendJsonString(buf: *std.ArrayList(u8), gpa: std.mem.Allocator, text: []const u8) !void {
    const quoted = try std.json.Stringify.valueAlloc(gpa, text, .{});
    defer gpa.free(quoted);
    try buf.appendSlice(gpa, quoted);
}

/// 启发式字段值：对象/数组/数字/布尔/null 原样写入，其余按字符串转义。
fn appendHeuristicValue(buf: *std.ArrayList(u8), gpa: std.mem.Allocator, text: []const u8) !void {
    if (text.len > 0) {
        if (std.json.parseFromSlice(std.json.Value, gpa, text, .{})) |parsed| {
            var p = parsed;
            defer p.deinit();
            switch (p.value) {
                .object, .array, .integer, .float, .bool, .null => {
                    try buf.appendSlice(gpa, text);
                    return;
                },
                else => {},
            }
        } else |_| {}
    }
    try appendJsonString(buf, gpa, text);
}

/// 消息流信息（对应 C# `StreamInfo`）。
pub const StreamInfo = struct {
    length: i64 = 0,
    radix_tree_keys: i64 = 0,
    radix_tree_nodes: i64 = 0,
    groups: i64 = 0,
    last_generated_id: ?[]u8 = null,
    first_id: ?[]u8 = null,
    first_values: [][]u8 = &.{},
    last_id: ?[]u8 = null,
    last_values: [][]u8 = &.{},
    entries_added: i64 = 0,
    /// 最后生成时间（按 Id 毫秒时间戳换算），0 表示未知
    last_generated_seconds: i64 = 0,

    pub fn deinit(self: *StreamInfo, gpa: std.mem.Allocator) void {
        if (self.last_generated_id) |s| gpa.free(s);
        if (self.first_id) |s| gpa.free(s);
        if (self.last_id) |s| gpa.free(s);
        freeStringList(gpa, self.first_values);
        freeStringList(gpa, self.last_values);
        self.* = .{};
    }

    /// 从 `XINFO STREAM` 应答解析（借用 `items`，内部复制）。
    pub fn fromItems(gpa: std.mem.Allocator, items: []const Value) !StreamInfo {
        var info = StreamInfo{};
        errdefer info.deinit(gpa);
        var i: usize = 0;
        while (i + 1 < items.len) : (i += 2) {
            const key = items[i].asString() orelse continue;
            const value = &items[i + 1];
            if (std.mem.eql(u8, key, "length")) {
                info.length = value.asInt() orelse 0;
            } else if (std.mem.eql(u8, key, "radix-tree-keys")) {
                info.radix_tree_keys = value.asInt() orelse 0;
            } else if (std.mem.eql(u8, key, "radix-tree-nodes")) {
                info.radix_tree_nodes = value.asInt() orelse 0;
            } else if (std.mem.eql(u8, key, "groups")) {
                info.groups = value.asInt() orelse 0;
            } else if (std.mem.eql(u8, key, "entries-added")) {
                info.entries_added = value.asInt() orelse 0;
            } else if (std.mem.eql(u8, key, "last-generated-id")) {
                if (value.asString()) |s| info.last_generated_id = try gpa.dupe(u8, s);
            } else if (std.mem.eql(u8, key, "first-entry")) {
                const msg = (try parseEntry(gpa, value.*)) orelse continue;
                info.first_id = try gpa.dupe(u8, msg.id);
                info.first_values = msg.body;
                gpa.free(msg.id);
            } else if (std.mem.eql(u8, key, "last-entry")) {
                const msg = (try parseEntry(gpa, value.*)) orelse continue;
                info.last_id = try gpa.dupe(u8, msg.id);
                info.last_values = msg.body;
                gpa.free(msg.id);
            }
        }
        if (info.last_generated_id) |id| {
            if (idToSeconds(id)) |secs| info.last_generated_seconds = secs;
        }
        return info;
    }
};

/// 消费组信息（对应 C# `GroupInfo`）。
pub const GroupInfo = struct {
    name: ?[]u8 = null,
    consumers: i64 = 0,
    pending: i64 = 0,
    last_delivered_id: ?[]u8 = null,
    last_delivered_seconds: i64 = 0,

    pub fn deinit(self: *GroupInfo, gpa: std.mem.Allocator) void {
        if (self.name) |s| gpa.free(s);
        if (self.last_delivered_id) |s| gpa.free(s);
        self.* = .{};
    }

    pub fn fromItems(gpa: std.mem.Allocator, items: []const Value) !GroupInfo {
        var g = GroupInfo{};
        errdefer g.deinit(gpa);
        var i: usize = 0;
        while (i + 1 < items.len) : (i += 2) {
            const key = items[i].asString() orelse continue;
            const value = &items[i + 1];
            if (std.mem.eql(u8, key, "name")) {
                if (value.asString()) |s| g.name = try gpa.dupe(u8, s);
            } else if (std.mem.eql(u8, key, "consumers")) {
                g.consumers = value.asInt() orelse 0;
            } else if (std.mem.eql(u8, key, "pending")) {
                g.pending = value.asInt() orelse 0;
            } else if (std.mem.eql(u8, key, "last-delivered-id")) {
                if (value.asString()) |s| g.last_delivered_id = try gpa.dupe(u8, s);
            }
        }
        if (g.last_delivered_id) |id| {
            if (idToSeconds(id)) |secs| g.last_delivered_seconds = secs;
        }
        return g;
    }
};

/// 消费者信息（对应 C# `ConsumerInfo`）。
pub const ConsumerInfo = struct {
    name: ?[]u8 = null,
    pending: i64 = 0,
    idle: i64 = 0,

    pub fn deinit(self: *ConsumerInfo, gpa: std.mem.Allocator) void {
        if (self.name) |s| gpa.free(s);
        self.* = .{};
    }

    pub fn fromItems(gpa: std.mem.Allocator, items: []const Value) !ConsumerInfo {
        var c = ConsumerInfo{};
        errdefer c.deinit(gpa);
        var i: usize = 0;
        while (i + 1 < items.len) : (i += 2) {
            const key = items[i].asString() orelse continue;
            const value = &items[i + 1];
            if (std.mem.eql(u8, key, "name")) {
                if (value.asString()) |s| c.name = try gpa.dupe(u8, s);
            } else if (std.mem.eql(u8, key, "pending")) {
                c.pending = value.asInt() orelse 0;
            } else if (std.mem.eql(u8, key, "idle")) {
                c.idle = value.asInt() orelse 0;
            }
        }
        return c;
    }
};

/// 等待信息汇总（对应 C# `PendingInfo`，`XPENDING key group`）。
pub const PendingInfo = struct {
    count: i64 = 0,
    start_id: ?[]u8 = null,
    end_id: ?[]u8 = null,
    consumers: []ConsumerCount = &.{},

    pub const ConsumerCount = struct { name: []u8, count: i64 };

    pub fn deinit(self: *PendingInfo, gpa: std.mem.Allocator) void {
        if (self.start_id) |s| gpa.free(s);
        if (self.end_id) |s| gpa.free(s);
        for (self.consumers) |c| gpa.free(c.name);
        if (self.consumers.len > 0) gpa.free(self.consumers);
        self.* = .{};
    }

    pub fn fromItems(gpa: std.mem.Allocator, items: []const Value) !PendingInfo {
        var p = PendingInfo{};
        errdefer p.deinit(gpa);
        if (items.len > 0) p.count = items[0].asInt() orelse 0;
        if (items.len > 1) {
            if (items[1].asString()) |s| p.start_id = try gpa.dupe(u8, s);
        }
        if (items.len > 2) {
            if (items[2].asString()) |s| p.end_id = try gpa.dupe(u8, s);
        }
        if (items.len > 3) {
            const list = items[3].asArray() orelse &.{};
            var consumers = try gpa.alloc(PendingInfo.ConsumerCount, list.len);
            var filled: usize = 0;
            errdefer {
                for (consumers[0..filled]) |c| gpa.free(c.name);
                gpa.free(consumers);
            }
            for (list) |item| {
                const pair = item.asArray() orelse continue;
                if (pair.len != 2) continue;
                const name = pair[0].asString() orelse continue;
                consumers[filled] = .{ .name = try gpa.dupe(u8, name), .count = pair[1].asInt() orelse 0 };
                filled += 1;
            }
            p.consumers = consumers[0..filled];
        }
        return p;
    }
};

/// 等待项（对应 C# `PendingItem`）。
pub const PendingItem = struct {
    gpa: std.mem.Allocator,
    id: []u8 = &.{},
    consumer: []u8 = &.{},
    idle: i64 = 0,
    delivery: i64 = 0,

    pub fn deinit(self: *PendingItem) void {
        self.gpa.free(self.id);
        self.gpa.free(self.consumer);
        self.* = .{ .gpa = self.gpa };
    }

    pub fn fromItems(gpa: std.mem.Allocator, items: []const Value) !PendingItem {
        var p = PendingItem{ .gpa = gpa };
        const id = if (items.len > 0) (items[0].asString() orelse "") else "";
        const consumer = if (items.len > 1) (items[1].asString() orelse "") else "";
        p.id = try gpa.dupe(u8, id);
        errdefer gpa.free(p.id);
        p.consumer = try gpa.dupe(u8, consumer);
        if (items.len > 2) p.idle = items[2].asInt() orelse 0;
        if (items.len > 3) p.delivery = items[3].asInt() orelse 0;
        return p;
    }
};

/// Redis Stream 队列。
pub const RedisStream = struct {
    gpa: std.mem.Allocator,
    redis: *Redis,
    key: []u8,
    settings: base.Settings = .{},

    /// 死信重试间隔（秒）。默认 60，对应 C# `RetryInterval`
    retry_interval_seconds: i64 = 60,
    /// 基元类型消息的字段名。默认 `__data`
    primitive_key: []u8,
    /// 最大长度，超过则裁剪（生产时每 1000 条带一次 `MAXLEN ~`）。默认 100 万
    max_length: i64 = 1_000_000,
    /// 最大重试次数，超过则丢弃死信。默认 10
    max_retry: i64 = 10,
    /// 阻塞读取毫秒数。默认 15000，对应 C# `BlockTime`
    block_time_ms: i64 = 15_000,
    /// 独立消费时的起始 Id（消费组模式不使用）。默认 `0-0`
    start_id: []u8,
    /// 消费组名称（设置后使用消费组消费）
    group_name: ?[]u8 = null,
    /// 消费者名称（同组内唯一）
    consumer: []u8,
    /// 首次消费策略：true 表示从最后位置开始（`SETID $`）
    from_last_offset: bool = false,

    count: std.atomic.Value(i64) = .init(0),
    set_group_id: std.atomic.Value(i32) = .init(0),
    claims: std.atomic.Value(i64) = .init(0),
    next_retry_ms: i64 = 0,

    pub fn init(redis: *Redis, key: []const u8) !RedisStream {
        const gpa = redis.gpa;
        const consumer_suffix = status_mod.newConsumerKey(redis.io);
        const consumer = try std.fmt.allocPrint(gpa, "unknown@{s}", .{&consumer_suffix});
        errdefer gpa.free(consumer);
        const primitive_key = try gpa.dupe(u8, "__data");
        errdefer gpa.free(primitive_key);
        const start_id = try gpa.dupe(u8, "0-0");
        errdefer gpa.free(start_id);
        return .{
            .gpa = gpa,
            .redis = redis,
            .key = try gpa.dupe(u8, key),
            .primitive_key = primitive_key,
            .start_id = start_id,
            .consumer = consumer,
        };
    }

    pub fn deinit(self: *RedisStream) void {
        self.settings.deinit(self.gpa);
        self.gpa.free(self.key);
        self.gpa.free(self.primitive_key);
        self.gpa.free(self.start_id);
        self.gpa.free(self.consumer);
        if (self.group_name) |s| self.gpa.free(s);
    }

    /// 队列公共设置（可变）。
    pub fn settingsMut(self: *RedisStream) *base.Settings {
        return &self.settings;
    }

    /// 实际键名（含前缀）。
    pub fn keyName(self: *const RedisStream) []const u8 {
        return self.key;
    }

    /// 当前消费组（无则 null）。
    pub fn groupName(self: *const RedisStream) ?[]const u8 {
        return self.group_name;
    }

    /// 设置消费组，不存在则自动创建（对应 C# `SetGroup`）。
    pub fn setGroup(self: *RedisStream, name: []const u8) Error!bool {
        // 先复制再释放：调用方可能传入 `self.group_name` 自身
        const copy = try self.gpa.dupe(u8, name);
        if (self.group_name) |s| self.gpa.free(s);
        self.group_name = copy;

        const groups = try self.getGroups();
        defer freeGroupInfos(self.gpa, groups);
        for (groups) |g| {
            if (g.name) |n| {
                if (std.mem.eql(u8, n, name)) return false;
            }
        }
        return self.groupCreate(name, null);
    }

    /// 消息总数（`XLEN`）。
    pub fn countMessages(self: *RedisStream) Error!i64 {
        var v = try self.redis.execute(&.{ "XLEN", self.key });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    pub fn isEmpty(self: *RedisStream) Error!bool {
        return (try self.countMessages()) == 0;
    }

    // ───────────────────────── 生产 ─────────────────────────

    /// 生产消息（`XADD`）。编码规则同 C# `AddInternal`。
    pub fn add(self: *RedisStream, value: anytype, msg_id: ?[]const u8) Error!?[]u8 {
        var fields = try valueToFields(self.gpa, value, self.primitive_key);
        defer freeFieldsList(self.gpa, &fields);
        const n = self.count.fetchAdd(1, .acq_rel) + 1;
        const trim = self.max_length > 0 and @mod(n, 1000) == 0;
        return self.addFields(fields.items, msg_id, trim);
    }

    /// 直接写字段（`XADD`）。
    pub fn addFields(self: *RedisStream, fields: []const Field, msg_id: ?[]const u8, trim: bool) Error!?[]u8 {
        if (fields.len == 0) return error.Type;

        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        try args.append(self.gpa, "XADD");
        try args.append(self.gpa, self.key);
        var len_buf: [24]u8 = undefined;
        if (trim) {
            try args.append(self.gpa, "MAXLEN");
            try args.append(self.gpa, "~");
            try args.append(self.gpa, std.fmt.bufPrint(&len_buf, "{d}", .{self.max_length}) catch return error.Config);
        }
        try args.append(self.gpa, msg_id orelse "*");
        for (fields) |f| {
            try args.append(self.gpa, f.name);
            try args.append(self.gpa, f.value);
        }

        var attempt: usize = 0;
        while (attempt <= self.settings.retry_times_when_send_failed) : (attempt += 1) {
            var v = try self.redis.execute(args.items);
            defer v.deinit(self.gpa);
            if (v.asString()) |id| return try self.gpa.dupe(u8, id);
            if (attempt < self.settings.retry_times_when_send_failed) {
                zzig.compat.sleep(self.settings.retry_interval_ms * std.time.ns_per_ms);
            }
        }
        if (self.settings.throw_on_failure) {
            self.redis.setErrorDiag("发布到队列[{s}]失败！", .{self.key});
            return error.Server;
        }
        return null;
    }

    // ───────────────────────── 消费 ─────────────────────────

    /// 批量消费原始消息（对应 C# `TakeMessagesAsync`）。
    pub fn takeMessages(self: *RedisStream, count: usize, block_ms: i64) Error![]Message {
        if (self.group_name) |group| {
            if (self.from_last_offset and self.set_group_id.swap(1, .acq_rel) == 0) {
                _ = try self.groupSetId(group, "$");
            }

            const stolen = try self.retryAck();
            if (stolen > 0) _ = self.claims.fetchAdd(@intCast(stolen), .acq_rel);

            if (self.claims.load(.acquire) > 0) {
                const rs = try self.readGroup(group, self.consumer, count, 3_000, "0");
                if (rs.len > 0) {
                    _ = self.claims.fetchSub(@intCast(rs.len), .acq_rel);
                    return rs;
                }
                self.claims.store(0, .release);
            }
        }

        const rs = if (self.group_name) |group|
            try self.readGroup(group, self.consumer, count, block_ms, ">")
        else
            try self.readBlocking(self.start_id, count, block_ms);

        if (rs.len > 0) {
            if (self.group_name == null) {
                self.gpa.free(self.start_id);
                self.start_id = try self.gpa.dupe(u8, rs[rs.len - 1].id);
            }
            return rs;
        }

        if (self.group_name) |group| {
            return self.readGroup(group, self.consumer, count, 3_000, "0");
        }
        return rs;
    }

    /// 消费一条原始消息。
    pub fn takeMessage(self: *RedisStream) Error!?Message {
        const list = try self.takeMessages(1, 0);
        defer self.gpa.free(list); // 元素已转移出去
        if (list.len == 0) return null;
        // 其余元素释放（count=1 时通常没有）
        var i: usize = 1;
        while (i < list.len) : (i += 1) list[i].deinit();
        return list[0];
    }

    /// 批量消费基元消息（`__data` 字段解码）。
    pub fn takeBodies(self: *RedisStream, comptime T: type, gpa: std.mem.Allocator, count: usize) Error![]T {
        const messages = try self.takeMessages(count, 0);
        defer self.gpa.free(messages);
        defer for (messages) |*m| m.deinit();

        var out: std.ArrayList(T) = .empty;
        errdefer out.deinit(gpa);
        for (messages) |*m| {
            if (try m.primitive(T, gpa)) |value| try out.append(gpa, value);
        }
        return out.toOwnedSlice(gpa);
    }

    /// 批量消费对象消息（字段映射为结构体）。
    pub fn takeStructs(self: *RedisStream, comptime T: type, gpa: std.mem.Allocator, count: usize) Error![]T {
        const messages = try self.takeMessages(count, 0);
        defer self.gpa.free(messages);
        defer for (messages) |*m| m.deinit();

        var out: std.ArrayList(T) = .empty;
        errdefer out.deinit(gpa);
        for (messages) |*m| {
            if (try m.toStruct(T, gpa)) |parsed| {
                var p = parsed;
                defer p.deinit();
                try out.append(gpa, p.value);
            }
        }
        return out.toOwnedSlice(gpa);
    }

    /// 消费一条基元消息。
    pub fn takeOneBody(self: *RedisStream, comptime T: type, gpa: std.mem.Allocator) Error!?T {
        const list = try self.takeBodies(T, gpa, 1);
        defer gpa.free(list);
        if (list.len == 0) return null;
        return list[0];
    }

    /// 消费一条对象消息。
    pub fn takeOneStruct(self: *RedisStream, comptime T: type, gpa: std.mem.Allocator) Error!?T {
        const list = try self.takeStructs(T, gpa, 1);
        defer gpa.free(list);
        if (list.len == 0) return null;
        return list[0];
    }

    /// 确认消息（`XACK`）。返回确认成功条数。
    pub fn acknowledge(self: *RedisStream, ids: []const []const u8) Error!i64 {
        const group = self.group_name orelse return 0;
        if (ids.len == 0) return 0;

        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        try args.append(self.gpa, "XACK");
        try args.append(self.gpa, self.key);
        try args.append(self.gpa, group);
        try args.appendSlice(self.gpa, ids);

        var v = try self.redis.execute(args.items);
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    /// 处理未确认死信（对应 C# `RetryAck`）。返回本次抢回的消息数。
    pub fn retryAck(self: *RedisStream) Error!usize {
        const group = if (self.group_name) |g|
            try self.gpa.dupe(u8, g)
        else
            return 0;
        defer self.gpa.free(group);

        const now_ms: i64 = @intCast(@divTrunc(zzig.compat.nanoTimestamp(), std.time.ns_per_ms));
        if (self.next_retry_ms >= now_ms) return 0;
        self.next_retry_ms = now_ms + self.retry_interval_seconds * 1000;

        // 消费组不存在时自动创建（Redis 重启/主从切换后可能丢失）
        _ = try self.setGroup(group);

        const ms_idle = self.retry_interval_seconds * 1000;
        var count: usize = 0;
        var start_id: ?[]u8 = null;
        defer if (start_id) |s| self.gpa.free(s);
        var times: usize = 10;

        while (times > 0) {
            times -= 1;

            const list = try self.pending(group, start_id, null, 100);
            defer {
                for (list) |*p| p.deinit();
                self.gpa.free(list);
            }
            if (list.len == 0) break;
            const last_id = list[list.len - 1].id;

            for (list) |p| {
                if (p.id.len == 0) continue;
                if (std.mem.eql(u8, p.consumer, self.consumer)) {
                    // 不抢自己的消息；失败次数过多的直接确认丢弃
                    if (p.delivery >= self.max_retry) _ = try self.ackOne(group, p.id);
                } else if (p.idle > ms_idle) {
                    if (p.delivery >= self.max_retry) {
                        // 抢夺后确认，即丢弃
                        try self.claimValue(group, self.consumer, p.id, ms_idle);
                        _ = try self.ackOne(group, p.id);
                    } else {
                        try self.claimValue(group, self.consumer, p.id, ms_idle);
                        count += 1;
                    }
                }
            }

            const next = nextStreamIdPrefix(self.gpa, last_id) orelse break;
            if (start_id) |s| self.gpa.free(s);
            start_id = next;
        }

        // 清理空闲消费者：无挂起且超 1 小时不活跃
        const consumers = try self.getConsumers(group);
        defer freeConsumerInfos(self.gpa, consumers);
        for (consumers) |c| {
            const name = c.name orelse continue;
            if (name.len > 0 and c.pending == 0 and c.idle > 3_600_000) {
                _ = try self.groupDeleteConsumer(group, name);
            }
        }

        return count;
    }

    fn ackOne(self: *RedisStream, group: []const u8, id: []const u8) Error!i64 {
        var v = try self.redis.execute(&.{ "XACK", self.key, group, id });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    // ───────────────────────── 内部命令 ─────────────────────────

    /// 删除消息（`XDEL`）。
    pub fn delete(self: *RedisStream, id: []const u8) Error!i64 {
        var v = try self.redis.execute(&.{ "XDEL", self.key, id });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    /// 裁剪长度（`XTRIM MAXLEN`）。
    pub fn trimMaxLen(self: *RedisStream, max_len: i64, accurate: bool) Error!i64 {
        var buf: [24]u8 = undefined;
        const text = std.fmt.bufPrint(&buf, "{d}", .{max_len}) catch return error.Config;
        var v = if (accurate)
            try self.redis.execute(&.{ "XTRIM", self.key, "MAXLEN", text })
        else
            try self.redis.execute(&.{ "XTRIM", self.key, "MAXLEN", "~", text });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    /// 丢弃指定时间（毫秒）之前的消息（`XTRIM MINID`，Redis 6.2+）。
    pub fn trimBeforeMs(self: *RedisStream, ms: i64) Error!i64 {
        var buf: [24]u8 = undefined;
        const text = std.fmt.bufPrint(&buf, "{d}", .{ms}) catch return error.Config;
        var v = try self.redis.execute(&.{ "XTRIM", self.key, "MINID", text });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    /// 区间读取（`XRANGE`，`-`/`+` 表示最小/最大）。
    pub fn range(self: *RedisStream, start_id: ?[]const u8, end_id: ?[]const u8, count: i64) Error![]Message {
        const start = if (start_id) |s| (if (s.len > 0) s else "-") else "-";
        const end = if (end_id) |s| (if (s.len > 0) s else "+") else "+";

        var v = if (count > 0) blk: {
            var buf: [24]u8 = undefined;
            const text = std.fmt.bufPrint(&buf, "{d}", .{count}) catch return error.Config;
            break :blk try self.redis.execute(&.{ "XRANGE", self.key, start, end, "COUNT", text });
        } else try self.redis.execute(&.{ "XRANGE", self.key, start, end });
        defer v.deinit(self.gpa);
        return parseMessages(self.gpa, v);
    }

    /// 时间段读取（按毫秒时间戳换算 Id）。
    pub fn rangeTime(self: *RedisStream, start_ms: i64, end_ms: i64, count: i64) Error![]Message {
        var sb: [24]u8 = undefined;
        var eb: [24]u8 = undefined;
        const s = std.fmt.bufPrint(&sb, "{d}-0", .{start_ms}) catch return error.Config;
        const e = std.fmt.bufPrint(&eb, "{d}-0", .{end_ms}) catch return error.Config;
        return self.range(s, e, count);
    }

    /// 独立消费（`XREAD`）。
    pub fn read(self: *RedisStream, start_id: []const u8, count: usize, block_ms: i64) Error![]Message {
        return self.readBlocking(start_id, count, block_ms);
    }

    fn readBlocking(self: *RedisStream, start_id: []const u8, count: usize, block_ms: i64) Error![]Message {
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        var bb: [24]u8 = undefined;
        var cb: [24]u8 = undefined;
        try args.append(self.gpa, "XREAD");
        if (block_ms > 0) {
            try args.append(self.gpa, "BLOCK");
            try args.append(self.gpa, std.fmt.bufPrint(&bb, "{d}", .{block_ms}) catch return error.Config);
        }
        if (count > 0) {
            try args.append(self.gpa, "COUNT");
            try args.append(self.gpa, std.fmt.bufPrint(&cb, "{d}", .{count}) catch return error.Config);
        }
        try args.append(self.gpa, "STREAMS");
        try args.append(self.gpa, self.key);
        try args.append(self.gpa, start_id);

        var v = try self.callBlocking(args.items, block_ms);
        defer v.deinit(self.gpa);
        return parseXRead(self.gpa, v);
    }

    /// 消费组消费（`XREADGROUP`）。`id` 为 `>` 表示新消息，`0` 表示自身挂起。
    pub fn readGroup(
        self: *RedisStream,
        group: []const u8,
        consumer: []const u8,
        count: usize,
        block_ms: i64,
        id: ?[]const u8,
    ) Error![]Message {
        const start = if (id) |s| (if (s.len > 0) s else ">") else ">";

        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.gpa);
        var bb: [24]u8 = undefined;
        var cb: [24]u8 = undefined;
        try args.append(self.gpa, "XREADGROUP");
        try args.append(self.gpa, "GROUP");
        try args.append(self.gpa, group);
        try args.append(self.gpa, consumer);
        if (block_ms > 0) {
            try args.append(self.gpa, "BLOCK");
            try args.append(self.gpa, std.fmt.bufPrint(&bb, "{d}", .{block_ms}) catch return error.Config);
        }
        if (count > 0) {
            try args.append(self.gpa, "COUNT");
            try args.append(self.gpa, std.fmt.bufPrint(&cb, "{d}", .{count}) catch return error.Config);
        }
        try args.append(self.gpa, "STREAMS");
        try args.append(self.gpa, self.key);
        try args.append(self.gpa, start);

        var v = try self.callBlocking(args.items, block_ms);
        defer v.deinit(self.gpa);
        return parseXRead(self.gpa, v);
    }

    /// 等待列表汇总（`XPENDING key group`）。
    pub fn pendingInfo(self: *RedisStream, group: []const u8) Error!?PendingInfo {
        var v = try self.redis.execute(&.{ "XPENDING", self.key, group });
        defer v.deinit(self.gpa);
        const items = v.asArray() orelse return null;
        return PendingInfo.fromItems(self.gpa, items) catch null;
    }

    /// 等待列表明细（`XPENDING key group start end count`）。
    pub fn pending(
        self: *RedisStream,
        group: []const u8,
        start_id: ?[]const u8,
        end_id: ?[]const u8,
        count: i64,
    ) Error![]PendingItem {
        const start = if (start_id) |s| (if (s.len > 0) s else "-") else "-";
        const end = if (end_id) |s| (if (s.len > 0) s else "+") else "+";

        var v = if (count > 0) blk: {
            var buf: [24]u8 = undefined;
            const text = std.fmt.bufPrint(&buf, "{d}", .{count}) catch return error.Config;
            break :blk try self.redis.execute(&.{ "XPENDING", self.key, group, start, end, text });
        } else try self.redis.execute(&.{ "XPENDING", self.key, group, start, end });
        defer v.deinit(self.gpa);

        const items = v.asArray() orelse return self.gpa.alloc(PendingItem, 0) catch error.OutOfMemory;
        var out: std.ArrayList(PendingItem) = .empty;
        errdefer {
            for (out.items) |*p| p.deinit();
            out.deinit(self.gpa);
        }
        for (items) |item| {
            const pair = item.asArray() orelse continue;
            try out.append(self.gpa, PendingItem.fromItems(self.gpa, pair) catch continue);
        }
        return out.toOwnedSlice(self.gpa);
    }

    /// 抢夺挂起消息（`XCLAIM`）。
    pub fn claim(self: *RedisStream, group: []const u8, consumer: []const u8, id: []const u8, ms_idle: i64) Error!Value {
        var buf: [24]u8 = undefined;
        const text = std.fmt.bufPrint(&buf, "{d}", .{ms_idle}) catch return error.Config;
        return self.redis.execute(&.{ "XCLAIM", self.key, group, consumer, text, id });
    }

    fn claimValue(self: *RedisStream, group: []const u8, consumer: []const u8, id: []const u8, ms_idle: i64) Error!void {
        var v = try self.claim(group, consumer, id, ms_idle);
        v.deinit(self.gpa);
    }

    // ───────────────────────── 消费组 ─────────────────────────

    /// 创建消费组（`XGROUP CREATE ... MKSTREAM`）。`start_id` 默认 `0`。
    pub fn groupCreate(self: *RedisStream, group: []const u8, start_id: ?[]const u8) Error!bool {
        const start = if (start_id) |s| (if (s.len > 0) s else "0") else "0";
        var v = try self.redis.execute(&.{ "XGROUP", "CREATE", self.key, group, start, "MKSTREAM" });
        defer v.deinit(self.gpa);
        if (v.asString()) |s| return std.mem.eql(u8, s, "OK");
        return false;
    }

    /// 销毁消费组（`XGROUP DESTROY`）。
    pub fn groupDestroy(self: *RedisStream, group: []const u8) Error!i64 {
        var v = try self.redis.execute(&.{ "XGROUP", "DESTROY", self.key, group });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    /// 删除消费者（`XGROUP DELCONSUMER`），返回其挂起消息数。
    pub fn groupDeleteConsumer(self: *RedisStream, group: []const u8, consumer: []const u8) Error!i64 {
        var v = try self.redis.execute(&.{ "XGROUP", "DELCONSUMER", self.key, group, consumer });
        defer v.deinit(self.gpa);
        return v.asInt() orelse 0;
    }

    /// 设置消费组起始 Id（`XGROUP SETID`）。`start_id` 默认 `$`。
    pub fn groupSetId(self: *RedisStream, group: []const u8, start_id: []const u8) Error!bool {
        const start = if (start_id.len == 0) "$" else start_id;
        var v = try self.redis.execute(&.{ "XGROUP", "SETID", self.key, group, start });
        defer v.deinit(self.gpa);
        if (v.asString()) |s| return std.mem.eql(u8, s, "OK");
        return false;
    }

    // ───────────────────────── 队列信息 ─────────────────────────

    /// 流信息（`XINFO STREAM`），键不存在返回 null。
    pub fn getInfo(self: *RedisStream) Error!?StreamInfo {
        var v = self.redis.execute(&.{ "XINFO", "STREAM", self.key }) catch |err| {
            if (err == error.Server) return null;
            return err;
        };
        defer v.deinit(self.gpa);
        const items = v.asArray() orelse return null;
        return StreamInfo.fromItems(self.gpa, items) catch null;
    }

    /// 消费组列表（`XINFO GROUPS`），键不存在返回空。
    pub fn getGroups(self: *RedisStream) Error![]GroupInfo {
        var v = self.redis.execute(&.{ "XINFO", "GROUPS", self.key }) catch |err| {
            if (err == error.Server) return self.gpa.alloc(GroupInfo, 0) catch error.OutOfMemory;
            return err;
        };
        defer v.deinit(self.gpa);
        const list = v.asArray() orelse return self.gpa.alloc(GroupInfo, 0) catch error.OutOfMemory;

        var out: std.ArrayList(GroupInfo) = .empty;
        errdefer {
            for (out.items) |*g| g.deinit(self.gpa);
            out.deinit(self.gpa);
        }
        for (list) |item| {
            const pair = item.asArray() orelse continue;
            try out.append(self.gpa, GroupInfo.fromItems(self.gpa, pair) catch continue);
        }
        return out.toOwnedSlice(self.gpa);
    }

    /// 消费者列表（`XINFO CONSUMERS`）。
    pub fn getConsumers(self: *RedisStream, group: []const u8) Error![]ConsumerInfo {
        var v = self.redis.execute(&.{ "XINFO", "CONSUMERS", self.key, group }) catch |err| {
            if (err == error.Server) return self.gpa.alloc(ConsumerInfo, 0) catch error.OutOfMemory;
            return err;
        };
        defer v.deinit(self.gpa);
        const list = v.asArray() orelse return self.gpa.alloc(ConsumerInfo, 0) catch error.OutOfMemory;

        var out: std.ArrayList(ConsumerInfo) = .empty;
        errdefer {
            for (out.items) |*c| c.deinit(self.gpa);
            out.deinit(self.gpa);
        }
        for (list) |item| {
            const pair = item.asArray() orelse continue;
            try out.append(self.gpa, ConsumerInfo.fromItems(self.gpa, pair) catch continue);
        }
        return out.toOwnedSlice(self.gpa);
    }

    // ───────────────────────── 底层 ─────────────────────────

    fn callBlocking(self: *RedisStream, args: []const []const u8, block_ms: i64) Error!Value {
        if (block_ms <= 0) return self.redis.execute(args);
        const seconds = @divTrunc(block_ms, 1000) + 2;
        return self.redis.executeBlocking(args, seconds) catch |err| {
            // Redis BLOCK 超时与网络读超时都按「无消息」处理
            if (err == error.Timeout or err == error.IoError) return Value{ .null_ = {} };
            return err;
        };
    }
};

// ───────────────────────── 编解码辅助 ─────────────────────────

/// 序列化值与 C# `ToDictionary()` 的字段展开规则（基元 → `__data`；对象/数组 → 扁平字段）。
pub fn valueToFields(gpa: std.mem.Allocator, value: anytype, primitive_key: []const u8) Error!std.ArrayList(Field) {
    var out: std.ArrayList(Field) = .empty;
    errdefer freeFieldsList(gpa, &out);

    const encoded = encoder.encodeValueAlloc(gpa, value) catch return error.Type;
    if (encoded == null) {
        try out.append(gpa, .{ .name = try gpa.dupe(u8, primitive_key), .value = try gpa.alloc(u8, 0) });
        return out;
    }
    defer gpa.free(encoded.?);

    // 字符串等基元无法作为 JSON 解析 → 直接作为 `__data`
    var parsed = std.json.parseFromSlice(std.json.Value, gpa, encoded.?, .{}) catch null;
    if (parsed == null) {
        try out.append(gpa, .{ .name = try gpa.dupe(u8, primitive_key), .value = try gpa.dupe(u8, encoded.?) });
        return out;
    }
    defer parsed.?.deinit();

    const p = &parsed.?;
    switch (p.value) {
        .object => |map| {
            var it = map.iterator();
            while (it.next()) |entry| {
                try out.append(gpa, .{
                    .name = try gpa.dupe(u8, entry.key_ptr.*),
                    .value = try encodeFieldValue(gpa, entry.value_ptr.*),
                });
            }
        },
        .array => |items| {
            var i: usize = 0;
            while (i + 1 < items.items.len) : (i += 2) {
                const name = switch (items.items[i]) {
                    .string => |s| s,
                    else => return error.Type,
                };
                try out.append(gpa, .{
                    .name = try gpa.dupe(u8, name),
                    .value = try encodeFieldValue(gpa, items.items[i + 1]),
                });
            }
        },
        else => {
            try out.append(gpa, .{
                .name = try gpa.dupe(u8, primitive_key),
                .value = try encodeFieldValue(gpa, p.value),
            });
        },
    }
    return out;
}

fn encodeFieldValue(gpa: std.mem.Allocator, value: std.json.Value) Error![]u8 {
    return switch (value) {
        .string => |s| try gpa.dupe(u8, s),
        .integer => |i| try std.fmt.allocPrint(gpa, "{d}", .{i}),
        .float => |f| try resp.formatF64Alloc(gpa, f),
        .bool => |b| try gpa.dupe(u8, if (b) "True" else "False"),
        .null => try gpa.alloc(u8, 0),
        else => try std.json.Stringify.valueAlloc(gpa, value, .{}),
    };
}

pub fn freeFieldsList(gpa: std.mem.Allocator, list: *std.ArrayList(Field)) void {
    for (list.items) |f| {
        gpa.free(f.name);
        gpa.free(f.value);
    }
    list.deinit(gpa);
}

fn freeStringList(gpa: std.mem.Allocator, list: [][]u8) void {
    for (list) |s| gpa.free(s);
    if (list.len > 0) gpa.free(list);
}

fn freeGroupInfos(gpa: std.mem.Allocator, list: []GroupInfo) void {
    for (list) |*g| g.deinit(gpa);
    if (list.len > 0) gpa.free(list);
}

fn freeConsumerInfos(gpa: std.mem.Allocator, list: []ConsumerInfo) void {
    for (list) |*c| c.deinit(gpa);
    if (list.len > 0) gpa.free(list);
}

/// 解析 `XRANGE` / `XREAD` 的单条消息。
pub fn parseEntry(gpa: std.mem.Allocator, value: Value) Error!?Message {
    const items = value.asArray() orelse return null;
    if (items.len != 2) return null;

    const id = (items[0].asBytesAlloc(gpa) catch return error.IoError) orelse return null;
    errdefer gpa.free(id);

    const body_items = items[1].asArray() orelse {
        gpa.free(id);
        return null;
    };
    var body = try gpa.alloc([]u8, body_items.len);
    var filled: usize = 0;
    errdefer {
        for (body[0..filled]) |s| gpa.free(s);
        gpa.free(body);
    }
    for (body_items, 0..) |item, i| {
        body[i] = (item.asBytesAlloc(gpa) catch return error.IoError) orelse try gpa.alloc(u8, 0);
        filled += 1;
    }
    return Message{ .gpa = gpa, .id = id, .body = body };
}

/// 解析 `XRANGE` 应答（消息数组）。
pub fn parseMessages(gpa: std.mem.Allocator, value: Value) Error![]Message {
    const items = value.asArray() orelse return gpa.alloc(Message, 0) catch error.OutOfMemory;
    var out: std.ArrayList(Message) = .empty;
    errdefer {
        for (out.items) |*m| m.deinit();
        out.deinit(gpa);
    }
    for (items) |item| {
        const msg = (try parseEntry(gpa, item)) orelse continue;
        try out.append(gpa, msg);
    }
    return out.toOwnedSlice(gpa);
}

/// 解析 `XREAD` / `XREADGROUP` 应答：`[[key, [entry...]]]`。
fn parseXRead(gpa: std.mem.Allocator, value: Value) Error![]Message {
    var out: std.ArrayList(Message) = .empty;
    errdefer {
        for (out.items) |*m| m.deinit();
        out.deinit(gpa);
    }

    const streams = value.asArray() orelse return out.toOwnedSlice(gpa);
    if (streams.len != 1) return out.toOwnedSlice(gpa);
    const items = streams[0].asArray() orelse return out.toOwnedSlice(gpa);
    if (items.len != 2) return out.toOwnedSlice(gpa);
    const entries = items[1].asArray() orelse return out.toOwnedSlice(gpa);
    for (entries) |entry| {
        const msg = (try parseEntry(gpa, entry)) orelse continue;
        try out.append(gpa, msg);
    }
    return out.toOwnedSlice(gpa);
}

/// 下一个开始 Id：最后一条的毫秒序号 +1。
pub fn nextStreamIdPrefix(gpa: std.mem.Allocator, id: []const u8) ?[]u8 {
    const dash = std.mem.indexOfScalar(u8, id, '-') orelse return null;
    const ms = std.fmt.parseInt(i64, id[0..dash], 10) catch return null;
    return std.fmt.allocPrint(gpa, "{d}-0", .{ms + 1}) catch null;
}

/// Stream Id 前缀（毫秒）转 Unix 秒。
fn idToSeconds(id: []const u8) ?i64 {
    const dash = std.mem.indexOfScalar(u8, id, '-') orelse return null;
    const ms = std.fmt.parseInt(i64, id[0..dash], 10) catch return null;
    return @divTrunc(ms, 1000);
}
