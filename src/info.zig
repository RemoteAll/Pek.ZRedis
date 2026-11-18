//! `INFO` 文本解析（键值均自有，`deinit` 统一释放）。
//!
//! 独立成模块以避免 `redis ↔ topology_discovery` 的循环导入。

const std = @import("std");

pub const InfoMap = struct {
    gpa: std.mem.Allocator,
    map: std.StringHashMapUnmanaged([]u8) = .empty,

    pub fn get(self: *const InfoMap, key: []const u8) ?[]const u8 {
        return self.map.get(key);
    }

    pub fn deinit(self: *InfoMap) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.gpa.free(e.key_ptr.*);
            self.gpa.free(e.value_ptr.*);
        }
        self.map.deinit(self.gpa);
    }
};

/// 解析 `INFO` 文本（`# Section` 行忽略，`key:value` 取首个冒号）。
pub fn parseInfo(gpa: std.mem.Allocator, text: []const u8) !InfoMap {
    var out = InfoMap{ .gpa = gpa };
    errdefer out.deinit();

    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \t\r");
        if (line.len == 0 or line[0] == '#') continue;
        const idx = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const key = std.mem.trim(u8, line[0..idx], " \t\r");
        const value = std.mem.trim(u8, line[idx + 1 ..], " \t\r");
        if (key.len == 0) continue;
        const gop = try out.map.getOrPut(gpa, key);
        if (!gop.found_existing) {
            gop.key_ptr.* = try gpa.dupe(u8, key);
            gop.value_ptr.* = try gpa.dupe(u8, value);
        }
    }
    return out;
}
