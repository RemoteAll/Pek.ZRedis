const std = @import("std");

/// Pek.ZRedis —— DH.NRedis（C#）与 Pek.RRedis（Rust）的 Zig 实现。
///
/// 口径与另两个 Zig 移植库一致：库只暴露模块 `pek_zredis`，`zig build test` 串起
/// 单元测试 + 回环假 Redis 集成测试；`zig build zredis` 产出对拍 CLI。
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zzig_dep = b.dependency("zzig", .{
        .target = target,
        .optimize = optimize,
    });

    const mod = b.addModule("pek_zredis", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "zzig", .module = zzig_dep.module("zzig") },
        },
    });

    // 单元测试：协议 / 配置 / 编码器 / 前缀 / 键处理（不依赖网络）
    const unit_tests = b.addTest(.{ .root_module = mod });
    const run_unit = b.addRunArtifact(unit_tests);
    const unit_step = b.step("unit", "单元测试（不依赖网络）");
    unit_step.dependOn(&run_unit.step);

    // 集成测试：回环假 Redis 服务端 + 客户端/连接池/数据结构/队列
    const integration_mod = b.createModule(.{
        .root_source_file = b.path("tests/all.zig"),
        .target = target,
        .optimize = optimize,
    });
    integration_mod.addImport("pek_zredis", mod);
    integration_mod.addImport("zzig", zzig_dep.module("zzig"));
    const integration_tests = b.addTest(.{ .root_module = integration_mod });
    const run_integration = b.addRunArtifact(integration_tests);
    const integration_step = b.step("integration", "集成测试（回环假 Redis）");
    integration_step.dependOn(&run_integration.step);

    const test_step = b.step("test", "单元 + 集成测试");
    test_step.dependOn(&run_unit.step);
    test_step.dependOn(&run_integration.step);

    // 对拍 CLI：负责跨语言（C#/Rust/Zig）读写同一实例时的写/读/自检子命令
    const cli_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    cli_mod.addImport("zzig", zzig_dep.module("zzig"));
    cli_mod.addImport("pek_zredis", mod);

    const exe = b.addExecutable(.{ .name = "zredis", .root_module = cli_mod });
    b.installArtifact(exe);

    const run_cli = b.addRunArtifact(exe);
    run_cli.step.dependOn(b.getInstallStep());
    const run_step = b.step("run", "运行 zredis CLI（默认打印帮助）");
    run_step.dependOn(&run_cli.step);
}
