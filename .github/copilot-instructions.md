# Pek.ZRedis · Copilot 工作约定

## 开工顺序

1. 读 `.github/copilot-instructions.md` → `docs/项目接续指南.md` → `docs/用户档案.md` → `docs/项目记忆.md`
   → `docs/移植对照-NRedis.md`。
2. 跑一次接续自检：`powershell -NoProfile -ExecutionPolicy Bypass -File tools/continuity/install-global-instruction.ps1`。
3. 改动前先检索（公共库 `zzig` → 兄弟项目 `Pek.RRedis`/`DH.NRedis` → 本仓库既有实现）。

## 工程规则

- **对齐口径以本地源码为准**：C# `DH.NRedis`、Rust `Pek.RRedis`；不凭印象改字节格式/键布局/锁算法。
- **按值语义**：不要在被按值返回的结构体里保存指向自身字段的指针（`Reader.stream`、`Client.cfg` 已按值）。
- **超时**：统一走 `setSocketTimeouts`（Windows 不支持 `operateTimeout` 与连接超时）。
- **内存**：`expectArray` 借用；`Value.deinit` 释放；新增数组复制路径必须测泄漏（`zig build test` 会自动报）。
- **错误日志**：失败必须包含「做什么 + 为什么 + 上下文」；禁止只返回笼统文案。
- **中文提交**：`type(scope): 中文主题`；用 `git commit -F <UTF-8 文件>`。

## 验证门槛

- 任何源码/脚本改动后：`zig fmt --check src build.zig tests` + `zig build test --summary all`。
- 交付前跑一键回归：`tools/regression.ps1`（报告落 `artifacts/regression/`，退出码 0/1）。
- 测试数不得下降（当前基线 **27**）；新增用例后同步抬高阈值。
- 改协议/编码/锁：必须同时补/改 `docs/移植对照-NRedis.md` 对应节，并说明证据用例。
