# Sentinel 成本核查修复实施证据

日期：2026-11-12。目标仓库：`rs-event-bus-redis`。实施分支提交：`06462b948369807be90dcf39e4543189b4943394`；基础提交：`d1f9121690ec53277320d164c32ccdba51d04b04`。

## 目标与实现

- **RER-101 — 排除后置观测耗时。** `tests/sentinel_cost/workload.rs::scenario_run` 现在在 worker join 完成后立即冻结 `elapsed`，之后才执行 provider/Redis 计数 capture。`settle_only` 保留 release 至 finished 批次计时，排除准备和 capture。
- **RER-102 — 保存证据并门控结果。** `measurement::assert_complete` 要求 attempts 与预期数相等、successes 与预期数相等、errors 和 unknown 为零。正式格先写并 flush 样本及汇总，再做内容、计数和 PEL 验收；失败会带模式/传输/并发/场景/轮次。预热样本和汇总先写入独立 `sentinel-cost-warmup.csv` 并 flush，再应用同一成功门槛。
- **RER-103 — 核验纯发布内容并修正双语报告。** `tests/sentinel_cost/validation.rs` 校验 stream 行数、预期业务 EventId 唯一性、64 字节 `x` payload 和 `WireFields` 解码；`workload.rs` 在计时冻结与计数 capture 后，通过 128 行、排他游标 XRANGE observer 扫描纯发布 stream。TLS observer 复用 fixture 的证书校验连接。回归用例覆盖错误行集合，并以明文和 TLS fixture 各核验 129 行，确认 TLS observer 不增加 Sentinel proxy 计数。双语 benchmark 报告保留旧结果并说明旧纯发布/往返吞吐受后置观测影响，且空 PEL 不证明纯发布内容。

## 验证

| 命令 | 结果 |
| --- | --- |
| `cargo test --locked --all-features --test sentinel_cost_tests -- --test-threads=1` | 13 passed、0 failed、1 ignored |
| `cargo test --locked --all-features --test sentinel_cost_tests test_publish_content_observer_paginates_plaintext_and_tls -- --exact --nocapture --test-threads=1` | 通过；明文和 TLS 各验证 129 行，TLS proxy 计数不变 |
| `cargo test --locked --all-features --test sentinel_cost_tests test_failed_warmup_flushes_dimensional_samples_and_counts_first -- --exact --nocapture --test-threads=1` | 1 passed |
| `./.infra/bin/align-ci.sh` | 通过 style 和 all-target/all-feature Clippy；修复了新模块版权头、冗余闭包及 warmup helper 参数过多问题后重跑成功 |
| 正式 120 格矩阵 | 见下节，120/120 格各 1000/1000 成功，0 errors、0 unknown，退出码 0 |
| `./.infra/bin/ci-check.sh`（首次） | 失败：Docker socket 不可访问，未配置 Redis 7 本地 fixture；ACL 集成测试无法启动。按仓库已有的 `RER_REDIS_7_SERVER` 回退重试 |
| `RER_REDIS_7_SERVER=/usr/bin/redis-server ./.infra/bin/ci-check.sh` | ACL 测试通过；后续 6 项 recovery regression 因缺少 Redis 6.2 fixture 失败。按仓库已有的 `RER_REDIS_6_2_SERVER` 回退构建 Redis 6.2.17 后重跑 |
| `RER_REDIS_6_2_SERVER=/tmp/rer-ef620e32-redis-6.2.17/redis-6.2.17/src/redis-server RER_REDIS_7_SERVER=/usr/bin/redis-server ./.infra/bin/ci-check.sh` | exit 0；完整测试、feature matrix、package、10 秒 bounded-wire fuzz、coverage 和 security audit 均通过 |

CI 覆盖率为 functions 95.40%（664/696，门槛 ≥95%）、lines 96.48%（6587/6827，门槛 >90%）、regions 95.62%（10462/10941，门槛 >85%）。RustSec 扫描检查了 `Cargo.lock` 中 153 个依赖并成功完成；未发现需升级的依赖。`coverage-cfg-clippy` 未配置；loom 未配置并由仓库脚本报告跳过。CI 临时覆盖率报告在脚本清理步骤中删除，未生成仓库 coverage snapshot。

## 正式矩阵

命令和退出码保存在 `.infra/benchmark/evidence-ef620e32/final/formal-command.txt`。运行于 2026-11-12 01:52:23–02:18:44 UTC，耗时约 26 分 21 秒。fixture 使用本地 Redis 7.0.15（Docker socket 不可访问时采用仓库既有回退）。完整原始样本、逐格汇总、预热样本、清单和日志保存在 `.infra/benchmark/evidence-ef620e32/final/`；SHA-256 见该目录的 `SHA256SUMS`。

清单记录源提交 `06462b948369807be90dcf39e4543189b4943394`、Rust/Cargo 1.94.0、Redis 7.0.15、`Cargo.lock` SHA-256 `97dadb1b1c59e14217960f36d4d791b136dd9cc82b472d6db9dac17b1d8bdae1`，以及矩阵运行时工作区内容哈希。每格 1,000 次正式操作，所有 120,000 次均成功。统计表及计时定义见双语 [benchmark 报告](connection-reuse-benchmark.md) 与[中文报告](connection-reuse-benchmark.zh_CN.md)。

另有一个在独立复审指出预热证据缺口后中断的修前尝试，保存在 `.infra/benchmark/evidence-ef620e32/partial-prewarm-before-fix/`。该尝试退出码 130，完成的 5 格均为 1000/1000 成功；它不是失败矩阵，不计入正式结果。新矩阵运行增加了 warmup CSV 并成功覆盖同一最终代码提交。

## 兼容性与范围

改动限于 Sentinel 成本测试 harness、fixture observer helper、双语测量报告和本轮证据；未修改生产连接策略、公共 API、依赖版本、数据格式或调用方。失败和 unknown 现在会使矩阵退出非零，这是验收行为修正。设计文档和实施计划依用户要求保留于原工作区，未纳入提交。
