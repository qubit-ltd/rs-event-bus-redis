# RBR-001 T1 依赖与响应帧证据

检查时间：2026-11-04 14:26–14:32 UTC。执行目录：`/data/working/qubit/rust-common/rs-event-bus-redis-impl`。基线 HEAD：`7275445f64501fc3bf3646939e1df0d4fce921d6`。检查时工作树另有 `tests/redis_fault_tests.rs` 修改，属于并行 T2；本任务未编辑该文件。目标仓库原有两份设计/计划文档未改动。

## Registry 核实

使用独立 Cargo home `/tmp/rbr-t1-clean-home`，目录中无 path、patch 或 source replacement 配置。时间均为 2026-11-04 UTC。

- `CARGO_HOME=/tmp/rbr-t1-clean-home cargo info qubit-event-bus@0.21`（`/tmp`，exit 101）：
  ```text
  Updating crates.io index
  error: could not find `qubit-event-bus@0.21` in registry `https://github.com/rust-lang/crates.io-index`
  ```
- `CARGO_HOME=/tmp/rbr-t1-clean-home cargo info redis@2.0.0-rc.1`（`/tmp`，exit 0）：官方 crates.io 页面确认精确版本 2.0.0-rc.1，repo `https://github.com/redis-rs/redis-rs`，`rust-version: 1.88`。
- `CARGO_HOME=/tmp/rbr-t1-clean-home cargo info redis@2.0.0-rc.2`（`/tmp`，exit 0）：确认精确版本 2.0.0-rc.2，latest 当时为 rc.2，repo 同上，`rust-version: 1.88`。
- `rg -n 'max_response_bytes|set_max_response_bytes' /tmp/rbr-t1-clean-home/registry/src/index.crates.io-*/redis-2.0.0-rc.{1,2}/src`（exit 1，无匹配）。官方 registry 源码中没有现有 Git fork 使用的完整响应帧预算 API。rc.2 的 `src/parser.rs` 对外提供 `parse_redis_value(bytes: &[u8])`，而同步/异步连接读取路径没有配置完整帧预算。因此不能只通过现有 API 调用获得在 Value 构造前限制完整响应帧的等价行为。
- 来源：Cargo 官方 crates.io sparse registry/index 与下载的官方 registry crate 源码（上面精确命令）；不是 Git fork、path 或 sibling metadata。

结论：没有满足两个前提的 registry 组合：core 0.21 当时未在 crates.io 发布；可用 Redis rc.1/rc.2 都不具备等价的有界完整响应帧读取。故没有修改 provider `Cargo.toml`/`Cargo.lock`、rs-task manifest/lock 或 fixture lock；保留原有效 Git/rev 依赖及 package gate。RBR-001 依赖闭合未完成，风险是发布 archive 无法解析 core 0.21，且替换 Redis 会丢失完整响应帧预算语义。

## 当前依赖边

- provider manifest 生产和 dev dependency 均为 Redis `=2.0.0-rc.1`，Git `https://github.com/qubit-ltd/redis-rs.git`，rev `a668891e9fcfed32f8905c2fd729fa05c1732aa5`；provider lock Redis 为同版本同 rev。core manifest 为 `qubit-event-bus = { version = "0.21", path = "../rs-event-bus" }`。
- `../rs-task/Cargo.toml:77` 有相同 Redis Git/rev 的直接 dev dependency（features `streams`），其 `Cargo.lock` Redis 来源同一 rev。检查的 `all-features-consumer`、`consumer`、`crash-worker`、`provider`、`readme-quick-start`、`typed-store-consumer` fixture locks 无 Redis package；`doc-examples/Cargo.lock` 有相同 Redis 2.0.0-rc.1 Git/rev。无可用客户端升级，因此均无需调整。

## 响应帧限额回归

现有客户端配置 `ConnectionConfig::set_max_response_bytes` / `AsyncConnectionConfig::set_max_response_bytes` 仍保留；测试覆盖 sync 与 async：

- `cargo +1.94.0 test --locked --test wire_limit_tests`（新 worktree，exit 101）：编译通过，16 项中 11 通过、5 项真实 Redis/Docker 测试因环境无法访问 Docker socket 失败，错误均为 `permission denied while trying to connect to the Docker API at unix:///var/run/docker.sock`。5 个未完成断言包括真实超限响应后 PEL 保留/恢复、receiver admission、持久记录恢复。
- `cargo +1.94.0 test --locked --test wire_limit_tests scripted_oversized`（exit 0）：2 项通过，分别为 sync 与 async scripted 超大 RESP 完整帧测试；可验证 receiver 因超限停止，但 scripted 测试不代替真实 Redis 的 PEL/恢复断言。

## 独立 package candidate

运行命令：`bash ci/check-package-candidate.sh`（新 worktree，exit 1）。日志：`target/package-candidate-logs/20261104T142626Z-2087442/package.log`。首失败阶段完整输出：

```text
FIRST_CHECK phase=package
    Updating crates.io index
    Updating git repository `https://github.com/qubit-ltd/redis-rs.git`
   Packaging qubit-event-bus-redis v0.7.0 (/data/working/qubit/rust-common/rs-event-bus-redis-impl)
    Updating crates.io index
error: failed to prepare local package for uploading

Caused by:
  failed to select a version for the requirement `qubit-event-bus = "^0.21"`
  candidate versions found which didn't match: 0.20.1, 0.20.0, 0.14.0, ...
  location searched: crates.io index
  required by package `qubit-event-bus-redis v0.7.0 (/data/working/qubit/rust-common/rs-event-bus-redis-impl)`
BLOCKED phase=package exit=1 logs=/data/working/qubit/rust-common/rs-event-bus-redis-impl/target/package-candidate-logs/20261104T142626Z-2087442
```

package 检查未到 normalized manifest、feature builds 或 metadata source checks 阶段；不得视为独立包验证通过。此前在旧嵌套 worktree 路径运行的尝试因缺 sibling `rs-event-bus` 而失败，已在迁移后上述正确路径重跑。全程未使用 no-verify。

## T1 状态

- registry 依赖闭合：受外部 registry 发布状态和缺少等价 Redis API 阻塞，RBR-001 incomplete。
- 保留响应帧限额和 package gate：是。
- 初次 T1 运行时 Docker socket 不可用；用户随后授权临时 ACL 后，重新执行的 `./.infra/bin/ci-check.sh` 在 package 验证之前通过了完整 `wire_limit_tests`（16/16），包含 sync/async 真实 Redis 超限响应、PEL 保留和恢复断言，以及 scripted 帧限额检查。CI 因 package tarball 验证而以 exit 1 结束；该失败属于 registry 客户端/Core 发布闭合问题，不影响此前通过的帧限额测试。
- Docker 验证结束后已撤销临时 socket ACL；`getfacl -cp /var/run/docker.sock` 不再显示 `user:starfish` 条目。
