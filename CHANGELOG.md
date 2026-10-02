# Changelog / 更新记录

[English README](README.md) | [简体中文 README](README.zh-CN.md)

## Unreleased / 未发布

This section describes the current source changes, not a published release. No release date or package-version bump is implied.

本节记录当前源码变更，不代表已发布版本，也不表示已确定发布日期或提升包版本。

### Added / 新增能力

- **Persistent exact main identity:** persist canonical session file/cwd/session ID with runId/nonce; restore that identity on a same-session cold start and preserve an already-live identity on first in-process migration.
  **持久精确主身份：** 保存 canonical session 文件/cwd/session ID 与 runId/nonce；同会话 cold start 恢复原身份，首次进程内迁移保留已有存活身份。
- **Exclusive main controller:** a packaged Python guardian holds POSIX `flock` through a private pipe, releases on parent exit/crash, and prevents competing same-session receivers. Controller exclusion requests Pi shutdown instead of merely reporting an extension error.
  **排他主 controller：** 随包 Python guardian 通过私有 pipe 持有 POSIX `flock`，父进程退出/崩溃时释放，拒绝竞争同一 session 的 receiver；被排他时请求 Pi shutdown，而非仅报告扩展错误。
- **Exact-ID offline sending:** persisted canonical main sessions can accept offline publication within 24 hours of their last heartbeat. Each event must expire within one hour of sending and before the recovery window closes; no offline cwd guessing.
  **精确 ID 离线发送：** 持久 canonical main session 可在最后 heartbeat 后 24 小时内接受离线发布；每个事件须在发送后一小时内且恢复窗口结束前过期，不允许猜测离线 cwd 目标。
- **Receiver-owned worker routing:** validate exact live worker/owner/receiver PID, task, cwd, identity path, run nonce, identical key sets and fresh receiver heartbeat without depending on a dead parent's heartbeat. Require receiver/lease identity agreement for leased sends.
  **Receiver-owned worker 路由：** 校验精确存活 worker/owner/receiver PID、task、cwd、identity 路径、运行 nonce、完全一致的 key 集合及新鲜 receiver heartbeat，不依赖死亡父进程的 heartbeat；lease 发送还要求 receiver/lease 身份一致。
- **Durable inflight and disk receipts:** journal before injecting; retain outbox until the exact session JSONL contains a fsynced user/custom-entry receipt. Cold start reconstructs ACK from disk or replays eligible unacknowledged events.
  **持久 inflight 与磁盘 receipt：** 注入前写 journal，精确 session JSONL 出现完成 fsync 的 user/custom-entry receipt 后才确认 outbox；cold start 从磁盘重建 ACK，或重放满足条件的未确认事件。
- **Public bilingual documentation:** corresponding English/Chinese READMEs, this bilingual changelog, configuration/CLI reference and first-migration/prepared-upgrade instructions; both new documents are included in the package allowlist.
  **公共双语文档：** 对应的中英文 README、本双语 changelog、配置/CLI 参考、首次迁移与 prepared upgrade 流程；两份新文档加入发布 allowlist。

### Fixed / 修复

- Do not acknowledge an event merely because Pi accepted the queue or emitted `message_end`; the event may precede session persistence. Same-process scans/reloads do not repeatedly queue existing inflight events, and persisted receipts survive a crash before dedup/outbox acknowledgment.
  不再仅因 Pi 接受队列或发出 `message_end` 就确认事件，该事件可能早于 session 持久化。同进程扫描/reload 不重复排入已有 inflight；在 dedup/outbox 确认前崩溃后仍可利用持久 receipt 恢复。
- Fail closed on unknown ownership modes, receiver PID/task/cwd/key-set/freshness mismatches, mismatched canonical main identities and wrong nonces. Reject excessively future-dated heartbeats; do not treat a live legacy main's stale registration as permission to open a second controller.
  未知 ownership mode、receiver PID/task/cwd/key 集合/新鲜度不匹配、canonical main identity 不匹配或错误 nonce 均 fail-closed。拒绝过度超前的 heartbeat；不能因旧 main 注册陈旧就允许另一 controller 打开仍存活的同一会话。
- Preserve inbox-only pending events across shutdown rather than silently discarding them; keep published v2 events in the durable outbox until receipt ACK or an explicit rejection/expiry decision.
  Shutdown 时保留仅存在于 inbox 的 pending 事件，而非静默丢弃；已发布 v2 事件保留在持久 outbox，直到 receipt ACK 或明确拒绝/过期处理。

### Verification and limits / 验证与边界

- GitHub Actions CI is configured in `.github/workflows/ci.yml` to run `npm ci` and `npm test` on `ubuntu-latest` with Node.js 22 and Python 3.12 for pushes to `main` and pull requests. This records workflow configuration, not a claim that a remote run or live upgrade has already passed.
  GitHub Actions CI 在 `.github/workflows/ci.yml` 中配置为：push 到 `main` 和 pull request 时，在 `ubuntu-latest` 上使用 Node.js 22、Python 3.12 执行 `npm ci`、`npm test`。此处记录 workflow 配置，不宣称远端运行或 live upgrade 已通过。
- The check suite includes real OS subprocess/JSONL fixture tests for SIGKILL/cold restart, offline sender TTL, competing/legacy controllers and shutdown requests, first-migration identity preservation, same-process reload, crashes before/after disk receipts, wrong-nonce quarantine, receiver-owned delivery after parent exit and fork isolation. Python sender tests add receiver mismatch/unknown-mode refusal and offline-window checks.
  Check 套件包含真实 OS 子进程/JSONL fixture 测试：SIGKILL/cold restart、离线 sender TTL、竞争/旧 controller 与 shutdown 请求、首次迁移身份保留、同进程 reload、磁盘 receipt 前后崩溃、错误 nonce 隔离、父进程退出后 receiver-owned 投递、fork 隔离。Python sender 测试增加 receiver 不匹配/未知模式拒绝和离线窗口校验。
- Existing regressions cover lease lifecycle/one-time auto-rearm, v1 ingestion, live steering, sender idempotence/concurrent sequences and bookkeeping-dotfile exclusion. Use `npm test` / `npm run check`; inspect publication with `npm pack --dry-run` and check whitespace with `git diff --check`.
  既有回归覆盖 lease 生命周期/一次 auto-rearm、v1 摄取、存活 steering、sender 幂等/并发序号、bookkeeping dotfile 排除。使用 `npm test` / `npm run check`；通过 `npm pack --dry-run` 核对发布，通过 `git diff --check` 检查空白问题。
- Delivery is **at-least-once**, not exactly-once external execution; receipt ACK proves transcript persistence, not task completion. Expired events do not gain a longer lifetime from the 24-hour offline identity window. Ephemeral sessions cannot promise disk recovery.
  投递为 **at-least-once**，不是外部操作 exactly-once；receipt ACK 证明 transcript 持久化，不证明任务完成。24 小时离线身份窗口不会延长过期事件寿命。Ephemeral session 无法承诺磁盘恢复。
- Worker-preserving parent upgrades require a matching `pi-subagent-durable` implementation and successful preparation **before parent exit**, with async RMUX workers only. First migration uses its external bootstrap, not a mandatory root `/reload`. Old notify workers additionally require the bounded receiver-heartbeat keeper and retain their old ACK behavior until naturally restarted with updated code.
  保留 worker 的父进程升级要求配套 `pi-subagent-durable` 实现，且须在 **父进程退出前** 成功准备，仅支持异步 RMUX worker。首次迁移使用外部 bootstrap，不要求 root 先 `/reload`。旧 notify worker 还需要有界 receiver-heartbeat keeper；自然重启并加载新代码前仍保留旧 ACK 行为。
- Worker shutdown/reload still releases the wait lease. This is not automatic worker restart/job recovery or a host/RMUX failure guarantee. Fixture tests do not replace a live model/RMUX upgrade validation or establish compatibility with every Pi/provider version. Keep sender/controller scripts and companion capability revisions matched.
  Worker shutdown/reload 仍会释放 wait lease。这不是自动 worker restart/job recovery 或主机/RMUX 故障保证。Fixture 测试不能替代 live model/RMUX 升级验证，也不证明所有 Pi/provider 版本兼容；sender/controller 脚本与配套能力 revision 须匹配。

## Historical highlights / 历史摘要

Selected existing commits, listed newest first. These are Git history references, **not dated release entries** or a complete release-to-version mapping.

以下为既有 commit 摘要，按新到旧排列；它们是 Git 历史参考，**不是带发布日期的 release 条目**，也不是完整版本映射。

| Commit | Highlight / 摘要 |
| --- | --- |
| `7222152` | Public installation guidance and protection of private local material / 公共安装说明与私有本地材料保护 |
| `ca658e7` | Exclude bookkeeping dotfiles from inbound envelope collection / 入站 envelope 收集排除 bookkeeping dotfile |
| `a16e57a` | Direct live-subagent steering tool / 直接向存活 subagent 追加指令的工具 |
| `9a396e6` | Generic and legacy notification routing / 通用与旧通知路由统一 |
| `236d0b7` | One-time re-arm when a released lease's watcher remains alive / 已释放 lease 的 watcher 仍存活时一次重新武装 |
| `164abe5` | Notification wait lifecycle hardening / 强化通知 wait 生命周期 |
| `6192d35` | Keep durable workers alive for notifications / 为通知保持 durable worker 存活 |
| `b8d0e7e` | Routing and replay hardening / 强化路由与重放处理 |
| `1c91811` | Initial script-to-agent notification channel / 初始 script-to-agent 通知通道 |
