# pi-agent-notify

[English](README.md) | **简体中文** | [更新记录 / Changelog](CHANGELOG.md)

将外部事件以 **fail-closed（无法验证就拒绝）** 的方式投递到精确的 Pi 主会话或 worker。Watcher 发布带版本的事件；扩展在注入用户消息或 follow-up 前，校验目标身份、运行 nonce、事项所有权、有效期、序号及去重状态。

通知只是 **唤醒提示，不是权威状态证明**。恢复后必须重新查询来源系统。投递采用 **at-least-once（至少一次）** 语义，不保证外部操作恰好执行一次；外部副作用必须按业务/事件身份实现幂等。

## 功能

- 领域无关的 `itemKey`，例如 `ci:run-42`、`task:example-42`；兼容旧六位数字事项 ID。
- 按 `targetKind + targetId + runId + nonce` 精确路由；同一 cwd 下多个主会话时拒绝歧义路由。
- `notify_subagent` 向存活 durable worker 追加指令，无需重启。已武装的 watcher 交接还需要 `arm_notification_wait` 和存活 producer 的 marker。
- Receiver-owned worker 注册在完成准备的父进程退出后仍有效，无需重启 worker；仍严格校验 task/PID/cwd/key 集合及新鲜度。
- 持久化主会话运行身份与 POSIX 排他 controller guardian，支持同一 canonical session 的离线事件恢复。
- 使用持久化 inflight journal 与 **会话磁盘 receipt ACK** 决定确认和重放，而非把 sender 发布或队列接收当作确认。
- 按事件流单调分配序号，事件 ID/语义去重，拒绝过期及超前时间事件，隔离无效 envelope。
- Sender 通过 `flock`、fsync 和原子 rename，并发安全地持久化状态及发布 outbox。
- 迁移期间兼容 protocol-v1 inbox 输入；新 sender 输出 v2 envelope。

## 安装与配套要求

```bash
pi install git:github.com/roshameow/pi-agent-notify
```

本地 checkout 可在仓库内执行 `pi install .`，也可以配置 Pi 直接加载 `extensions/index.ts`。默认脚本路径依赖包内布局：扩展的上级目录中须保留 `scripts/notify_agent.py` 与 `scripts/session_controller.py`。两者均随包发布。

要求 Node.js **20+**、Python **3.9+**、提供 `@earendil-works` SDK peer 包的 Pi runtime，以及 POSIX 主机（macOS/Linux）。Python 使用 `fcntl`，不支持原生 Windows。尚未验证所有 Pi/provider 版本的兼容性。

定向 durable worker、所有权注册和父进程升级恢复还需要公开的 [pi-subagent-durable](https://github.com/roshameow/pi-subagent-durable) 包：

```bash
pi install git:github.com/roshameow/pi-subagent-durable
```

其当前文档要求 Pi **0.80+**、Node.js **22+**；同时安装两个包时采用更严格的 Node 要求。保留 worker 的升级路径要求 **异步 RMUX worker**，以及实现 receiver ownership、`/agent:prepare-upgrade`、外部 bootstrap/keeper 和精确父会话恢复的配套 durable revision。仅支持基础委派/reload 的旧版本不具备此交接能力。包版本号本身不能证明这些能力；应核对安装源码及成功的准备报告。仅使用主会话通知不需要 durable、RMUX、私有仓库或个人配置。

## 主会话工具：普通 steering 与 watcher 交接

主会话注册 `notify_subagent`：

```json
{
  "taskId": "task-example",
  "message": "Re-read the updated instructions and continue"
}
```

当前 durable 会让每个 worker 自动拥有 `worker:<taskId>`。省略 `itemKey` 时，工具使用这个控制 key。领域消息应明确指定 worker 拥有的领域 key：

```json
{
  "taskId": "task-example",
  "message": "CI changed; re-query its authoritative status",
  "itemKey": "ci:run-42",
  "requireLease": true
}
```

- 普通 steering：`requireLease=false`（默认）。Sender 优先使用匹配的 wait lease，否则使用存活 receiver identity；忙碌 worker 收到 follow-up。
- Durable watcher 交接：`requireLease=true`。Sender 必须绑定精确的已武装 lease，不会回退到未武装 receiver。
- 已武装 worker **只接受 lease 的领域 `itemKey`**。这时即使普通 steering 也必须传该 key；`worker:<taskId>` 不是绕过 lease 的通道。
- 可选 `state` 默认 `main.followup`；`level` 为 `yellow`（默认）或 `red`。工具成功只表示 **已发布**，不表示已收到或已完成。

向存活 worker 追加指令应使用 `notify_subagent`。只有恢复已结束/暂停的 worker，或必须重启进程以加载变更后的 tools、extensions、MCP runtime 时，才使用 `subagent_reload`。Worker reload 不属于保留 worker 的升级。

## Sender CLI

外部脚本应使用配套 sender，而非手写 inbox 文件。以下示例假设 cwd 是包 checkout；否则请使用实际安装的脚本路径。

```bash
python3 scripts/notify_agent.py send \
  "CI run completed; re-read its authoritative status" \
  --item ci:run-42 \
  --to task-example \
  --state ci.run.completed \
  --source ci-watcher \
  --event-id ci-run-42-completed-1 \
  --level yellow --ttl 300

# 规范化领域事件并路由到当前 receiver：
python3 scripts/notify_agent.py send --event-file /path/to/event.json

# 必须唤醒被 agent_end hold 保持的 worker 的 watcher：
python3 scripts/notify_agent.py send --event-file /path/to/event.json --require-lease
```

输入事件格式（请生成当前时间戳；下面的日期仅为示例）：

```json
{
  "schemaVersion": 1,
  "eventId": "ci-run-42-completed-1",
  "producer": "ci-watcher",
  "occurredAt": "2026-10-02T12:30:00Z",
  "expiresAt": "2026-10-02T12:35:00Z",
  "itemKey": "ci:run-42",
  "target": {"taskId": "task-example"},
  "eventType": "ci.run.completed",
  "terminal": true,
  "payload": {"status": "completed"}
}
```

输入的 `schemaVersion: 1` 表示领域事件格式，不是 inbox 协议版本。Sender 解析注册/receiver identity，分配单调序号，并在持久 outbox 中原子发布规范化的 **v2** envelope。顶层 `terminal: true`（或以 `.terminal` 结尾的事件类型）设置 terminal 提示；仅放在 `payload` 内的标记不会设置该提示。提示不是业务状态验证。

CLI 参数：

| 参数 | 用途 / 默认值 |
| --- | --- |
| `--item KEY`、`--to TASK` | 显式指定 worker 拥有的事项及 task ID |
| `--session-id ID` | 精确主会话，包括满足条件的离线会话 |
| `--cwd DIR` | 按 cwd 路由主会话，必须只匹配一个存活会话 |
| `--event-file FILE` | 领域 JSON 输入；`target.taskId` / `target.sessionId` 选择 receiver |
| `--event-id ID` | 用于重试的稳定身份；未提供时从输入派生 |
| `--state TYPE`、`--source NAME` | 默认分别为 `actionable`、`script` |
| `--level green\|yellow\|red` | 默认 `yellow` |
| `--ttl SECONDS` | 默认 `3600`；event-file 的 `expiresAt` 优先 |
| `--require-lease` | 必须存在已武装 worker wait，不回退到 receiver |
| `--lease-wait-seconds N` | 等待 lease 创建，默认 `60`；`0` 表示不存在时立即拒绝 |

主会话通知未指定事项时，sender 派生 cwd 范围的 key；worker 必须显式指定已拥有的 key。Item key 长度为 1–200 字符，匹配 `[A-Za-z0-9][A-Za-z0-9._:@+/-]*`，排除 `.`/`..` 和 `//`。Target/event ID 使用 `[A-Za-z0-9][A-Za-z0-9._:-]*`，不能含斜杠。

输出为 JSON：成功包含 `ok`、`alreadySent`、`path`、`event`；拒绝时向 stderr 输出 `ok=false` 及错误信息，并以非零码退出。重用已发布的 `eventId` 在 sender 保留的历史范围内（14 天）是幂等的：`alreadySent=true` **不会** 替换或重新绑定原事件，也不能证明投递成功。重试时提供稳定事件 ID；新生成的时间戳可能改变自动派生的 ID。过期、不安全、无所有权或有歧义的输入均 fail-closed。

仍兼容省略 `send` 的旧调用：

```bash
python3 scripts/notify_agent.py "CI changed" --item ci:run-42 --to task-example
```

## Durable worker 等待协议

在委派任务文本中明确声明领域所有权：

```text
itemKey: ci:run-42
```

随后：

1. 启动 **一个有界的外部 watcher**，目标为该 worker 当前的 `PI_SUBAGENT_TASK_ID`；不要复用旧 worker 的 task ID。
2. 在仓库内持久化 JSON producer marker，例如：
   ```json
   {"pid": 12345, "itemKey": "ci:run-42", "notifyTo": "task-example"}
   ```
3. 验证 producer PID 存活，marker 的 PID/item/target 均匹配。
4. 调用 `arm_notification_wait`，传入 `itemKey`、`reason`、精确的 `wakeCondition`、`producerPid`、`producerMarkerPath`、有界 `leaseSeconds` 和可选 `checkpointPath`。Marker/checkpoint 路径必须位于 worker cwd 内；推荐使用仓库相对路径。扩展记录 `wakeCondition`，不会代替你查询外部系统。
5. 结束当前 turn。不要用 sleep/polling 阻塞前台工具。Lease 会在 `agent_end` 保持该进程存活。
6. Watcher 使用 `--require-lease` 发送。匹配 task/item/runId/nonce 的事件消耗 lease，并恢复 **同一个会话**。恢复后先重新查询权威状态，再行动。
7. 通常应明确武装下一次 wait。作为有界安全网，若原 producer 仍存活、turn 结束时没有重新武装，扩展会 **仅一次** 恢复同一个已释放 lease，且不超过原有效期。Producer 已死则不恢复。仅有 terminal 提示不会关闭这个安全网：确认权威终态后，应完成/停止 producer。

Lease 路径：`${PI_AGENT_NOTIFY_DIR:-/tmp/pi-agent-notify}/<taskId>/.notification-wait-lease`。时长为 **30 秒–24 小时**，默认 **6 小时**。Producer 缺失/死亡、marker/item 不匹配、cwd 越界、所有权/receiver heartbeat 陈旧或 nonce 错误都会导致拒绝。Lease 到期会排入 follow-up，要求 worker 重查状态/checkpoint。

释放 `agent_end` hold 只是让队列中的唤醒消息运行，**不是 ACK**。Worker shutdown/**reload 仍会释放并删除 lease**。协议保留的是完成准备后跨父进程退出的现有 worker，不是任意 worker 进程重启；不保证自动重启 worker、恢复作业或跨主机故障存活。

## Receiver 所有权与精确身份

Durable 提供加锁、原子更新、校验 ownership token 的 `.active-workers.json` 注册表。Receiver-owned 记录包含：

```json
{
  "ownershipMode": "receiver",
  "taskId": "task-example",
  "workerPid": 12345,
  "ownerPid": 12345,
  "parentSessionId": "parent-session-example",
  "cwd": "/path/to/project",
  "receiverIdentityPath": "/path/to/notify/task-example/.receiver-identity.json",
  "itemKeys": ["ci:run-42", "worker:task-example"]
}
```

路径仅为示例，不应据此手写注册文件。Notify 校验：

- 精确 task/target 与预期 receiver-identity 路径，拒绝符号链接替代。
- `workerPid` 存活，`ownerPid == workerPid`，receiver 的 `pid == workerPid`。
- 解析后的 cwd 匹配、**item-key 集合完全相同**，且拥有本次请求的 key。
- RunId/nonce 合法，receiver heartbeat 新鲜（不超过 **90 秒**；notify 拒绝超前 **5 分钟** 以上的时间戳）。
- 绑定 lease 时，receiver/lease 的 runId、nonce、PID、cwd 均匹配。

Receiver-owned 存活判断 **不依赖** 已退出父进程的 heartbeat。旧 parent-owned 注册仍要求 parent owner 存活，所有权 heartbeat 不超过 **120 秒**。未知 ownership mode 会被拒绝，不会降级处理。Durable 准备流程可能采用更严格的新鲜度检查；应遵循其拒绝/报告，而非放宽身份校验。

## 主会话路由与离线恢复

使用精确的 `target.sessionId` / `--session-id`；或在 **只匹配一个存活、已注册主会话** 时按 cwd 路由。同一 cwd 有多个会话会被拒绝，不存在“挑最新会话”的回退。

持久化主会话在 durable state root 下保存 `main-identities/<sha256(sessionId)[0:24]>.json`。恢复 **同一 canonical session 文件、session ID、cwd** 时，恢复原 runId/nonce。首次进程内迁移 `/reload`（如果使用）保留已有的 `globalThis` 身份；下文的外部 durable bootstrap **并不要求先 reload**。新建/fork 的 session ID 不继承另一个会话的身份。同 ID 但 canonical 路径/cwd 不同的 mirror、损坏状态或另一活动 controller 都会 fail-closed。

Python guardian 通过 POSIX 排他 `flock` 持有 `<identity>.controller`，直到 Pi 关闭私有 pipe，异常退出也会关闭。被排他的进程停止摄取通知并请求 Pi shutdown，避免继续作为第二个 transcript writer。陈旧 PID 元数据不等于锁；**绝不可删除 controller lock 文件绕过竞争**。Controller 脚本必须与 sender 配套安装，notify/state roots 保持不变。

只有精确 ID 能离线路由，且必须存在持久身份并通过 canonical session header 校验：

```bash
python3 scripts/notify_agent.py send "Re-read CI status" \
  --item ci:run-42 --session-id EXACT_SESSION_ID \
  --event-id ci-run-42-offline-1 --ttl 300
```

离线路由窗口为 **最后一次主会话 heartbeat 后 24 小时**。离线事件必须在 **发送后一小时内** 且在该恢复窗口内过期；接近窗口边界时使用更短 TTL。24 小时窗口不意味着事件可存活 24 小时：过期事件不会重放。拒绝离线 cwd 路由、未知/ephemeral session 和 task mirror 文件。重发已发布事件 ID 不会刷新 TTL，也不会重新绑定 nonce。

## 只升级父进程，不重启 worker

需要安装章节说明的配套 durable 能力。仅 **异步 RMUX** worker 符合要求；plain-spawn、同步、chain 任务不在此范围内。保持 RMUX/主机/session 存储可用，并保留 cwd，以及已配置的 `PI_CODING_AGENT_DIR`、`PI_AGENT_NOTIFY_DIR`、`PI_AGENT_NOTIFY_STATE_DIR`。

### 首次迁移：旧父进程没有 prepare 命令

**不要仅为准备升级而 `/reload` root/父会话。** 准备好更新后的 notify/durable 源码，在旧父会话用 `/session` 获取精确 canonical JSONL 路径。让父会话保持 idle、没有待处理对话工作，并停止委派新任务。在另一个终端执行：

```bash
node /path/to/pi-subagent-durable/scripts/prepare-upgrade.mjs \
  --session '/absolute/canonical-parent.jsonl'
```

Bootstrap 不加载 Pi，也不向业务 worker 发信号或 reload。它必须同时保留旧 main 已注册的 runId/nonce **和** worker 所有权。必须确认成功的 `Prepared …`、`Preserved exact main notification identity …` 报告、预期 task 数量、receiver-keeper PID/deadline。被拒绝时 **不要退出父进程**。准备成功后 **立即只退出该父进程**；旧代码无法自动冻结之后的 dispatch。

### 后续 prepared upgrade

1. 在已更新且 **idle 的父进程** 执行 `/agent:prepare-upgrade`。它验证 canonical parent/child session、RMUX worker、receiver identity，迁移所有权、持久化 handoff、冻结 dispatch，并打印精确重启命令。被拒绝时不要退出。
2. 只退出父 Pi。不要使用 `subagent_stop`、`subagent_reload`、`/agent:stop-all`，也不要停止 worker 所在 RMUX workspace。
3. 推荐 durable 的 side-by-side launcher：先检查 dry-run，再移除 `--dry-run` 执行：
   ```bash
   /path/to/pi-subagent-durable/scripts/pi-safe-upgrade.sh \
     --version EXACT_VERSION --session '/absolute/canonical-parent.jsonl' --dry-run
   ```
   它检查 preparation，单独安装精确 Pi 版本，不替换存活 worker 使用的全局安装。恢复打印出的 **精确父会话**，不要用 `--continue`、猜最新会话或 fork。同时避免覆盖旧 worker 仍使用的 extension 文件。
4. Durable cold-start recovery 为该父会话恢复监控；必要时用 `/agent:recover` 显式 reconcile。核对恢复报告及权威任务状态。现有 worker/watcher 保持 task ID、receiver PID、lease identity，**不会被重启**。

### 旧 worker 的迁移边界

旧 notify 代码可能仍检查 parent registry heartbeat，并在离线 **120 秒** 后清空 receiver item keys。配套 durable preparation 会在 **父进程退出前** 启动有界、detached 的 **receiver-heartbeat keeper**。它只镜像新鲜且已验证的 receiver 时间戳，不伪造存活、不发事件，最多运行 **24 小时**。必须确认成功的 handoff/PID/deadline 报告；只迁移 PID 不足以保证交接。应在报告的期限内恢复父进程。

迁移不会热补丁旧 worker：它仍保留旧 queue-time dedup/ACK 行为，直到自然启动时加载新扩展。下文的会话磁盘 ACK 保证只适用于运行新代码的 receiver。超出 keeper 期限的停机、机器/RMUX 重启、`/tmp` lease 丢失、producer 死亡、worker 进程 reload 均不在保证范围。Keeper 期限与主会话离线路由窗口是两个独立限制。

## 会话磁盘 receipt ACK 与重放

每个可行动用户消息带有稳定的 `[[agent-notify:<eventId>:<identity digest>]]` 标记。发布先于接收，队列接受先于 ACK。

扩展在调用 Pi **之前** 持久化 inflight journal，阻止反复扫描及同进程 reload 重复入队；只有匹配的用户消息字节已写入 **精确 session JSONL** 并完成 fsync，才确认并移除 outbox。`message_end` 仅安排磁盘检查：Pi 可能在消息持久化 **之前** 发出该事件。被动 green 条目同样需要持久化的 custom-entry receipt。

Cold start 时，即使旧进程在保存 dedup 前崩溃，已有磁盘 receipt 也可完成 ACK。有效、未过期、未确认的 journal/outbox 事件可能 **至少一次** 重放。SessionManager 首轮内存条目不是持久 receipt；ephemeral session 无法承诺恢复。结果不明确的异步 queue failure 可能留在 inflight，直到 controller 重启。Receipt ACK 证明的是 transcript 持久化，**不是 agent 执行或业务完成**。重新查询权威状态，并让副作用幂等。

## Envelope v2 与等级

```json
{
  "version": 2,
  "eventId": "ci-run-42-completed-1",
  "runId": "run-example",
  "itemKey": "ci:run-42",
  "targetKind": "worker",
  "targetId": "task-example",
  "state": "ci.run.completed",
  "sequence": 4,
  "occurredAt": "2026-10-02T12:30:00Z",
  "expiresAt": "2026-10-02T12:35:00Z",
  "nonce": "0123456789abcdef0123456789abcdef",
  "level": "yellow",
  "source": "ci-watcher",
  "message": "CI changed; re-query status",
  "actionable": true,
  "terminal": true
}
```

实际身份字段应由 sender 获取；不要复制示例 run ID/nonce。

- `green`：被动 memo/custom entry，通常不触发 agent turn。**匹配已武装 lease 的定向 green 事件仍会唤醒 worker。**
- `yellow`：agent 可行动消息；忙碌 agent 收到 follow-up。
- `red`：请求用户关注/决策，有 UI 时显示提示；仍然去重。

Actionable/terminal 事件和匹配 lease 的唤醒会尽快 flush；普通非立即事件使用 batch window。事件 ID 历史保留 14 天；语义重复采用可配置 cooldown（默认 30 分钟），所以不同 ID 的相似消息仍可能合并。序号/时间戳校验拒绝陈旧、乱序的事件流消息。通知不是完整审计日志，也不是 exactly-once 工作队列。

## 状态目录与配置

| 环境变量 | 默认值 / 用途 |
| --- | --- |
| `PI_AGENT_NOTIFY_DIR` | `/tmp/pi-agent-notify`：inbox、存活注册与 wait lease |
| `PI_AGENT_NOTIFY_STATE_DIR` | `~/.pi/agent/agent-notify`：持久 outbox、identity、dedup、inflight、quarantine |
| `PI_AGENT_NOTIFY_LOG` | `<state>/events.log`：receiver 事件日志 |
| `PI_AGENT_NOTIFY_SENDER` | 包内 `scripts/notify_agent.py`：主会话工具 sender 路径覆盖 |
| `PI_AGENT_NOTIFY_CONTROLLER` | 所选 sender 同目录的 `session_controller.py`：guardian 路径覆盖 |
| `PI_AGENT_NOTIFY_SCAN_MS` | `2000`：扫描间隔，最小 `50` ms |
| `PI_AGENT_NOTIFY_BATCH_MS` | `15000`：批处理窗口，最小 `0` ms |
| `PI_AGENT_NOTIFY_DEDUP_MS` | `1800000`：语义去重 cooldown，最小 `1000` ms |
| `PI_SUBAGENT_TASK_ID` | Durable 为 worker 设置；主会话不设置 |

Sender、父进程、worker 必须使用一致的 inbox/state 覆盖；应在启动这些进程 **之前** export。`PI_CODING_AGENT_DIR` 是 durable/Pi 配置，不能替代 notify 独立的 state-root 覆盖。

重要路径：

- `<inbox>/.active-workers.json`：durable 管理的 worker 注册表。
- `<inbox>/<taskId>/`：worker inbox、`.receiver-identity.json`、`.notification-wait-lease`、sender `.last-event` marker。
- `<inbox>/.main-sessions/*.json` 与 `<inbox>/main/<sessionId>/`：主会话注册/inbox。
- `<inbox>/main-pending/<cwdHash>/`：旧 main pending 桥接目录。
- `<state>/main-identities/`：持久主身份与 controller lock 文件。
- `<state>/outbox/`、`sender-state.json`、`sender-state.lock`：持久 sender 发布/序号历史。
- `<state>/inflight/` 与 `dedup/`：可恢复 pending/inflight 事件及 receipt 确认后的去重状态。
- `<state>/quarantine/`：无效/不匹配 envelope 及原因文件。

私有目录/文件以限制性权限创建。保守清理陈旧 temp/processing 文件、死亡 main 注册和孤立 worker inbox；孤立清理不会删除注册中的 worker。Bookkeeping dotfile **绝不** 作为入站 envelope 收集。不要删除 state/lock/lease 文件强制恢复；先检查日志和权威状态。

## Protocol-v1 迁移兼容

扩展仍摄取 worker/main inbox 中的旧 `{level, source, message, itemId, ts}` 文件。Worker 所有权仍需验证；缺失的 v2 身份只绑定到存活 receiver。Main 注册提供旧数字 `heartbeat` 别名，主会话读取 `main-pending/<cwdHash>`。

这只是 **输入桥接**，不为旧 cwd sender 提供离线精确身份安全。新代码应始终使用通用 sender 和 v2 envelope。

## 开发与验证边界

在源码 checkout 内执行（runtime 包不发布 tests/dev dependencies）：

```bash
npm ci
npm test                 # 与 npm run check 相同的测试套件
npm pack --dry-run       # 核对实际发布文件 allowlist
git diff --check
```

GitHub Actions CI（`.github/workflows/ci.yml`）配置为在 push 到 `main` 和 pull request 时，于 `ubuntu-latest` 上使用 Node.js **22**、Python **3.12** 执行 `npm ci` 和同一套 `npm test`。这描述的是已配置的 CI 覆盖，不是更广泛的 runtime 兼容矩阵，也不是完成 live upgrade 的证据。

测试覆盖通用 item key、v1 worker/main 摄取及 legacy pending 路由、忙碌 worker identity fallback、陈旧 lease 清理、所有权/路径拒绝、精确 nonce 路由、一次 auto-rearm（包括 green 唤醒）、重放/过期拒绝、steering 工具默认值、bookkeeping dotfile 排除、sender 幂等和并发序号分配。

真实 OS 子进程搭配可控 Pi API/JSONL fixture，验证 main SIGKILL/cold restart、离线精确 sender/TTL、同 session controller 排他及 shutdown 请求、首次 reload 身份保留、扫描/同进程 reload 不重复入队、持久 receipt 前后崩溃、错误 nonce 隔离、父进程退出后的 receiver-owned 投递与 fork 隔离。Sender 测试还拒绝 receiver PID/task/cwd/key 集合/新鲜度不匹配及未知 ownership mode。

这些是 **fixture/回归测试**，不是 live model/RMUX 父进程升级测试、任意 worker restart recovery、主机故障保证或 Pi/provider 兼容矩阵。生产升级前需验证配套 durable 集成。可复现检查方法：用 `npm pack --dry-run` 检查发布内容，不能仅相信 ignore；queue acceptance/`message_end` 不等于磁盘 ACK；24 小时离线身份窗口不会延长一小时事件 TTL。

## 公共文档与私有材料

个人文档放在被忽略的 `docs/private/` 或 `docs/local/`；实际配置/状态放在公开源码树之外（或 ignored local 路径内）。包使用显式文件 allowlist，包含两份 README 和 changelog。Ignore 规则不会移除已跟踪文件或旧 commit 中的文件。公开示例应使用通用路径/事项，不应包含私有部署或业务材料。

[MIT License](LICENSE)
