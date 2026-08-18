# Polymarket 实盘跟单系统：评审交接与修复发布手册

> 交接对象：负责独立 review、定位和在得到 Luke 明确授权后修复系统的 Agent  
> 文档生成时间：2026-08-17 01:22（Asia/Shanghai，实证值：本机与服务器即时采样）  
> 权威修复版本：**3.17**（实证值：服务器版本解析器 11 / 11 检查通过）  
> 当前 release：`/opt/polymarket-live/releases/20260816T152612Z-partial-gtc-immediate-cancel-v3.17`  
> 当前版本切换时间：`2026-08-16T15:43:04.916712Z`（实证值：服务器不可变版本时间线）

## 0. 先读：评审权限和安全边界

本文件可以直接交给另一个 Agent，但默认只授权**只读评审**。评审 Agent 可以连接服务器、读取代码、服务状态、脱敏配置元数据、状态文件、SQLite 和日志；不能因为看到问题就自行扩大真实交易授权。

未经 Luke 在当前任务中明确授权，不得：

- 提交、撤销或补做任何真实订单；
- 追补历史漏单，或用当前盘口伪造旧动作的成交；
- 重启、unmask 或恢复已删除的 CD90 跟单；
- 改变源钱包、跟单比例、价格保护、策略范围或资金授权；
- 删除、重建或手改 live SQLite、持仓、订单回执、结算回执；
- 对 `UNKNOWN_SUBMISSION` 或已接受但尚未完成最终链上核验的订单重新提交；
- 把私钥、CLOB API key/secret/passphrase、Bark key、签名对象或完整环境变量复制到聊天、Markdown、补丁、测试夹具、日志或命令行。

### 凭证如何让 Agent 测试，但不泄漏

真实凭证只保留在服务器受限环境文件中：

| Profile | 服务器凭证文件 | 权限（2026-08-17 实证值） |
|---|---|---|
| Zockdo | `/etc/polymarket-live/zockdo-live.env` | `0640 root:polymarket-live` |
| wallet9506 | `/etc/polymarket-live/wallet-9506-live.env` | `0640 root:polymarket-live` |
| CD90（跟单已删除） | `cd90-live.env` 必须不存在 | 禁止重建 |
| Bark/独立告警 | `/etc/polymarket-live/deadman-alerter.env` | `0640 root:polymarket-live` |

不要 `cat`、`grep`、打印或复制这些文件内容。需要鉴权验证时，优先运行已部署的 systemd health service 或 closed-loop release 自带的鉴权检查，让进程在服务器内部读取 EnvironmentFile。日志只能输出检查结果、订单数量和哈希，不能输出凭证值。

共享实盘锁与 coordinator：

- `/srv/polymarket-live/runtime/authenticated-wallet.lock`，权限 `0600 polymarket-live:polymarket-live`；
- `/srv/polymarket-live/runtime/shared_wallet/coordinator.sqlite3`，权限 `0600 polymarket-live:polymarket-live`。

如果确实需要轮换密钥，由 Luke 通过服务器受限文件或密钥管理方式单独提供；不要把密钥补进本文件。

## 1. SSH 连接方法

本机已经配置 SSH alias：

```bash
ssh polymarket-hk
```

即时解析结果（实证值）：

- alias：`polymarket-hk`
- host：`154.204.176.56`
- port：`26325`
- user：`lukeadmin`
- server hostname：`ser884149206582`

建议始终使用 alias，不要重新分发私钥路径或复制 SSH 私钥。验证 alias：

```bash
ssh -G polymarket-hk | grep -E '^(hostname|user|port) '
```

服务器环境（2026-08-17 01:22 Asia/Shanghai 实证值）：

- Ubuntu 24.04.1 LTS
- x86_64
- Python 3.12.3
- Python venv：`/opt/polymarket-live/venv/bin/python`
- 根盘：20G 总量、6.1G 已用、14G 可用、使用率 32%

只读评审常用入口：

```bash
ssh polymarket-hk
readlink -f /opt/polymarket-live/current
sudo systemctl status com.luke.polymarket.zockdo-live.service --no-pager
sudo systemctl status com.luke.polymarket.wallet-9506-live.service --no-pager
sudo systemctl status com.luke.polymarket.cd90-live.service --no-pager
```

## 2. 系统目标：到底在做什么

P0 目标只有一个：**尽可能忠实复刻源钱包的完整动作序列**。源钱包买，我方按已确认的固定份额比例买；源钱包卖，我方只卖该 sleeve 由跟单形成且可证明归属的库存。

这不是一个追求“每笔都正期望”的系统。BUY、SELL、亏损、回撤、退出和多腿篮子都属于需要复制的源动作。源钱包的 edge 来自其完整组合；系统不能为了让本地结果好看而选择性丢弃亏损腿、低胜率动作或 SELL。

### 最小核心链

1. 从 Polygon/官方公开数据发现源钱包的新动作。
2. 用 `(transaction_hash, token_id, side, order_hash)` 作为动作唯一身份。
3. 用 `block_number + source_log_index` 保持链上因果顺序。
4. 按 profile 的固定 share scale 计算目标份额。
5. 读取官方市场状态、tick、最低量、盘口和费用。
6. 在共享认证钱包锁内生成并持久化预测订单哈希，再提交官方 CLOB 订单。
7. 用官方订单、官方成交和链上 `OrderFilled` 回执对账。
8. 本地 SQLite 只负责审计、去重、游标、订单状态和 sleeve 库存归属，不是第二个真实钱包余额。
9. `UNKNOWN_SUBMISSION` 只读对账，绝不 repost。
10. 已删除的跟单钱包保留 residual 账本和证据，但不能被巡检或发布工具自动恢复。

### 价格与数量合同

- Zockdo：固定复制源份额的 50%（用户指定值，不代表数学最优）。
- wallet9506：固定复制源份额的 10%（用户指定值，不代表数学最优）。
- CD90：跟单已删除且不得恢复；协调器 residual 账本必须保留。
- BUY 限价是**最差可接受价格上限**，不是要求按该价格成交。实际成交应从最优 Ask 开始。
- 高价 BUY 的既有保护、最低量保护和固定比例不得被修复顺手改变。
- 剩余量低于官方最低量时，不得向上凑整放大。
- SELL 数量不能超过本 sleeve 的可证明库存，绝不卖空或借用别的 sleeve/人工持仓。

### 三套账必须分清

| 对象 | 权威数据 | 用途 |
|---|---|---|
| 源钱包公开账 | Polymarket Data API、公开持仓和链上动作 | 判断源做了什么；公开接口看不到其真实现金全貌和未成交挂单 |
| 我方真实共享钱包 | authenticated collateral、官方 open orders/trades、链上 receipt | 真实现金、真实订单、真实成交、费用和结算权威 |
| sleeve 本地账 | 各 profile 的 `live.sqlite3` | 动作去重、游标、订单审计、跟单形成的库存归属；不能冒充第二个真实余额 |

## 3. 当前部署结构

### 3.1 代码与发布目录

- 当前软链接：`/opt/polymarket-live/current`
- 当前真实目标：`/opt/polymarket-live/releases/20260816T152612Z-partial-gtc-immediate-cancel-v3.17`
- 固定版本索引：`/opt/polymarket-live/CURRENT_REPAIR_VERSION.json`
- 版本时间线：`/srv/polymarket-live/runtime/server_health/repair_version_timeline.jsonl`
- 当前 release 内至少包含：`app/`、`ops/`、`systemd/`、`tests/`、`tools/`、`MANIFEST.sha256`、`CANDIDATE_TEST_RECEIPT.json`、`COMMITTED.json`

重要：本机 `/Users/luke/Documents/polymarket/COMMITTED.json` 当前仍是 3.15，而生产服务器是 3.17。**任何修复必须以服务器 current 的 3.17 为代码基线，不能把本机旧目录直接部署覆盖服务器。** 本地变更要先与 3.17 对齐并检查差异。

共享核心主要文件：

- `app/cd90_live_copy.py`：核心发现、执行、订单状态、对账与状态输出；
- `app/cd90_live_sizing.py`：固定比例、最低量、盘口和 BUY/SELL sizing；
- `app/live_chain_client.py`：链上客户端；
- `app/live_copy_profiles.py`：profile 范围和源动作规则；
- `app/live_wallet_coordinator.py`：共享钱包现金、库存、condition 和赎回归属；
- `app/zockdo_live_copy.py`、`app/wallet9506_live_copy.py`：profile wrapper；
- `app/server_health_heartbeat.py`：多 profile 健康检查；
- `ops/polymarket-deadman-alerter.py`：独立 Bark 告警。

发布入口：

- `tools/deploy_three_wallet_core_hotfix_release.sh`
- `tools/live_release_transaction.py`
- `tools/verify_repair_version_authority.py`
- `tools/assert_no_authenticated_open_orders.py`

### 3.2 当前 profile 拓扑

截至 2026-08-17 01:22 Asia/Shanghai：

| Profile | 源地址 | 模式 | Primary | Hot standby | 当前意图 |
|---|---|---|---|---|---|
| Zockdo | `0xcd741947f7430f96bf1820a0b30d8a0fad3100a1` | CASH_LIVE | active/enabled | active/enabled | 运行，50% 份额 |
| wallet9506 | `0x9506e646497107cabf2d5b941a8e6a60d0db1c4f` | CASH_LIVE | active/enabled | active/enabled | 运行，10% 份额 |
| CD90 | `0xcd90fe632f3068abe89a15503a22c364db494bfc` | CASH_LIVE | inactive/disabled | inactive/disabled | 用户明确暂停，禁止巡检恢复 |

当前进程实证：Zockdo primary/standby 各一个，wallet9506 primary/standby 各一个，CD90 为零进程。systemd 曾出现 unit 为 active 但 `MainPID=0` 的读取瞬间，而 `pgrep` 能看到实际进程；评审应同时核对 cgroup、锁持有者和进程 cwd，不能只看一个字段。

### 3.3 运行时与数据库

| 对象 | 路径 | 2026-08-17 大小 | integrity | foreign key |
|---|---|---:|---|---:|
| Zockdo | `/srv/polymarket-live/runtime/zockdo_live/live.sqlite3` | 18,993,152 bytes | ok | 0 violations |
| wallet9506 | `/srv/polymarket-live/runtime/wallet_9506_live/live.sqlite3` | 2,568,192 bytes | ok | 0 violations |
| CD90 | `/srv/polymarket-live/runtime/cd90_live/live.sqlite3` | 20,590,592 bytes | ok | 0 violations |
| coordinator | `/srv/polymarket-live/runtime/shared_wallet/coordinator.sqlite3` | 61,440 bytes | ok | 0 violations |

另有 `/srv/polymarket-live/runtime/live.sqlite3` 零字节游离文件。它不应被误认作正式账本；先确认创建者和引用关系，再决定是否清理，不能直接删除。

动态运行时快照会在下单/对账期间变化。2026-08-17 01:24 附近的只读采样：

- Zockdo：72 条 source action receipt，66 条有 target；target 状态为 42 external-unfillable、7 filled、2 partial、15 skipped；0 活动 reservation，0 未决 submission。
- wallet9506：88 条 source action receipt，87 条 target；target 状态为 47 external-unfillable、2 filled、28 partial、9 skipped，并在采样瞬间出现 1 条 submitted-unreconciled、1 个活动 reservation。随后数据库处于短暂写锁，说明该值是活跃交易时点快照，评审必须重新读取官方订单和链回执，不能据此重发。
- CD90：122 条历史 source action receipt；当前服务暂停，其 status 已陈旧，历史 pending 不能被重新打开或追单。

### 3.4 定时任务

- `com.luke.polymarket.live-health.timer`：服务器健康审计；
- `com.luke.polymarket.deadman-alerter.timer`：独立存活/Bark 告警；
- `com.luke.polymarket.daily-safe-gc.timer`：安全磁盘清理；
- 旧 paper-fleet maintenance 已禁用。

Bark 只应通知当前新问题，并使用中文写清楚：哪个 profile、哪个市场、哪个动作、实际状态、具体原因、是否可能产生订单。不能只发内部错误码，也不能反复发送历史问题。

## 4. 当前应优先 review 的问题

以下不是“都已经确认是代码 bug”，而是从服务器当前证据得出的 review 队列。

### P0：订单副作用与重复单

1. 任一 accepted/UNKNOWN/取消中订单是否可能进入 retry 队列。
2. 预测 order hash、CLOB response order ID、链上 `OrderFilled` 是否逐笔一致。
3. 取消后是否等待足够的 finalized-chain 覆盖再认定零成交。
4. partial fill 是否立即取消剩余挂单，再只重试“目标减累计官方成交”的精确剩余量。
5. wallet9506 在 01:24 快照出现 1 个未决 submission 和 1 个活动 reservation；必须先只读对账官方订单和链上回执，禁止 repost。

### P0：结算和共享现金归属

Zockdo 最近错误中反复出现：

- `BLOCK_DELTA_MISMATCH`
- `SETTLEMENT_RECLASSIFICATION_EXCEEDS_EXTERNAL_CASH_RESERVE`
- 多个 condition 的 `BLOCK_AMBIGUOUS_ONCHAIN_INVENTORY`

这可能包含人工交易、跨 sleeve 共享现金和自动赎回归属之间的差异。审查原则：官方 authenticated collateral、官方交易和链上 receipt 是权威；本地 sleeve 不得为了“闭合数字”编造补差或改写历史。

### P0：活动撤单状态

Zockdo 在 `2026-08-16T15:29:37Z` 左右记录过 `ACTIVE_CANCEL_ORDER_STILL_OPEN`。要检查当时 exact order ID 的官方最终状态、取消回执和其后链上最终区块覆盖；不能只看当前 open-orders 为零就删除历史错误。

### P1：错误日志放大

Zockdo 的 `runtime_errors` 累计 19,770 条，其中：

- 18,835 条 `EXTERNAL_ORDER_RECONCILIATION`
- 698 条 `EXTERNAL_ONCHAIN_ORDER_HASH_RECONCILIATION`
- 66 条 `EXTERNAL_OFFICIAL_REDEMPTION_ACTIVITY`
- 48 条 `EXTERNAL_ACCOUNT_CASH_RECONCILIATION`
- 46 条 `INTERNAL_RUNTIME`

这些是累计实证值，不等于 19,770 个独立事故。需要按 `(action_id, order_id, category, root cause, incident window)` 聚合，判断是否每个轮询都重复落同一错误，避免日志噪音淹没新事故。修复日志去重不能删除历史表；应以前向去重/单次 incident receipt 实现。

wallet9506 累计 34 条 runtime error，其中 30 条是外部 Polygon receipt 暂时不可用；这是累计外部错误，不等于 30 个漏单。

### P1：进程与遗留文件卫生

服务器上存在几组旧测试产生的 shell wait-loop 进程，等待早已结束的 pytest 名称；它们不是 executor，但应确认创建者和父进程后安全停止。不要用宽泛 `pkill python` 或 `pkill bash`。

同时审查：

- primary/standby 是否各只有一个进程；
- 两者是否通过同一 profile runtime lock 实现单一提交所有权；
- 进程 cwd 是否为 current release 的 `app/`；
- unit 的 `EnvironmentFile`、coordinator path 和 wallet lock 是否与提交拓扑一致；
- 巡检不得把用户暂停的 CD90 当故障自动恢复。

### 当前外部流动性案例：不是静默漏发现

用户指出 Zockdo 的 `ATP O'Connell vs Ruud 2026-08-16` 没跟到。服务器证据：

- action id：`3f89b752...863b77d`
- 源动作：BUY，3,485.7931 shares，source notional 3,032.639997 USD，源成交均价 0.87
- 50% 固定比例目标：1,742.89655 shares（公式推导值）
- 从 source timestamp 到首次发现约 0.956 秒（公式推导值）
- 处理状态：`PENDING_EXTERNAL_RETRY`
- 原因：`BOOK_SNAPSHOT_ERROR: RuntimeError: NO_ASK_BOOK_LEVEL`
- submission attempt：0

这说明系统约一秒内发现了动作，但发现时官方盘口没有 Ask；当前再次核验仍无 Ask。它是外部不可成交，不是内部静默漏单。后续只能在既有前向价格/重试合同内等待可成交盘口，不能用当前不同价格补造历史成交。

## 5. 之前的主要错误与修复历史

### 5.1 灾难级重复单：accepted order 被误判零成交

典型事件：`Cincinnati Open: Zachary Svajda vs Mattia Bellucci`。

错误链：

1. 订单已经被 CLOB 接受；
2. 当时查询还看不到最终链上成交；
3. 系统过早认定 FAK/订单零成交，释放 reservation；
4. 同一 source action 被重新提交；
5. 晚到的多个成交最终同时出现，形成超额复制。

更严重的复发案例：Jan Kumstat – Marvin Moeller。源动作 30.37 shares，50% 目标应为 15.185 shares；同一动作被提交 59 次，最终累计 890.8027 shares。首次扫描 finalized head 为 92,118,396，而真实 fill 位于 92,118,400 等更晚区块。根因不是源钱包重复买，而是“取消/查询后尚未覆盖真实 fill 区块”就允许 retry。

永久规则：accepted、`SUBMITTED_UNRECONCILED`、`UNKNOWN_SUBMISSION` 只能只读对账；必须用 exact order hash、官方 order/trade、取消状态和足够的 finalized-chain 覆盖证明未成交，才能释放 reservation。绝不能因为 `get_order()` 暂时空、RPC 未返回或当前 open-orders 为零就 repost。

### 5.2 partial fill 后剩余挂单处理不正确

早期系统会让 GTC 剩余挂单继续停留到统一取消窗口，或者在部分成交和后续 retry 之间存在竞态。3.17 的修复方向是：已确认 partial fill 后立即取消剩余挂单；保留 reservation，直到 post-cancel finalized-chain 对账覆盖；随后仅处理精确剩余量。SELL 路径不随 BUY 修复改变。

当前 3.17 release receipt 记录：相关 copy/release 测试 332 项通过、独立 targeted red-team 5 项通过、发布验证时 authenticated open orders 为 0、验证期真实提交为 0。它证明发布时合同通过，不等于今后运行永远无 bug；仍需按新水位审查新动作。

### 5.3 旧巡检错误停止或恢复 profile

曾发生旧巡检按硬编码 profile 列表管理服务，导致 CD90 被错误停止；当时排查没有先识别“是谁停止了服务”，反而继续修改代码，形成无效修复。之后又暴露新 profile 加入后，旧巡检可能把它当异常进程处理。

永久规则：服务拓扑必须来自当前已提交 release/registry 和用户当前明确暂停状态，不能来自旧聊天或硬编码旧名单。当前 CD90 是用户明确暂停，不是故障。

### 5.4 唯一版本号不统一

过去不同对话把发布目录时间、hotfix 名称或候选号当版本，导致同一系统出现多个“当前版本”。现已建立固定权威：

- `/opt/polymarket-live/CURRENT_REPAIR_VERSION.json`
- current release 的 `COMMITTED.json`
- `repair_version_timeline.jsonl` 最新 `verified_repair_release`

三者必须一致，固定解析器必须全部通过。任何一项不一致都应报告 `BLOCK_VERSION_AUTHORITY`，不能猜版本。

### 5.5 deadman/Bark 使用旧脚本

3.3 只改了新 release 内文案，但 systemd 仍执行 `/usr/local/sbin` 的旧副本，生产实际没有使用新逻辑。3.4 将入口改为 `/opt/polymarket-live/current/ops/polymarket-deadman-alerter.py`。

永久规则：不能只验证文件内容；必须验证 systemd `ExecStart`、进程 cwd/exe 和真实运行输出都指向 current release。

### 5.6 版本后效果与历史问题混算

过去报告会把旧版本遗留的 skipped/未成交混入新版本效果，导致无法判断修复是否有效。每次 review 必须以 current `COMMITTED.json` 的精确 cutover 为分界；旧记录保留为历史证据，但新版本评价只统计 cutover 后首次出现的动作和错误。

注意：这不意味着可以删除 live SQLite 的历史行。正确做法是在查询/状态页按 release cutoff 过滤，或建立前向 review receipt；不能通过删账本制造“新开始”。

### 5.7 账务口径反复混乱

过去曾混用 Polymarket 页面资产、共享钱包真实现金、sleeve 归属现金、allocation 输入、持仓盯市和已实现盈亏，出现“一个账户两个可用金额”和报账与网页不一致。

永久规则：回答前先说对象。真实钱包余额只认 authenticated collateral 减真实活动 BUY reservation；页面资产用官方公开仓位/盯市；sleeve 的本地现金只表示策略归属，不是第二个真实余额。已结算盈亏不能混入未结算仓位或可赎回未入账价值。

### 5.8 最新动作报错对象错误

曾把较旧的 Jan Kumstat 事件误报成“最新成交”，而当时真正最新动作已经是 Real Racing Club。查询“最新”必须按服务器当前时间和 source timestamp/live receipt 排序，不能拿印象最深的历史事故回答。

### 5.9 BUY 限价被错误解释

曾把更高的 BUY limit 解释成“会按更高价格成交”。实际限价是最坏上限，成交从当前最优 Ask 开始。修复或评审价格损耗时，必须区分 source fill、我方 limit、实际 official fill 和费用。

### 5.10 流动性、最低量、现金和 UNKNOWN 被笼统称为“外部受阻”

过去未成交报告只给一个总称，无法判断系统问题。现在每笔都必须落具体原因：无 Ask、盘口深度不足、价格保护、低于官方最低量、共享现金不足、SELL 无本 sleeve 库存、市场关闭、RPC 不可用、订单状态不确定、内部错误。每类处理不同，不能统一 retry。

### 5.11 网球风险处置过度工程化

用户明确要求清仓并暂停网球时，系统先花时间设计 operator-exit 工具，导致用户只能手动卖出。永久规则：先识别用户要的即时风险结果，再做最小安全动作；不能在风险窗口把简单操作扩张成新框架。人工成交随后只能依据官方链回执做幂等归属，不得伪装成源动作。

### 5.12 只记录纠错但不回放

多次出现同类错误：报账对象不清、版本号不统一、旧问题混入新版本、发现问题后无闭环、简单任务过度设计。当前强制流程是：理解任务 → 最小修改 → 最小验证 → 交付；开始前读取纠错规则，结束前逐条防复发复核。

## 6. 权威版本时间线（3.0—3.17）

以下均为服务器不可变时间线中的实证记录。版本号只表示一次 `VERIFIED_FIXED` 修复，不代表系统从此无缺陷。

| 版本 | 时间（UTC） | 修复主题 |
|---|---|---|
| 3.0 | 2026-08-14 18:46 | current-only review 与 Bark 基础 |
| 3.1 | 2026-08-14 19:15 | wallet9506 10% live 与 finalized reconcile |
| 3.2 | 2026-08-15 04:43 | transaction receipt order-hash fallback |
| 3.3 | 2026-08-15 06:07 | deadman 当前版本文案；后被证明生产入口仍旧 |
| 3.4 | 2026-08-15 07:35 | deadman systemd 指向 current release |
| 3.5 | 2026-08-15 09:44 | liquidity-only retry |
| 3.6 | 2026-08-15 11:32 | GTD30 全钱包尝试 |
| 3.7 | 2026-08-15 12:18 | 唯一版本权威解析器 |
| 3.8 | 2026-08-15 12:25 | GTD60 / minimum 修复 |
| 3.9 | 2026-08-15 12:40 | GTC + 主动取消 |
| 3.10 | 2026-08-15 12:55 | 私有版本时间线权限修复 |
| 3.11 | 2026-08-15 14:55 | 测试覆盖合同 |
| 3.12 | 2026-08-15 15:37 | deadman 正确认识暂停 profile |
| 3.13 | 2026-08-15 19:41 | active cancel 非阻塞化 |
| 3.14 | 2026-08-16 07:38 | BUY 60 秒 / SELL FAK |
| 3.15 | 2026-08-16 11:46 | dynamic BUY depth |
| 3.16 | 2026-08-16 14:08 | GTC 取消后的 finalized-chain 终局核验 |
| 3.17 | 2026-08-16 15:43 | partial GTC 后立即取消并精确处理剩余量 |

## 7. 独立 review 的建议顺序

### 7.1 第一步：版本和代码身份

```bash
ssh polymarket-hk /opt/polymarket-live/venv/bin/python \
  /opt/polymarket-live/current/tools/verify_repair_version_authority.py
```

必须看到 `state=VERIFIED_FIXED`、`semantic_repair_version=3.17`、`checks_passed=11`、`checks_total=11`。如果失败，先报告 `BLOCK_VERSION_AUTHORITY`，停止任何发布操作。

再核对：

```bash
ssh polymarket-hk 'readlink -f /opt/polymarket-live/current'
ssh polymarket-hk 'sudo sha256sum /opt/polymarket-live/current/COMMITTED.json /opt/polymarket-live/current/MANIFEST.sha256'
```

### 7.2 第二步：服务与单一所有权

```bash
ssh polymarket-hk "systemctl list-units --type=service --all --no-legend 'com.luke.polymarket.*live*.service'"
ssh polymarket-hk "pgrep -af 'zockdo_live_copy.py|wallet9506_live_copy.py|cd90_live_copy.py'"
```

期望：Zockdo 和 wallet9506 各一个 primary、一个 standby；CD90 零进程且 disabled。继续核对进程 cwd、unit EnvironmentFiles、profile lock 和 shared wallet lock 的 inode/持有者。

### 7.3 第三步：SQLite 和动作守恒

所有 live DB 使用只读 URI；不要复制 WAL 中间态后直接分析主文件。最小检查：

```sql
PRAGMA integrity_check;
PRAGMA foreign_key_check;
SELECT state, COUNT(*) FROM action_targets GROUP BY state;
SELECT state, COUNT(*) FROM submission_attempts GROUP BY state;
SELECT active, COUNT(*) FROM order_reservations GROUP BY active;
```

核验公式：观察到的 source actions 必须全部进入有证据的 target/skip/scope 状态；订单的累计官方成交不能超过 target；一个 source action 不能存在多个仍可能成交的有效订单。

### 7.4 第四步：官方副作用

在不打印环境变量的前提下，通过已部署 health/release 工具核验：

- authenticated open orders；
- submitted/unknown exact order IDs；
- official order status；
- exact order-hash 的链上 `OrderFilled`；
- authenticated collateral；
- redemption/settlement transaction identity。

公共 Data API 适合核源动作，但不是我方订单是否成交的最终权威。官方接口暂时不可用时保持 reservation 和 UNKNOWN，只读重试，不得推断零成交。

### 7.5 第五步：只看当前版本切换后的新问题

3.17 的评审起点是 `2026-08-16T15:43:04.916712Z`。报告必须分开：

- 3.17 切换前历史问题；
- 3.17 切换后新 source action；
- 3.17 切换后新内部错误；
- 外部不可成交；
- 从旧版本携带、但在切换后完成对账的动作。

不要把历史累计 19,770 条 runtime error 直接当成 3.17 新问题数量。

## 8. 修复手册

### 8.1 修复前的最小闭环

1. 用一句话确认真实问题和影响对象。
2. 保存不可变证据：source action、order ID/hash、时间、区块/log index、当前 target、reservation、官方 order/trade、链回执、服务和版本。
3. 判断是内部 bug 还是外部限制。
4. 如果可能造成重复单、反向单、错误数量或账本损坏，只 fail-close 受影响的新下单路径；保留只读对账、安全赎回和无关 profile。
5. 不追历史漏单，不改旧账。

### 8.2 必须从服务器 3.17 基线开始

本地 checkout 落后于服务器。安全做法：

1. 从 `/opt/polymarket-live/current` 取得 3.17 的非敏感 release 代码、测试、systemd、tools 和 manifest；
2. 不复制 `/etc/polymarket-live/*.env`、runtime DB、私钥或任何凭证；
3. 在独立工作树中应用最小 patch；
4. 对比修改前后，只允许任务需要的文件发生变化。

不要基于本地 3.15 直接补丁后部署，否则会把 3.16/3.17 的订单终局和 partial-cancel 修复回退。

### 8.3 TDD 与验证

先写一个能复现真实事故的最小失败测试，再修共享根因。重点不变量：

- 相同 source action 不重复提交；
- accepted/UNKNOWN 不 repost；
- partial 只补精确剩余量；
- BUY limit 是 ceiling，实际成交价来自官方 fill；
- 新保护不能让之前可复制的动作变得不可复制；
- 最低量不向上凑；
- SELL 不卖空、不借库存；
- primary/standby 同一时刻只有一个提交所有者；
- paused CD90 保持暂停；
- 状态页和 Bark 只报告当前版本后的具体问题。

最小验证顺序：

1. 新失败测试先红；
2. 最小实现后该测试绿；
3. 运行受影响 profile wrapper 测试；
4. 运行共享核心和 release contract 相关完整回归；
5. `py_compile`/静态敏感信息扫描；
6. 候选 release 的 manifest 和测试回执校验。

不要在承重测试已经通过后无限扩张开放式审计。发现独立新问题，单独记录；只有它会破坏本次修复的安全性时才阻断当前发布。

### 8.4 建立候选 release

若当前权威版本为 3.17，下一次真正 `VERIFIED_FIXED` 修复的候选版本是 **3.18**（公式：当前 minor 17 + 1；只有一次完整修复发布才加一次）。诊断、报告、测试失败、外部问题或未部署候选都继续叫 3.17。

候选目录格式示例：

```text
/opt/polymarket-live/releases/YYYYMMDDTHHMMSSZ-<short-change>-v3.18
```

要求：

- 目录位于 `/opt/polymarket-live/releases`；
- 不是 symlink；
- root:root 拥有；
- group/other 不可写；
- 包含完整 required assets；
- `MANIFEST.sha256` 覆盖 required assets；
- `CANDIDATE_TEST_RECEIPT.json` 绑定测试输入和 manifest payload；
- 不包含 env、密钥、runtime DB、日志或缓存。

### 8.5 使用现有 closed-loop 发布器

不要手动改 `current` symlink，不要逐个复制 app 文件，也不要手动“先重启看看”。候选准备完成后，从**候选 release 自己的** wrapper 执行：

```bash
/opt/polymarket-live/releases/<candidate>/tools/deploy_three_wallet_core_hotfix_release.sh \
  /opt/polymarket-live/releases/<candidate> \
  <MANIFEST.sha256 文件本身的 SHA-256> \
  <change_id> \
  /var/lib/polymarket-live-release-snapshots/<unique-snapshot-name>
```

四个参数分别是：不可变候选路径、manifest 文件 SHA-256、唯一 change ID、位于受控 snapshot root 下的唯一快照路径。具体值必须现场计算，不能复制本文示例。

release transaction 会负责：

- 候选 immutable/manifest/test receipt 校验；
- release 事务锁、共享钱包锁和 profile locks；
- live DB/coordinator 精确快照；
- active reservations、UNKNOWN、官方 open orders 等 fail-closed gate；
- 停止/切换/启动正确的已注册 profile；
- 保持用户暂停 profile 的原状态；
- current symlink、systemd unit、bridge 和 health 入口切换；
- 失败时回滚；
- 成功后写 `COMMITTED.json` 和发布证据。

### 8.6 唯一版本号更新方法

版本号不能单独手改。一次成功修复发布必须在同一个提交中同时满足：

1. `/opt/polymarket-live/current` 指向新 release；
2. 新 release 的 `COMMITTED.json` 为 `VERIFIED_FIXED`，版本为 3.18；
3. `repair_version_timeline.jsonl` 最新 `verified_repair_release` 指向同一 release、同一版本和 receipt hash；
4. `/opt/polymarket-live/CURRENT_REPAIR_VERSION.json` 指向同一 release、同一 COMMITTED receipt，并保存其真实 SHA-256；
5. 版本解析器 11 / 11 通过。

发布后执行：

```bash
ssh polymarket-hk /opt/polymarket-live/venv/bin/python \
  /opt/polymarket-live/current/tools/verify_repair_version_authority.py
```

只有看到新版本、`VERIFIED_FIXED` 和全部检查通过，才能对 Luke 说“已修复，当前版本 3.18”。如果发布或服务器即时复核失败，版本保持 3.17；不能把候选版本冒充当前版本。

### 8.7 发布后即时复核

必须在服务器上用新鲜证据复核：

- current symlink、MANIFEST、COMMITTED、固定版本索引和 timeline 一致；
- Zockdo/wallet9506 primary + standby 的数量、cwd、exe、unit、锁持有者正确；
- CD90 仍 inactive/disabled；
- 心跳新鲜，WebSocket 活跃，current head 与 cursor 无异常积压；
- 三个 live DB 与 coordinator integrity/foreign key 通过；
- active reservation 与 unresolved submission 每一条都有明确官方状态；
- authenticated open orders 与预期相符；
- 发布期间没有重复订单、反向订单、错误数量或未授权真实提交；
- 切换前后 source-action 前缀、历史订单回执、持仓和结算证据不被改写；
- Bark 用中文发送当前新问题，不重复历史事故。

### 8.8 回滚

发布器失败时让 closed-loop transaction 按其不可变 stop/snapshot evidence 自动回滚。不要手动把 `current` 指向旧目录，也不要手动恢复单个 SQLite 文件。回滚后必须验证：

- old release 重新成为 current；
- 原活动/暂停拓扑恢复；
- 版本索引仍指向旧 verified 版本；
- DB、coordinator、source-action 前缀和官方 open orders 一致；
- 失败候选没有被标成 `VERIFIED_FIXED`。

## 9. 给评审 Agent 的最终交付格式

Review 完成后请按以下格式直接回复 Luke：

1. 第一行：`当前版本: <解析器返回值>`；解析失败则 `当前版本: BLOCK_VERSION_AUTHORITY`。
2. 分别给 Zockdo、wallet9506、CD90 的 PASS/FAIL/PAUSED。
3. 对每个结论给：对象、时间窗口、动作分母、具体状态数、数据源和数字来源分类。
4. 每个未成交逐原因解释，不能写笼统“外部受阻”。
5. 明确区分：内部 bug、外部限制、用户暂停、旧版本历史携带项。
6. 若只 review 未修改，明确写“未修改、未部署、未下单”。
7. 若已获授权修复，给最小 diff、失败测试、通过测试、release、版本解析器和服务器即时副作用证据。
8. 未实际发布并验证，不得写“已修复”。

## 10. 一句话总览

这是一个在同一认证钱包下运行多个独立 sleeve 的实时真实跟单系统：Zockdo 以 50% 源份额运行、wallet9506 以 10% 运行、CD90 按用户要求暂停；当前权威版本 3.17 的核心安全目标是避免 accepted/UNKNOWN/partial-cancel 场景下的重复订单，同时最大化源动作复制完整性。当前最需要独立 review 的不是“再造一个框架”，而是订单终局与 retry 不变量、共享结算差额、错误日志放大、profile 单一所有权，以及 3.17 切换后每一笔新动作的真实官方生命周期。
