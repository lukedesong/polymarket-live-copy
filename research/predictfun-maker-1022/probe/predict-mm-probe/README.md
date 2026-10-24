# predict-mm-probe:Predict.fun BTC 5m 实盘探针

目的:**校准回放器(model/mm_grid.py)的成交模型,不是赚钱。** 同一批轮次实盘 vs 回放逐笔对比:成交率、挂单时前面排了多少、成交后 5 秒币价 / 盘口走向、撤单生效前被吃的比例。规格见 `../PROBE_SPEC.md`,背景见 `../../HANDOFF_1024.md` 第 2 节。

单个可执行文件,Rust 2021(tokio、tokio-tungstenite、reqwest + rustls、alloy、serde)。不依赖服务器上任何现有文件。

> 默认 **`--dry-run`**:连真实行情、做全部决策、只打印将要发的请求,**不发任何写请求(包括鉴权)**。加 `--send` 才真发单。

---

## 1. 编译

```bash
cd research/predictfun-maker-1022/probe/predict-mm-probe
cargo build --release          # 产物:target/release/predict-mm-probe
```

在 Rust 1.97 上编译和测试过。只用 rustls(aws-lc-rs 后端),不需要系统 OpenSSL。

## 2. 运行

### dry-run(默认,不需要任何凭证)

```bash
./target/release/predict-mm-probe --log-dir ~/probe_logs --duration-s 3600
```

- 没有私钥时用**临时随机私钥**走一遍签名流程(签出来的单不会发出去)。
- 有 `PREDICT_API_KEY` 就带在公开 GET 请求上,没有就不带。
- 下单 / 撤单的回报按实测往返 35ms / 18ms 模拟,所以改价、撤单、冷却这些流程都会完整走一遍。没有成交。
- 打印的请求体里**签名被隐藏**:签好的单谁拿到都能替你提交。
- 账本写 `state-dry.json`,和实盘账本互不影响。

### --send(真发单)

凭证**只从环境变量读**,不进命令行,不进 `--help`,不打印,写日志前统一脱敏:

| 环境变量 | 说明 |
|---|---|
| `PREDICT_API_KEY` | 请求头 `x-api-key`;WebSocket 连接 `?apiKey=` |
| `PREDICT_PRIVATE_KEY` | Privy EOA 私钥(签名者) |
| `PREDICT_ACCOUNT_ADDRESS` | Predict 账户 = 智能钱包存款地址(订单的 maker 和 signer) |
| `PREDICT_RPC_URL` | 可选,BNB 链 RPC,默认 `https://bsc-rpc.publicnode.com` |

东京机上的启动命令(变量来自 `~/pflat/.env`,不回显):

```bash
set -a; . ~/pflat/.env; set +a; ./target/release/predict-mm-probe --send --log-dir ~/probe_logs --quiet
```

`--send` 启动时按顺序检查,任何一步失败都不启动:

1. 账本里有停机标记 → 拒绝启动。需人工确认后加 `--clear-halt`(亏损超限仍会立刻再停)。
2. 链上查存款地址在 ECDSA 验证器里登记的 owner,必须等于私钥对应的地址(与 SDK `OrderBuilder.make` 的检查相同)。
3. 鉴权:`GET /v1/auth/message` → Kernel 包装签名 → `POST /v1/auth` → JWT。
4. 全撤(列出账户所有挂单,全部撤掉)。

退出时(Ctrl-C、SIGTERM、`--duration-s` 或 `--max-rounds` 到了、停机)先按状态机已知的单全撤,再用 REST 列一遍兜底全撤,然后账本落盘。退出码:0 = 正常,1 = 出错,2 = 风控停机。

## 3. 配置项

全部参数都可以用命令行或对应的环境变量设置,默认值 = 规格第 2 节。`--help` 可看完整列表。

| 参数 | 环境变量 | 默认 | 说明 |
|---|---|---|---|
| `--send` / `--dry-run` | | dry-run | 模式 |
| `--log-dir` | `PROBE_LOG_DIR` | `logs` | 日志目录 |
| `--state-file` | `PROBE_STATE_FILE` | `<log_dir>/state.json`(dry-run 为 `state-dry.json`) | 风控账本 |
| `--clear-halt` | | | 清除账本里的停机标记 |
| `--quiet` | `PROBE_QUIET` | | 不同时打印到 stderr |
| `--duration-s` | `PROBE_DURATION_S` | 0(不限) | 跑多少秒后全撤退出 |
| `--max-rounds` | `PROBE_MAX_ROUNDS` | 0(不限) | 跑多少轮后全撤退出 |
| `--api-base` | `PROBE_API_BASE` | `https://api.predict.fun` | |
| `--predict-ws` | `PROBE_PREDICT_WS` | `wss://ws.predict.fun/ws` | |
| `--binance-ws` | `PROBE_BINANCE_WS` | `wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker` | |
| `--rpc-url` | `PREDICT_RPC_URL` | `https://bsc-rpc.publicnode.com` | |
| `--no-claim` | `PROBE_NO_CLAIM` | | 不做结算后的 merge / redeem |
| `--stop-before-end-s` | `PROBE_STOP_BEFORE_END_S` | 30 | 剩余秒数 ≤ 此值全撤停挂 |
| `--min-notional-usd` | `PROBE_MIN_NOTIONAL_USD` | 1.0 | 单量 = ceil(此值 / 价) 股 |
| `--reprice-min-age-ms` | `PROBE_REPRICE_MIN_AGE_MS` | 1000 | 挂单存在满这么久才允许改价 |
| `--bn-window-ms` | `PROBE_BN_WINDOW_MS` | 300 | 币安急动窗口 |
| `--bn-move-bp` | `PROBE_BN_MOVE_BP` | 0.2 | 窗口内不利方向变动 ≥ 此值 → 撤该侧 |
| `--bn-quiet-ms` | `PROBE_BN_QUIET_MS` | 300 | 连续这么久不再触发才算"回到正常" |
| `--inventory-cap-usd` | `PROBE_INVENTORY_CAP_USD` | 6 | 净持仓等值 ≥ 此值,加仓侧不挂 |
| `--max-daily-loss-usd` | `PROBE_MAX_DAILY_LOSS_USD` | 30 | 当日已实现 + 未实现亏损 ≥ 此值 → 全撤停机 |
| `--max-total-loss-usd` | `PROBE_MAX_TOTAL_LOSS_USD` | 100 | 累计亏损 ≥ 此值 → 全撤停机 |
| `--feed-down-ms` | `PROBE_FEED_DOWN_MS` | 3000 | 任一连接断开 / 静默超过此值 → 全撤冷却 |
| `--binance-stale-ms` | `PROBE_BINANCE_STALE_MS` | 1000 | 币安这么久没消息就禁止新挂 |
| `--predict-stale-ms` | `PROBE_PREDICT_STALE_MS` | 15000 | Predict 连接这么久没有任何消息(含心跳)视为异常 |
| `--cooldown-s` | `PROBE_COOLDOWN_S` | 60 | 冷却时长 |
| `--ack-timeout-ms` | `PROBE_ACK_TIMEOUT_MS` | 5000 | 下单 / 撤单回报超时 → 冷却 |
| `--max-cooldowns-per-hour` | `PROBE_MAX_COOLDOWNS_PER_HOUR` | 5 | 1 小时内冷却次数超过此值 → 停机 |
| `--day-offset-hours` | `PROBE_DAY_OFFSET_HOURS` | 0 | "当日"的时区:0 = UTC,8 = 北京时间 |
| `--rate-per-sec` / `--rate-per-min` | `PROBE_RATE_PER_SEC` / `_PER_MIN` | 40 / 500 | 平台限额(实测,全站所有接口共用) |
| `--rate-margin` | `PROBE_RATE_MARGIN` | 0.2 | 余量 → 本地预算每秒 32、每分钟 400 |
| `--poll-ms` | `PROBE_POLL_MS` | 3000 | REST 查挂单兜底的间隔 |

`--send` 有硬上限,防止手滑把探针变成真做市:单量 ≤ $5、库存 ≤ $30、日亏 ≤ $30、累计 ≤ $100、限额不超过实测值且余量 ≥ 10%。超过直接拒绝启动。

## 4. 策略(与规格逐条对应)

两个挂单侧,都是**只做挂单(`isPostOnly: true`)的 BUY 限价单**:

- **UpBid**:买 Up,价格 = Up 买一。
- **DownBid**:买 Down,价格 = 1 − Up 卖一。等价于在卖一卖 Up,不需要先持有 Up。

规则:

1. 只做当前轮;开盘前不挂;剩余 ≤ 30 秒全撤停挂。当前轮和下一轮的盘口提前订阅,开盘前的数据也会记下来。
2. 单量 = ceil($1 / 价) 股(价 0.5 → 2 股,0.01 → 100 股)。
3. **改价**:我们的价 ≠ 当前最优价,且挂单已存在 ≥ 1 秒 → 先撤旧单,撤单确认后再挂新单。同一侧任何时刻最多 1 张。语义与回放器 behind=0 一致:我们自己就在最优价时不动。
4. **币安保护**:BTCUSDT bookTicker 中间价在 300ms 窗口内对该侧不利 ≥ 0.2bp 就立刻撤该侧,不看挂单年龄。下跌撤 UpBid,上涨撤 DownBid。之后连续 300ms 不再触发,按第 3 条重挂。
5. **库存上限**:净持仓 = Up 股数 − Down 股数,按 Predict 中间价折成美元;拿不到中间价时按每股 $1 计。≥ $6 时加仓那一侧不挂,已挂的也撤。
6. 留到结算,不吃单平仓。收盘后:等 60 秒,两边都有余额就 merge;出结果后 redeem 赢的一边(平局两边都 redeem)。
7. **风控**:
   - 当日亏损 ≥ $30 或累计亏损 ≥ $100 → 全撤停机,停机标记写进账本。
   - 以下任一情况 → 全撤,冷却 60 秒:连接断开 / 静默 > 3 秒;签名、下单、撤单报错;任何接口返回 429;回报超时;401(同时重新鉴权)。
   - 1 小时内冷却超过 5 次 → 停机。
8. **限额**:每个请求先从本地令牌桶取令牌,优先级 撤单 > 新挂 > 查询。新挂必须给撤单留出每秒 4 个、每分钟 40 个,查询留得更多。
   - 令牌桶用**滑动窗口计数**实现,保证任意 1 秒 ≤ 32、任意 60 秒 ≤ 400。普通令牌桶(容量 C、速率 r)在 60 秒里最多能放出 C + 60r 个,会超过平台的分钟限额,所以不用。
   - 响应头 `ratelimit` 显示剩余 ≤ 2 时,1 秒内只放撤单。
   - 查询(结算结果、REST 查挂单兜底)排队超过 5 秒就升到和新挂同级,防止改价频繁时被饿死;撤单的保留额度不受影响。

## 5. 签名(与官方 SDK 逐字节一致)

`src/signing.rs` 复现 `@predictdotfun/sdk` **1.3.8**(`ref/package.json` 写的 `^1.3.8` 实际解析到 1.3.8)的完整流程:

`getLimitOrderAmounts → buildOrder("LIMIT") → buildTypedData → signTypedDataOrder → buildTypedDataHash`

- **金额**:价格保留 3 位有效数字、股数保留 5 位,BUY 时 `makerAmount = 价 × 股 / 1e18`(向下取整)、`takerAmount = 股`;SELL 相反。
- **EIP-712**:
  - domain = `predict.fun CTF Exchange` / `1` / 56 / 交易所合约。交易所合约由 isNegRisk × isYieldBearing 四选一,BTC 5m 是 CTFExchange `0x8BC0…B689`。
  - 类型 = SDK `ORDER_STRUCTURE` 的 12 个字段。
  - `expiration` = 4102444800(SDK 默认的 2100 年),`nonce` = 0,`signatureType` = 0,salt 随机 0..=2³¹。
- **predictAccount(智能钱包)模式**:
  - `maker` = `signer` = 存款地址,**原样保留配置的字符串**(SDK 也不做大小写规范化)。
  - 签名 = 先算 typed-data hash,再按 Kernel EIP-712 包一层(域 `Kernel` / `0.3.1` / 56 / 存款地址,类型 `Kernel(bytes32 hash)`),对包好的 32 字节做 EIP-191 personal_sign。
  - 输出 `0x01 ‖ ECDSA 验证器地址 0x845A…cE57 ‖ r ‖ s ‖ v`,共 86 字节,v = 27/28。
  - 鉴权消息签名同理,只是内层 hash 换成 EIP-191 `hashMessage(消息)`。
- **EOA 模式**(只用于测试覆盖):直接对 typed-data hash 签名,65 字节。
- **请求体**:与 `ref/pf_latency.mjs signLimit()` 的序列化逐字节相同,即 `{"data":{"order":{…,"signature","hash"},"pricePerShare","strategy":"LIMIT","isPostOnly":true}}`。bigint 转字符串,`expiration`、`side`、`signatureType` 是数字。

## 6. 日志格式

- 文件:`<log_dir>/probe_<UTC 小时 YYYYMMDDHH>_<启动时刻 HHMMSS>.jsonl.gz`。gzip 压缩的 JSON Lines,按 UTC 小时滚动;每次启动写新文件,每 5 秒刷新一次,写着的文件也能 `zcat`。
- 每行都有 `mono_ns`(本机 CLOCK_MONOTONIC 纳秒,只能在同一台机器内比较)、`wall_ms`(墙钟毫秒,和录制器的 `time.time_ns()` 对齐用)、`kind`。
- API key、JWT、私钥、RPC 地址在写入前一律替换成 `<redacted>`。

| kind | 内容 |
|---|---|
| `bn` | 币安每条 bookTicker 原文(`raw` 字段,字符串,与录制器原文可直接对比) |
| `pf` | Predict WS 每条推送原文:盘口(含 bids / asks / version / settlementsPending / lastOrderSettled)、心跳、钱包事件、订阅回包 |
| `pf_rest` | 开盘时 REST 拉的盘口快照(只在还没收到 WS 盘口时用) |
| `ws_send` | 发出的订阅 / 退订消息 |
| `feed_state` / `feed_error` | 连接断开 / 恢复与原因 |
| `market` | 每轮的市场信息:token、feeRateBps、isNegRisk、isYieldBearing、交易所、conditionId |
| `round_start` / `round_end` | 轮次开始 / 收盘;`round_end` 带该轮持仓和全部订单记录 |
| `decision` | 每次决策。`action` = place / cancel / cancel_all / hold;`reason`;`ctx` = {best_bid, best_ask(分), bn_mid, target(分), ms_left, queue_ahead(挂单时该价位已有的股数,即前面排了多少), net_up_shares};place 时还有完整订单摘要(hash、token、金额) |
| `dry_request` | dry-run 下将要发的请求(签名已隐藏) |
| `req` / `resp` | 每个 REST 请求:tag、method、path、`sent_mono_ns`、`sent_wall_ms`、`rtt_ms`、status、响应头 `ratelimit` / `ratelimit-policy` / `retry-after`、响应体(鉴权回包不记) |
| `ack` / `cancel_ack` / `cancel_all_done` | 下单(orderId)/ 撤单 / 全撤结果 |
| `order_detail` | `GET /v1/orders/<hash>` 结果:filled、status、open;`after_cancel=true` 是撤单后查的(撤单生效前被吃了多少) |
| `wallet_event` | 钱包事件的容错解析结果(原文在 `pf`) |
| `fill` | 每笔成交:order_id、`source`(wallet / rest_list / rest_detail / rest_after_cancel / rest_assumed)、本次股数、累计股数、订单(价、侧)、报告价、对手方(有就记)、当时 bn_mid 与 up_mid |
| `settle` | 结算结果(Up / Down 每股兑付)、该轮持仓、盈亏、累计已实现、当日盈亏 |
| `chain` | merge / redeem:余额、估算 gas、交易哈希、回执状态或错误 |
| `start` / `exit` / `halt` / `info` / `warn` / `error` / `fatal` | 运行状态;`exit` 带最终账本 |

读法示例:

```bash
zcat ~/probe_logs/probe_*.jsonl.gz | jq -c 'select(.kind=="fill")'
zcat ~/probe_logs/probe_*.jsonl.gz | jq -c 'select(.kind=="decision" and .action=="place") | [.wall_ms, .order.side, .order.price, .ctx.queue_ahead]'
zcat ~/probe_logs/probe_*.jsonl.gz | jq -r 'select(.kind=="pf") | .raw' | head   # 还原成录制器同款原文
```

账本 `state.json`(实盘)/ `state-dry.json`(dry-run)是普通 JSON:未结的轮(含持仓与成本)、按日已实现盈亏、累计已实现、停机标记。先写临时文件再改名,断电不会留下半个文件。

## 7. 测试

```bash
(cd testvec && npm ci)   # 可选:装官方 SDK 1.3.8,启用"现场随机私钥 vs Node SDK"测试;不装则该项跳过
cargo test               # 全部测试(约 1 分钟;端到端测试离收盘不足 45 秒时会等到下一轮)
cargo test --release --test e2e_mock long_run -- --ignored --nocapture   # 10 分钟长跑(本地模拟服务器)
```

| 测试 | 内容 |
|---|---|
| `tests/sign_vectors.rs` | **签名逐字节一致**。<br>固定向量(`tests/data/sign_vectors.json`,由 `testvec/gen_vectors.mjs` 用官方 SDK 生成):<br>· 买/卖 × isNegRisk × isYieldBearing × 价 0.01/0.5/0.99,predictAccount 与 EOA 两种模式,外加小写地址抽查,共 54 组;每组比对金额、订单 JSON、请求体 JSON、typed-data hash、签名(含 v)<br>· 金额取整 38 组<br>· 鉴权签名 3 组<br>· merge / redeem 的 Kernel.execute calldata 12 组<br>现场向量:Rust 随机生成私钥和存款地址,交给 node 跑 SDK,再逐条比对 |
| `tests/strategy.rs` | 策略单测,把录制格式的 pf / bn 原文过解析器喂给状态机,不连网:改价(含 1 秒门槛、先撤后挂)、币安急动撤单与安静期后重挂、库存上限、剩 30 秒停挂、开盘前不挂、当日亏损停机、累计亏损停机(含重启后仍拒挂)、429 全撤冷却、下单错误 / 回报超时 / 迟到回报、断线 > 3 秒、币安静默、冷却次数过多停机、限额耗尽时撤单仍能发而新挂被挡、成交记账 |
| `tests/ratelimit.rs` | 令牌桶:每秒 32 / 每分钟 400、任意窗口不超限(3 分钟模拟)、撤单优先、429 后只放撤单 |
| `tests/parse_samples.rs` | 用 `ref/ws_samples.tsv`(副本在 `tests/data/`)的真实消息做解析测试:币安现货 / 合约 bookTicker 与 trade、Predict 盘口(单边盘、settlementsPending、长小数)、心跳、订阅回包、钱包事件容错 |
| `tests/e2e_mock.rs` | 端到端:本机起假的 Predict REST + WS、币安 WS、BNB RPC,跑编译好的可执行文件。<br>· dry-run 不发任何写请求<br>· --send 全流程:鉴权 → 启动全撤 → 挂单(请求体结构)→ 改价 → 钱包事件成交 → REST 核实 → 退出全撤后没有残留挂单<br>· 429 后不再下单<br>· owner 不符拒绝启动<br>· 日志和 stderr 里不出现任何凭证 |
| `tests/fake_logs.rs` | 仿真场景模拟器:币安随机游走、Predict 盘口跟公允价走、逆向 / 随机成交、收盘结算;生成 `../analysis/tests/data/fake_run/` 的假日志与真值(`generate`,默认忽略),外加 15 秒冒烟测试 |
| 各模块内单测 | 取整、日期、账本落盘、日志 gzip 与脱敏、市场 / 订单 / 结算回包解析 |

重新生成固定向量(会换一把新的随机私钥):`cd testvec && npm ci && node gen_vectors.mjs --out ../tests/data/sign_vectors.json`。

## 8. 已知局限,以及规格没写清、按"更保守"实现的地方

**必须在首次实盘前用 dry-run 或第一轮 --send 的日志核对**(云端会话连不上 Predict 和币安,下面这些字段都没对过真实数据):

1. **钱包事件格式没录过。** 按多种可能的字段名容错解析,原文照记,只当"该查单了"的提示用;成交以 REST 为准。兜底规则:
   - 每 3 秒 `GET /v1/orders` 列一次挂单;我们以为还挂着、列表里却没有的单,去查单。
   - 查单显示已关闭但读不出成交量 → **按全部成交记**(`source=rest_assumed`,宁可多算库存)。
2. **订单详情 / 挂单列表的字段名是猜的**:`amountFilled`(wei 或小数都认)、`status`(OPEN / FILLED / CANCELLED…)、`id` / `orderId`、`order.hash`。第一次实盘请看 `order_detail` 日志确认能读出 `filled`。
3. **结算结果按 `outcomes[].status = WON/LOST` 解析**(平局两边都 WON 各 0.5)。解析不出来时,该轮在账本里一直按最坏结果 min(Up, Down) − 成本估值,而且所有未结的轮都算进"当日"。这会让日亏停机**提早**触发(保守方向)。第一次实盘请确认有 `settle` 日志。
4. **盘口**:假定 `predictOrderbook/<id>` 的每条推送都是 Up(第一个结果)视角的全量快照(ref/pf_latency.mjs 也是这么用的),买一 / 卖一取价格的最大 / 最小值,不依赖推送里的排序。
5. **`queue_ahead`** = 挂单那一刻盘口在该价位显示的股数,不含我们自己这张单;推送有延迟时,可能还含着我们刚撤掉的单。
6. **Predict 连接健康**:心跳间隔没实测过。程序每 5 秒发一次 WebSocket ping,收到 pong、心跳或任何推送都算连接活着;默认 15 秒什么都没收到就算异常(`--predict-stale-ms` 可调)。币安 1 秒没消息就禁止新挂;断开或静默超过 3 秒全撤冷却。外部长时间故障时,冷却会反复触发,1 小时超过 5 次就停机(保守方向)。
7. **改价是先撤后挂**,同一侧永远最多 1 张单;撤单往返(约 18ms)期间该侧没有单。
8. **冷却与停机**:规格只写了"冷却",没写时长和次数,这里定为冷却 60 秒、1 小时内超过 5 次就停机。停机写进账本,重启也不会恢复,需人工 `--clear-halt`。累计亏损跨重启累计。
9. **"当日"默认 UTC**(`--day-offset-hours 8` 改成北京时间)。
10. **库存上限**:到上限时加仓侧不挂,已挂的也撤;还在路上的那一张若成交,最多超出约 $1。
11. **撤单失败先查单**:已成交完 / 已关闭不算错误;仍挂着或查不到才进冷却。回报超时被放弃的单,如果之后回报才到,立刻撤掉;限额不够就每个 tick 重试,直到撤掉为止。
12. **全撤的范围是账户的所有挂单**(包括其它市场的)。探针账户应专用。被 `kill -9` 时来不及全撤,下次启动会先全撤。
13. **claim / merge 只验证了 calldata 与 SDK 一致,没在真实链上跑过。**
    - 流程:收盘 60 秒后 merge(两边余额的较小值);每 60 秒查一次结果,最多查 2 小时;出结果后 redeem 赢的一边。
    - 每个动作最多发一笔交易;gas 估算失败(比如还没出结果)就不发、不花钱;所有链上交易串行发,避免 nonce 冲突;发出后失败只记日志,不重试。
    - 需要 EOA 里有 BNB 付 gas;市场信息缺 `conditionId` 时跳过。
14. **JWT 有效期未知**:收到 401 → 全撤冷却 + 重新鉴权(30 秒最多一次)+ 换钱包事件订阅。
15. **dry-run 没有成交**(回报是模拟的),所以库存、风控、结算相关的路径只能在 --send 或单测里验证。
16. 云端只能对着**本地模拟服务器**跑端到端和 10 分钟长跑;真实行情下的 dry-run 需要在东京机上做(交接卡第 2 节:先空跑 ≥ 1 小时核对日志)。
