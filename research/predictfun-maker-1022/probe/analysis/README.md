# 探针日志分析工具

读 `predict-mm-probe` 写出的日志 `probe_<UTC小时>_<启动时刻>.jsonl.gz`（按小时滚动；日志格式见 `../predict-mm-probe/README.md` 第 6 节），给出校准回放器要用的数字。

- 只用 Python 标准库（3.8 及以上），不联网，不需要装任何东西。
- 参数可以是日志目录，也可以是具体的日志文件，能一次给多个。正在写入的 gz 文件也能读，已经写完的行照常使用。

| 工具 | 回答什么 |
|---|---|
| `markouts.py` | 每笔成交之后 1、5、30 秒以及结算时，我们赚还是亏，各多少 |
| `queue_position.py` | 挂单那一刻我们前面排了多少股；排在队首、队中、队尾时，成交率和成交后走向各是什么样 |
| `replay_compare.py` | 同一批单子按实际挂撤时间交给回放器重放，回放的成交率、成交时刻、成交价比实盘偏多少、偏向哪一边 |
| `speed_queue.py` | 下单、撤单、改价一圈各要多久；挂单时前面多少股；撤单在路上被成交的比例 |
| `fill_model.py` | 回放成交模型(盘口 + 链上挂单成交推进排队),用实盘真挂过的单逐单校准 |
| `maker_rank.py` | 在做市商队伍里我们排第几、排在我们前面的是谁（链上成交 + 我们录的盘口）。唯一要联网的一个：`fetch` 从 BNB 链公共节点拉成交 |

公共部分在 `probelog.py`：读日志、重建订单与成交、计算两种参考价。

```bash
cd research/predictfun-maker-1022/probe/analysis
python3 markouts.py       ~/probe_logs/dry_xxx --out out/
python3 queue_position.py ~/probe_logs/dry_xxx --out out/
python3 replay_compare.py export  ~/probe_logs/dry_xxx --out out/live_orders.tsv
python3 replay_compare.py compare out/live_orders.tsv out/replay_fills.tsv --out out/
```

每个工具都会在屏幕上打印分组表，并把明细和分组写成 TSV。

## 共同口径

**两种挂单方向**

- **买Up**（探针里叫 UpBid）：在 Up 的买一买入 Up。
- **卖Up（买Down）**（探针里叫 DownBid）：在 1 − Up 卖一的价格买入 Down，等价于在 Up 卖一卖出。

**两种参考价**（都是 Up 的价格，取值 0~1）

- **predict**：Predict 盘口中间价，即 (买一 + 卖一) / 2。取 t 时刻及之前最后一条盘口推送。
- **fair**：用币安价格算的公允价 Φ(ln(S/K) / (σ√τ))。
  - S：t 时刻的币安 BTCUSDT 中间价。
  - K：本轮开盘时的币安中间价。取开盘时刻及之前最后一个价，前后 2 秒内必须有数据，否则这一轮没有 fair。
  - σ：近 5 分钟的实际波动。把币安中间价按 1 秒采样，算对数收益的平方和除以秒数再开方；数据不足 60 秒时不给。
  - τ：距收盘的秒数。τ ≤ 0 时用收盘价和 K 比较：高 1、低 0、相等 0.5（平局两边各 0.5）。

**盈亏的算法**

- 我们持有那一边的价值：买 Up 时就是参考价，买 Down 时是 1 − 参考价。
- 每股盈亏 = t+h 时刻的价值 − 成交价；总盈亏 = 每股盈亏 × 股数。
- 挂单不收手续费（返佣另计，这里不算）。
- t+h 已经过了收盘时：predict 改用平台的结算兑付；fair 改用币安收盘价和 K 比较。
- "结算"这一列一律用平台结算结果（日志里的 `settle` 事件）。

**成交时间**

成交时间是探针**发现**这笔成交的时间。`source` 列写明来源：

- `wallet`（钱包事件）：最准。
- `rest_list`、`rest_detail`（REST 查单）：最多晚一个查单间隔（默认 3 秒）。
- `rest_after_cancel`：撤单之后查单才发现。
- `rest_assumed`：单子不见了、又查不到成交量，按全部成交算。

对时间敏感的统计可以用 `--sources wallet` 只看钱包事件。

**覆盖范围**

- t+h 超出了日志的时间范围时，这一格留空，不拿后面不存在的数据去猜。
- 日志从一轮的中途开始时，这一轮没有 K，所以 fair 留空；predict 和结算照常有值。

## 1. markouts.py：成交后价格走向

```bash
python3 markouts.py <日志 ...> [--out 目录] [--horizons 1,5,30] [--sources wallet,rest_list]
```

一次部分成交算一笔。输出两个文件：

- `markout_fills.tsv`：每笔成交一行。包含方向、价格、股数、剩余时间、来源、是否发生在撤单之后，以及两种参考价在成交时的价值和各时点的每股盈亏（列名如 `pnl_predict_5s`、`pnl_fair_30s`、`pnl_settle`）。
- `markout_groups.tsv`：按"方向 × 剩余时间"分组，剩余时间分 240-300s、180-240s、120-180s、60-120s、30-60s、0-30s 六档，另有"全部"汇总行。每个分组 × 参考价 × 时点一行，列有：笔数、股数、每股盈亏、每笔均值、t 值、总盈亏。
  - 每股盈亏按股数加权；每笔均值和 t 值按笔数算，不加权。

## 2. queue_position.py：排队位置

```bash
python3 queue_position.py <日志 ...> [--out 目录] [--edges a,b] [--horizons 1,5,30]
```

- **前面排了多少股**：探针决策日志里的 `ctx.queue_ahead`，即挂单那一刻盘口在我们这个价位上已经显示的股数。我们总是排在已有队伍的后面，所以这就是我们前面的股数，不含我们自己这张单。
- **分组**：
  - 默认按本批订单"前面股数"的三分位切成三组：最少的三分之一为**队首**，中间为**队中**，最多的三分之一为**队尾**。切分点会打印出来。
  - 用 `--edges a,b` 可以改成按固定股数切：≤ a 为队首，(a, b] 为队中，> b 为队尾。
- **每组统计**：挂单数、成交单数、成交率（成交单数 / 挂单数）、成交股数比、前面股数的最小 / 中位 / 最大值、首笔成交用时的中位数、成交后各时点的每股盈亏（两种参考价，按成交股数加权）。
- **输出**：`queue_orders.tsv`（每单一行）、`queue_groups.tsv`（分组）。

## 3. replay_compare.py：实盘与回放对比

**现状**：`research/predictfun-maker-1022/model/mm_grid.py` **不在仓库里**，所有分支都查过了。交接卡记录它在本机 `model/` 下，sha 3f9e99e2d2b0cd47，一直没有提交。它的成交规则读不到，所以"按 mm_grid 规则重放"的那一步（`mm_grid_adapter.py`）只留了接口，没有实现。我没有凭猜测写一套规则来冒充它。导出时间线和对比这两步已经完成并测过。

三个子命令：

1. **`export <日志 ...> --out live_orders.tsv`**
   - 导出每张实际挂上的单（回报成功的单）。
   - 列：所在轮次、方向、价格、它在 Up 盘口上的价位与哪一边（买Up 排在 Up 买盘；买Down 排在 Up 卖盘，价位 = 1 − 价格）、股数、前面排了多少。
   - 还有三个时间：`post_ms` 下单回报到达，即开始挂着；`cancel_sent_ms` 发出撤单；`remove_ms` 实际不再挂着，即撤单确认、全部成交或收盘，取先发生的那个。
   - 以及实盘结果：成交股数、首笔成交时刻、成交价、成交来源。
   - 回放器应只在 [post_ms, remove_ms) 这个区间里判断成交。
2. **`replay live_orders.tsv --model-dir <model 目录> --out replay_fills.tsv [--adapter-arg ...]`**
   - 调用 `mm_grid_adapter.replay_orders`。现在会直接报错并说明缺什么。
   - 也可以在本机直接跑回放器，按下面的格式写出 `replay_fills.tsv`，再用第 3 步对比。
3. **`compare live_orders.tsv replay_fills.tsv --out 目录`**
   - 逐单对比。`replay_fills.tsv` 的列是 `order_id  rp_filled_shares  rp_first_fill_ms  rp_fill_price`，没成交的单可以不写。
   - 每张单分到四类之一：两边都成交、只有实盘成交、只有回放成交、都没成交。
   - 统计项：
     - 成交率的偏差倍数 = 回放成交率 / 实盘成交率，< 1 表示回放低估成交。
     - 成交股数比。
     - 成交时刻差 = 回放首笔 − 实盘首笔，只对两边都成交的单计算，> 0 表示回放成交得晚。
     - 成交价差 = 回放 − 实盘。我们是买方，> 0 表示回放给的价对我们更差。
   - 分组：全部、方向、排队（三分位）、实盘成交来源。
   - 每组打印一句结论，例如"回放成交率 = 实盘 × 0.25,回放低估成交"。
   - 输出 `compare_orders.tsv`（逐单）、`compare_summary.tsv`（分组）。

**拿到 mm_grid.py 之后要做的**：在 `mm_grid_adapter.py` 里实现 `replay_orders`，用 mm_grid 的排队和成交逻辑处理每张单的 [post_ms, remove_ms) 区间。另外需要它依赖的 `pending_fills.py`，以及输入数据（bookx、mkh、fills_pending.parquet）的格式说明。对比部分不用改。

## 4. maker_rank.py：在做市商队伍里排第几

```bash
python3 maker_rank.py fetch  ~/probe_logs/live_xxx --out out/chain.json        # 拉这段时间交易所全部 OrderFilled(公共节点,几分钟)
python3 maker_rank.py report ~/probe_logs/live_xxx --chain out/chain.json --out out/
```

- 平台先挂先成；一笔吃单在链上按撮合顺序逐个发出挂单方的成交，所以同一价位各单在链上第一次出现的先后 = 排队先后，并且能看到每张单的挂单地址。
  买 Up@p 与卖 Down@(1−p) 是同一队。
- 先挂先成用我们自己的单核对两项：同一笔吃单里我们后面还有人成交时我们必须已全成；我们自己同价位的几张单链上先后与挂单先后一致。
- 区块时间比平台撮合晚约 0.4 秒，用我们自己的单（钱包"已撮合"推送里的平台时刻）实测，判断"别人的单在我们挂上之后还在队里"时扣掉。
- 抢新价位：某价位在盘口从无到有后 1 秒内我们决定挂上去。给出：我们被接受时这一价位已有多少股（包括后来撤掉的）、链上排第几、排在前面的地址。
  链上排名只在我们成交时才有，队越长越难成交，所以偏乐观；"被接受时前面已有多少股"没有这个偏差，比较速度时以它为主。
- 输出 `rank_orders.tsv`、`rank_races.tsv`、`rank_rivals.tsv`。

## 5. fill_model.py：回放成交模型与校准

```bash
python3 fill_model.py validate ~/probe_logs/live_xxx --chain out/chain.json --out out/   # chain.json 用 maker_rank.py fetch 拉
```

- 虚拟挂单挂上时,前面的量 = 当时盘口这一档的量;之后链上这一队(买 Up@p ≡ 卖 Down@1−p)每笔挂单方成交先吃前面的量,吃穿了才轮到我们
  (撮合时刻 = 区块时间 − 实测延迟)。盘口变多 = 有人排在后面;变少且不是成交 = 撤单,撤的在前还是在后看不到,三种口径都算:
  `pess` 全在后面、`mixed` 先算后面再一半算前面(旧回放器口径)、`opt` 全在前面。
- 校准:每张实盘真挂过的单,把它自己的链上成交拿掉再预测,和链上真实结果逐单比。盘口里属于它自己的量按真实剩余扣掉。
- 10-24 三轮 621 张单(未审):实盘有成交 314 张;`opt` 预测 322 张(张数比 1.03、股数比 1.00),分轮 1.10 / 1.00 / 0.96、
  按排队三分位 1.02 / 1.05 / 1.00 都稳定;`pess` 0.70、`mixed` 0.72,且排队越长低估越多。旧回放器是实际的 1/4~1/16。
  口径是在这三轮上选的,要在下一次实盘的新数据上预先写下预测再核对。
- 输出 `fill_model_orders.tsv`(逐单:前面的量、存活时长、真实与三种口径的成交股数和首笔时刻)。

## 6. 回测:backtest_data.py + Rust `replay`

```bash
python3 backtest_data.py <录制器小时文件 .log.gz ...> --out bt/ [--chain-json 已下载的链上数据]   # 每小时一次
python3 backtest_data.py index --out bt/                                                     # 写每轮结果与清单
cd ../predict-mm-probe && cargo build --release
target/release/replay --data ../analysis/bt [--from 开盘秒 --to 开盘秒 --exclude …] [策略参数] [--out 每轮.tsv]
```

- 数据:录制器里 BTC 5 分钟盘口的推送、币安现货报价、平台成交记录(只用来把链上 token 对到 市场 / 涨跌)、同一时段链上挂单方成交。
  收盘价 = 元数据里的收盘价,没有就用下一轮开盘价(10-24 三轮核对一致)。区块固定 450ms 一块。
- 引擎(`src/replay.rs`)把事件按时间喂给**实盘同一份策略代码**,模拟挂单生效 22ms / 回报 42ms、撤单生效 11ms / 确认 29ms、
  "已撮合"推送 270ms、只挂单被拒;排队与成交同 `fill_model.py`(默认 opt 口径)。连续的轮一起跑(上一轮没结算的钱照样占额度)。
- 引擎核对(未审):用当时实盘的 v3 代码与参数回放 10-24 三轮,第一轮成交 146 笔 / Up 396 Down 133 / 盈亏 −18.61
  (实盘 130 笔 / 443 / 126 / −20.82),第二轮 146 笔 / 155 / 163(实盘 134 / 145 / 181);第三轮实盘中途停机,不可比。
- 链上历史日志:公共节点 publicnode 查历史区块要个人 token(10-24 起),需要一个节点服务商的 key(环境变量 `BSC_RPC`)。

## 测试

```bash
cd research/predictfun-maker-1022/probe/analysis
python3 -m unittest discover -s tests -v
```

- **`tests/test_unit.py`**：手工构造、能手算出结果的小日志，逐项核对数值。
  - 订单重建：成交、撤单后被吃、被拒、全撤、分两次成交。
  - 两种参考价：交替 ±a 的币安价格应得到 σ 恰好等于 a；fair 应等于 Φ(1/√199)；平局、收盘后、超出日志范围这几种情况。
  - 各时点盈亏与分组加权、来源过滤、排队分组（固定切点与三分位）。
  - 导出的时间线；带偏差的回放和完全一致的回放分别对比；缺 mm_grid.py 时给出清楚的报错。
- **`tests/test_fill_model.py`**:三种撤单口径的排队推进(手算)、成交造成的减少不算撤单、前面的量不超过盘口这一档、存活区间之外不算;整套校准。
- **`tests/test_maker_rank.py`**：手工构造的链上成交与盘口：同一队的两种写法、吃单方那一行与别的市场不算、先挂先成核对、抢位排名、没成交时的下限、区块延迟。
- **`tests/test_fake_run.py`**：用端到端模拟器生成的约 7 分钟假日志（`tests/data/fake_run/`），对照模拟器写下的真值 `mock_truth.json` 核对：
  - 日志里重建出的成交和模拟器里真实发生的逐单一致。
  - 工具算的 K 与 σ 和模拟器设定一致；工具的 fair 和模拟器内部的公允价相差很小。
  - 结算盈亏等于兑付减成交价。
  - 逆向成交（市场穿过我们的价位）用 fair 衡量时，成交后是亏的。
  - 排队位置和模拟器记录的一致；队首成交率不低于队尾。
  - 用真值构造一个"只成交逆向单、晚 300ms"的回放去对比，结论应为"回放低估成交、晚 300ms"。

**重新生成假日志**（需要 Rust，约 7 分钟，会跨一次收盘和结算）：

```bash
cd ../predict-mm-probe
PROBE_FAKE_LOG_DIR=$(cd ../analysis/tests/data/fake_run && pwd) cargo test --release --test fake_logs generate -- --ignored --nocapture
```

模拟器做了这些事（`../predict-mm-probe/tests/fake_logs.rs`）：

- 币安价格每 100ms 随机游走一步，偶尔有 ±4bp 的跳动。
- Predict 盘口按公允价挂：买一 = floor(公允 × 100 − 1) 分，卖一 = 买一 + 2 分；公允价用含跳动的有效 σ 计算。
- 成交有两种：
  - 逆向（through）：市场穿过我们的价位，整张单全部成交。
  - 随机（random）：价位仍是最优价时，以 30% 的概率来一笔 1~10 股的对手单，先吃掉我们前面的排队，再吃我们。
- 收盘按币安收盘价和 K 比较出结果。

## 已知局限

- 成交时间是探针"发现"成交的时间，不是撮合时间。REST 来源会晚最多一个查单间隔。
- `queue_ahead` 是挂单那一刻盘口显示的量。盘口推送有延迟时，这个数可能还含着我们刚撤掉的单。之后队伍怎么前移（前面的人撤单或被吃），日志里看不到，所以这里只按挂单时刻分组。
- 结算以 Chainlink 价格为准，fair 用的是币安价格，两者在收盘附近可能不一致。所以收盘附近的成交，fair 和结算的结论可能对不上。
- 如果某一轮的结算结果没进日志（解析不到 WON/LOST），这一轮的"结算"列和收盘后的 predict 参考价都留空。
- 回放对比的第 2 步要等 mm_grid.py。
