# 回放器 mm_grid.py 的输入格式(给 probe/analysis/mm_grid_adapter.py 用)

冻结版 `model/mm_grid.py`(sha256 前 16 位 3f9e99e2d2b0cd47)。依赖 `research/lib/sanity.py`(脚本里写死了本机绝对路径 `/Users/luke/Documents/polymarket/research/lib`,适配时改成相对路径即可;sanity 只做覆盖/唯一/抽样检查与报告,不影响回放逻辑)。

用法:`python3 mm_grid.py <bookx目录> <mk目录> <fills_pending.parquet> <起ms> <止ms> <候选json> [撤单ms=25] [排队 mixed] [NMAX]`;输出 `results/mm_grid_<时间>/grid.csv`、`per_round_<ci>.csv`(ci = 候选顺序)。核心逻辑在 `replay((market_id, start), params)`。

## 1. bookx 目录:`*.parquet`(盘口快照)+ `*.meta.parquet`(推送元数据)
- 快照:每行一档。列 `mk`(marketId,int)、`srv`(Predict 推送里的服务器时间 ms,int64)、`side`(0 = 买,1 = 卖;**都是"涨"那边的价格**)、`lvl`(档位 0..4,只存前 5 档)、`px`(价,回放器 round 到 2 位)、`sz`(股)、`start`(本轮开始秒)。同一 `(mk, srv)` 的所有行 = 一次完整前 5 档快照;回放器按 `(mk, srv, side, lvl)` 去重。
- meta:每次推送一行。列 `mk`、`srv`、`start`、`version`、`order_count`、`settle_pending`(dict:`{"bids":[[涨价, USD 金额累计], ...], "asks":[...]}`,即平台推送里的 settlementsPending)、`last_settled`(lastOrderSettled)。
- 来源:东京录制器 pfrec5(原始 WS 消息)经 `export_book.py` 导出。

## 2. mk 目录
- `bn_btc.parquet`:币安 BTCUSDT bookTicker,列 `t_ms`(**东京本机收到时间** ms;回放器用 `t_ms − 6` 近似撮合时间)、`b`、`a`(买一/卖一)。
- `chain/fills.tsv.gz`:链上 CTFExchange OrderFilled,无表头,制表符,列 `block, li, tx, oh, maker, taker, mA, tA, mAmt, tAmt, fee`(金额 1e18 精度字符串;`mA == "0"` 表示 maker 用 USDT 买,`tA` 为结果币 token;`taker` == 交易所地址 0x8bc070bedab741406f4b1eb65a72bee27894b689 的行是吃单方行,回放器只用挂单行)。回放器只用它算每个 `(mk, 涨价 round2)` 的挂单成交量上限 CAP。
- `chain/blockts.tsv`:`block \t unix秒`(每 200 块一行,插值)。
- `chain/tokens.json`:list of `{marketId, slug, outcome: "Up"/"Down", token, status: "WON"/"LOST"}`;平局两边都 WON(各付 0.5);必须按 token 唯一。

## 3. fills_pending.parquet(成交事件源,由 pending_fills.py 生成)
- 列 `mk`、`start`、`srv`(推送时刻 ms)、`pside`(`bids`/`asks`,被吃的那一侧)、`px`(涨价)、`usd`、`shares`(= usd / px)。
- 生成:对每次推送,`settle_pending` 各价位累计 USD 的增量 = 该时刻在该价位发生的成交;同一推送同价两侧等量的重复只留一条。回放器再按链上 CAP 截断(pending 比链上多约 15%)。

## 4. 回放器关键口径(对比时要一致)
- 我们看到币安 +6ms、新单 +45ms 上盘口(`NEW`)、撤单 +25ms 生效(`CXL`);`MIN_REST` 1000ms;排队 `mixed`:挂上时前方 = 当时该价位量;此后该价位减少量先扣本段成交,剩余视为撤单,先算在我们身后(悲观),在币安急动 300ms 内按 1.0、否则按 0.5 推进前方。成交按挂上先后在我们多张单之间共享。
- 限额 rl(默认 240/分钟,留 20;轮交界半额)——**探针实测是 500/分钟 + 40/秒**,对比实盘时把 `rl` 设 500。
- 盈亏:挂单成交按结算 0/1(平局 0.5)计,返佣按对手吃单费 25% 记(`fee = −0.25·0.02·min(p,1−p)·q`);平仓吃单费 `0.02·min(p,1−p)·q`。
