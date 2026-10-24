# 交接卡:Predict BTC 5m 做市研究(10-24 14:00 CST,Luke 额度剩 6% 时写)

## 0. 一句话现状
- "在回放上挑参数"三次都失败(调试段挑 → 留出 200 轮判死;Poly 25 天按链上判定未过;开发段挑 → 新留出 150 轮两个都略负)。根因判断:**回放器成交模型不准**(复现老师打法时成交量只有实际的 1/4~1/16,"我们永远排队尾")。
- 下一步候选(待 Luke 定):①**小额实盘探针**校准成交模型(Rust 写);②先测"币安急动后顺势吃单"(吃单不依赖排队,回放较准)。
- 规则:必须 Luke 亲自做的只给他一条命令(memory feedback-luke-only-does-the-one-action)。

## 1. 正在进行:新留出集 300 轮终检(预注册 PREREG_holdout3_1024.md,§3/§3b 写死口径)
- 候选:model/hold3_candidates.json(#21 = 上轮 #2 原样;#22 = 开发段网格入选);回放器 model/mm_grid.py sha 3f9e99e2d2b0cd47。
- 中途 150 轮(10-23 13:10 ~ 10-24 05:20 UTC):#21 −0.88 分 t −0.37;#22 −0.31 t −0.15(results/mm_grid_20261024_134324,已快检)。
- **终检步骤**(约 10-24 18:00 UTC 后 300 个干净轮攒够;干净轮 ≈ 原始轮 × 0.8~0.9):
  1. 东京补数据:参照 ~/prep_h3.sh(token 去重合并 → chain_backfill.py 2026-10-24 [2026-10-25] → export_book.py <起ms> <NOW> → export_bn_win.py <起小时> <out> <止小时>,币安分段防 3GB 内存爆;~/BN_MAX 可设上限)。
  2. 本机拼:scratchpad/h3/ 下 cat 链上 tsv.gz 与 blockts;model/export_tokens_json.py 去重 token;model/export_merge_bn.py 合并币安;盘口同名文件覆盖拷进 bookx_h3。
  3. W1 = 链上最后区块时间 −60s 向下取整到 5 分钟;`python3 model/pending_fills.py <bookx_h3> <mkh3> 1792761000000 <W1>`。
  4. 在 model/ 下:先核 sha,再 `python3 mm_grid.py <bookx_h3> <mkh3> <pending>/fills_pending.parquet 1792761000000 <W1> hold3_candidates.json 25 mixed 300`;"干净轮数够"不成立就等数据不改参数;若因断档中止改用 poly/mm_grid_poly.py(sha 5c610eeb)。
  5. 门槛 t ≥ 2.2(按轮聚类,每股净利,返佣 25%);报合计、按天、时段、账户最低点、去掉最好 1 天 / 1% 轮;交 Opus 深审(AUDIT_CHECKLIST.md)再报 Luke。
- 判断:中途两个都负,后 150 轮需 t≈3.5 才能过,几乎不可能;跑完是为了预注册收尾。

## 2. 实盘探针规格(待 Luke 批准;Rust 独立程序,可移植,不依赖东京环境)
- 目的:**校准回放器**,不是赚钱。对比同轮回放 vs 实盘:成交率、排队位置、成交后 5 秒走向、撤单前被吃比例。
- 策略:两边挂最优价;每张单按平台最小金额 ≈ $1(股数 = ceil(1/价),价 0.5 约 2 股);库存上限 ≈ $6 等值;币安 300ms 内动 ≥0.2bp 就撤;剩 30 秒停挂;留到结算(不吃单平仓,吃单费 2%·min(p,1−p));结算后自动 claim/merge(BNB gas)。
- 风控:每天最多亏 $30、累计 $100 停;异常(断线、签名错、额度 429)全撤停机。
- 额度:实测每 key 40/秒、500/分钟,全站共用(memory predict-ratelimit-measured-1024);无撤单锁定;挂 35ms/撤 18ms。
- 实现要点:币安 bookTicker WS + Predict 盘口 WS(predictOrderbook,带 settlementsPending)+ 钱包事件 WS(predictWalletEvents/<jwt>)+ REST(POST /v1/orders、/v1/orders/remove、/v1/auth)。签名参照 Tokyo ~/pflat/pf_latency.mjs 的 signLimit(SDK OrderBuilder:getLimitOrderAmounts → buildOrder LIMIT → buildTypedData → signTypedDataOrder,isPostOnly),**Rust 签名须与 Node SDK 逐字节一致**(先做这一步)。参考:nautilus_trader 的 Rust 适配器结构;graceyangfan/catalog_capture 的 crates/nautilus-predict(LGPL,依赖作者分支,只参考)。凭证在东京 ~/pflat/.env(不打印)。
- 交付:先不发单空跑 ≥1 小时核对日志 → Opus 审 → 给 Luke 一条启动命令。估计 6~10 小时。

## 3. 关键文件
- 预注册:PREREG_holdout2_1023.md(上轮,已判死)、PREREG_holdout3_1024.md(本轮)、PREREG_poly_1023.md。任务队列 LOOP_QUEUE.md。
- 回放器:model/mm_grid.py(冻结)、poly/mm_grid_poly.py(按轮加载,等价)、poly/mm_grid_v2.py(F1 币安偏移/F2 梯子/F3 急动吃单,等价性与 9 单测通过)。
- 老师规则书:teachers/RULEBOOK_dev.md(15 位;急动顺势吃单全市场扣费后 +2.1 分 t3.2,待回放按 150ms 延迟复核)。
- 东京:ubuntu@54.95.203.70(key ~/.ssh/lightsail_eu_north_1.pem),录制器 pfrec5(临时单元,10-24 加了开机自启 pfrec5-boot;参数 14 天,约 11-06 06:45 UTC 正常退出需重启)。
- memory:predict-mm-three-candidates-dead-1024、predict-ratelimit-measured-1024、feedback-luke-only-does-the-one-action。
