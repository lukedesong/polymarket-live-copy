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

## 云端完成情况(10-24,云端会话;只写代码和测试,未接触任何真实凭证、未连真实账户、未下单、未部署)

**位置**:`probe/predict-mm-probe/`(Cargo 工程,单个可执行文件 `predict-mm-probe`),分支 `predict-probe`。使用说明见该目录 README.md(编译、配置项、dry-run / --send 命令、日志格式、已知局限)。

**做了什么**
- 签名(`src/signing.rs`):逐字节复现 `@predictdotfun/sdk` 1.3.8(`ref/package.json` 的 `^1.3.8` 实际解析到 1.3.8)。
  - 覆盖:金额取整(价格 3 位、股数 5 位有效数字)、EIP-712 domain 与类型、typed-data hash、请求体序列化。
  - predictAccount 模式:maker = signer = 存款地址(原样字符串);签名 = Kernel 包装 + personal_sign,共 86 字节(0x01 ‖ 验证器 ‖ r s v)。
  - 鉴权签名;claim / merge 的 Kernel.execute calldata。
- 策略(`src/strategy.rs`):纯状态机,覆盖规格第 2 节全部规则(两侧挂最优价、改价、币安急动撤单、库存上限、剩 30 秒停挂、风控停机、冷却、限额)。
- 限额(`src/ratelimit.rs`):40/秒、500/分钟,留 20% 余量;撤单 > 新挂 > 查询。用滑动窗口计数实现,保证任意窗口不超限(普通令牌桶会超过分钟限额)。
- 风控账本(`src/risk.rs`):当日 / 累计 已实现 + 未实现盈亏;结果未出的轮按最坏结果估值;落盘,重启不清零;停机标记要人工 `--clear-halt`。
- 运行时:
  - 币安 bookTicker WS;Predict WS(盘口 + 钱包事件 + 心跳,外加 5 秒一次 ping 判活)。
  - REST(鉴权、挂、撤、每 3 秒查挂单兜底、查单)。
  - 日志:JSON Lines gzip 按小时滚动,每行带 mono_ns + wall_ms;凭证脱敏。
  - 结算后 merge / redeem:每个动作最多一笔交易,失败只记日志。
  - --send 启动时依次:链上核对存款地址 owner、鉴权、全撤;退出时全撤两遍。

**测试结果**:`cargo test` 48 个全过,clippy 零警告。
- 签名一致性:固定向量对比全部一致。
  - 54 组订单:买/卖 × isNegRisk × isYieldBearing × 价 0.01/0.5/0.99,predictAccount 与 EOA 两种模式,另加小写地址抽查;每组对比金额、订单 JSON、请求体 JSON、typed-data hash、签名(含 v)。
  - 38 组金额取整、3 组鉴权签名、12 组 merge / redeem calldata。
  - 另有现场测试:Rust 随机生成私钥,调 node 跑官方 SDK 现场比对,也一致。
- 策略单测 15 个:改价、币安急动、库存上限、剩 30 秒、开盘前、日亏 / 累计停机、429、下单错误 / 超时 / 迟到回报、断线 > 3 秒、币安静默、冷却过多停机、限额耗尽撤单仍可发、成交记账。
- 限额 6 个;用 ws_samples.tsv 真实消息的解析测试 7 个;库内单测 14 个。
- 端到端 4 个:本机模拟 Predict REST + WS、币安、RPC,跑完整可执行文件。
  - dry-run 不发任何写请求。
  - --send 全流程:鉴权 → 启动全撤 → 挂单 → 改价 → 钱包事件成交 → REST 核实 → 退出无残留挂单。
  - 429 后停止下单;owner 不符拒绝启动。
  - 日志和输出里不出现凭证。
- 10 分钟长跑(对着本地模拟服务器,dry-run 与 --send 各一次):两次都正常退出、无崩溃。各自跨了 3 轮开盘、2 次收盘,至少 1 次结算。--send 那次:367 次下单、365 次撤单,心跳回复 571 次,退出后无残留挂单;最后 30 秒和开盘前都没有挂单。dry-run 那次:0 个写请求。复现命令:`cargo test --release --test e2e_mock long_run -- --ignored --nocapture`

**未完成 / 需本机会话与 Luke 做**
1. **真实行情 dry-run 10 分钟没做**:云端网络策略拦了 api.predict.fun、ws.predict.fun、stream.binance.com(代理 403 / 连接重置),只能用本地模拟服务器代替。按交接卡,东京机先空跑 ≥ 1 小时核对日志:
   `cd probe/predict-mm-probe && cargo build --release && ./target/release/predict-mm-probe --log-dir ~/probe_logs --duration-s 3600`
2. 以下字段都是按多种可能的名字容错解析的,需要用首次实盘日志核对,细节见 README 第 8 节:
   - 钱包事件格式(未录过);
   - `GET /v1/orders` 与 `/v1/orders/<hash>` 的成交量 / 状态字段;
   - 结算结果字段(假定 `outcomes[].status` = WON / LOST;解析不到会按最坏结果估值,日亏停机会提早触发);
   - Predict 心跳间隔。
3. claim / merge 只验证了 calldata 与 SDK 一致,没在真实链上跑过。EOA 需要有 BNB 付 gas。
4. 实盘启动命令(Opus 审完后交给 Luke 执行的那一条):
   `set -a; . ~/pflat/.env; set +a; ./target/release/predict-mm-probe --send --log-dir ~/probe_logs --quiet`

## 云端完成情况(二):探针日志分析工具(10-24,只写代码和测试,不联网)

**位置**:`probe/analysis/`(Python,只用标准库)。读探针按小时滚动的 `probe_*.jsonl.gz` 日志。用法和口径见该目录 README.md。
- `markouts.py`(第 1 项):每笔成交之后 1、5、30 秒以及结算时,我们的每股和总盈亏。
  - 参考价两种都算:Predict 盘口中间价;币安公允价 Φ(ln(S/K)/(σ√τ)),其中 K = 本轮开盘时的币安中间价,σ = 近 5 分钟的 1 秒实际波动。
  - 按 买Up / 卖Up(买Down)× 剩余时间(6 档)分组;给出按股数加权的每股盈亏,以及按笔数的均值和 t 值。
- `queue_position.py`(第 2 项):挂单那一刻我们这个价位前面已有多少股(探针日志里的 `queue_ahead`)。
  - 按三分位分成 队首 / 队中 / 队尾,也可用 `--edges` 改成固定股数切分。
  - 每组统计成交率、成交股数比、首笔成交用时,以及成交后各时点的盈亏(两种参考价)。
- `replay_compare.py`(第 3 项):
  - `export`:导出每张单实际的挂撤时间线 [post_ms, remove_ms),作为回放器的输入。
  - `compare`:逐单对比回放与实盘,输出成交率倍数、成交股数比、逐单四分类、成交时刻差和成交价差,并给出一句结论(例如"回放低估成交、晚 300ms")。按方向、排队分组、成交来源分开给。
  - **按 mm_grid 规则重放这一步没做**:`model/mm_grid.py` 不在仓库任何分支里,读不到它的成交规则。`mm_grid_adapter.py` 只留了接口,没有凭猜测写规则。

**测试**:`cd probe/analysis && python3 -m unittest discover -s tests`,共 20 个,全过。
- 11 个手工构造、能手算结果的单测(σ 恰好等于 a、Φ(1/√199)、加权、分组、对比结论等)。
- 9 个用端到端模拟器生成的约 7 分钟假日志,对照模拟器写下的真值核对:
  - 逐单成交量与真值完全一致;
  - K 误差 < 1e-4,σ 在真值 ±35% 以内,fair 和模拟器公允价平均差 < 0.04;
  - 结算盈亏 = 兑付 − 价;
  - 逆向成交按 fair 是亏的、随机成交是赚的;
  - 排队队首被随机吃单的比例高于队尾;
  - 假造一个悲观回放(只成交逆向单、晚 300ms),对比结论正确。
- 假日志由 `predict-mm-probe/tests/fake_logs.rs` 新加的仿真模拟器生成(币安随机游走带跳动、Predict 盘口跟公允价走、逆向 / 随机两类成交、收盘结算)。数据放在 `probe/analysis/tests/data/fake_run/`(约 800KB)。

**顺带修了探针的一个 bug(模拟器暴露出来的)**:
- 问题:限额器对查询类请求保留额度太大,改价频繁时查询会被饿死。模拟器里收盘后 3 分钟都没查到结算结果,REST 查挂单兜底也会因此延迟。
- 修法:查询排队超过 5 秒就升到和新挂同级,撤单的保留额度不变;加了单测。Rust 测试 50 个全过,clippy 零警告。
- 另外:之前写的 `probe/tokyo_dryrun.sh` 留给本机会话部署时可选使用,已改成跑完自动调用这里的分析工具;被它取代的 `probe/analyze_probe.py` 已删除。

**需要 Luke / 本机做的**
1. 把 `model/mm_grid.py` 推到 `predict-probe` 分支,连同它依赖的 `pending_fills.py`,以及 bookx、mkh、fills_pending.parquet 的格式说明。之后云端在 `mm_grid_adapter.py` 里按它的成交规则实现重放,对比部分不用改。
2. 也可以先在本机直接跑回放器,按 README 里 `replay_fills.tsv` 的格式写出结果,再用 `replay_compare.py compare` 对比。
