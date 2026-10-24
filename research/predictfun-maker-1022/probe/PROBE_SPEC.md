# Predict.fun BTC 5m 实盘探针 · Rust 实现规格(云端会话照此写代码 + 测试)

**目的**:校准回放器(research/predictfun-maker-1022/model/mm_grid.py)的成交模型,不是赚钱。同一批轮次实盘 vs 回放逐笔对比:成交率、挂单时前面排了多少、成交后 5 秒币价/盘口走向、撤单生效前被吃的比例。背景见 ../HANDOFF_1024.md。

**云端会话的边界(硬性)**:只写代码和测试;**不接触任何真实密钥/凭证**,不连真实账户下单;签名测试用代码里临时生成的随机私钥;不部署。部署、空跑、启动由本机会话和 Luke 完成。

## 1. 平台事实(已实测,见 ref/)
- REST 基址 `https://api.predict.fun`,请求头 `x-api-key`,鉴权:`GET /v1/auth/message` → 用 Predict 账户签名 → `POST /v1/auth {signer, message, signature}` → JWT(`authorization: Bearer`)。见 ref/pf_latency.mjs `init()`。
- 下单 `POST /v1/orders`,body `{data:{order:<已签名订单>, pricePerShare, strategy:"LIMIT", isPostOnly:true}}`;撤单 `POST /v1/orders/remove {data:{ids:[orderId]}}`(一次最多 100);查挂单 `GET /v1/orders?first=50`。无批量下单、无改单。
- 订单签名:ref/pf_latency.mjs `signLimit()` 用官方 SDK `@predictdotfun/sdk`(版本见 ref/package.json):`getLimitOrderAmounts({side, pricePerShareWei, quantityWei})` → `buildOrder("LIMIT", {side, tokenId, makerAmount, takerAmount, nonce:0n, feeRateBps})` → `buildTypedData(order, {isNegRisk, isYieldBearing})` → `signTypedDataOrder` → `buildTypedDataHash`;序列化时 bigint 转字符串、`expiration` 转数字。Predict 账户是智能钱包(`predictAccount` = 存款地址),签名者是 Privy EOA。**Rust 必须逐字节复现**:EIP-712 domain、类型、金额取整、hash、签名格式(含 v)。
- 交易所合约(BNB 链):CTFExchange 0x8BC070BEdAB741406F4B1Eb65A72bee27894B689(BTC 5m 全在这里);isNegRisk/isYieldBearing 取自市场信息。
- 限额(10-24 实测):每 API key 每秒 40、每分钟 500,全站所有接口所有市场共用;响应头 `ratelimit-policy`、`ratelimit` 带剩余量。无撤单锁定(removalLockedUntil 实测 0)。往返中位:挂单 35ms、撤单 18ms(东京)。
- 最小单:约 $1 名义(股数 = ceil(1/价),Up 买按价、卖 Up 等价买 Down 按 1−价)。tick 0.01。
- 吃单费 = 0.02·min(p,1−p)·股;挂单 0 费(返佣另计)。平局(收=开)两边都 WON 各 0.5。
- 市场:`GET /v1/categories/btc-updown-5m-<开始秒>` → markets[0].id;`GET /v1/markets/<id>` 有 outcomes(Up/Down,onChainId)、feeRateBps、isNegRisk、isYieldBearing;每轮开始前约 30 分钟开放。
- WebSocket:录制器 ref/pfrec5.py 的订阅与消息格式;样本 ref/ws_samples.tsv(制表符分隔:本机纳秒 \t 来源 \t 原始 JSON;pf = Predict 盘口 `predictOrderbook/<marketId>`,含 bids/asks/version/settlementsPending/lastOrderSettled;bn = 币安 bookTicker)。钱包事件频道 `predictWalletEvents/<jwt>`(见 ref/pf_latency.mjs `listen()`;格式未录,需容错解析并同时用 REST 查单兜底)。

## 2. 探针策略(全部参数可配置,默认值如下)
- 只做当前轮 BTC 5m;剩余秒数 ≤ 30 停挂;不在开盘前挂。
- 两边各一张"只做挂单"单,挂在当前最优价(买一 / 卖一);单量 = 平台最小金额 ≈ $1(ceil(1/价) 股)。
- 改价:最优价变化后若我们不在最优价且挂单已存在 ≥ 1s,则撤旧挂新(与回放器 behind=0 语义一致)。
- 币安保护:币安 bookTicker 中间价 300ms 内对我们不利方向变动 ≥ 0.2bp → 立刻撤该侧;回到正常后按上条重挂。
- 库存上限:净持仓等值 ≥ $6 时,加仓那一侧不挂。
- 留到结算,不吃单平仓。结算后自动领回(claim)与合并(merge),需 BNB gas;失败只记日志不重试轰炸。
- 风控:当日已实现+未实现亏损 ≥ $30 或累计 ≥ $100 → 全撤停机;任一连接断开 > 3s、签名/下单返回错误、收到 429 → 全撤、冷却、记日志;启动时和退出时都 cancel-all。
- 限额预算:按 40/秒、500/分钟本地令牌桶,留 20% 余量;撤单优先于新挂。

## 3. 日志(用于和回放逐笔对比,必须完整)
每条带本机单调时钟纳秒 + 墙钟毫秒:币安每条 bookTicker;Predict 每条盘口推送(原始);每次决策(原因、当时最优价、币安价、目标价);每个请求的发出/返回时间、状态、orderId、响应头限额;每笔成交(来源:钱包事件或 REST,价、量、对手、时间);每轮结束时持仓与结算。输出按小时滚动的 JSON Lines(gzip)。

## 4. 代码要求
- Rust 2021,tokio + tokio-tungstenite + reqwest(rustls)+ alloy 或 ethers-rs(EIP-712)+ serde;单个可执行文件,配置走环境变量/命令行(凭证只从环境变量读,绝不打印)。
- 模式:`--dry-run`(默认:连真实行情、做全部决策、只打印将要发的请求,不发)、`--send`(真发)。
- 放在仓库 `probe/predict-mm-probe/`(Cargo 工程),不依赖服务器上任何现有文件。

## 5. 测试(全部要过)
1. **签名逐字节一致**:用随机私钥 + 固定的市场参数/价格/股数,Rust 产出的订单 JSON、typed-data hash、签名 与 Node 官方 SDK(用 ref/package.json 的版本 npm 安装,写一个 node 脚本生成参考向量)完全一致;至少覆盖买/卖、isNegRisk true/false、isYieldBearing true/false、价格 0.01/0.5/0.99 各组合。
2. 策略单测(用录好的消息回放,不连网):最优价变化改价、币安急动撤单、库存上限、剩 30 秒停挂、风控停机、令牌桶限额。
3. 用 ref/ws_samples.tsv 的真实消息做解析测试。
4. dry-run 可以对着公开行情连 10 分钟不崩(若云端能访问外网;不能则跳过并说明)。

## 6. 交付
推到分支 `predict-probe`;README 写清:编译、配置项、dry-run 命令、--send 命令、日志格式、已知局限。完成后在 HANDOFF_1024.md 末尾追加一段"云端完成情况"。
