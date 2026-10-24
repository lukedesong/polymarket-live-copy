// Predict.fun 下单链路测速(东京机)。默认只读体检;加 --send 才真的提交订单。
//
//   node pf_latency.mjs check            体检:私钥归属、USDT/BNB 余额、授权、JWT、挂单(只读,不动钱)
//   node pf_latency.mjs post  [--send]   挂"只做挂单"的低价买单 → 测"提交→接受"与"盘口推送出现",随后撤掉;重复 N 次
//   node pf_latency.mjs fokmiss [--send] 按卖一价发吃不满的"全部成交否则撤"单 → 测吃单通道回报,正常不成交
//   node pf_latency.mjs take  [--send]   同一轮 BTC 5m 同时吃 Up 和 Down 各最小量(对冲) → 测"提交→成交回报"
//
// 不加 --send 时,post/take 只构建并签名订单、打印将发送的内容和最大亏损,不提交。
import "dotenv/config";
import { Wallet, JsonRpcProvider, parseUnits, formatUnits } from "ethers";
import { OrderBuilder, ChainId, Side } from "@predictdotfun/sdk";
import WebSocket from "ws";

const API = "https://api.predict.fun";
const KEY = process.env.PREDICT_API_KEY;
const ACCOUNT = process.env.PREDICT_ACCOUNT_ADDRESS;
const mode = process.argv[2] || "check";
const SEND = process.argv.includes("--send");
const N = Number((process.argv.find((a) => a.startsWith("--n=")) || "--n=5").slice(4));

const now = () => performance.timeOrigin + performance.now(); // 毫秒,带小数
const hdr = (jwt) => ({ "content-type": "application/json", "x-api-key": KEY, ...(jwt ? { authorization: `Bearer ${jwt}` } : {}) });
async function api(path, { method = "GET", body, jwt } = {}) {
  const t0 = now();
  const r = await fetch(API + path, { method, headers: hdr(jwt), body: body ? JSON.stringify(body) : undefined });
  const t1 = now();
  const j = await r.json().catch(() => ({}));
  return { status: r.status, json: j, ms: t1 - t0, t0, t1 };
}

async function init() {
  for (const k of ["PREDICT_API_KEY", "PREDICT_PRIVATE_KEY", "PREDICT_ACCOUNT_ADDRESS"]) if (!process.env[k]) throw new Error(`.env 缺 ${k}`);
  const provider = new JsonRpcProvider(process.env.PREDICT_RPC_URL || "https://bsc-rpc.publicnode.com");
  provider.pollingInterval = 300;
  const signer = new Wallet(process.env.PREDICT_PRIVATE_KEY, provider);
  const builder = await OrderBuilder.make(ChainId.BnbMainnet, signer, { predictAccount: ACCOUNT });
  const msg = await api("/v1/auth/message");
  const signature = await builder.signPredictAccountMessage(msg.json.data.message);
  const auth = await api("/v1/auth", { method: "POST", body: { signer: ACCOUNT, message: msg.json.data.message, signature } });
  if (!auth.json?.data?.token) throw new Error(`JWT 失败 ${auth.status} ${JSON.stringify(auth.json)}`);
  return { provider, signer, builder, jwt: auth.json.data.token };
}

// 当前这一轮 BTC 5m;剩余时间不足 minLeft 秒就用下一轮
async function btc5m(minLeft = 90) {
  const t = Math.floor(Date.now() / 300000) * 300;
  const s = Date.now() / 1000 - t > 300 - minLeft ? t + 300 : t;
  const c = await api(`/v1/categories/btc-updown-5m-${s}`);
  const m = (await api(`/v1/markets/${c.json.data.markets[0].id}`)).json.data;
  const book = (await api(`/v1/markets/${m.id}/orderbook`)).json.data;
  return { slug: `btc-updown-5m-${s}`, m, book };
}

function pct(a) {
  const s = [...a].sort((x, y) => x - y);
  return s.length ? ["min", 0, "p50", 0.5, "max", 1].reduce((o, v, i, arr) => (i % 2 ? o : o + `${v}=${s[Math.min(s.length - 1, Math.floor(arr[i + 1] * s.length))].toFixed(1)} `), "") : "无";
}

// 订阅自己的钱包事件和盘口,记录每条推送的本机收到时间
function listen(jwt, marketId, sink) {
  const ws = new WebSocket(`wss://ws.predict.fun/ws?apiKey=${KEY}`);
  let rid = 0;
  ws.on("open", () => {
    ws.send(JSON.stringify({ requestId: ++rid, method: "subscribe", params: [`predictWalletEvents/${jwt}`] }));
    if (marketId) ws.send(JSON.stringify({ requestId: ++rid, method: "subscribe", params: [`predictOrderbook/${marketId}`] }));
  });
  ws.on("message", (raw) => {
    const t = now(); const d = JSON.parse(raw);
    if (d.topic === "heartbeat") return ws.send(JSON.stringify({ method: "heartbeat", data: d.data }));
    sink.push({ t, d });
  });
  return new Promise((res) => ws.on("open", () => setTimeout(() => res(ws), 500)));
}

async function check() {
  const { provider, signer, builder, jwt } = await init();
  const usdt = await builder.balanceOf("USDT");
  const bnb = await provider.getBalance(signer.address);
  const { slug, m } = await btc5m(0);
  const steps = builder.getApprovalSteps({ operation: "TRADE", isNegRisk: m.isNegRisk, isYieldBearing: m.isYieldBearing });
  const appr = await builder.checkApprovals(steps);
  const acct = await api("/v1/account", { jwt });
  const orders = await api("/v1/orders?first=20", { jwt });
  const lat = [];
  for (let i = 0; i < 10; i++) lat.push((await api("/v1/account", { jwt })).ms);
  console.log({
    账户: acct.json?.data?.name, 存款地址: ACCOUNT, Privy钱包: signer.address,
    USDT余额: formatUnits(usdt, 18), BNB余额_付gas: formatUnits(bnb, 18),
    样本市场: slug, isNegRisk: m.isNegRisk, isYieldBearing: m.isYieldBearing, feeRateBps: m.feeRateBps,
    授权: appr.map((a) => `${a.step.id}:${a.satisfied ? "已有" : "缺"}`),
    未成交挂单: orders.json?.data?.length ?? orders.status,
    已鉴权往返ms: pct(lat),
  });
  const missing = appr.filter((a) => !a.satisfied);
  if (missing.length) console.log(`⚠ 缺 ${missing.length} 项授权:运行 node pf_latency.mjs approve --send(链上交易,Privy 钱包付 BNB gas)`);
}

async function approve() {
  const { builder } = await init();
  const { m } = await btc5m(0);
  const steps = builder.getApprovalSteps({ operation: "TRADE", isNegRisk: m.isNegRisk, isYieldBearing: m.isYieldBearing });
  if (!SEND) return console.log("将执行授权:", steps.map((s) => s.id), "(加 --send 执行)");
  const r = await builder.runApprovals(steps, { onProgress: ({ step, status }) => console.log(step.id, status) });
  console.log("授权结果", r.success);
}

function outcomeOf(m, name) {
  const o = m.outcomes.find((x) => x.name === name);
  if (!o) throw new Error(`找不到结果 ${name}: ${m.outcomes.map((x) => x.name)}`);
  return o;
}

async function signLimit(builder, m, outcome, side, price, shares) {
  const amt = builder.getLimitOrderAmounts({ side, pricePerShareWei: parseUnits(price.toFixed(2), 18), quantityWei: parseUnits(String(shares), 18) });
  const order = builder.buildOrder("LIMIT", { side, tokenId: outcome.onChainId, makerAmount: amt.makerAmount, takerAmount: amt.takerAmount, nonce: 0n, feeRateBps: m.feeRateBps });
  const typed = builder.buildTypedData(order, { isNegRisk: m.isNegRisk, isYieldBearing: m.isYieldBearing });
  const signed = await builder.signTypedDataOrder(typed);
  const hash = builder.buildTypedDataHash(typed);
  const ser = JSON.parse(JSON.stringify({ ...signed, hash }, (k, v) => (typeof v === "bigint" ? v.toString() : v)));
  ser.expiration = Number(ser.expiration);
  return { body: { data: { order: ser, pricePerShare: amt.pricePerShare.toString(), strategy: "LIMIT" } }, hash };
}

// 挂单测速:只做挂单(isPostOnly),价格远低于买一 → 不可能吃单;测接受回报 + 盘口推送出现 + 撤单
async function post() {
  const { builder, jwt } = await init();
  const { slug, m, book } = await btc5m(150);
  const up = outcomeOf(m, "Up");
  const bestBid = book.bids?.[0]?.[0] ?? 0.05;
  const price = Math.max(0.01, Math.round((bestBid - 0.15) * 100) / 100);
  const shares = Math.ceil(0.95 / price);
  console.log(`市场 ${slug} Up 买一 ${bestBid} → 挂 ${price} × ${shares} 股(只做挂单)。最大亏损:万一被成交且 Up 输,亏 $${(price * shares).toFixed(2)}`);
  if (!SEND) return console.log("dry run:未提交。加 --send 执行。");
  const ev = []; const ws = await listen(jwt, m.id, ev);
  const acc = [], seen = [], rm = [];
  for (let i = 0; i < N; i++) {
    const { body, hash } = await signLimit(builder, m, up, Side.BUY, price, shares);
    body.data.isPostOnly = true;
    const r = await api("/v1/orders", { method: "POST", body, jwt });
    console.log(`#${i} 提交 ${r.status} ${r.ms.toFixed(1)}ms`, JSON.stringify(r.json).slice(0, 200));
    if (r.status >= 300) break;
    acc.push(r.ms);
    await new Promise((s) => setTimeout(s, 1500));
    const hit = ev.find((e) => e.t > r.t0 && e.d.topic?.startsWith("predictOrderbook/") && e.d.data?.bids?.some(([p, q]) => Math.abs(p - price) < 1e-9 && q >= shares - 1e-6));
    if (hit) seen.push(hit.t - r.t0);
    const lock = r.json?.data?.removalLockedUntil ? Date.parse(r.json.data.removalLockedUntil) - Date.now() : 0;
    if (lock > 0) await new Promise((s) => setTimeout(s, lock + 50));
    const x = await api("/v1/orders/remove", { method: "POST", body: { data: { ids: [r.json.data.orderId] } }, jwt });
    console.log(`   撤单 ${x.status} ${x.ms.toFixed(1)}ms 锁定 ${Math.max(0, lock).toFixed(0)}ms`, JSON.stringify(x.json).slice(0, 160));
    rm.push(x.ms);
    await new Promise((s) => setTimeout(s, 1000));
  }
  ws.close();
  console.log({ "提交→接受回报ms": pct(acc), "提交→盘口推送出现ms": pct(seen), "撤单往返ms": pct(rm) });
}

// 吃单测速:同一轮同时吃 Up 和 Down(对冲,方向风险≈0),全部成交否则撤(isFillOrKill)
async function take() {
  const { builder, jwt } = await init();
  const { slug, m, book } = await btc5m(120);
  const upAsk = book.asks?.[0]?.[0];
  const dnAsk = book.bids?.[0]?.[0] != null ? Math.round((1 - book.bids[0][0]) * 100) / 100 : null; // Down 卖一 = 1 − Up 买一
  if (!upAsk || !dnAsk) throw new Error("盘口为空");
  const shares = Math.max(Math.ceil(0.95 / Math.min(upAsk, dnAsk)), 2);
  const fee = (p) => 0.02 * Math.min(p, 1 - p) * shares;
  const maxLoss = (upAsk + dnAsk - 1) * shares + fee(upAsk) + fee(dnAsk);
  console.log(`市场 ${slug}:吃 Up@${upAsk} + Down@${dnAsk} 各 ${shares} 股。占用 $${((upAsk + dnAsk) * shares).toFixed(2)} 到收盘,收回 $${shares};最大亏损约 $${maxLoss.toFixed(3)}(价差+吃单费)`);
  if (!SEND) return console.log("dry run:未提交。加 --send 执行。");
  const ev = []; const ws = await listen(jwt, m.id, ev);
  const legs = [];
  for (const [name, price] of [["Up", upAsk], ["Down", dnAsk]]) {
    const { body } = await signLimit(builder, m, outcomeOf(m, name), Side.BUY, price, shares);
    body.data.isFillOrKill = true;
    legs.push({ name, body });
  }
  const rs = await Promise.all(legs.map((l) => api("/v1/orders", { method: "POST", body: l.body, jwt })));
  await new Promise((s) => setTimeout(s, 3000));
  ws.close();
  rs.forEach((r, i) => {
    const fill = ev.find((e) => e.t > r.t0 && e.d.topic?.startsWith("predictWalletEvents/"));
    console.log(`${legs[i].name}: 提交→回报 ${r.ms.toFixed(1)}ms (${r.status}) 钱包事件 ${fill ? (fill.t - r.t0).toFixed(1) + "ms" : "无"}`, JSON.stringify(r.json).slice(0, 200));
  });
  for (const e of ev.filter((e) => e.d.topic?.startsWith("predictWalletEvents/"))) console.log("  钱包事件", (e.t - rs[0].t0).toFixed(1), "ms", JSON.stringify(e.d.data).slice(0, 200));
}

// 吃单通道测速但不成交:按卖一价发"全部成交否则全撤",数量 = 卖一挂量 × 1.5 → 吃不满整单被撤
// 风险:提交的几十毫秒内卖一刚好补到足量 → 真成交(买到这些股,可随即卖回)
// 整单金额必须在余额以内,否则在余额检查就被拒、走不到撮合;所以在 BTC/ETH/BNB 的 5m/15m 当前轮里找卖一挂量小的
async function pickThin(budget) {
  const t5 = Math.floor(Date.now() / 300000) * 300, t15 = Math.floor(Date.now() / 900000) * 900;
  const cands = [];
  for (const c of ["btc", "eth", "bnb"]) for (const [p, t, len] of [["5m", t5, 300], ["15m", t15, 900]]) {
    if (Date.now() / 1000 - t > len - 60) continue; // 剩余不足 60 秒不用
    const cat = await api(`/v1/categories/${c}-updown-${p}-${t}`);
    const id = cat.json?.data?.markets?.[0]?.id; if (!id) continue;
    const b = (await api(`/v1/markets/${id}/orderbook`)).json.data;
    const [a, d] = b?.asks?.[0] ?? [];
    if (!a || a < 0.05 || a > 0.95) continue;
    const n = Math.max(Math.ceil(d * 1.5), Math.ceil(0.95 / a));
    if (a * n <= budget) cands.push({ slug: `${c}-updown-${p}-${t}`, id, a, d, n });
  }
  return cands.sort((x, y) => x.a * x.n - y.a * y.n)[0];
}

// "全部成交否则撤"只支持市价单:按给定最高价和股数手工构建市价买单(不按盘口均价算)
async function signMarketFok(builder, m, outcome, price, shares) {
  const priceWei = parseUnits(price.toFixed(2), 18), qtyWei = parseUnits(String(shares), 18);
  const order = builder.buildOrder("MARKET", { side: Side.BUY, tokenId: outcome.onChainId, makerAmount: (priceWei * qtyWei) / 10n ** 18n, takerAmount: qtyWei, nonce: 0n, feeRateBps: m.feeRateBps });
  const typed = builder.buildTypedData(order, { isNegRisk: m.isNegRisk, isYieldBearing: m.isYieldBearing });
  const signed = await builder.signTypedDataOrder(typed);
  const hash = builder.buildTypedDataHash(typed);
  const ser = JSON.parse(JSON.stringify({ ...signed, hash }, (k, v) => (typeof v === "bigint" ? v.toString() : v)));
  ser.expiration = Number(ser.expiration);
  return { data: { order: ser, pricePerShare: priceWei.toString(), strategy: "MARKET", slippageBps: "0", isFillOrKill: true } };
}

// A:最高价=卖一、数量=卖一挂量×1.5 → 进撮合但吃不满,整单撤;B:最高价=卖一−1分、约 $1 → 碰不到卖单,整单撤
async function fokmiss() {
  const { builder, jwt } = await init();
  const budget = Number(formatUnits(await builder.balanceOf("USDT"), 18)) * 0.8;
  console.log(`A:按卖一价发吃不满的市价单(整单 ≤ $${budget.toFixed(0)},每轮重挑卖一挂量小的市场);B:比卖一低 1 分、约 $1 的市价单。两者正常都整单撤、不成交。若意外成交:A 最多占用 $${budget.toFixed(0)},B 约 $1`);
  if (!SEND) return console.log("dry run:未提交。加 --send 执行。");
  const ev = []; const ws = await listen(jwt, null, ev);
  const latA = [], latB = [];
  for (let i = 0; i < N; i++) {
    const pick = await pickThin(budget);
    if (pick) {
      const m = (await api(`/v1/markets/${pick.id}`)).json.data;
      const body = await signMarketFok(builder, m, outcomeOf(m, "Up"), pick.a, pick.n);
      const r = await api("/v1/orders", { method: "POST", body, jwt });
      if (r.status !== 400) latA.push(r.ms);
      console.log(`#${i} A ${pick.slug} ${pick.n}股@${pick.a}(卖一挂量 ${pick.d}) → ${r.status} ${r.ms.toFixed(1)}ms`, JSON.stringify(r.json).slice(0, 220));
    } else console.log(`#${i} A 没有卖一挂量够小的市场,跳过`);
    const { m, book } = await btc5m(90);
    const ask = book.asks?.[0]?.[0];
    if (ask && ask > 0.06) {
      const p = Math.round((ask - 0.01) * 100) / 100, n = Math.ceil(0.95 / p);
      const body = await signMarketFok(builder, m, outcomeOf(m, "Up"), p, n);
      const r = await api("/v1/orders", { method: "POST", body, jwt });
      if (r.status !== 400) latB.push(r.ms);
      console.log(`#${i} B btc5m ${n}股@${p}(卖一 ${ask}) → ${r.status} ${r.ms.toFixed(1)}ms`, JSON.stringify(r.json).slice(0, 220));
    }
    await new Promise((s) => setTimeout(s, 1500));
  }
  ws.close();
  const fills = ev.filter((e) => e.d.topic?.startsWith("predictWalletEvents/"));
  for (const e of fills) console.log("  钱包事件", JSON.stringify(e.d.data).slice(0, 220));
  console.log({ "A 进撮合吃不满 提交→回报ms": pct(latA), "B 碰不到卖单 提交→回报ms": pct(latB), "钱包事件条数(有成交才会有)": fills.length });
}

const run = { check, approve, post, take, fokmiss }[mode];
if (!run) { console.log("用法: node pf_latency.mjs check | approve [--send] | post [--send] [--n=5] | fokmiss [--send] [--n=5] | take [--send]"); process.exit(1); }
run().catch((e) => { console.error("错误:", e.message); process.exit(1); });
