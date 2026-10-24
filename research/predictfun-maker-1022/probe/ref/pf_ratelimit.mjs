// Predict.fun 请求限额实测(东京机,Luke 手动执行;一条命令跑完全部)
//   node pf_ratelimit.mjs            只打印计划(自动挑好的慢市场、BTC 5m 市场、价格、最大亏损),不发任何订单
//   node pf_ratelimit.mjs --send     依次跑 3 步,每步 75 秒,步与步之间等限额恢复:
//     第 1 步 safe:不挂单。只发"撤销不存在的订单"和"提交缺签名的空订单",数第几次被拒(429)
//     第 2 步 slow:慢市场挂"只做挂单"的 0.01 买单 → 到撤单锁定期结束立刻撤 → 再挂;数 429、测撤单锁定时长
//     第 3 步 mix :慢市场 和 还没开盘(≥10 分钟后开始)的 BTC 5m 轮 交替挂单/撤单 → 看两个市场是否共用同一额度
//   同一时刻最多 1 张单;结束时检查并撤掉残留挂单。结果写 ~/ratelimit_<时间>.json,汇总打印在屏幕上。不打印 key/JWT。
// 最大亏损:单张 0.01 × 100 股 = $1(只有在价格崩到 0.01 且该结果最终输时才会发生);同一时刻最多 1 张。
import "dotenv/config";
import { Wallet, parseUnits } from "ethers";
import { OrderBuilder, ChainId, Side } from "@predictdotfun/sdk";
import { writeFileSync } from "fs";
const API = "https://api.predict.fun"; const KEY = process.env.PREDICT_API_KEY; const ACCOUNT = process.env.PREDICT_ACCOUNT_ADDRESS;
const SEND = process.argv.includes("--send"); const STEP_MS = 75000, PRICE = 0.01, SHARES = 100;
const hdr = (jwt) => ({ "content-type": "application/json", "x-api-key": KEY, ...(jwt ? { authorization: `Bearer ${jwt}` } : {}) });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rows = [];
async function call(kind, path, { method = "POST", body, jwt } = {}) {
  const t0 = Date.now();
  const r = await fetch(API + path, { method, headers: hdr(jwt), body: body ? JSON.stringify(body) : undefined });
  const h = {}; for (const [k, v] of r.headers) if (/rate|limit|retry|remaining|reset/i.test(k)) h[k] = v;
  let j = {}; try { j = await r.json(); } catch {}
  const row = { t: t0, ms: Date.now() - t0, kind, status: r.status, h, msg: JSON.stringify(j).slice(0, 200) };
  if (!kind.startsWith("setup")) rows.push(row); return { ...row, json: j };
}
const get = async (path) => (await call("setup", path, { method: "GET" })).json;
const fakeId = () => "0x" + [...Array(64)].map(() => "0123456789abcdef"[Math.floor(Math.random() * 16)]).join("");
async function init() {
  const signer = new Wallet(process.env.PREDICT_PRIVATE_KEY);
  const builder = await OrderBuilder.make(ChainId.BnbMainnet, signer, { predictAccount: ACCOUNT });
  const m = await (await fetch(API + "/v1/auth/message", { headers: hdr() })).json();
  const signature = await builder.signPredictAccountMessage(m.data.message);
  const a = await (await fetch(API + "/v1/auth", { method: "POST", headers: hdr(), body: JSON.stringify({ signer: ACCOUNT, message: m.data.message, signature }) })).json();
  if (!a?.data?.token) throw new Error("JWT 失败"); return { builder, jwt: a.data.token };
}
async function signLimit(builder, m, outcome, price, shares) {                   // 与 pf_latency.mjs 相同的签名方式
  const amt = builder.getLimitOrderAmounts({ side: Side.BUY, pricePerShareWei: parseUnits(price.toFixed(2), 18), quantityWei: parseUnits(String(shares), 18) });
  const order = builder.buildOrder("LIMIT", { side: Side.BUY, tokenId: outcome.onChainId, makerAmount: amt.makerAmount, takerAmount: amt.takerAmount, nonce: 0n, feeRateBps: m.feeRateBps });
  const typed = builder.buildTypedData(order, { isNegRisk: m.isNegRisk, isYieldBearing: m.isYieldBearing });
  const signed = await builder.signTypedDataOrder(typed); const hash = builder.buildTypedDataHash(typed);
  const ser = JSON.parse(JSON.stringify({ ...signed, hash }, (k, v) => (typeof v === "bigint" ? v.toString() : v))); ser.expiration = Number(ser.expiration);
  return { data: { order: ser, pricePerShare: amt.pricePerShare.toString(), strategy: "LIMIT", isPostOnly: true } };
}
// 自动挑慢市场:开放交易、非加密涨跌类、某个结果买一在 0.05~0.6 且买一挂单 ≥100 股(0.01 买单前面隔着好几档,不可能被成交)
async function pickSlow() {
  let cursor = null;
  for (let page = 0; page < 30; page++) {
    const d = await get(`/v1/markets?first=100&status=OPEN${cursor ? `&after=${encodeURIComponent(cursor)}` : ""}`);
    for (const m of d.data || []) {
      if (m.tradingStatus !== "OPEN" || m.status !== "REGISTERED") continue;
      if (/updown|up-or-down|btc|eth|sol|xrp|bnb|crypto|price|tweet/i.test(`${m.categorySlug} ${m.title}`)) continue;
      for (const o of m.outcomes || []) {
        const bb = o.bestBid?.price;
        if (bb >= 0.05 && bb <= 0.6 && (o.bestBid?.size || 0) >= 100) return { m, o, bb };
      }
    }
    if (!d.cursor || !(d.data || []).length) break; cursor = d.cursor;
  }
  throw new Error("没找到合适的慢市场");
}
// 还没开盘的 BTC 5m 轮(≥10 分钟后开始),Up 结果
async function pickBtcFuture() {
  const t = Math.floor(Date.now() / 300000) * 300;
  for (const s of [t + 900, t + 1200, t + 1500, t + 1800]) {
    const c = await get(`/v1/categories/btc-updown-5m-${s}`); const mk = c?.data?.markets?.[0]; if (!mk) continue;
    const m = (await get(`/v1/markets/${mk.id}`)).data; const o = m.outcomes.find((x) => x.name === "Up");
    if (m.tradingStatus === "OPEN" && o) return { m, o, start: s };
  }
  return null;
}
async function recover(jwt) { const t0 = Date.now(); for (let k = 0; k < 120; k++) { const r = await call("probe", "/v1/orders/remove", { body: { data: { ids: [fakeId()] } }, jwt }); if (r.status !== 429) return Date.now() - t0; await sleep(5000); } return null; }
let lockMax = 0;
async function postCancel(tag, builder, jwt, tgt) {
  const r = await call(`${tag}:post`, "/v1/orders", { body: await signLimit(builder, tgt.m, tgt.o, PRICE, SHARES), jwt });
  if (r.status < 300 && r.json?.data?.orderId) {
    const lock = r.json.data.removalLockedUntil ? Date.parse(r.json.data.removalLockedUntil) - Date.now() : 0; lockMax = Math.max(lockMax, lock);
    if (lock > 0) await sleep(lock + 20);
    for (let k = 0; k < 20; k++) { const x = await call(`${tag}:cancel`, "/v1/orders/remove", { body: { data: { ids: [r.json.data.orderId] } }, jwt }); if (x.status < 300) break; await sleep(1000); }
  }
  return r;
}
async function step(tag, fn) { const end = Date.now() + STEP_MS; let i = 0; while (Date.now() < end) { const t0 = Date.now(); await fn(i++); await sleep(Math.max(0, 200 - (Date.now() - t0))); } }
function summary(tag) {
  const xs = rows.filter((r) => r.kind.startsWith(tag + ":")); if (!xs.length) return;
  const f = xs.findIndex((r) => r.status === 429);
  const c = {}; for (const r of xs) c[`${r.kind}→${r.status}`] = (c[`${r.kind}→${r.status}`] || 0) + 1;
  const w60 = f >= 0 ? xs.filter((r) => r.t >= xs[f].t - 60000 && r.t < xs[f].t && r.status !== 429).length : null;
  console.log(`\n【${tag}】共 ${xs.length} 个请求;` + (f >= 0 ? `第 ${f + 1} 个开始被拒(开始后 ${((xs[f].t - xs[0].t) / 1000).toFixed(1)} 秒;被拒前 60 秒内成功 ${w60} 个)` : "全程没被拒"));
  console.log("   分类:", JSON.stringify(c));
  console.log("   " + (f >= 0 ? `被拒返回:${xs[f].msg} 头:${JSON.stringify(xs[f].h)}` : `示例响应头:${JSON.stringify(xs[0].h)}`));
}
async function main() {
  const slow = await pickSlow(); const btc = await pickBtcFuture();
  console.log(`慢市场:${slow.m.id}「${(slow.m.title || "").slice(0, 60)}」结果 ${slow.o.name} 买一 ${slow.bb}`);
  console.log(btc ? `BTC 5m:${btc.m.id}(${new Date(btc.start * 1000).toISOString()} 开始)结果 Up` : "BTC 5m:没找到 ≥10 分钟后开始的轮,第 3 步改为只用慢市场");
  console.log(`挂单:只做挂单的买单 ${PRICE} × ${SHARES} 股;同一时刻最多 1 张;最大亏损每张 $${(PRICE * SHARES).toFixed(2)}`);
  if (!SEND) return console.log("\n只打印了计划,没有发任何请求。确认后执行:node pf_ratelimit.mjs --send");
  const { builder, jwt } = await init();
  console.log("\n第 1 步 safe(不挂单)…");
  await step("safe", (i) => (i % 2 ? call("safe:badorder", "/v1/orders", { body: { data: {} }, jwt }) : call("safe:cancel", "/v1/orders/remove", { body: { data: { ids: [fakeId()] } }, jwt })));
  summary("safe"); console.log("   恢复用时 ms:", await recover(jwt)); await sleep(60000);
  console.log("\n第 2 步 slow(慢市场挂撤)…");
  await step("slow", () => postCancel("slow", builder, jwt, slow));
  summary("slow"); console.log("   撤单锁定最长 ms:", lockMax); console.log("   恢复用时 ms:", await recover(jwt)); await sleep(60000);
  console.log("\n第 3 步 mix(慢市场 与 BTC 5m 交替挂撤)…");
  await step("mix", (i) => postCancel("mix", builder, jwt, i % 2 && btc ? btc : slow));
  summary("mix"); console.log("   恢复用时 ms:", await recover(jwt));
  const left = await call("setup", "/v1/orders?first=50", { method: "GET", jwt }); const ids = (left.json?.data || []).map((o) => o.id || o.orderId).filter(Boolean);
  if (ids.length) { await call("cleanup", "/v1/orders/remove", { body: { data: { ids } }, jwt }); console.log(`\n结束时有 ${ids.length} 张残留挂单,已发撤单;请到网页确认为 0`); }
  else console.log("\n结束时没有残留挂单");
  const f = `${process.env.HOME}/ratelimit_${Date.now()}.json`; writeFileSync(f, JSON.stringify(rows)); console.log("明细:", f);
}
main().catch((e) => { console.error("出错:", e.message); process.exit(1); });
