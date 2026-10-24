// 只测速,不下单:① 本地签名耗时 ② 到 api.predict.fun 的往返(长连接复用 vs 每次新连接)。不发任何订单请求。
import "dotenv/config";
import { Wallet, JsonRpcProvider, parseUnits } from "ethers";
import { OrderBuilder, ChainId, Side } from "@predictdotfun/sdk";
import https from "node:https";
const now = () => performance.now();
const pct = (a, q) => { const s = [...a].sort((x, y) => x - y); return s[Math.floor(q * (s.length - 1))].toFixed(2); };
const provider = new JsonRpcProvider(process.env.PREDICT_RPC_URL || "https://bsc-rpc.publicnode.com");
const signer = new Wallet(process.env.PREDICT_PRIVATE_KEY, provider);
const builder = await OrderBuilder.make(ChainId.BnbMainnet, signer, { predictAccount: process.env.PREDICT_ACCOUNT_ADDRESS });
const tokenId = "86675541217400175431015071230207253729647490951854712850886532176499327385594";   // 已结算市场的 token,只用来构造签名
const sign = [], build = [];
for (let i = 0; i < 300; i++) {
  const t0 = now();
  const amt = builder.getLimitOrderAmounts({ side: Side.BUY, pricePerShareWei: parseUnits((0.30 + (i % 40) / 100).toFixed(2), 18), quantityWei: parseUnits("10", 18) });
  const order = builder.buildOrder("LIMIT", { side: Side.BUY, tokenId, makerAmount: amt.makerAmount, takerAmount: amt.takerAmount, nonce: 0n, feeRateBps: 200 });
  const typed = builder.buildTypedData(order, { isNegRisk: false, isYieldBearing: false });
  const t1 = now(); await builder.signTypedDataOrder(typed); builder.buildTypedDataHash(typed); const t2 = now();
  build.push(t1 - t0); sign.push(t2 - t1);
}
console.log(`构造订单 ms: p50 ${pct(build, .5)} p90 ${pct(build, .9)} | 签名+哈希 ms: p50 ${pct(sign, .5)} p90 ${pct(sign, .9)} p99 ${pct(sign, .99)}  (n=300,前 20 次含预热)`);
const H = { "x-api-key": process.env.PREDICT_API_KEY };
const keep = new https.Agent({ keepAlive: true });
const get = (agent) => new Promise((res) => { const t0 = now(); https.get("https://api.predict.fun/v1/markets?first=1", { headers: H, agent }, (r) => { r.resume(); r.on("end", () => res(now() - t0)); }); });
const k = [], f = [];
await get(keep);
for (let i = 0; i < 30; i++) { k.push(await get(keep)); f.push(await get(new https.Agent({ keepAlive: false }))); await new Promise((r) => setTimeout(r, 200)); }
console.log(`GET 往返 ms:长连接复用 p50 ${pct(k, .5)} p90 ${pct(k, .9)} | 每次新建连接 p50 ${pct(f, .5)} p90 ${pct(f, .9)}`);
process.exit(0);
