// 只读:查最近订单的事件时间线 + 持仓,确认是否成交、撮合花了多久
import "dotenv/config";
import { Wallet, JsonRpcProvider } from "ethers";
import { OrderBuilder, ChainId } from "@predictdotfun/sdk";
const API = "https://api.predict.fun", KEY = process.env.PREDICT_API_KEY, ACC = process.env.PREDICT_ACCOUNT_ADDRESS;
const h = (j) => ({ "content-type": "application/json", "x-api-key": KEY, ...(j ? { authorization: `Bearer ${j}` } : {}) });
const get = async (p, j) => (await fetch(API + p, { headers: h(j) })).json();
const b = await OrderBuilder.make(ChainId.BnbMainnet, new Wallet(process.env.PREDICT_PRIVATE_KEY, new JsonRpcProvider(process.env.PREDICT_RPC_URL)), { predictAccount: ACC });
const msg = (await get("/v1/auth/message")).data.message;
const jwt = (await (await fetch(API + "/v1/auth", { method: "POST", headers: h(), body: JSON.stringify({ signer: ACC, message: msg, signature: await b.signPredictAccountMessage(msg) }) })).json()).data.token;
const act = await get("/v1/account/activity?first=60", jwt);
console.log("activity 字段示例:", JSON.stringify(act.data?.[0]).slice(0, 600));
for (const a of act.data || []) console.log(a.createdAt || a.timestamp, a.name || a.type || a.event, a.market?.id ?? a.marketId ?? "", a.amountFilled ?? "", a.price ?? "", (a.order?.id ?? a.orderId ?? "").toString().slice(-6));
const pos = await get("/v1/positions", jwt);
console.log("持仓条数", pos.data?.length, JSON.stringify(pos.data).slice(0, 300));
const ords = await get("/v1/orders?first=20", jwt);
console.log("未结挂单", ords.data?.length);
const o = await get("/v1/orders/0x7d0b25d89060c493b334dca0a634ecdf2485d0240e143de1969f0ddf0c55a356", jwt);
console.log("A#0 订单详情:", JSON.stringify(o.data).slice(0, 900));
