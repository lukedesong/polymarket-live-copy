// 用官方 SDK(@predictdotfun/sdk 1.3.8)生成签名参考向量,给 Rust 的逐字节一致测试用。
// 不连网:直接 new OrderBuilder(...),不走 OrderBuilder.make(predictAccount 模式下 make 会上链查 owner)。
// 私钥:默认每次运行随机生成(Wallet.createRandom),也可 --key 0x... 传入(Rust 测试会传它自己随机生成的私钥)。
// 绝不使用真实密钥。
//
//   node gen_vectors.mjs [--key 0x<64hex>] [--account 0x<40hex>] [--out file.json]
//
// 签名流程与 ref/pf_latency.mjs signLimit() 完全相同:
//   getLimitOrderAmounts → buildOrder("LIMIT") → buildTypedData → signTypedDataOrder → buildTypedDataHash
//   序列化:bigint 转字符串,expiration 转数字;请求体 {data:{order, pricePerShare, strategy:"LIMIT", isPostOnly:true}}
import { createRequire } from "node:module";
import { writeFileSync } from "node:fs";
import { Wallet, parseUnits, getAddress, version as ETHERS_VERSION } from "ethers";
const require = createRequire(import.meta.url);
const { OrderBuilder, ChainId, Side, AddressesByChainId } = require("@predictdotfun/sdk");
const { Logger } = require("@predictdotfun/sdk/dist/Logger.js");
const SDK_VERSION = require("@predictdotfun/sdk/package.json").version;

const arg = (name) => { const i = process.argv.indexOf(name); return i > 0 ? process.argv[i + 1] : undefined; };
const wallet = arg("--key") ? new Wallet(arg("--key")) : Wallet.createRandom();
const account = getAddress(arg("--account") ?? Wallet.createRandom().address); // 测试用的"存款地址",随机
const out = arg("--out");

// 确定性的伪随机(只为让 tokenId/salt 看起来像真的;向量文件里会写明具体值)
let seed = 0x1234567;
const rnd = () => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed >> 8; }; // 丢掉 LCG 低位,低位周期太短
const bigDec = (digits) => { let s = String(1 + (rnd() % 9)); while (s.length < digits) s += String(rnd() % 10); return s; };

let currentSalt = "0";
const makeBuilder = (predictAccount) =>
  new OrderBuilder(ChainId.BnbMainnet, BigInt(1e18), AddressesByChainId[ChainId.BnbMainnet], () => currentSalt, new Logger("ERROR"), wallet, predictAccount, undefined);

async function signLimit(builder, { side, priceWei, quantityWei, tokenId, feeRateBps, isNegRisk, isYieldBearing }) {
  const amt = builder.getLimitOrderAmounts({ side, pricePerShareWei: priceWei, quantityWei });
  const order = builder.buildOrder("LIMIT", { side, tokenId, makerAmount: amt.makerAmount, takerAmount: amt.takerAmount, nonce: 0n, feeRateBps });
  const typed = builder.buildTypedData(order, { isNegRisk, isYieldBearing });
  const signed = await builder.signTypedDataOrder(typed);
  const hash = builder.buildTypedDataHash(typed);
  const ser = JSON.parse(JSON.stringify({ ...signed, hash }, (k, v) => (typeof v === "bigint" ? v.toString() : v)));
  ser.expiration = Number(ser.expiration);
  const body = { data: { order: ser, pricePerShare: amt.pricePerShare.toString(), strategy: "LIMIT" } };
  body.data.isPostOnly = true;
  return { amt, typed, hash, signature: signed.signature, orderJson: JSON.stringify(ser), bodyJson: JSON.stringify(body) };
}

const cases = [];
const modes = [
  { mode: "predictAccount", predictAccount: account },
  { mode: "predictAccountLowercase", predictAccount: account.toLowerCase() }, // SDK 原样使用传入字符串,验证大小写处理
  { mode: "eoa", predictAccount: undefined },
];
const sides = [["BUY", Side.BUY], ["SELL", Side.SELL]];
const prices = ["0.01", "0.5", "0.99"];
let n = 0;
for (const m of modes) {
  const builder = makeBuilder(m.predictAccount);
  for (const [sideName, side] of sides)
    for (const isNegRisk of [false, true])
      for (const isYieldBearing of [false, true])
        for (const p of prices) {
          if (m.mode === "predictAccountLowercase" && (isNegRisk || isYieldBearing)) continue; // 小写只抽查几组
          const price = Number(p);
          const shares = Math.ceil(1 / price); // 策略实际用的股数 = ceil(1/价)
          const tokenId = n % 3 === 0 ? bigDec(77) : n % 3 === 1 ? bigDec(76) : String(1 + (rnd() % 1000000));
          const feeRateBps = n % 2 === 0 ? 200 : 0;
          currentSalt = String(rnd() % 2147483649);
          const input = { side, priceWei: parseUnits(p, 18), quantityWei: parseUnits(String(shares), 18), tokenId, feeRateBps, isNegRisk, isYieldBearing };
          const r = await signLimit(builder, input);
          cases.push({
            name: `${m.mode}/${sideName}/negRisk=${isNegRisk}/yb=${isYieldBearing}/p=${p}`,
            mode: m.mode, predictAccount: m.predictAccount ?? null,
            side: sideName, priceWei: input.priceWei.toString(), quantityWei: input.quantityWei.toString(),
            tokenId, feeRateBps, salt: currentSalt, isNegRisk, isYieldBearing,
            verifyingContract: r.typed.domain.verifyingContract,
            pricePerShare: r.amt.pricePerShare.toString(), makerAmount: r.amt.makerAmount.toString(), takerAmount: r.amt.takerAmount.toString(),
            typedDataHash: r.hash, signature: r.signature, orderJson: r.orderJson, bodyJson: r.bodyJson,
          });
          n++;
        }
}

// 金额取整专项:价格保留 3 位有效数字、股数保留 5 位有效数字,再相乘向下取整
const amountCases = [];
const amtBuilder = makeBuilder(undefined);
const amtInputs = [
  ["10000000000000000", "1000000000000000000"],          // 0.01 × 1
  ["123456789000000000", "123456789000000000000000"],  // 0.1234… × 123456.789
  ["990000000000000000", "1010000000000000000"],
  ["333333333333333333", "3333333333333333333"],
  ["500000000000000000", "10000000000000000"],          // 最小数量 1e16
  ["70000000000000000", "15000000000000000000"],
  ["999999999999999999", "99999999999999999999999"],
];
for (let i = 0; i < 12; i++) amtInputs.push([String(1 + (rnd() % 99)) + "0".repeat(16) + String(rnd() % 10), bigDec(19 + (i % 6))]);
for (const [priceWei, quantityWei] of amtInputs)
  for (const [sideName, side] of sides) {
    const a = amtBuilder.getLimitOrderAmounts({ side, pricePerShareWei: BigInt(priceWei), quantityWei: BigInt(quantityWei) });
    amountCases.push({ side: sideName, priceWei, quantityWei, pricePerShare: a.pricePerShare.toString(), makerAmount: a.makerAmount.toString(), takerAmount: a.takerAmount.toString() });
  }

// 鉴权消息签名(POST /v1/auth 用的 signPredictAccountMessage(字符串))
const authCases = [];
const accBuilder = makeBuilder(account);
for (const msg of [
  "Sign in to predict.fun\nNonce: 1234567890",
  "predict.fun 登录 · nonce=abc-DEF_123 · 2026-10-24T05:00:00.000Z",
  "x",
]) authCases.push({ message: msg, signature: await accBuilder.signPredictAccountMessage(msg) });

const result = {
  comment: "由 testvec/gen_vectors.mjs 用官方 SDK 生成;私钥为随机生成的测试私钥,与任何真实账户无关",
  sdkVersion: SDK_VERSION, ethersVersion: ETHERS_VERSION, chainId: 56,
  privateKey: wallet.privateKey, signerAddress: wallet.address, predictAccount: account,
  cases, amountCases, authCases,
};
const json = JSON.stringify(result, null, 1);
if (out) writeFileSync(out, json + "\n"); else process.stdout.write(json + "\n");
