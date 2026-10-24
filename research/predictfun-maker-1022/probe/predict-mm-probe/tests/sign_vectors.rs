//! 签名逐字节一致:Rust 产出的金额、订单 JSON、请求体、typed-data hash、签名(含 v)必须与官方 Node SDK 完全相同。
//!
//! 1. 固定向量:tests/data/sign_vectors.json(testvec/gen_vectors.mjs 用随机测试私钥生成后提交)
//! 2. 现场向量:若 testvec/node_modules 已安装,Rust 现场随机生成私钥和存款地址,调 node 生成向量后逐条比对
use alloy_primitives::U256;
use predict_mm_probe::chain;
use predict_mm_probe::signing::*;
use serde_json::Value;
use std::path::Path;
use std::str::FromStr;

fn side_of(s: &str) -> Side {
    match s {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        _ => panic!("未知 side {s}"),
    }
}

fn u256(v: &Value) -> U256 {
    U256::from_str(v.as_str().unwrap()).unwrap()
}

fn check_vectors(v: &Value) -> usize {
    let key = v["privateKey"].as_str().unwrap();
    let mut n = 0;
    for c in v["cases"].as_array().unwrap() {
        let name = c["name"].as_str().unwrap();
        let signer = OrderSigner::new(key, c["predictAccount"].as_str()).unwrap();
        let input = LimitOrderInput {
            side: side_of(c["side"].as_str().unwrap()),
            token_id: u256(&c["tokenId"]),
            price_per_share_wei: u256(&c["priceWei"]),
            quantity_wei: u256(&c["quantityWei"]),
            fee_rate_bps: c["feeRateBps"].as_u64().unwrap(),
            salt: c["salt"].as_str().unwrap().parse().unwrap(),
            is_neg_risk: c["isNegRisk"].as_bool().unwrap(),
            is_yield_bearing: c["isYieldBearing"].as_bool().unwrap(),
        };
        let b = build_limit_order(&signer, &input).unwrap();
        assert_eq!(b.verifying_contract.to_checksum(None), c["verifyingContract"].as_str().unwrap(), "{name} verifyingContract");
        assert_eq!(b.amounts.price_per_share.to_string(), c["pricePerShare"].as_str().unwrap(), "{name} pricePerShare");
        assert_eq!(b.amounts.maker_amount.to_string(), c["makerAmount"].as_str().unwrap(), "{name} makerAmount");
        assert_eq!(b.amounts.taker_amount.to_string(), c["takerAmount"].as_str().unwrap(), "{name} takerAmount");
        assert_eq!(format!("{:#x}", b.hash), c["typedDataHash"].as_str().unwrap(), "{name} typed-data hash");
        assert_eq!(b.signature, c["signature"].as_str().unwrap(), "{name} 签名");
        assert_eq!(serde_json::to_string(&b.order_json).unwrap(), c["orderJson"].as_str().unwrap(), "{name} 订单 JSON");
        assert_eq!(b.body_json(true), c["bodyJson"].as_str().unwrap(), "{name} 请求体 JSON");
        n += 1;
    }
    for c in v["amountCases"].as_array().unwrap() {
        let a = limit_order_amounts(side_of(c["side"].as_str().unwrap()), u256(&c["priceWei"]), u256(&c["quantityWei"])).unwrap();
        assert_eq!(a.price_per_share.to_string(), c["pricePerShare"].as_str().unwrap());
        assert_eq!(a.maker_amount.to_string(), c["makerAmount"].as_str().unwrap(), "{c}");
        assert_eq!(a.taker_amount.to_string(), c["takerAmount"].as_str().unwrap(), "{c}");
        n += 1;
    }
    // 结算 merge / redeem 的链上 calldata
    let ch = &v["chain"];
    let cond = alloy_primitives::B256::from_str(ch["conditionId"].as_str().unwrap()).unwrap();
    let amount = u256(&ch["amount"]);
    for c in ch["cases"].as_array().unwrap() {
        let yb = c["isYieldBearing"].as_bool().unwrap();
        let name = c["name"].as_str().unwrap();
        let (target, inner) = match name {
            "merge" => chain::merge_call(cond, amount, false, yb),
            "redeem/1" => chain::redeem_call(cond, 1, amount, false, yb),
            "redeem/2" => chain::redeem_call(cond, 2, amount, false, yb),
            "negRiskMerge" => chain::merge_call(cond, amount, true, yb),
            "negRiskRedeem/1" => chain::redeem_call(cond, 1, amount, true, yb),
            "negRiskRedeem/2" => chain::redeem_call(cond, 2, amount, true, yb),
            _ => panic!("{name}"),
        };
        assert_eq!(target.to_checksum(None), c["target"].as_str().unwrap(), "{name} yb={yb}");
        assert_eq!(hex0x(&inner), c["inner"].as_str().unwrap(), "{name} yb={yb}");
        assert_eq!(hex0x(&chain::kernel_execute_calldata(target, &inner)), c["kernelCalldata"].as_str().unwrap(), "{name} yb={yb}");
        n += 1;
    }
    let account = alloy_primitives::Address::from_str(v["predictAccount"].as_str().unwrap()).unwrap();
    assert_eq!(hex0x(&chain::balance_of_calldata(account, U256::from(123456789u64))), ch["balanceOfCalldata"].as_str().unwrap());
    assert_eq!(hex0x(&chain::owner_calldata(account)), ch["ownerCalldata"].as_str().unwrap());

    let acc = OrderSigner::new(key, v["predictAccount"].as_str()).unwrap();
    for c in v["authCases"].as_array().unwrap() {
        assert_eq!(acc.sign_auth_message(c["message"].as_str().unwrap()).unwrap(), c["signature"].as_str().unwrap(), "鉴权签名 {c}");
        n += 1;
    }
    n
}

#[test]
fn committed_vectors_match_sdk() {
    let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sign_vectors.json");
    let v: Value = serde_json::from_str(&std::fs::read_to_string(p).unwrap()).unwrap();
    assert_eq!(v["sdkVersion"], "1.3.8");
    let cases = v["cases"].as_array().unwrap();
    // 覆盖面:买/卖 × isNegRisk × isYieldBearing × 价格 0.01/0.5/0.99,predictAccount 与 EOA 两种模式
    for mode in ["predictAccount", "eoa"] {
        for side in ["BUY", "SELL"] {
            for nr in [false, true] {
                for yb in [false, true] {
                    for p in ["10000000000000000", "500000000000000000", "990000000000000000"] {
                        assert!(
                            cases.iter().any(|c| c["mode"] == mode && c["side"] == side && c["isNegRisk"] == nr && c["isYieldBearing"] == yb && c["priceWei"] == p),
                            "缺组合 {mode} {side} {nr} {yb} {p}"
                        );
                    }
                }
            }
        }
    }
    let n = check_vectors(&v);
    assert!(n >= 90, "向量条数 {n}");
}

#[test]
fn fresh_random_key_vs_node_sdk() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("testvec");
    if !dir.join("node_modules/@predictdotfun/sdk").exists() {
        eprintln!("跳过:testvec/node_modules 未安装(cd testvec && npm ci)");
        return;
    }
    // 私钥与存款地址都在 Rust 里现场随机生成,不落盘
    let s = OrderSigner::random(None).unwrap();
    let account = OrderSigner::random(None).unwrap().signer_address().to_checksum(None);
    let out = std::process::Command::new("node")
        .current_dir(&dir)
        .args(["gen_vectors.mjs", "--key", &s.private_key_hex_for_tests(), "--account", &account])
        .output()
        .expect("运行 node 失败");
    assert!(out.status.success(), "node 报错: {}", String::from_utf8_lossy(&out.stderr));
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["signerAddress"].as_str().unwrap(), s.signer_address().to_checksum(None));
    let n = check_vectors(&v);
    assert!(n >= 90);
}
