//! 订单签名:逐字节复现官方 SDK `@predictdotfun/sdk` 1.3.8 的 OrderBuilder。
//!
//! 对应关系(SDK dist/OrderBuilder.js):
//! - `limit_order_amounts`   ↔ `getLimitOrderAmounts`(价格保留 3 位有效数字、股数保留 5 位,再乘后向下取整)
//! - `build_order`           ↔ `buildOrder("LIMIT", ...)`(predictAccount 模式:maker = signer = 存款地址原样字符串)
//! - `typed_data_hash`       ↔ `buildTypedDataHash`(EIP-712,domain = "predict.fun CTF Exchange" / "1" / 56 / 交易所合约)
//! - `sign_order`            ↔ `signTypedDataOrder`
//!     * EOA 模式:直接对 typed-data hash 签名(65 字节 r‖s‖v,v = 27/28)
//!     * predictAccount 模式:hash 再包一层 Kernel EIP-712(域 "Kernel" / "0.3.1" / 56 / 存款地址),
//!       对包好的 32 字节做 EIP-191 personal_sign,输出 0x01 ‖ ECDSA 验证器地址 ‖ 65 字节签名(共 86 字节)
//! - `sign_auth_message`     ↔ `signPredictAccountMessage(字符串)`(POST /v1/auth 用)
//! - `OrderJson`             ↔ pf_latency.mjs signLimit() 的序列化(bigint 转字符串,expiration 转数字,末尾加 hash)

use alloy_primitives::{keccak256, Address, B256, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{sol, Eip712Domain, SolStruct, SolValue};
use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::borrow::Cow;
use std::str::FromStr;

pub const CHAIN_ID: u64 = 56;
pub const PROTOCOL_NAME: &str = "predict.fun CTF Exchange";
pub const PROTOCOL_VERSION: &str = "1";
pub const KERNEL_NAME: &str = "Kernel";
pub const KERNEL_VERSION: &str = "0.3.1";
/// SDK `buildOrder` 默认过期时间 2100-01-01T00:00:00Z
pub const DEFAULT_EXPIRATION: u64 = 4_102_444_800;
/// SDK `MAX_SALT`;默认 salt = round(random * MAX_SALT),取值 0..=MAX_SALT
pub const MAX_SALT: u64 = 2_147_483_648;
pub const WEI: u128 = 1_000_000_000_000_000_000;

/// BNB 主网合约地址(SDK Constants.AddressesByChainId[56])
pub mod addr {
    use alloy_primitives::{address, Address};
    pub const CTF_EXCHANGE: Address = address!("8BC070BEdAB741406F4B1Eb65A72bee27894B689");
    pub const NEG_RISK_CTF_EXCHANGE: Address = address!("365fb81bd4A24D6303cd2F19c349dE6894D8d58A");
    pub const YIELD_BEARING_CTF_EXCHANGE: Address = address!("6bEb5a40C032AFc305961162d8204CDA16DECFa5");
    pub const YIELD_BEARING_NEG_RISK_CTF_EXCHANGE: Address = address!("8A289d458f5a134bA40015085A8F50Ffb681B41d");
    pub const CONDITIONAL_TOKENS: Address = address!("22DA1810B194ca018378464a58f6Ac2B10C9d244");
    pub const YIELD_BEARING_CONDITIONAL_TOKENS: Address = address!("9400F8Ad57e9e0F352345935d6D3175975eb1d9F");
    pub const NEG_RISK_ADAPTER: Address = address!("c3Cf7c252f65E0d8D88537dF96569AE94a7F1A6E");
    pub const YIELD_BEARING_NEG_RISK_ADAPTER: Address = address!("41dCe1A4B8FB5e6327701750aF6231B7CD0B2A40");
    pub const USDT: Address = address!("55d398326f99059fF775485246999027B3197955");
    pub const ECDSA_VALIDATOR: Address = address!("845ADb2C711129d4f3966735eD98a9F09fC4cE57");
}

sol! {
    /// 字段顺序与类型必须与 SDK ORDER_STRUCTURE 一致
    #[derive(Debug)]
    struct Order {
        uint256 salt;
        address maker;
        address signer;
        address taker;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint256 expiration;
        uint256 nonce;
        uint256 feeRateBps;
        uint8 side;
        uint8 signatureType;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

/// SDK getExchangeIdentifier
pub fn exchange_address(is_neg_risk: bool, is_yield_bearing: bool) -> Address {
    match (is_neg_risk, is_yield_bearing) {
        (true, true) => addr::YIELD_BEARING_NEG_RISK_CTF_EXCHANGE,
        (true, false) => addr::NEG_RISK_CTF_EXCHANGE,
        (false, true) => addr::YIELD_BEARING_CTF_EXCHANGE,
        (false, false) => addr::CTF_EXCHANGE,
    }
}

/// SDK retainSignificantDigits(非负):保留前 n 位有效数字,其余截断为 0
pub fn retain_significant_digits(num: U256, n: usize) -> U256 {
    if num.is_zero() {
        return num;
    }
    let len = num.to_string().len();
    if len <= n {
        return num;
    }
    let div = U256::from(10u64).pow(U256::from(len - n));
    (num / div) * div
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitAmounts {
    pub price_per_share: U256,
    pub maker_amount: U256,
    pub taker_amount: U256,
}

/// SDK getLimitOrderAmounts(precision = 1e18)
pub fn limit_order_amounts(side: Side, price_per_share_wei: U256, quantity_wei: U256) -> Result<LimitAmounts> {
    if quantity_wei < U256::from(10_000_000_000_000_000u64) {
        bail!("InvalidQuantityError: quantityWei 必须 ≥ 1e16");
    }
    let price = retain_significant_digits(price_per_share_wei, 3);
    let qty = retain_significant_digits(quantity_wei, 5);
    let prec = U256::from(WEI);
    let notional = price * qty / prec;
    Ok(match side {
        Side::Buy => LimitAmounts { price_per_share: price, maker_amount: notional, taker_amount: qty },
        Side::Sell => LimitAmounts { price_per_share: price, maker_amount: qty, taker_amount: notional },
    })
}

/// 价格以"分"(tick 0.01)表示:1..=99 → wei。对应 JS parseUnits(price.toFixed(2), 18)
pub fn cents_to_wei(cents: u32) -> U256 {
    U256::from(cents as u128 * (WEI / 100))
}

pub fn shares_to_wei(shares: u64) -> U256 {
    U256::from(shares as u128 * WEI)
}

/// 签名身份:Privy EOA 私钥 + 可选的 Predict 账户(智能钱包存款地址)
#[derive(Clone)]
pub struct OrderSigner {
    key: PrivateKeySigner,
    /// 原样保留用户配置的字符串(SDK 也是原样放进 maker/signer 字段)
    predict_account: Option<(Address, String)>,
}

impl std::fmt::Debug for OrderSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 绝不打印私钥
        f.debug_struct("OrderSigner").field("signer", &self.key.address()).field("predict_account", &self.predict_account.as_ref().map(|p| &p.1)).finish()
    }
}

impl OrderSigner {
    pub fn new(private_key_hex: &str, predict_account: Option<&str>) -> Result<Self> {
        let key = PrivateKeySigner::from_str(private_key_hex.trim()).context("私钥格式不对(内容不打印)")?;
        let predict_account = match predict_account {
            Some(s) => {
                let s = s.trim();
                let a = Address::from_str(s).context("PREDICT_ACCOUNT_ADDRESS 不是合法地址")?;
                Some((a, s.to_string()))
            }
            None => None,
        };
        Ok(Self { key, predict_account })
    }

    /// 临时随机私钥(测试与 dry-run 无私钥时用)
    pub fn random(predict_account: Option<&str>) -> Result<Self> {
        let key = PrivateKeySigner::random();
        let mut s = Self::new(&format!("0x{}", alloy_primitives::hex::encode(key.to_bytes())), None)?;
        if let Some(p) = predict_account {
            let a = Address::from_str(p.trim())?;
            s.predict_account = Some((a, p.trim().to_string()));
        }
        Ok(s)
    }

    pub fn signer_address(&self) -> Address {
        self.key.address()
    }

    pub fn predict_account(&self) -> Option<Address> {
        self.predict_account.as_ref().map(|p| p.0)
    }

    /// maker/signer 字段的字符串:predictAccount 原样;否则 EOA 地址(ethers Wallet.address = EIP-55 校验和格式)
    pub fn maker_string(&self) -> String {
        match &self.predict_account {
            Some((_, s)) => s.clone(),
            None => self.key.address().to_checksum(None),
        }
    }

    /// 对 32 字节哈希做原始 ECDSA 签名(链上交易用)
    pub fn sign_hash(&self, h: &B256) -> Result<alloy_primitives::Signature> {
        Ok(self.key.sign_hash_sync(h)?)
    }

    /// 仅测试用:导出私钥 hex,用于把同一把随机私钥交给 Node 生成参考向量
    #[doc(hidden)]
    pub fn private_key_hex_for_tests(&self) -> String {
        format!("0x{}", alloy_primitives::hex::encode(self.key.to_bytes()))
    }

    /// SDK eip712WrapHash + signer.signMessage(32 字节) + concat(0x01, validator, sig)
    fn sign_kernel_wrapped(&self, account: Address, message_hash: B256) -> Result<Vec<u8>> {
        let domain = Eip712Domain {
            name: Some(Cow::Borrowed(KERNEL_NAME)),
            version: Some(Cow::Borrowed(KERNEL_VERSION)),
            chain_id: Some(U256::from(CHAIN_ID)),
            verifying_contract: Some(account),
            salt: None,
        };
        let type_hash = keccak256("Kernel(bytes32 hash)".as_bytes());
        let kernel_msg = keccak256((type_hash, message_hash).abi_encode());
        let mut buf = Vec::with_capacity(66);
        buf.extend_from_slice(&[0x19, 0x01]);
        buf.extend_from_slice(domain.separator().as_slice());
        buf.extend_from_slice(kernel_msg.as_slice());
        let digest = keccak256(&buf);
        let sig = self.key.sign_message_sync(digest.as_slice())?;
        let mut out = Vec::with_capacity(86);
        out.push(0x01);
        out.extend_from_slice(addr::ECDSA_VALIDATOR.as_slice());
        out.extend_from_slice(&sig.as_bytes());
        Ok(out)
    }

    /// POST /v1/auth 的 signature(仅 predictAccount 模式;SDK signPredictAccountMessage)
    pub fn sign_auth_message(&self, message: &str) -> Result<String> {
        let Some((account, _)) = &self.predict_account else {
            bail!("鉴权签名需要 PREDICT_ACCOUNT_ADDRESS(Predict 账户 = 智能钱包)");
        };
        let h = alloy_primitives::eip191_hash_message(message.as_bytes());
        Ok(hex0x(&self.sign_kernel_wrapped(*account, h)?))
    }

    pub fn sign_order(&self, order: &Order, verifying_contract: Address) -> Result<SignedOrder> {
        let hash = typed_data_hash(order, verifying_contract);
        let sig = match &self.predict_account {
            Some((account, _)) => self.sign_kernel_wrapped(*account, hash)?,
            None => self.key.sign_hash_sync(&hash)?.as_bytes().to_vec(),
        };
        Ok(SignedOrder { hash, signature: hex0x(&sig) })
    }
}

pub fn hex0x(b: &[u8]) -> String {
    format!("0x{}", alloy_primitives::hex::encode(b))
}

pub fn order_domain(verifying_contract: Address) -> Eip712Domain {
    Eip712Domain {
        name: Some(Cow::Borrowed(PROTOCOL_NAME)),
        version: Some(Cow::Borrowed(PROTOCOL_VERSION)),
        chain_id: Some(U256::from(CHAIN_ID)),
        verifying_contract: Some(verifying_contract),
        salt: None,
    }
}

/// SDK buildTypedDataHash
pub fn typed_data_hash(order: &Order, verifying_contract: Address) -> B256 {
    order.eip712_signing_hash(&order_domain(verifying_contract))
}

#[derive(Debug, Clone)]
pub struct SignedOrder {
    pub hash: B256,
    pub signature: String,
}

/// 下单参数(价格、股数已是 wei)
#[derive(Debug, Clone)]
pub struct LimitOrderInput {
    pub side: Side,
    pub token_id: U256,
    pub price_per_share_wei: U256,
    pub quantity_wei: U256,
    pub fee_rate_bps: u64,
    pub salt: u64,
    pub is_neg_risk: bool,
    pub is_yield_bearing: bool,
}

/// 一张签好的单 + 发给 POST /v1/orders 的完整请求体
#[derive(Debug, Clone)]
pub struct BuiltOrder {
    pub amounts: LimitAmounts,
    pub order: Order,
    pub verifying_contract: Address,
    pub hash: B256,
    pub signature: String,
    pub order_json: OrderJson,
}

impl BuiltOrder {
    /// 请求体 JSON(字节与 SDK + pf_latency.mjs 一致):{"data":{"order":…,"pricePerShare":…,"strategy":"LIMIT","isPostOnly":true}}
    pub fn body_json(&self, post_only: bool) -> String {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Data<'a> {
            order: &'a OrderJson,
            price_per_share: String,
            strategy: &'static str,
            #[serde(skip_serializing_if = "Option::is_none")]
            is_post_only: Option<bool>,
        }
        #[derive(Serialize)]
        struct Body<'a> {
            data: Data<'a>,
        }
        serde_json::to_string(&Body {
            data: Data { order: &self.order_json, price_per_share: self.amounts.price_per_share.to_string(), strategy: "LIMIT", is_post_only: post_only.then_some(true) },
        })
        .expect("序列化不会失败")
    }
}

/// 序列化后的订单(字段顺序 = SDK buildOrder 返回对象 + signature + hash)
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderJson {
    pub salt: String,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    pub token_id: String,
    pub maker_amount: String,
    pub taker_amount: String,
    pub expiration: u64,
    pub nonce: String,
    pub fee_rate_bps: String,
    pub side: u8,
    pub signature_type: u8,
    pub signature: String,
    pub hash: String,
}

/// getLimitOrderAmounts → buildOrder("LIMIT") → buildTypedData → signTypedDataOrder → buildTypedDataHash
pub fn build_limit_order(signer: &OrderSigner, input: &LimitOrderInput) -> Result<BuiltOrder> {
    let amounts = limit_order_amounts(input.side, input.price_per_share_wei, input.quantity_wei)?;
    let maker_s = signer.maker_string();
    let maker = Address::from_str(&maker_s)?;
    let order = Order {
        salt: U256::from(input.salt),
        maker,
        signer: maker,
        taker: Address::ZERO,
        tokenId: input.token_id,
        makerAmount: amounts.maker_amount,
        takerAmount: amounts.taker_amount,
        expiration: U256::from(DEFAULT_EXPIRATION),
        nonce: U256::ZERO,
        feeRateBps: U256::from(input.fee_rate_bps),
        side: input.side as u8,
        signatureType: 0, // SignatureType.EOA(SDK 注释:EOA 也支持 EIP-1271)
    };
    let verifying_contract = exchange_address(input.is_neg_risk, input.is_yield_bearing);
    let signed = signer.sign_order(&order, verifying_contract)?;
    let order_json = OrderJson {
        salt: input.salt.to_string(),
        maker: maker_s.clone(),
        signer: maker_s,
        taker: Address::ZERO.to_checksum(None),
        token_id: input.token_id.to_string(),
        maker_amount: amounts.maker_amount.to_string(),
        taker_amount: amounts.taker_amount.to_string(),
        expiration: DEFAULT_EXPIRATION,
        nonce: "0".into(),
        fee_rate_bps: input.fee_rate_bps.to_string(),
        side: input.side as u8,
        signature_type: 0,
        signature: signed.signature.clone(),
        hash: format!("{:#x}", signed.hash),
    };
    Ok(BuiltOrder { amounts, order, verifying_contract, hash: signed.hash, signature: signed.signature, order_json })
}

/// SDK generateOrderSalt:String(Math.round(Math.random() * MAX_SALT))
pub fn random_salt() -> u64 {
    use rand::Rng;
    rand::rng().random_range(0..=MAX_SALT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retain_digits() {
        assert_eq!(retain_significant_digits(U256::from(123456u64), 3), U256::from(123000u64));
        assert_eq!(retain_significant_digits(U256::from(99u64), 3), U256::from(99u64));
        assert_eq!(retain_significant_digits(U256::ZERO, 3), U256::ZERO);
    }

    #[test]
    fn debug_does_not_leak_key() {
        let s = OrderSigner::random(None).unwrap();
        let k = s.private_key_hex_for_tests();
        let d = format!("{s:?}");
        assert!(!d.contains(&k[2..]), "Debug 输出不能包含私钥");
    }
}
