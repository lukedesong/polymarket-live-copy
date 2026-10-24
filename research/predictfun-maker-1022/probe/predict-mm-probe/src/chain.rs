//! 结算后的链上操作(BNB 链):合并(merge)与领回(redeem / claim),以及启动时核对存款地址的 owner。
//! 编码与 SDK OrderBuilder.mergePositions / redeemPositions(predictAccount 模式)相同:
//!   EOA 发交易到存款地址,调用 Kernel.execute(0x00…00, 目标合约(20B) ‖ value(32B) ‖ 内层 calldata),gasLimit = 估算 × 1.25。
//! 每个动作最多发一次交易;估算失败(比如还没出结果)不发、不花 gas,只记日志。

use crate::signing::{addr, OrderSigner};
use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, Bytes, TxKind, B256, U256};
use alloy_sol_types::{sol, SolCall};
use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};

sol! {
    interface IConditionalTokens {
        function mergePositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] partition, uint256 amount);
        function redeemPositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] indexSets);
        function balanceOf(address account, uint256 id) external view returns (uint256);
    }
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }
    interface INegRiskAdapter {
        function mergePositions(bytes32 _conditionId, uint256 _amount);
        function redeemPositions(bytes32 _conditionId, uint256[] _amounts);
    }
    interface IKernel {
        function execute(bytes32 execMode, bytes executionCalldata) payable;
    }
    interface IECDSAValidator {
        function ecdsaValidatorStorage(address) external view returns (address owner);
    }
}

pub fn ctf_address(is_neg_risk: bool, is_yield_bearing: bool) -> Address {
    // SDK getCtfIdentifier:非收益型的 NEG_RISK_CONDITIONAL_TOKENS 与 CONDITIONAL_TOKENS 是同一地址
    match (is_neg_risk, is_yield_bearing) {
        (_, false) => addr::CONDITIONAL_TOKENS,
        (false, true) => addr::YIELD_BEARING_CONDITIONAL_TOKENS,
        (true, true) => alloy_primitives::address!("F64b0b318AAf83BD9071110af24D24445719A07F"),
    }
}

/// (目标合约, 内层 calldata)
pub fn merge_call(condition_id: B256, amount: U256, is_neg_risk: bool, is_yield_bearing: bool) -> (Address, Vec<u8>) {
    if is_neg_risk {
        let t = if is_yield_bearing { addr::YIELD_BEARING_NEG_RISK_ADAPTER } else { addr::NEG_RISK_ADAPTER };
        (t, INegRiskAdapter::mergePositionsCall { _conditionId: condition_id, _amount: amount }.abi_encode())
    } else {
        let t = if is_yield_bearing { addr::YIELD_BEARING_CONDITIONAL_TOKENS } else { addr::CONDITIONAL_TOKENS };
        let c = IConditionalTokens::mergePositionsCall {
            collateralToken: addr::USDT,
            parentCollectionId: B256::ZERO,
            conditionId: condition_id,
            partition: vec![U256::from(1), U256::from(2)],
            amount,
        };
        (t, c.abi_encode())
    }
}

/// index_set:1 = 第一个结果,2 = 第二个。neg-risk 需要数量。
pub fn redeem_call(condition_id: B256, index_set: u8, amount: U256, is_neg_risk: bool, is_yield_bearing: bool) -> (Address, Vec<u8>) {
    if is_neg_risk {
        let t = if is_yield_bearing { addr::YIELD_BEARING_NEG_RISK_ADAPTER } else { addr::NEG_RISK_ADAPTER };
        let amounts = if index_set == 1 { vec![amount, U256::ZERO] } else { vec![U256::ZERO, amount] };
        (t, INegRiskAdapter::redeemPositionsCall { _conditionId: condition_id, _amounts: amounts }.abi_encode())
    } else {
        let t = if is_yield_bearing { addr::YIELD_BEARING_CONDITIONAL_TOKENS } else { addr::CONDITIONAL_TOKENS };
        let c = IConditionalTokens::redeemPositionsCall {
            collateralToken: addr::USDT,
            parentCollectionId: B256::ZERO,
            conditionId: condition_id,
            indexSets: vec![U256::from(index_set)],
        };
        (t, c.abi_encode())
    }
}

/// SDK encodeExecutionCalldata + kernel.execute(executionMode = ZeroHash, …)
pub fn kernel_execute_calldata(target: Address, inner: &[u8]) -> Vec<u8> {
    let mut exec = Vec::with_capacity(52 + inner.len());
    exec.extend_from_slice(target.as_slice());
    exec.extend_from_slice(&U256::ZERO.to_be_bytes::<32>());
    exec.extend_from_slice(inner);
    IKernel::executeCall { execMode: B256::ZERO, executionCalldata: Bytes::from(exec) }.abi_encode()
}

pub fn balance_of_calldata(account: Address, token_id: U256) -> Vec<u8> {
    IConditionalTokens::balanceOfCall { account, id: token_id }.abi_encode()
}

pub fn owner_calldata(account: Address) -> Vec<u8> {
    IECDSAValidator::ecdsaValidatorStorageCall(account).abi_encode()
}

pub struct Chain {
    http: reqwest::Client,
    rpc: String,
    signer: OrderSigner,
    account: Address,
    id: std::sync::atomic::AtomicU64,
    /// 同一时刻只发一笔交易(否则并发取到同一个 nonce)
    tx_lock: tokio::sync::Mutex<()>,
}

fn hexs(b: &[u8]) -> String {
    format!("0x{}", alloy_primitives::hex::encode(b))
}

fn parse_u256(v: &Value) -> Result<U256> {
    let s = v.as_str().ok_or_else(|| anyhow!("RPC 返回不是字符串: {v}"))?;
    Ok(U256::from_str_radix(s.trim_start_matches("0x"), 16)?)
}

impl Chain {
    pub fn new(http: reqwest::Client, rpc: String, signer: OrderSigner) -> Result<Self> {
        let account = signer.predict_account().ok_or_else(|| anyhow!("claim/merge 需要 PREDICT_ACCOUNT_ADDRESS"))?;
        Ok(Self { http, rpc, signer, account, id: 1.into(), tx_lock: tokio::sync::Mutex::new(()) })
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let r: Value = self
            .http
            .post(&self.rpc)
            .json(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .timeout(std::time::Duration::from_secs(15))
            .send()
            .await
            .with_context(|| format!("RPC {method} 连接失败"))?
            .json()
            .await?;
        if let Some(e) = r.get("error") {
            bail!("RPC {method} 错误: {e}");
        }
        Ok(r.get("result").cloned().unwrap_or(Value::Null))
    }

    pub async fn eth_call(&self, to: Address, data: &[u8]) -> Result<Vec<u8>> {
        let r = self.rpc("eth_call", json!([{"to": to, "data": hexs(data)}, "latest"])).await?;
        Ok(alloy_primitives::hex::decode(r.as_str().unwrap_or("0x"))?)
    }

    /// 存款地址(Kernel 智能钱包)在 ECDSA 验证器里登记的 owner;应等于 EOA
    pub async fn account_owner(&self) -> Result<Address> {
        let out = self.eth_call(addr::ECDSA_VALIDATOR, &owner_calldata(self.account)).await?;
        if out.len() < 32 {
            bail!("ecdsaValidatorStorage 返回长度 {}", out.len());
        }
        Ok(Address::from_slice(&out[12..32]))
    }

    pub async fn token_balance(&self, ctf: Address, token_id: U256) -> Result<U256> {
        let out = self.eth_call(ctf, &balance_of_calldata(self.account, token_id)).await?;
        if out.len() < 32 {
            bail!("balanceOf 返回长度 {}", out.len());
        }
        Ok(U256::from_be_slice(&out[..32]))
    }

    /// 存款地址的 USDT 余额(18 位小数)
    pub async fn usdt_balance(&self) -> Result<f64> {
        let out = self.eth_call(addr::USDT, &IERC20::balanceOfCall { account: self.account }.abi_encode()).await?;
        if out.len() < 32 {
            bail!("USDT balanceOf 返回长度 {}", out.len());
        }
        Ok(U256::from_be_slice(&out[..32]).to_string().parse::<f64>().unwrap_or(f64::NAN) / 1e18)
    }

    /// 估算 → 签名 → 发送 → 等回执(最多 120 秒),全程持锁。返回 (估算 gas, 交易哈希, 回执 status)
    pub async fn execute_and_wait(&self, target: Address, inner: &[u8]) -> Result<(u64, Option<B256>, Option<u64>)> {
        let _g = self.tx_lock.lock().await;
        let (gas, h) = self.execute(target, inner, false).await?;
        let st = match h {
            Some(h) => self.wait_receipt(h, std::time::Duration::from_secs(120)).await?,
            None => None,
        };
        Ok((gas, h, st))
    }

    /// 估算 → 签名 → 发送 Kernel.execute。dry=true 只估算不发。返回 (估算 gas, 交易哈希)
    pub async fn execute(&self, target: Address, inner: &[u8], dry: bool) -> Result<(u64, Option<B256>)> {
        let data = kernel_execute_calldata(target, inner);
        let from = self.signer.signer_address();
        let est = self.rpc("eth_estimateGas", json!([{"from": from, "to": self.account, "data": hexs(&data)}])).await.context("估算 gas 失败(未发交易)")?;
        let est: u64 = parse_u256(&est)?.try_into()?;
        if dry {
            return Ok((est, None));
        }
        let chain_id: u64 = parse_u256(&self.rpc("eth_chainId", json!([])).await?)?.try_into()?;
        if chain_id != crate::signing::CHAIN_ID {
            bail!("RPC 链 id {chain_id} 不是 BNB 主网 56,拒绝发交易");
        }
        let nonce: u64 = parse_u256(&self.rpc("eth_getTransactionCount", json!([from, "pending"])).await?)?.try_into()?;
        let gas_price: u128 = parse_u256(&self.rpc("eth_gasPrice", json!([])).await?)?.try_into()?;
        let tx = TxLegacy { chain_id: Some(chain_id), nonce, gas_price, gas_limit: est * 125 / 100, to: TxKind::Call(self.account), value: U256::ZERO, input: Bytes::from(data) };
        let sig = self.signer.sign_hash(&tx.signature_hash())?;
        let env: TxEnvelope = tx.into_signed(sig).into();
        let raw = env.encoded_2718();
        let h = self.rpc("eth_sendRawTransaction", json!([hexs(&raw)])).await?;
        let h: B256 = h.as_str().ok_or_else(|| anyhow!("无交易哈希"))?.parse()?;
        Ok((est, Some(h)))
    }

    /// 等回执(最多 timeout);返回 status(1 成功)
    pub async fn wait_receipt(&self, h: B256, timeout: std::time::Duration) -> Result<Option<u64>> {
        let t0 = std::time::Instant::now();
        while t0.elapsed() < timeout {
            let r = self.rpc("eth_getTransactionReceipt", json!([h])).await?;
            if !r.is_null() {
                return Ok(Some(parse_u256(r.get("status").unwrap_or(&json!("0x0")))?.try_into()?));
            }
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
        Ok(None)
    }
}
