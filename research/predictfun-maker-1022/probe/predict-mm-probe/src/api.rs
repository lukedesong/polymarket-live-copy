//! Predict REST 客户端 + 响应容错解析。每个请求都先从限额器取令牌,并记录发出/返回时间、状态、限额响应头。
//! 文档里没写清的响应字段全部按"多种可能的字段名"容错解析;解析不出来时调用方按保守规则处理。

use crate::jsonlog::{mono_ns, Log};
use crate::msgs::{parse_shares, BookSnapshot};
use crate::ratelimit::{parse_ratelimit_remaining, Priority, RateLimiter};
use alloy_primitives::{B256, U256};
use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub struct MarketInfo {
    pub id: u64,
    pub up_token: U256,
    pub down_token: U256,
    /// 结果在链上的 index set(1/2);拿不到按 outcomes 顺序
    pub up_index_set: u8,
    pub down_index_set: u8,
    pub fee_rate_bps: u64,
    pub is_neg_risk: bool,
    pub is_yield_bearing: bool,
    pub condition_id: Option<B256>,
    pub status: Option<String>,
    pub trading_status: Option<String>,
}

fn as_u64(v: &Value) -> Option<u64> {
    match v {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn as_bool(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        Value::String(s) => match s.to_ascii_lowercase().as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// GET /v1/markets/<id> 的 data
pub fn parse_market(d: &Value) -> Result<MarketInfo> {
    let id = as_u64(&d["id"]).ok_or_else(|| anyhow!("市场缺 id"))?;
    let outcomes = d["outcomes"].as_array().ok_or_else(|| anyhow!("市场缺 outcomes"))?;
    let find = |name: &str| -> Result<(usize, &Value)> {
        outcomes.iter().enumerate().find(|(_, o)| o["name"].as_str().is_some_and(|n| n.eq_ignore_ascii_case(name))).ok_or_else(|| anyhow!("找不到结果 {name}"))
    };
    let (ui, up) = find("Up")?;
    let (di, dn) = find("Down")?;
    let tok = |o: &Value| -> Result<U256> {
        let s = match &o["onChainId"] {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            _ => bail!("结果缺 onChainId"),
        };
        Ok(U256::from_str(&s)?)
    };
    let idx = |o: &Value, i: usize| as_u64(&o["indexSet"]).map(|x| x as u8).unwrap_or(1 << i);
    // 字段缺失时按"更保守"处理:feeRateBps 必须有;isNegRisk/isYieldBearing 必须有(签错交易所会被拒)
    Ok(MarketInfo {
        id,
        up_token: tok(up)?,
        down_token: tok(dn)?,
        up_index_set: idx(up, ui),
        down_index_set: idx(dn, di),
        fee_rate_bps: as_u64(&d["feeRateBps"]).ok_or_else(|| anyhow!("市场缺 feeRateBps"))?,
        is_neg_risk: as_bool(&d["isNegRisk"]).ok_or_else(|| anyhow!("市场缺 isNegRisk"))?,
        is_yield_bearing: as_bool(&d["isYieldBearing"]).ok_or_else(|| anyhow!("市场缺 isYieldBearing"))?,
        condition_id: d["conditionId"].as_str().and_then(|s| B256::from_str(s).ok()),
        status: d["status"].as_str().map(String::from),
        trading_status: d["tradingStatus"].as_str().map(String::from),
    })
}

/// 结算结果:(Up 每股兑付, Down 每股兑付)。认 outcomes[].status = WON/LOST;平局两边都 WON → 各 0.5。
pub fn parse_resolution(d: &Value) -> Option<(f64, f64)> {
    let outcomes = d["outcomes"].as_array()?;
    let st = |name: &str| -> Option<String> {
        outcomes.iter().find(|o| o["name"].as_str().is_some_and(|n| n.eq_ignore_ascii_case(name))).and_then(|o| o["status"].as_str()).map(|s| s.to_ascii_uppercase())
    };
    let (u, w) = (st("Up")?, st("Down")?);
    match (u.as_str(), w.as_str()) {
        ("WON", "WON") => Some((0.5, 0.5)),
        ("WON", "LOST") => Some((1.0, 0.0)),
        ("LOST", "WON") => Some((0.0, 1.0)),
        _ => None,
    }
}

/// POST /v1/orders 成功回包:(orderId, hash)
pub fn parse_place_resp(v: &Value) -> Option<(String, Option<String>)> {
    let d = &v["data"];
    let id = match &d["orderId"] {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => match &d["id"] {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            _ => return None,
        },
    };
    let hash = d["orderHash"].as_str().or(d["hash"].as_str()).map(|s| s.to_ascii_lowercase());
    Some((id, hash))
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct OrderDetail {
    pub id: Option<String>,
    pub hash: Option<String>,
    /// 累计成交股数
    pub filled: Option<f64>,
    pub status: Option<String>,
    /// Some(true) = 仍挂着;Some(false) = 已关闭(成交完/撤销/过期);None = 不确定
    pub open: Option<bool>,
}

pub fn parse_order(o: &Value) -> OrderDetail {
    let inner = &o["order"];
    let id = match &o["id"] {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => match &o["orderId"] {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        },
    };
    let hash = inner["hash"].as_str().or(o["hash"].as_str()).or(o["orderHash"].as_str()).map(|s| s.to_ascii_lowercase());
    let filled = ["amountFilled", "filledAmount", "quantityFilled", "filled"].iter().find_map(|k| o.get(*k).and_then(parse_shares));
    let status = o["status"].as_str().map(|s| s.to_ascii_uppercase());
    let open = status.as_deref().map(|s| matches!(s, "OPEN" | "LIVE" | "ACTIVE" | "PENDING" | "PARTIALLY_FILLED"));
    OrderDetail { id, hash, filled, status, open }
}

#[derive(Debug, Clone)]
pub struct Resp {
    pub status: u16,
    pub json: Value,
    pub rtt_ms: f64,
}

impl Resp {
    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

pub struct Api {
    http: reqwest::Client,
    base: String,
    api_key: Option<String>,
    jwt: RwLock<Option<String>>,
    log: Log,
    pub rl: Arc<Mutex<RateLimiter>>,
    /// 最近一次 429 / 401 的单调时钟(任何接口都算;主循环据此全撤冷却 / 重新鉴权)
    pub last_429_ns: AtomicU64,
    pub last_401_ns: AtomicU64,
}

impl Api {
    pub fn new(http: reqwest::Client, base: String, api_key: Option<String>, log: Log, rl: Arc<Mutex<RateLimiter>>) -> Self {
        Self { http, base, api_key, jwt: RwLock::new(None), log, rl, last_429_ns: AtomicU64::new(0), last_401_ns: AtomicU64::new(0) }
    }

    pub fn set_jwt(&self, jwt: Option<String>) {
        if let Some(j) = &jwt {
            self.log.add_secret(j);
        }
        *self.jwt.write().unwrap() = jwt;
    }

    pub fn jwt(&self) -> Option<String> {
        self.jwt.read().unwrap().clone()
    }

    pub async fn acquire(&self, prio: Priority) {
        loop {
            let wait = {
                let mut rl = self.rl.lock().unwrap();
                let t = mono_ns();
                if rl.try_acquire(t, prio) {
                    return;
                }
                if prio == Priority::Cancel {
                    rl.next_cancel_slot(t).saturating_sub(t).max(1_000_000)
                } else {
                    20_000_000
                }
            };
            tokio::time::sleep(Duration::from_nanos(wait)).await;
        }
    }

    /// prio = None:调用方(策略)已经拿过令牌
    #[allow(clippy::too_many_arguments)]
    pub async fn request(
        &self,
        tag: &str,
        method: reqwest::Method,
        path: &str,
        body: Option<String>,
        auth: bool,
        prio: Option<Priority>,
        log_body: bool,
        extra: Value,
    ) -> Result<Resp> {
        if let Some(p) = prio {
            self.acquire(p).await;
        }
        let mut rb = self.http.request(method.clone(), format!("{}{}", self.base, path)).header("content-type", "application/json").timeout(Duration::from_secs(10));
        if let Some(k) = &self.api_key {
            rb = rb.header("x-api-key", k);
        }
        if auth {
            let j = self.jwt().ok_or_else(|| anyhow!("没有 JWT"))?;
            rb = rb.header("authorization", format!("Bearer {j}"));
        }
        if let Some(b) = body {
            rb = rb.body(b);
        }
        let t0 = crate::jsonlog::now();
        self.log.ev_at(t0, "req", json!({"tag": tag, "method": method.as_str(), "path": path, "extra": extra}));
        let r = rb.send().await;
        let t1 = crate::jsonlog::now();
        let rtt_ms = (t1.mono_ns - t0.mono_ns) as f64 / 1e6;
        let r = match r {
            Ok(r) => r,
            Err(e) => {
                self.log.ev_at(t1, "resp", json!({"tag": tag, "path": path, "sent_mono_ns": t0.mono_ns, "rtt_ms": rtt_ms, "error": e.to_string()}));
                return Err(e).context(format!("{tag} 网络错误"));
            }
        };
        let status = r.status().as_u16();
        if status == 429 {
            self.last_429_ns.store(t1.mono_ns, Ordering::Relaxed);
        } else if status == 401 && auth {
            self.last_401_ns.store(t1.mono_ns, Ordering::Relaxed);
        }
        let hdr = |k: &str| r.headers().get(k).and_then(|v| v.to_str().ok()).map(String::from);
        let (rl_h, rlp_h, retry) = (hdr("ratelimit"), hdr("ratelimit-policy"), hdr("retry-after"));
        let text = r.text().await.unwrap_or_default();
        let json: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        let mut f = json!({"tag": tag, "path": path, "status": status, "sent_mono_ns": t0.mono_ns, "sent_wall_ms": t0.wall_ms, "rtt_ms": rtt_ms,
            "ratelimit": rl_h, "ratelimit_policy": rlp_h, "retry_after": retry});
        if log_body {
            f["body"] = json!(text.chars().take(4000).collect::<String>());
        }
        self.log.ev_at(t1, "resp", f);
        // 限额头显示快用完:1 秒内只放撤单
        if let Some(rem) = rl_h.as_deref().and_then(parse_ratelimit_remaining) {
            if rem <= 2 {
                self.rl.lock().unwrap().pause_non_cancel(mono_ns() + 1_000_000_000);
            }
        }
        Ok(Resp { status, json, rtt_ms })
    }

    pub async fn get_public(&self, tag: &str, path: &str) -> Result<Resp> {
        self.request(tag, reqwest::Method::GET, path, None, false, Some(Priority::Query), true, Value::Null).await
    }

    /// btc-updown-5m-<开始秒> → 市场 id(还没开放返回 None)
    pub async fn category_market_id(&self, start_s: i64) -> Result<Option<u64>> {
        let r = self.get_public("category", &format!("/v1/categories/btc-updown-5m-{start_s}")).await?;
        if r.status == 404 {
            return Ok(None);
        }
        if !r.ok() {
            bail!("category {} 返回 {}", start_s, r.status);
        }
        Ok(r.json["data"]["markets"].get(0).and_then(|m| as_u64(&m["id"])))
    }

    pub async fn market(&self, id: u64) -> Result<Value> {
        let r = self.get_public("market", &format!("/v1/markets/{id}")).await?;
        if !r.ok() {
            bail!("market {id} 返回 {}", r.status);
        }
        Ok(r.json["data"].clone())
    }

    pub async fn orderbook(&self, id: u64) -> Result<BookSnapshot> {
        let r = self.get_public("orderbook", &format!("/v1/markets/{id}/orderbook")).await?;
        if !r.ok() {
            bail!("orderbook {id} 返回 {}", r.status);
        }
        let d = &r.json["data"];
        let wrapped = json!({"type": "M", "topic": format!("predictOrderbook/{id}"), "data": d});
        match crate::msgs::parse_predict(&wrapped.to_string()) {
            Some(crate::msgs::PredictMsg::Book(mut b)) => {
                b.market_id = id;
                Ok(b)
            }
            _ => bail!("orderbook 解析失败"),
        }
    }

    /// GET /v1/auth/message → 签名 → POST /v1/auth → JWT
    pub async fn auth(&self, signer: &crate::signing::OrderSigner) -> Result<String> {
        let account = signer.maker_string();
        let m = self.request("auth_message", reqwest::Method::GET, "/v1/auth/message", None, false, Some(Priority::Cancel), true, Value::Null).await?;
        let msg = m.json["data"]["message"].as_str().ok_or_else(|| anyhow!("auth/message 返回 {}", m.status))?.to_string();
        let signature = signer.sign_auth_message(&msg)?;
        let body = json!({"signer": account, "message": msg, "signature": signature}).to_string();
        // 回包里有 JWT:不记 body
        let a = self.request("auth", reqwest::Method::POST, "/v1/auth", Some(body), false, Some(Priority::Cancel), false, Value::Null).await?;
        let jwt = a.json["data"]["token"].as_str().ok_or_else(|| anyhow!("鉴权失败,状态 {}", a.status))?.to_string();
        self.set_jwt(Some(jwt.clone()));
        Ok(jwt)
    }

    pub async fn place(&self, body: String, extra: Value) -> Result<Resp> {
        self.request("place", reqwest::Method::POST, "/v1/orders", Some(body), true, None, true, extra).await
    }

    pub async fn cancel(&self, ids: &[String], prio: Option<Priority>) -> Result<Resp> {
        let body = json!({"data": {"ids": ids}}).to_string();
        self.request("cancel", reqwest::Method::POST, "/v1/orders/remove", Some(body), true, prio, true, json!({"ids": ids})).await
    }

    /// 当前挂单(翻页,最多 10 页)
    pub async fn open_orders(&self, prio: Priority) -> Result<Vec<OrderDetail>> {
        let mut out = vec![];
        let mut cursor: Option<String> = None;
        for _ in 0..10 {
            let path = match &cursor {
                Some(c) => format!("/v1/orders?first=50&after={}", urlencode(c)),
                None => "/v1/orders?first=50".to_string(),
            };
            let r = self.request("open_orders", reqwest::Method::GET, &path, None, true, Some(prio), true, Value::Null).await?;
            if !r.ok() {
                bail!("open_orders 返回 {}", r.status);
            }
            let rows = r.json["data"].as_array().cloned().unwrap_or_default();
            let n = rows.len();
            out.extend(rows.iter().map(parse_order));
            cursor = r.json["cursor"].as_str().map(String::from);
            if n < 50 || cursor.is_none() {
                break;
            }
        }
        Ok(out)
    }

    pub async fn order(&self, hash: &str) -> Result<OrderDetail> {
        let r = self.request("order", reqwest::Method::GET, &format!("/v1/orders/{hash}"), None, true, Some(Priority::Query), true, Value::Null).await?;
        if !r.ok() {
            bail!("order {hash} 返回 {}", r.status);
        }
        Ok(parse_order(&r.json["data"]))
    }
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market_json() -> Value {
        json!({"id": 3698227, "title": "BTC Up or Down", "feeRateBps": 200, "isNegRisk": false, "isYieldBearing": false,
            "conditionId": "0x1111111111111111111111111111111111111111111111111111111111111111",
            "status": "REGISTERED", "tradingStatus": "OPEN",
            "outcomes": [{"name": "Up", "indexSet": 1, "onChainId": "86675541217400175431015071230207253729647490951854712850886532176499327385594", "status": null},
                         {"name": "Down", "indexSet": 2, "onChainId": "123", "status": null}]})
    }

    #[test]
    fn market_parse() {
        let m = parse_market(&market_json()).unwrap();
        assert_eq!(m.id, 3698227);
        assert_eq!(m.down_token, U256::from(123));
        assert_eq!((m.up_index_set, m.down_index_set), (1, 2));
        assert_eq!(m.fee_rate_bps, 200);
        assert!(!m.is_neg_risk && !m.is_yield_bearing);
        assert!(m.condition_id.is_some());
        // 缺关键字段直接报错(不猜)
        let mut bad = market_json();
        bad.as_object_mut().unwrap().remove("isNegRisk");
        assert!(parse_market(&bad).is_err());
        assert_eq!(parse_resolution(&market_json()), None);
    }

    #[test]
    fn resolution_parse() {
        let mut m = market_json();
        m["outcomes"][0]["status"] = json!("WON");
        m["outcomes"][1]["status"] = json!("LOST");
        assert_eq!(parse_resolution(&m), Some((1.0, 0.0)));
        m["outcomes"][1]["status"] = json!("WON");
        assert_eq!(parse_resolution(&m), Some((0.5, 0.5)), "平局两边都 WON 各 0.5");
    }

    #[test]
    fn order_parse() {
        let r = parse_place_resp(&json!({"success": true, "data": {"orderId": "6165626687", "orderHash": "0xABC"}})).unwrap();
        assert_eq!(r, ("6165626687".into(), Some("0xabc".into())));
        let d = parse_order(&json!({"id": "1", "status": "FILLED", "amountFilled": "2000000000000000000", "order": {"hash": "0xDEF"}}));
        assert_eq!(d.filled, Some(2.0));
        assert_eq!(d.open, Some(false));
        assert_eq!(d.hash.as_deref(), Some("0xdef"));
        let d = parse_order(&json!({"id": 5, "status": "OPEN"}));
        assert_eq!((d.id.as_deref(), d.open, d.filled), (Some("5"), Some(true), None));
    }
}
