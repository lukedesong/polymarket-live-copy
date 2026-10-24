//! 行情 / 钱包事件消息解析。格式来自 ref/ws_samples.tsv(真实录制)与 ref/pfrec5.py、ref/pf_latency.mjs。
//! 钱包事件格式未录到,全部按"容错"解析:认不出的字段不报错,原文照记日志。

use serde_json::Value;

/// 价格(美元,0~1 浮点)→ 分(tick 0.01)。超出 1..=99 的返回 None。
pub fn price_to_cents(p: f64) -> Option<u32> {
    if !p.is_finite() {
        return None;
    }
    let c = (p * 100.0).round();
    if (1.0..=99.0).contains(&c) && (p * 100.0 - c).abs() < 1e-6 {
        Some(c as u32)
    } else {
        None
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BookTicker {
    pub symbol: String,
    pub update_id: u64,
    pub bid: f64,
    pub bid_qty: f64,
    pub ask: f64,
    pub ask_qty: f64,
}

impl BookTicker {
    pub fn mid(&self) -> f64 {
        (self.bid + self.ask) / 2.0
    }
}

fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// 币安 bookTicker:组合流 {"stream":"btcusdt@bookTicker","data":{...}} 或单流 {...};现货与合约格式都认。
/// 不是 bookTicker(如 trade)返回 None。
pub fn parse_binance_book_ticker(raw: &str) -> Option<BookTicker> {
    let v: Value = serde_json::from_str(raw).ok()?;
    let d = if v.get("stream").is_some() { v.get("data")? } else { &v };
    if let Some(e) = d.get("e").and_then(|e| e.as_str()) {
        if e != "bookTicker" {
            return None;
        }
    }
    Some(BookTicker {
        symbol: d.get("s")?.as_str()?.to_string(),
        update_id: d.get("u")?.as_u64()?,
        bid: num(d.get("b")?)?,
        bid_qty: num(d.get("B")?)?,
        ask: num(d.get("a")?)?,
        ask_qty: num(d.get("A")?)?,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct BookSnapshot {
    pub market_id: u64,
    /// (价格, 数量);按推送原样保存
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
    pub version: Option<u64>,
    pub update_ts_ms: Option<i64>,
    pub settlements_pending_bids: Vec<(f64, f64)>,
    pub settlements_pending_asks: Vec<(f64, f64)>,
    pub order_count: Option<u64>,
}

impl BookSnapshot {
    /// 买一(取最高价,不依赖推送里的排序)
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.iter().filter(|l| l.1 > 0.0).map(|l| l.0).fold(None, |a, p| Some(a.map_or(p, |x: f64| x.max(p))))
    }
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.iter().filter(|l| l.1 > 0.0).map(|l| l.0).fold(None, |a, p| Some(a.map_or(p, |x: f64| x.min(p))))
    }
    pub fn best_bid_cents(&self) -> Option<u32> {
        self.best_bid().and_then(price_to_cents)
    }
    pub fn best_ask_cents(&self) -> Option<u32> {
        self.best_ask().and_then(price_to_cents)
    }
    /// 某价位挂量(用于记录"挂单时前面排了多少")
    pub fn bid_size_at(&self, cents: u32) -> f64 {
        self.bids.iter().filter(|l| price_to_cents(l.0) == Some(cents)).map(|l| l.1).sum()
    }
    pub fn ask_size_at(&self, cents: u32) -> f64 {
        self.asks.iter().filter(|l| price_to_cents(l.0) == Some(cents)).map(|l| l.1).sum()
    }
}

fn levels(v: Option<&Value>) -> Vec<(f64, f64)> {
    v.and_then(|x| x.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|l| {
                    let l = l.as_array()?;
                    Some((num(l.first()?)?, num(l.get(1)?)?))
                })
                .collect()
        })
        .unwrap_or_default()
}

#[derive(Debug, Clone, PartialEq)]
pub enum PredictMsg {
    /// {"type":"M","topic":"heartbeat","data":X} → 必须回 {"method":"heartbeat","data":X}
    Heartbeat(Value),
    Book(BookSnapshot),
    /// predictWalletEvents/<jwt> 的推送(格式未录,原样给出 data)
    Wallet(Value),
    /// 其它主题推送
    Topic {
        topic: String,
        data: Value,
    },
    /// 订阅等请求的回包(无 topic)
    Response(Value),
}

pub fn parse_predict(raw: &str) -> Option<PredictMsg> {
    let v: Value = serde_json::from_str(raw).ok()?;
    let topic = v.get("topic").and_then(|t| t.as_str());
    let Some(topic) = topic else {
        return Some(PredictMsg::Response(v));
    };
    let data = v.get("data").cloned().unwrap_or(Value::Null);
    if topic == "heartbeat" {
        return Some(PredictMsg::Heartbeat(data));
    }
    if let Some(id) = topic.strip_prefix("predictOrderbook/") {
        let market_id = data.get("marketId").and_then(|m| m.as_u64()).or_else(|| id.parse().ok())?;
        let sp = data.get("settlementsPending");
        return Some(PredictMsg::Book(BookSnapshot {
            market_id,
            bids: levels(data.get("bids")),
            asks: levels(data.get("asks")),
            version: data.get("version").and_then(|x| x.as_u64()),
            update_ts_ms: data.get("updateTimestampMs").and_then(|x| x.as_i64()),
            settlements_pending_bids: levels(sp.and_then(|s| s.get("bids"))),
            settlements_pending_asks: levels(sp.and_then(|s| s.get("asks"))),
            order_count: data.get("orderCount").and_then(|x| x.as_u64()),
        }));
    }
    if topic.starts_with("predictWalletEvents/") {
        return Some(PredictMsg::Wallet(data));
    }
    Some(PredictMsg::Topic { topic: topic.to_string(), data })
}

pub fn heartbeat_reply(data: &Value) -> String {
    serde_json::json!({"method": "heartbeat", "data": data}).to_string()
}

pub fn subscribe_msg(request_id: u64, topic: &str) -> String {
    serde_json::json!({"requestId": request_id, "method": "subscribe", "params": [topic]}).to_string()
}

pub fn unsubscribe_msg(request_id: u64, topic: &str) -> String {
    serde_json::json!({"requestId": request_id, "method": "unsubscribe", "params": [topic]}).to_string()
}

/// 数量容错解析:wei 整数字符串(如 "15383999999999999999",≥ 1e12 视为 wei)→ 股;小数或数字原样。
pub fn parse_shares(v: &Value) -> Option<f64> {
    match v {
        Value::String(s) => {
            let s = s.trim();
            if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) && s.len() >= 13 {
                // wei → 股;按整数部分 + 小数部分拆开,避免 f64 直接解析超长整数丢精度
                let (int, frac) = if s.len() > 18 { s.split_at(s.len() - 18) } else { ("0", s) };
                let frac = format!("{frac:0>18}");
                format!("{int}.{frac}").parse().ok()
            } else {
                s.parse().ok()
            }
        }
        Value::Number(n) => {
            let f = n.as_f64()?;
            Some(if f >= 1e12 { f / 1e18 } else { f })
        }
        _ => None,
    }
}

/// 价格容错:0~1 小数;或 wei(如 "220000000000000000")
pub fn parse_price(v: &Value) -> Option<f64> {
    let f = parse_shares(v)?;
    if (0.0..=1.0).contains(&f) {
        Some(f)
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillEventKind {
    /// orderTransactionSubmitted:平台已撮合(约 0.3 秒推到),上链结果未出
    Submitted,
    /// orderTransactionSuccess:上链成功
    Success,
    /// orderTransactionFailed:上链失败,这笔撮合作废
    Failed,
}

/// 钱包事件里的一笔撮合(10-24 实盘录到的格式):同一笔撮合的 已撮合 / 上链成功 / 上链失败 靠 settlementId 对上;
/// fill.executedSizeWei 是这一笔的股数(不是累计)
#[derive(Debug, Clone, PartialEq)]
pub struct WalletFill {
    pub kind: FillEventKind,
    pub order_id: String,
    pub settlement_id: String,
    pub shares: f64,
    pub price: Option<f64>,
    pub platform_ts: Option<i64>,
}

pub fn parse_wallet_fill(data: &Value) -> Option<WalletFill> {
    let kind = match data.get("type")?.as_str()? {
        "orderTransactionSubmitted" => FillEventKind::Submitted,
        "orderTransactionSuccess" => FillEventKind::Success,
        "orderTransactionFailed" => FillEventKind::Failed,
        _ => return None,
    };
    let order_id = match data.get("orderId")? {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => return None,
    };
    let fill = data.get("fill")?;
    Some(WalletFill {
        kind,
        order_id,
        settlement_id: data.get("settlementId")?.as_str()?.to_string(),
        shares: parse_shares(fill.get("executedSizeWei")?)?,
        price: fill.get("executedPriceWei").and_then(parse_price),
        platform_ts: data.get("timestamp").and_then(|t| t.as_i64()),
    })
}

/// 一张单子的所有撮合(settlementId → (股数, 是否上链失败))
#[derive(Debug, Clone, Default)]
pub struct Settlements(pub std::collections::HashMap<String, (f64, bool)>);

impl Settlements {
    /// 记一条撮合事件。filled = 记账里这张单已记的成交股数(可能含 REST 查单记上的)。
    /// 返回 (钱包累计, 新的记账目标, 是否允许往下扣)。
    /// 只有"上链失败"能往下扣,且只扣这一笔、之前记过的才扣:漏推的事件可能已由 REST 查单记上,
    /// 不能拿钱包累计去覆盖;其余事件只增不减
    pub fn apply(&mut self, w: &WalletFill, filled: f64) -> (f64, f64, bool) {
        let counted = self.0.get(&w.settlement_id).is_some_and(|x| !x.1);
        let s = self.0.entry(w.settlement_id.clone()).or_insert((w.shares, false));
        s.0 = w.shares;
        if w.kind == FillEventKind::Failed {
            s.1 = true;
        }
        let cum: f64 = self.0.values().filter(|x| !x.1).map(|x| x.0).sum();
        let (target, dec) = match w.kind {
            FillEventKind::Failed if counted => (filled - w.shares, true),
            FillEventKind::Failed => (filled, false),
            _ => (cum, false),
        };
        (cum, target, dec)
    }
}

/// 从钱包事件里尽量抠出与订单相关的信息。找不到就全是 None,调用方应改用 REST 查单。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WalletEventHint {
    pub kind: Option<String>,
    pub order_ids: Vec<String>,
    pub order_hashes: Vec<String>,
    pub amount_filled: Option<f64>,
    pub price: Option<f64>,
    pub counterparty: Option<String>,
}

pub fn wallet_event_hint(data: &Value) -> WalletEventHint {
    let mut h = WalletEventHint::default();
    fn walk(v: &Value, h: &mut WalletEventHint, depth: usize) {
        if depth > 6 {
            return;
        }
        match v {
            Value::Object(m) => {
                for (k, x) in m {
                    let kl = k.to_ascii_lowercase();
                    match kl.as_str() {
                        "type" | "name" | "event" | "kind" | "eventtype" if h.kind.is_none() => {
                            if let Some(s) = x.as_str() {
                                h.kind = Some(s.to_string());
                            }
                        }
                        "orderid" | "id" => {
                            let s = match x {
                                Value::String(s) => Some(s.clone()),
                                Value::Number(n) => Some(n.to_string()),
                                _ => None,
                            };
                            if let Some(s) = s {
                                if s.starts_with("0x") && s.len() == 66 {
                                    h.order_hashes.push(s.to_ascii_lowercase());
                                } else {
                                    h.order_ids.push(s);
                                }
                            }
                        }
                        "hash" | "orderhash" => {
                            if let Some(s) = x.as_str() {
                                if s.starts_with("0x") && s.len() == 66 {
                                    h.order_hashes.push(s.to_ascii_lowercase());
                                }
                            }
                        }
                        "amountfilled" | "filledamount" | "filled" | "quantityfilled" if h.amount_filled.is_none() => {
                            h.amount_filled = parse_shares(x);
                        }
                        "price" | "priceexecuted" | "pricepershare" | "executionprice" if h.price.is_none() => {
                            h.price = parse_price(x);
                        }
                        "counterparty" | "taker" | "maker" if h.counterparty.is_none() => {
                            if let Some(s) = x.as_str() {
                                h.counterparty = Some(s.to_string());
                            }
                        }
                        _ => {}
                    }
                    if x.is_object() || x.is_array() {
                        walk(x, h, depth + 1);
                    }
                }
            }
            Value::Array(a) => {
                for x in a {
                    walk(x, h, depth + 1);
                }
            }
            _ => {}
        }
    }
    walk(data, &mut h, 0);
    h.order_ids.dedup();
    h.order_hashes.dedup();
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cents() {
        assert_eq!(price_to_cents(0.5), Some(50));
        assert_eq!(price_to_cents(0.01), Some(1));
        assert_eq!(price_to_cents(0.99), Some(99));
        assert_eq!(price_to_cents(0.1 + 0.2), Some(30));
        assert_eq!(price_to_cents(1.0), None);
        assert_eq!(price_to_cents(0.0), None);
        assert_eq!(price_to_cents(0.505), None, "非整 tick 不接受");
    }

    #[test]
    fn wallet_fill_events_from_live_log() {
        // 10-24 13:35 实盘录到的原样(去掉 details)
        let sub: Value = serde_json::from_str(r#"{"fill":{"executedPriceWei":"480000000000000000","executedSizeWei":"1923076923076923200","executedValueWei":"923076923076923136"},"isMaker":true,"kind":"PURCHASE","orderHash":"0x47848a65ef90427c257522ea09767776f4310feb52706e08e6a66c47aa59b07e","orderId":"6188106220","settlementId":"01a16e1f-310f-7362-9c7a-354463463d30","timestamp":1792848900367,"type":"orderTransactionSubmitted"}"#).unwrap();
        let w = parse_wallet_fill(&sub).unwrap();
        assert_eq!(w.kind, FillEventKind::Submitted);
        assert_eq!(w.order_id, "6188106220");
        assert_eq!(w.settlement_id, "01a16e1f-310f-7362-9c7a-354463463d30");
        assert!((w.shares - 1.9230769230769232).abs() < 1e-12);
        assert_eq!(w.price, Some(0.48));
        assert_eq!(w.platform_ts, Some(1792848900367));
        let failed: Value = serde_json::from_str(r#"{"fill":{"executedPriceWei":"350000000000000000","executedSizeWei":"1050000000000000000","executedValueWei":"367500000000000000"},"orderHash":"0xc9","orderId":"6188183576","settlementId":"01a16e20-7d34-7f30-9713-98cc6aab8cbd","timestamp":1792848985421,"type":"orderTransactionFailed"}"#).unwrap();
        assert_eq!(parse_wallet_fill(&failed).unwrap().kind, FillEventKind::Failed);
        assert!(parse_wallet_fill(&serde_json::json!({"type": "orderAccepted", "orderId": "1"})).is_none());
    }

    #[test]
    fn settlements_only_failed_can_decrease() {
        let ev = |kind, sid: &str, sh| WalletFill { kind, order_id: "1".into(), settlement_id: sid.into(), shares: sh, price: None, platform_ts: None };
        let mut s = Settlements::default();
        assert_eq!(s.apply(&ev(FillEventKind::Submitted, "a", 2.0), 0.0), (2.0, 2.0, false));
        assert_eq!(s.apply(&ev(FillEventKind::Success, "a", 2.0), 2.0), (2.0, 2.0, false), "上链成功不重复记");
        assert_eq!(s.apply(&ev(FillEventKind::Submitted, "b", 3.0), 2.0), (5.0, 5.0, false));
        assert_eq!(s.apply(&ev(FillEventKind::Failed, "b", 3.0), 5.0), (2.0, 2.0, true), "失败扣回这一笔");
        assert_eq!(s.apply(&ev(FillEventKind::Failed, "b", 3.0), 2.0), (2.0, 2.0, false), "重复的失败不再扣");
        assert_eq!(s.apply(&ev(FillEventKind::Failed, "c", 1.0), 2.0), (2.0, 2.0, false), "没见过撮合的失败不扣");
        // 漏推:REST 已记 6 股(含钱包没推到的 a2),之后来了 b2 的已撮合:钱包累计 3 < 6,不能往下扣
        let mut s = Settlements::default();
        let (cum, target, dec) = s.apply(&ev(FillEventKind::Submitted, "b2", 3.0), 6.0);
        assert_eq!((cum, dec), (3.0, false));
        assert!(target < 6.0, "目标比已记少,但不允许扣 —— 记账保持 6");
        assert_eq!(s.apply(&ev(FillEventKind::Failed, "b2", 3.0), 6.0), (0.0, 3.0, true), "它失败了照样只扣它自己 3 股");
    }

    #[test]
    fn shares_wei() {
        assert!((parse_shares(&Value::String("15383999999999999999".into())).unwrap() - 15.384).abs() < 1e-9);
        assert_eq!(parse_shares(&Value::String("2".into())), Some(2.0));
        assert_eq!(parse_shares(&serde_json::json!(2.5)), Some(2.5));
        assert!((parse_price(&Value::String("220000000000000000".into())).unwrap() - 0.22).abs() < 1e-12);
    }
}
