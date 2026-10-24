//! 主循环:把行情 / 回报 / 定时事件喂给策略状态机,执行它产出的动作(--dry-run 只打印),并完整记日志。

use crate::api::{parse_market, parse_place_resp, parse_resolution, Api, MarketInfo, OrderDetail};
use crate::chain::{ctf_address, merge_call, redeem_call, Chain};
use crate::config::{Cli, Creds};
use crate::feeds::{binance_task, predict_task, FeedEv, PfCtrl};
use crate::jsonlog::{mono_ns, now, Log};
use crate::msgs::{parse_binance_book_ticker, parse_predict, wallet_event_hint, BookSnapshot, PredictMsg};
use crate::ratelimit::{Priority, RateLimiter};
use crate::risk::{Ledger, Outcome};
use crate::signing::{build_limit_order, cents_to_wei, exchange_address, random_salt, shares_to_wei, LimitOrderInput, OrderSigner, Side};
use crate::strategy::{Action, ErrorKind, Feed, Now, QSide, RoundCtx, Strategy};
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

const ROUND_MS: i64 = 300_000;

#[derive(Debug)]
enum Ev {
    PlaceDone { cid: u64, res: std::result::Result<String, (ErrorKind, String)> },
    CancelDone { order_id: String, res: std::result::Result<(), (ErrorKind, String)> },
    CancelAllDone { ok: Vec<String>, failed: Vec<(String, String)> },
    OpenOrders(Vec<OrderDetail>),
    Detail { order_id: String, detail: OrderDetail, after_cancel: bool },
    DetailFailed { order_id: String },
    Market { start_s: i64, info: MarketInfo },
    RestBook(BookSnapshot),
    Resolved { market_id: u64, payout_up: f64, payout_down: f64 },
    AuthDone(std::result::Result<String, String>),
}

struct Runner {
    cli: Cli,
    send: bool,
    log: Log,
    api: Arc<Api>,
    rl: Arc<Mutex<RateLimiter>>,
    strat: Strategy,
    signer: OrderSigner,
    chain: Option<Arc<Chain>>,
    state_path: std::path::PathBuf,
    /// market_id → 市场信息
    markets: HashMap<u64, MarketInfo>,
    /// 轮开始秒 → market_id
    by_start: BTreeMap<i64, u64>,
    ev_tx: mpsc::UnboundedSender<Ev>,
    pf_ctrl: mpsc::UnboundedSender<PfCtrl>,
    subscribed: BTreeSet<u64>,
    ws_book_seen: HashSet<u64>,
    /// 各市场最新 WS 盘口(下一轮提前订阅,切轮时立刻喂给策略)
    last_book: HashMap<u64, BookSnapshot>,
    rounds: Vec<RoundCtx>,
    ended: HashSet<u64>,
    rounds_done: u32,
    pending_cancel_all: usize,
    detail_inflight: HashSet<String>,
    exiting: Option<(String, i32)>,
    exit_started_ns: u64,
    last_reauth_ns: u64,
    wallet_topic: Option<String>,
    seen_429_ns: u64,
    seen_401_ns: u64,
}

fn order_side_label(q: QSide) -> &'static str {
    match q {
        QSide::UpBid => "买Up@买一",
        QSide::DownBid => "买Down@1-卖一",
    }
}

fn kind_of_status(status: u16) -> ErrorKind {
    match status {
        429 => ErrorKind::RateLimited,
        401 | 403 => ErrorKind::Auth,
        _ => ErrorKind::PlaceRejected,
    }
}

impl Runner {
    fn save_ledger(&mut self) {
        if self.strat.ledger_dirty {
            if let Err(e) = self.strat.ledger.save(&self.state_path) {
                self.log.ev("error", json!({"msg": format!("账本落盘失败: {e}")}));
            } else {
                self.strat.ledger_dirty = false;
            }
        }
    }

    fn with_rl<T>(&mut self, f: impl FnOnce(&mut Strategy, &mut RateLimiter) -> T) -> T {
        let rl = self.rl.clone();
        let mut g = rl.lock().unwrap();
        f(&mut self.strat, &mut g)
    }

    // ---------- 动作执行 ----------
    fn exec(&mut self, acts: Vec<Action>) {
        for a in acts {
            match a {
                Action::Place { cid, qside, market_id, price_cents, shares, reason, ctx } => self.exec_place(cid, qside, market_id, price_cents, shares, reason, ctx),
                Action::Cancel { cid, qside, order_id, reason, ctx } => {
                    self.log.ev(
                        "decision",
                        json!({"action": "cancel", "cid": cid, "qside": qside, "side": order_side_label(qside), "order_id": order_id, "reason": reason, "ctx": ctx}),
                    );
                    self.exec_cancel(order_id);
                }
                Action::CancelAll { reason, known } => {
                    self.log.ev("decision", json!({"action": "cancel_all", "reason": reason, "known": known}));
                    self.exec_cancel_all(known);
                }
                Action::Halt { reason } => {
                    self.log.ev("halt", json!({"reason": reason}));
                    if self.exiting.is_none() {
                        self.exiting = Some((format!("停机: {reason}"), 2));
                        self.exit_started_ns = mono_ns();
                    }
                }
                Action::Note { qside, msg, ctx } => {
                    self.log.ev("decision", json!({"action": "hold", "qside": qside, "reason": msg, "ctx": ctx}));
                }
            }
        }
        self.save_ledger();
    }

    #[allow(clippy::too_many_arguments)]
    fn exec_place(&mut self, cid: u64, qside: QSide, market_id: u64, price_cents: u32, shares: u64, reason: String, ctx: crate::strategy::DecisionCtx) {
        let Some(m) = self.markets.get(&market_id).cloned() else {
            let acts = self.with_rl(|s, rl| s.on_place_err(now(), cid, ErrorKind::Other, "未知市场", rl));
            return self.exec(acts);
        };
        let token = match qside.outcome() {
            Outcome::Up => m.up_token,
            Outcome::Down => m.down_token,
        };
        let input = LimitOrderInput {
            side: Side::Buy,
            token_id: token,
            price_per_share_wei: cents_to_wei(price_cents),
            quantity_wei: shares_to_wei(shares),
            fee_rate_bps: m.fee_rate_bps,
            salt: random_salt(),
            is_neg_risk: m.is_neg_risk,
            is_yield_bearing: m.is_yield_bearing,
        };
        let built = match build_limit_order(&self.signer, &input) {
            Ok(b) => b,
            Err(e) => {
                self.log.ev("error", json!({"msg": format!("签名失败: {e}"), "cid": cid}));
                let acts = self.with_rl(|s, rl| s.on_place_err(now(), cid, ErrorKind::Sign, &e.to_string(), rl));
                return self.exec(acts);
            }
        };
        let hash = format!("{:#x}", built.hash);
        let body = built.body_json(true);
        let summary = json!({"cid": cid, "qside": qside, "side": order_side_label(qside), "market_id": market_id, "outcome": format!("{:?}", qside.outcome()),
            "price": price_cents as f64 / 100.0, "shares": shares, "hash": hash, "token_id": token.to_string(), "maker_amount": built.amounts.maker_amount.to_string(),
            "taker_amount": built.amounts.taker_amount.to_string(), "fee_rate_bps": m.fee_rate_bps, "exchange": built.verifying_contract.to_checksum(None)});
        self.log.ev("decision", json!({"action": "place", "reason": reason, "ctx": ctx, "order": summary}));
        // 订单记录里先放好本地算出的 hash(= 平台订单 hash,用来 GET /v1/orders/<hash>)
        if let Some(r) = self.strat.orders.get_mut(&cid) {
            r.hash = Some(hash.clone());
        }
        let tx = self.ev_tx.clone();
        if !self.send {
            // 打印将要发的请求;签名不打印(拿到签名的人可以替你提交这张单)
            let mut shown: Value = serde_json::from_str(&body).unwrap();
            shown["data"]["order"]["signature"] = json!(format!("<{} 字节签名,已隐藏>", (built.signature.len() - 2) / 2));
            self.log.ev("dry_request", json!({"method": "POST", "path": "/v1/orders", "body": shown}));
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(35)).await;
                let _ = tx.send(Ev::PlaceDone { cid, res: Ok(format!("dry-{cid}")) });
            });
            return;
        }
        let api = self.api.clone();
        tokio::spawn(async move {
            let res = match api.place(body, summary).await {
                Ok(r) if r.ok() => match parse_place_resp(&r.json) {
                    Some((id, _)) => Ok(id),
                    None => Err((ErrorKind::PlaceRejected, format!("回包无 orderId: {}", r.json))),
                },
                Ok(r) => Err((kind_of_status(r.status), format!("{} {}", r.status, r.json))),
                Err(e) => Err((ErrorKind::Other, e.to_string())),
            };
            let _ = tx.send(Ev::PlaceDone { cid, res });
        });
    }

    fn exec_cancel(&mut self, order_id: String) {
        let tx = self.ev_tx.clone();
        if !self.send {
            self.log.ev("dry_request", json!({"method": "POST", "path": "/v1/orders/remove", "body": {"data": {"ids": [order_id]}}}));
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(18)).await;
                let _ = tx.send(Ev::CancelDone { order_id, res: Ok(()) });
            });
            return;
        }
        let api = self.api.clone();
        let hash = self.strat.order_by_id(&order_id).and_then(|o| o.hash.clone());
        tokio::spawn(async move {
            let r = api.cancel(std::slice::from_ref(&order_id), None).await;
            let res = match r {
                Ok(r) if r.ok() => Ok(()),
                Ok(r) => Err((if r.status == 429 { ErrorKind::RateLimited } else { ErrorKind::CancelFailed }, format!("{} {}", r.status, r.json))),
                Err(e) => Err((ErrorKind::CancelFailed, e.to_string())),
            };
            // 撤单失败:先查单。已经成交完/关闭的不算错误
            let res = match res {
                Err(e) => match &hash {
                    Some(h) => match api.order(h).await {
                        Ok(d) => {
                            let closed = d.open == Some(false);
                            let _ = tx.send(Ev::Detail { order_id: order_id.clone(), detail: d, after_cancel: true });
                            if closed {
                                Ok(())
                            } else {
                                Err(e)
                            }
                        }
                        Err(_) => Err(e),
                    },
                    None => Err(e),
                },
                ok => ok,
            };
            let succeeded = res.is_ok();
            let _ = tx.send(Ev::CancelDone { order_id: order_id.clone(), res });
            // 撤单成功后查一次单:撤单生效前被吃了多少(校准用)
            if succeeded {
                if let Some(h) = hash {
                    match api.order(&h).await {
                        Ok(d) => {
                            let _ = tx.send(Ev::Detail { order_id, detail: d, after_cancel: true });
                        }
                        Err(_) => {
                            let _ = tx.send(Ev::DetailFailed { order_id });
                        }
                    }
                }
            }
        });
    }

    fn exec_cancel_all(&mut self, known: Vec<String>) {
        self.pending_cancel_all += 1;
        let tx = self.ev_tx.clone();
        if !self.send {
            self.log
                .ev("dry_request", json!({"method": "POST", "path": "/v1/orders/remove", "body": {"data": {"ids": known}}, "note": "全撤:另会先 GET /v1/orders 列出账户所有挂单"}));
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(18)).await;
                let _ = tx.send(Ev::CancelAllDone { ok: known, failed: vec![] });
            });
            return;
        }
        let api = self.api.clone();
        let log = self.log.clone();
        tokio::spawn(async move {
            let (ok, failed) = cancel_all_sweep(&api, &log, known).await;
            let _ = tx.send(Ev::CancelAllDone { ok, failed });
        });
    }

    fn query_detail(&mut self, order_id: &str, after_cancel: bool) {
        if !self.send || self.detail_inflight.contains(order_id) {
            return;
        }
        let Some(hash) = self.strat.order_by_id(order_id).and_then(|o| o.hash.clone()) else { return };
        self.detail_inflight.insert(order_id.to_string());
        let (api, tx, oid) = (self.api.clone(), self.ev_tx.clone(), order_id.to_string());
        tokio::spawn(async move {
            match api.order(&hash).await {
                Ok(d) => {
                    let _ = tx.send(Ev::Detail { order_id: oid, detail: d, after_cancel });
                }
                Err(_) => {
                    let _ = tx.send(Ev::DetailFailed { order_id: oid });
                }
            }
        });
    }

    /// 记账 + 写成交日志
    #[allow(clippy::too_many_arguments)]
    fn record_fill(&mut self, t: Now, order_id: &str, cum: f64, source: &str, price: Option<f64>, counterparty: Option<String>, extra: Value) {
        let (delta, acts) = self.with_rl(|s, rl| s.on_fill(t, order_id, cum, rl));
        if delta > 0.0 {
            let o = self.strat.order_by_id(order_id).cloned();
            self.log.ev_at(
                t,
                "fill",
                json!({"order_id": order_id, "source": source, "delta_shares": delta, "cum_shares": cum,
                "order": o, "reported_price": price, "counterparty": counterparty, "bn_mid": self.strat.bn_mid(), "up_mid": self.strat.up_mid(), "extra": extra}),
            );
        }
        self.exec(acts);
    }

    // ---------- 事件 ----------
    fn on_feed(&mut self, ev: FeedEv) {
        match ev {
            FeedEv::Raw { feed: Feed::Binance, now: t, raw } => {
                self.log.raw(t, "bn", &raw);
                if let Some(bt) = parse_binance_book_ticker(&raw) {
                    if bt.symbol == "BTCUSDT" {
                        let acts = self.with_rl(|s, rl| s.on_binance(t, bt.mid(), rl));
                        self.exec(acts);
                    }
                }
            }
            FeedEv::Raw { feed: Feed::Predict, now: t, raw } => {
                self.log.raw(t, "pf", &raw);
                self.strat.feed_msg(t, Feed::Predict);
                match parse_predict(&raw) {
                    Some(PredictMsg::Book(b)) => {
                        self.ws_book_seen.insert(b.market_id);
                        self.last_book.insert(b.market_id, b.clone());
                        let acts = self.with_rl(|s, rl| s.on_book(t, b, rl));
                        self.exec(acts);
                    }
                    Some(PredictMsg::Wallet(d)) => self.on_wallet(t, d),
                    _ => {}
                }
            }
            FeedEv::Alive { feed, now: t } => self.strat.feed_msg(t, feed),
            FeedEv::State { feed, now: t, up, why } => {
                self.log.ev_at(t, "feed_state", json!({"feed": feed, "up": up, "why": why}));
                let acts = self.with_rl(|s, rl| s.on_feed_state(t, feed, up, rl));
                self.exec(acts);
            }
        }
    }

    fn on_wallet(&mut self, t: Now, d: Value) {
        let h = wallet_event_hint(&d);
        self.log.ev_at(t, "wallet_event", json!({"hint": format!("{h:?}")}));
        let mut ids: Vec<String> = h.order_ids.iter().filter(|i| self.strat.order_by_id(i).is_some()).cloned().collect();
        for hs in &h.order_hashes {
            if let Some(o) = self.strat.orders.values().find(|o| o.hash.as_deref() == Some(hs.as_str())) {
                if let Some(id) = &o.order_id {
                    ids.push(id.clone());
                }
            }
        }
        ids.sort();
        ids.dedup();
        for id in ids {
            // 钱包事件里的数量按"累计成交"用(只增不减,不会重复记账);同时用 REST 查单核实
            if let Some(f) = h.amount_filled {
                self.record_fill(t, &id, f, "wallet", h.price, h.counterparty.clone(), json!({"kind": h.kind}));
            }
            self.query_detail(&id, false);
        }
    }

    fn on_ev(&mut self, ev: Ev) {
        let t = now();
        match ev {
            Ev::PlaceDone { cid, res } => {
                self.log.ev_at(t, "ack", json!({"cid": cid, "ok": res.is_ok(), "order_id": res.as_ref().ok(), "error": res.as_ref().err().map(|e| format!("{:?} {}", e.0, e.1))}));
                let acts = match res {
                    Ok(id) => {
                        let hash = self.strat.orders.get(&cid).and_then(|o| o.hash.clone());
                        self.with_rl(|s, rl| s.on_place_ok(t, cid, id, hash, rl))
                    }
                    Err((k, msg)) => {
                        if k == ErrorKind::Auth {
                            self.reauth();
                        }
                        self.with_rl(|s, rl| s.on_place_err(t, cid, k, &msg, rl))
                    }
                };
                self.exec(acts);
            }
            Ev::CancelDone { order_id, res } => {
                self.log.ev_at(t, "cancel_ack", json!({"order_id": order_id, "ok": res.is_ok(), "error": res.as_ref().err().map(|e| format!("{:?} {}", e.0, e.1))}));
                let acts = match res {
                    Ok(()) => self.with_rl(|s, rl| s.on_cancel_ok(t, &order_id, rl)),
                    Err((k, msg)) => {
                        let mut a = self.with_rl(|s, rl| s.on_cancel_err(t, &order_id, &msg, rl));
                        if k == ErrorKind::RateLimited {
                            a.extend(self.with_rl(|s, rl| s.on_error(t, ErrorKind::RateLimited, &msg, rl)));
                        }
                        a
                    }
                };
                self.exec(acts);
            }
            Ev::CancelAllDone { ok, failed } => {
                self.pending_cancel_all = self.pending_cancel_all.saturating_sub(1);
                self.log.ev_at(t, "cancel_all_done", json!({"ok": ok, "failed": failed}));
                for id in &ok {
                    let acts = self.with_rl(|s, rl| s.on_cancel_ok(t, id, rl));
                    self.exec(acts);
                    if self.strat.order_by_id(id).is_some() {
                        self.query_detail(id, true);
                    }
                }
                for (id, msg) in &failed {
                    let acts = self.with_rl(|s, rl| s.on_cancel_err(t, id, msg, rl));
                    self.exec(acts);
                }
            }
            Ev::OpenOrders(list) => self.on_open_orders(t, list),
            Ev::Detail { order_id, detail, after_cancel } => {
                self.detail_inflight.remove(&order_id);
                self.log.ev_at(
                    t,
                    "order_detail",
                    json!({"order_id": order_id, "after_cancel": after_cancel, "filled": detail.filled, "status": detail.status, "open": detail.open}),
                );
                if let Some(f) = detail.filled {
                    self.record_fill(t, &order_id, f, if after_cancel { "rest_after_cancel" } else { "rest_detail" }, None, None, json!({"status": detail.status}));
                } else if detail.open == Some(false) && !after_cancel && !self.strat.is_cancelling(&order_id) && self.strat.order_by_id(&order_id).is_some_and(|o| !o.closed) {
                    // 不是我们撤的、已不在挂、又读不出成交量:按全部成交记(保守,宁可多算库存)
                    let sh = self.strat.order_by_id(&order_id).map(|o| o.shares as f64).unwrap_or(0.0);
                    self.record_fill(t, &order_id, sh, "rest_assumed", None, None, json!({"status": detail.status}));
                }
                if detail.open == Some(false) && !self.strat.is_cancelling(&order_id) {
                    let acts = self.with_rl(|s, rl| s.on_order_closed(t, &order_id, rl));
                    self.exec(acts);
                }
            }
            Ev::DetailFailed { order_id } => {
                self.detail_inflight.remove(&order_id);
            }
            Ev::Market { start_s, info } => {
                if !self.markets.contains_key(&info.id) {
                    let ex = exchange_address(info.is_neg_risk, info.is_yield_bearing);
                    self.log.ev_at(
                        t,
                        "market",
                        json!({"start_s": start_s, "market_id": info.id, "up_token": info.up_token.to_string(), "down_token": info.down_token.to_string(),
                        "fee_rate_bps": info.fee_rate_bps, "is_neg_risk": info.is_neg_risk, "is_yield_bearing": info.is_yield_bearing, "exchange": ex.to_checksum(None),
                        "condition_id": info.condition_id.map(|c| format!("{c:#x}")), "status": info.status, "trading_status": info.trading_status}),
                    );
                    if ex != crate::signing::addr::CTF_EXCHANGE {
                        self.log.ev_at(t, "warn", json!({"msg": "该市场不在 CTFExchange(规格说 BTC 5m 都在这里),按市场信息签名", "market_id": info.id}));
                    }
                    self.by_start.insert(start_s, info.id);
                    self.markets.insert(info.id, info);
                }
            }
            Ev::RestBook(b) => {
                // REST 快照只在还没收到 WS 盘口时用
                if !self.ws_book_seen.contains(&b.market_id) {
                    self.log.ev_at(t, "pf_rest", json!({"market_id": b.market_id, "bids": b.bids, "asks": b.asks}));
                    let acts = self.with_rl(|s, rl| s.on_book(t, b, rl));
                    self.exec(acts);
                }
            }
            Ev::Resolved { market_id, payout_up, payout_down } => {
                let pos = self.strat.ledger.round(market_id).cloned();
                let pnl = self.strat.on_resolution(market_id, payout_up, payout_down);
                self.log.ev_at(
                    t,
                    "settle",
                    json!({"market_id": market_id, "payout_up": payout_up, "payout_down": payout_down, "position": pos, "pnl": pnl,
                    "realized_total": self.strat.ledger.realized_total, "day_pnl": self.strat.ledger.day_pnl(t.wall_ms, None)}),
                );
                self.save_ledger();
            }
            Ev::AuthDone(r) => match r {
                Ok(jwt) => {
                    self.log.ev_at(t, "info", json!({"msg": "重新鉴权成功"}));
                    let topic = format!("predictWalletEvents/{jwt}");
                    if let Some(old) = self.wallet_topic.replace(topic.clone()) {
                        let _ = self.pf_ctrl.send(PfCtrl::Unsubscribe(old));
                    }
                    let _ = self.pf_ctrl.send(PfCtrl::Subscribe(topic));
                }
                Err(e) => self.log.ev_at(t, "error", json!({"msg": format!("重新鉴权失败: {e}")})),
            },
        }
    }

    fn on_open_orders(&mut self, t: Now, list: Vec<OrderDetail>) {
        let open_ids: HashSet<String> = list.iter().filter(|o| o.open != Some(false)).filter_map(|o| o.id.clone()).collect();
        let open_hashes: HashSet<String> = list.iter().filter(|o| o.open != Some(false)).filter_map(|o| o.hash.clone()).collect();
        for o in &list {
            let id = o.id.clone().or_else(|| o.hash.as_ref().and_then(|h| self.strat.orders.values().find(|r| r.hash.as_deref() == Some(h)).and_then(|r| r.order_id.clone())));
            if let (Some(id), Some(f)) = (id, o.filled) {
                if f > 0.0 && self.strat.order_by_id(&id).is_some() {
                    self.record_fill(t, &id, f, "rest_list", None, None, Value::Null);
                }
            }
        }
        // 我们以为还挂着、列表里却没有的单 → 查单
        let missing: Vec<String> = self
            .strat
            .orders
            .values()
            .filter(|o| !o.closed && o.acked_ns.is_some_and(|a| t.mono_ns > a + 3_000_000_000))
            .filter_map(|o| o.order_id.clone().map(|id| (id, o.hash.clone())))
            .filter(|(id, h)| !open_ids.contains(id) && !h.as_ref().is_some_and(|h| open_hashes.contains(h)))
            .map(|(id, _)| id)
            .filter(|id| !self.strat.is_cancelling(id))
            .collect();
        for id in missing {
            self.query_detail(&id, false);
        }
    }

    fn reauth(&mut self) {
        let n = mono_ns();
        if !self.send || n < self.last_reauth_ns + 30_000_000_000 {
            return;
        }
        self.last_reauth_ns = n;
        let (api, signer, tx) = (self.api.clone(), self.signer.clone(), self.ev_tx.clone());
        tokio::spawn(async move {
            let r = api.auth(&signer).await.map_err(|e| e.to_string());
            let _ = tx.send(Ev::AuthDone(r));
        });
    }

    /// 定时:轮次切换、订阅、收盘处理
    fn on_tick(&mut self) {
        let t = now();
        // 任何接口(含查询)收到 429 → 全撤冷却;401 → 重新鉴权
        let n429 = self.api.last_429_ns.load(std::sync::atomic::Ordering::Relaxed);
        if n429 > self.seen_429_ns {
            self.seen_429_ns = n429;
            let acts = self.with_rl(|s, rl| s.on_error(t, ErrorKind::RateLimited, "某接口返回 429", rl));
            self.exec(acts);
        }
        let n401 = self.api.last_401_ns.load(std::sync::atomic::Ordering::Relaxed);
        if n401 > self.seen_401_ns {
            self.seen_401_ns = n401;
            let acts = self.with_rl(|s, rl| s.on_error(t, ErrorKind::Auth, "某接口返回 401", rl));
            self.exec(acts);
            self.reauth();
        }
        let cur_start = t.wall_ms.div_euclid(ROUND_MS) * ROUND_MS / 1000;
        // 订阅当前轮和下一轮的盘口(下一轮开盘前就有数据)
        for s in [cur_start, cur_start + 300] {
            if let Some(&id) = self.by_start.get(&s) {
                if self.subscribed.insert(id) {
                    let _ = self.pf_ctrl.send(PfCtrl::Subscribe(format!("predictOrderbook/{id}")));
                }
            }
        }
        // 当前轮切换
        if let Some(&id) = self.by_start.get(&cur_start) {
            if self.strat.round().map(|r| r.market_id) != Some(id) && self.exiting.is_none() {
                let ctx = RoundCtx { market_id: id, start_ms: cur_start * 1000, end_ms: cur_start * 1000 + ROUND_MS };
                self.log.ev_at(t, "round_start", json!({"round": ctx}));
                self.rounds.push(ctx.clone());
                let acts = self.with_rl(|s, rl| s.set_round(t, ctx, rl));
                self.exec(acts);
                if let Some(b) = self.last_book.get(&id).cloned() {
                    let acts = self.with_rl(|s, rl| s.on_book(t, b, rl));
                    self.exec(acts);
                }
                let keep = self.subscribed.clone();
                self.last_book.retain(|m, _| keep.contains(m));
                let (api, tx) = (self.api.clone(), self.ev_tx.clone());
                tokio::spawn(async move {
                    if let Ok(b) = api.orderbook(id).await {
                        let _ = tx.send(Ev::RestBook(b));
                    }
                });
            }
        }
        // 收盘
        let ended: Vec<RoundCtx> = self.rounds.iter().filter(|r| t.wall_ms >= r.end_ms && !self.ended.contains(&r.market_id)).cloned().collect();
        for r in ended {
            self.ended.insert(r.market_id);
            self.rounds_done += 1;
            let pos = self.strat.ledger.round(r.market_id).cloned();
            let orders: Vec<_> = self.strat.orders.values().filter(|o| o.market_id == r.market_id).cloned().collect();
            self.log.ev_at(t, "round_end", json!({"round": r, "position": pos, "orders": orders}));
            // 退订旧盘口
            if self.subscribed.remove(&r.market_id) {
                let _ = self.pf_ctrl.send(PfCtrl::Unsubscribe(format!("predictOrderbook/{}", r.market_id)));
            }
            self.spawn_settlement(r.market_id, pos);
            self.strat.ledger.prune_empty(t.wall_ms);
            self.strat.ledger_dirty = true;
            if self.cli.max_rounds > 0 && self.rounds_done >= self.cli.max_rounds && self.exiting.is_none() {
                self.exiting = Some((format!("已跑满 {} 轮", self.rounds_done), 0));
                self.exit_started_ns = mono_ns();
            }
        }
        let acts = self.with_rl(|s, rl| s.on_tick(t, rl));
        self.exec(acts);
    }

    /// 结算:先 merge(两边都有就合并),等出结果后 redeem 赢的一边。每个链上动作最多发一次交易。
    fn spawn_settlement(&self, market_id: u64, pos: Option<crate::risk::RoundPos>) {
        let Some(info) = self.markets.get(&market_id).cloned() else { return };
        let (api, tx, log, chain, send) = (self.api.clone(), self.ev_tx.clone(), self.log.clone(), self.chain.clone(), self.send);
        let had_position = pos.is_some_and(|p| p.up_shares > 0.0 || p.down_shares > 0.0);
        tokio::spawn(async move {
            if let (Some(chain), Some(cond)) = (&chain, info.condition_id) {
                if send && had_position {
                    // 成交在链上结算有延迟(盘口里的 settlementsPending),等 60 秒再查余额合并
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    settle_merge(chain, &info, cond, &log).await;
                }
            } else if had_position {
                log.ev("chain", json!({"market_id": market_id, "msg": if send { "缺 conditionId 或未启用 claim,跳过 merge/redeem" } else { "dry-run:不做链上操作" }}));
            }
            // 等结果:每 60 秒查一次,最多 2 小时
            let mut payouts = None;
            for _ in 0..120 {
                tokio::time::sleep(Duration::from_secs(60)).await;
                if let Ok(d) = api.market(market_id).await {
                    if let Some(p) = parse_resolution(&d) {
                        payouts = Some(p);
                        break;
                    }
                }
            }
            let Some((pu, pd)) = payouts else {
                log.ev("warn", json!({"market_id": market_id, "msg": "2 小时内没查到结算结果;账本继续按最坏结果估值"}));
                return;
            };
            let _ = tx.send(Ev::Resolved { market_id, payout_up: pu, payout_down: pd });
            if let (Some(chain), Some(cond), true, true) = (&chain, info.condition_id, send, had_position) {
                settle_redeem(chain, &info, cond, pu, pd, &log).await;
            }
        });
    }
}

async fn settle_merge(chain: &Chain, info: &MarketInfo, cond: alloy_primitives::B256, log: &Log) {
    let ctf = ctf_address(info.is_neg_risk, info.is_yield_bearing);
    let (u, d) = match (chain.token_balance(ctf, info.up_token).await, chain.token_balance(ctf, info.down_token).await) {
        (Ok(u), Ok(d)) => (u, d),
        (a, b) => {
            log.ev("chain", json!({"market_id": info.id, "action": "merge", "error": format!("查余额失败 {:?} {:?}", a.err(), b.err())}));
            return;
        }
    };
    let amt = u.min(d);
    log.ev("chain", json!({"market_id": info.id, "action": "balances", "up": u.to_string(), "down": d.to_string()}));
    if amt.is_zero() {
        return;
    }
    let (target, inner) = merge_call(cond, amt, info.is_neg_risk, info.is_yield_bearing);
    chain_exec(chain, "merge", info.id, target, &inner, log).await;
}

async fn settle_redeem(chain: &Chain, info: &MarketInfo, cond: alloy_primitives::B256, pu: f64, pd: f64, log: &Log) {
    let ctf = ctf_address(info.is_neg_risk, info.is_yield_bearing);
    for (won, token, idx) in [(pu > 0.0, info.up_token, info.up_index_set), (pd > 0.0, info.down_token, info.down_index_set)] {
        if !won {
            continue;
        }
        let bal = match chain.token_balance(ctf, token).await {
            Ok(b) => b,
            Err(e) => {
                log.ev("chain", json!({"market_id": info.id, "action": "redeem", "error": format!("查余额失败 {e}")}));
                continue;
            }
        };
        if bal.is_zero() {
            continue;
        }
        let (target, inner) = redeem_call(cond, idx, bal, info.is_neg_risk, info.is_yield_bearing);
        chain_exec(chain, &format!("redeem/{idx}"), info.id, target, &inner, log).await;
    }
}

/// 只尝试一次:估算失败不发;发出后等回执,失败只记日志,不重试
async fn chain_exec(chain: &Chain, action: &str, market_id: u64, target: alloy_primitives::Address, inner: &[u8], log: &Log) {
    match chain.execute_and_wait(target, inner).await {
        Ok((gas, h, st)) => log.ev("chain", json!({"market_id": market_id, "action": action, "gas_estimate": gas, "tx": h.map(|h| format!("{h:#x}")), "receipt_status": st})),
        Err(e) => log.ev("chain", json!({"market_id": market_id, "action": action, "error": format!("{e:#}")})),
    }
}

/// 撤掉账户所有挂单:已知的 + GET /v1/orders 列出来的;每批 ≤ 100;失败退避重试(最多 8 次)
async fn cancel_all_sweep(api: &Api, log: &Log, known: Vec<String>) -> (Vec<String>, Vec<(String, String)>) {
    let mut ids: BTreeSet<String> = known.into_iter().collect();
    match api.open_orders(Priority::Cancel).await {
        Ok(list) => {
            for o in list {
                if o.open != Some(false) {
                    if let Some(id) = o.id {
                        ids.insert(id);
                    }
                }
            }
        }
        Err(e) => log.ev("error", json!({"msg": format!("全撤:列挂单失败 {e}")})),
    }
    let ids: Vec<String> = ids.into_iter().collect();
    let mut ok = vec![];
    let mut failed = vec![];
    for chunk in ids.chunks(100) {
        let mut wait = 1u64;
        let mut last = String::new();
        let mut done = false;
        for _ in 0..8 {
            match api.cancel(chunk, Some(Priority::Cancel)).await {
                Ok(r) if r.ok() => {
                    done = true;
                    break;
                }
                Ok(r) => last = format!("{} {}", r.status, r.json),
                Err(e) => last = e.to_string(),
            }
            tokio::time::sleep(Duration::from_secs(wait)).await;
            wait = (wait * 2).min(30);
        }
        if done {
            ok.extend(chunk.iter().cloned());
        } else {
            failed.extend(chunk.iter().map(|i| (i.clone(), last.clone())));
        }
    }
    (ok, failed)
}

/// 市场管理:提前取当前轮和下一轮的市场信息(每轮开始前约 30 分钟开放)
async fn market_task(api: Arc<Api>, tx: mpsc::UnboundedSender<Ev>, log: Log) {
    let mut have: HashSet<i64> = HashSet::new();
    loop {
        let cur = crate::jsonlog::wall_ms().div_euclid(ROUND_MS) * ROUND_MS / 1000;
        for s in [cur, cur + 300] {
            if have.contains(&s) {
                continue;
            }
            let r: Result<Option<MarketInfo>> = async {
                let Some(id) = api.category_market_id(s).await? else { return Ok(None) };
                let d = api.market(id).await?;
                Ok(Some(parse_market(&d).with_context(|| format!("市场 {id} 信息不完整"))?))
            }
            .await;
            match r {
                Ok(Some(info)) => {
                    have.insert(s);
                    let _ = tx.send(Ev::Market { start_s: s, info });
                }
                Ok(None) => {}
                Err(e) => log.ev("error", json!({"msg": format!("取市场 btc-updown-5m-{s} 失败: {e:#}")})),
            }
        }
        have.retain(|&s| s >= cur - 600);
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// REST 查挂单兜底(钱包事件格式未知,不能只靠它)
async fn poll_task(api: Arc<Api>, tx: mpsc::UnboundedSender<Ev>, every: Duration) {
    loop {
        tokio::time::sleep(every).await;
        if let Ok(list) = api.open_orders(Priority::Query).await {
            let _ = tx.send(Ev::OpenOrders(list));
        }
    }
}

pub async fn run(cli: Cli, creds: Creds) -> Result<i32> {
    cli.validate(&creds)?;
    let send = cli.is_send();
    let (log, log_thread) = Log::start(cli.log_dir.clone(), "probe", !cli.quiet)?;
    for s in [&creds.api_key, &creds.private_key].into_iter().flatten() {
        log.add_secret(s);
        log.add_secret(s.trim_start_matches("0x"));
    }
    // RPC 地址里常带服务商 key(报错信息会带 URL)
    log.add_secret(&cli.rpc_url);
    let code = match run_inner(cli, creds, send, log.clone()).await {
        Ok(c) => c,
        Err(e) => {
            log.ev("fatal", json!({"msg": format!("{e:#}")}));
            eprintln!("致命错误: {}", log.redact_str(&format!("{e:#}")));
            1
        }
    };
    log.close();
    let _ = log_thread.join();
    Ok(code)
}

async fn run_inner(cli: Cli, creds: Creds, send: bool, log: Log) -> Result<i32> {
    let state_path = cli.state_path();
    let mut ledger = Ledger::load(&state_path).with_context(|| format!("读账本 {state_path:?} 失败"))?;
    ledger.day_offset_hours = cli.day_offset_hours;
    if let Some(h) = ledger.halted.clone() {
        if cli.clear_halt {
            log.ev("info", json!({"msg": format!("清除停机标记: {h}")}));
            ledger.halted = None;
        } else if send {
            bail!("账本里有停机标记「{h}」,拒绝启动。确认后用 --clear-halt 清除(亏损超限仍会立刻再停)");
        } else {
            ledger.halted = None;
        }
    }
    let signer = match (&creds.private_key, send) {
        (Some(pk), _) => OrderSigner::new(pk, creds.account.as_deref())?,
        (None, true) => bail!("--send 需要 PREDICT_PRIVATE_KEY"),
        // dry-run 没私钥:用临时随机私钥走一遍签名流程(签出来的单不会发出去)
        (None, false) => OrderSigner::random(Some(creds.account.as_deref().unwrap_or("0x000000000000000000000000000000000000dEaD")))?,
    };
    log.ev(
        "start",
        json!({"mode": if send { "send" } else { "dry-run" }, "version": env!("CARGO_PKG_VERSION"), "sdk_compat": "@predictdotfun/sdk 1.3.8",
        "signer": signer.signer_address().to_checksum(None), "predict_account": signer.predict_account().map(|a| a.to_checksum(None)),
        "ephemeral_key": creds.private_key.is_none(), "has_api_key": creds.api_key.is_some(), "params": format!("{:?}", cli.params()), "rate": format!("{:?}", cli.rate()),
        "state_file": state_path, "api_base": cli.api_base, "binance_ws": cli.binance_ws}),
    );

    let http = reqwest::Client::builder().tcp_nodelay(true).pool_idle_timeout(Duration::from_secs(90)).build()?;
    let rl = Arc::new(Mutex::new(RateLimiter::new(cli.rate())));
    let api = Arc::new(Api::new(http.clone(), cli.api_base.clone(), creds.api_key.clone(), log.clone(), rl.clone()));
    let chain = if send { Some(Arc::new(Chain::new(http.clone(), cli.rpc_url.clone(), signer.clone())?)) } else { None };

    // --send 启动检查:存款地址 owner = 签名 EOA(SDK OrderBuilder.make 同样检查)、鉴权、全撤
    let mut wallet_topic = None;
    if send {
        let chain = chain.as_ref().unwrap();
        let owner = chain.account_owner().await.context("查存款地址 owner 失败(RPC 不通?);为安全起见不启动")?;
        if owner != signer.signer_address() {
            bail!("存款地址的 owner {owner} 不是私钥对应的地址 {}(SDK 的 InvalidSignerError)", signer.signer_address());
        }
        let jwt = api.auth(&signer).await.context("鉴权失败")?;
        wallet_topic = Some(format!("predictWalletEvents/{jwt}"));
        log.ev("info", json!({"msg": "鉴权成功;启动时全撤"}));
        let (ok, failed) = cancel_all_sweep(&api, &log, vec![]).await;
        log.ev("cancel_all_done", json!({"phase": "startup", "ok": ok, "failed": failed}));
        if !failed.is_empty() {
            bail!("启动时全撤失败 {} 张,不启动", failed.len());
        }
    }
    let chain = if cli.no_claim { None } else { chain };

    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<Ev>();
    let (feed_tx, mut feed_rx) = mpsc::unbounded_channel::<FeedEv>();
    let (pf_ctrl, pf_ctrl_rx) = mpsc::unbounded_channel::<PfCtrl>();
    if let Some(t) = &wallet_topic {
        let _ = pf_ctrl.send(PfCtrl::Subscribe(t.clone()));
    }
    let pf_url = match &creds.api_key {
        Some(k) if send => format!("{}?apiKey={}", cli.predict_ws, k),
        _ => cli.predict_ws.clone(),
    };
    tokio::spawn(binance_task(cli.binance_ws.clone(), feed_tx.clone(), log.clone()));
    tokio::spawn(predict_task(pf_url, Some("https://predict.fun".into()), pf_ctrl_rx, feed_tx.clone(), log.clone()));
    tokio::spawn(market_task(api.clone(), ev_tx.clone(), log.clone()));
    if send {
        tokio::spawn(poll_task(api.clone(), ev_tx.clone(), Duration::from_millis(cli.poll_ms)));
    }

    let mut r = Runner {
        strat: Strategy::new(cli.params(), ledger),
        send,
        log: log.clone(),
        api,
        rl,
        signer,
        chain,
        state_path,
        markets: HashMap::new(),
        by_start: BTreeMap::new(),
        ev_tx,
        pf_ctrl,
        subscribed: BTreeSet::new(),
        ws_book_seen: HashSet::new(),
        last_book: HashMap::new(),
        rounds: vec![],
        ended: HashSet::new(),
        rounds_done: 0,
        pending_cancel_all: 0,
        detail_inflight: HashSet::new(),
        exiting: None,
        exit_started_ns: 0,
        last_reauth_ns: 0,
        wallet_topic,
        seen_429_ns: 0,
        seen_401_ns: 0,
        cli,
    };
    let acts = r.with_rl(|s, rl| s.set_ready(now(), rl));
    r.exec(acts);

    let deadline = (r.cli.duration_s > 0).then(|| tokio::time::Instant::now() + Duration::from_secs(r.cli.duration_s));
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut shutdown_issued = false;
    loop {
        tokio::select! {
            Some(ev) = feed_rx.recv() => r.on_feed(ev),
            Some(ev) = ev_rx.recv() => r.on_ev(ev),
            _ = tick.tick() => r.on_tick(),
            _ = tokio::signal::ctrl_c() => {
                if r.exiting.is_none() { r.exiting = Some(("收到 Ctrl-C".into(), 0)); r.exit_started_ns = mono_ns(); }
            }
            _ = sigterm.recv() => {
                if r.exiting.is_none() { r.exiting = Some(("收到 SIGTERM".into(), 0)); r.exit_started_ns = mono_ns(); }
            }
            _ = async { match deadline { Some(d) => tokio::time::sleep_until(d).await, None => std::future::pending().await } } => {
                if r.exiting.is_none() { r.exiting = Some((format!("运行满 {} 秒", r.cli.duration_s), 0)); r.exit_started_ns = mono_ns(); }
            }
        }
        if let Some((reason, code)) = r.exiting.clone() {
            if !shutdown_issued {
                shutdown_issued = true;
                r.log.ev("info", json!({"msg": format!("退出中: {reason};全撤")}));
                let acts = r.strat.shutdown(&reason);
                r.exec(acts);
            }
            let waited = mono_ns().saturating_sub(r.exit_started_ns);
            if r.pending_cancel_all == 0 || waited > 20_000_000_000 {
                // 退出前再全撤一次兜底(--send)
                if r.send {
                    let (ok, failed) = cancel_all_sweep(&r.api, &r.log, r.strat.known_open_order_ids()).await;
                    r.log.ev("cancel_all_done", json!({"phase": "exit", "ok": ok, "failed": failed}));
                }
                r.strat.ledger_dirty = true;
                r.save_ledger();
                r.log.ev("exit", json!({"reason": reason, "code": code, "rounds": r.rounds_done, "ledger": r.strat.ledger}));
                return Ok(code);
            }
        }
    }
}
