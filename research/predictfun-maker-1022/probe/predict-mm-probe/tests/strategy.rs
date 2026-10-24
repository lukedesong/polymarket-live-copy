//! 策略单测:把"录制格式"的原始消息(pf = Predict 盘口推送原文,bn = 币安 bookTicker 原文)
//! 逐条过解析器喂给状态机,检查动作。不连网。
use predict_mm_probe::msgs::{parse_binance_book_ticker, parse_predict, PredictMsg};
use predict_mm_probe::ratelimit::{Priority, RateLimitConfig, RateLimiter};
use predict_mm_probe::risk::{Ledger, Outcome};
use predict_mm_probe::strategy::*;

const MKT: u64 = 3698227;
const START_MS: i64 = 1_792_818_000_000;
const END_MS: i64 = START_MS + 300_000;

struct Sim {
    s: Strategy,
    rl: RateLimiter,
    now: Now,
    next_oid: u64,
}

fn pf(bids: &[(f64, f64)], asks: &[(f64, f64)]) -> String {
    serde_json::json!({"type":"M","topic":format!("predictOrderbook/{MKT}"),"data":{
        "asks": asks.iter().map(|l| [l.0, l.1]).collect::<Vec<_>>(),
        "bids": bids.iter().map(|l| [l.0, l.1]).collect::<Vec<_>>(),
        "marketId": MKT, "orderCount": 10, "settlementsPending": {"asks": [], "bids": []},
        "updateTimestampMs": 0, "version": 1}})
    .to_string()
}

fn bn(mid: f64) -> String {
    // 价差 0.01,mid 精确等于给定值
    format!(r#"{{"stream":"btcusdt@bookTicker","data":{{"u":1,"s":"BTCUSDT","b":"{:.3}","B":"1.0","a":"{:.3}","A":"1.0"}}}}"#, mid - 0.005, mid + 0.005)
}

impl Sim {
    fn new(p: Params, ledger: Ledger) -> Self {
        let mut s =
            Self { s: Strategy::new(p, ledger), rl: RateLimiter::new(RateLimitConfig::default()), now: Now { mono_ns: 10_000_000_000, wall_ms: START_MS + 1_000 }, next_oid: 100 };
        let now = s.now;
        s.s.on_feed_state(now, Feed::Binance, true, &mut s.rl);
        s.s.on_feed_state(now, Feed::Predict, true, &mut s.rl);
        s.s.set_round(now, RoundCtx { market_id: MKT, start_ms: START_MS, end_ms: END_MS }, &mut s.rl);
        s.s.set_ready(now, &mut s.rl);
        s
    }
    fn default() -> Self {
        Self::new(Params::default(), Ledger::default())
    }
    /// 推进时间(两条消息之间),期间 Predict 有心跳、币安有消息(价不变),保证连接健康
    fn advance(&mut self, ms: u64) {
        self.now = self.now.plus_ms(ms);
        self.s.feed_msg(self.now, Feed::Predict);
        self.s.feed_msg(self.now, Feed::Binance);
    }
    fn line(&mut self, src: &str, raw: &str) -> Vec<Action> {
        match src {
            "pf" => match parse_predict(raw).expect("pf 解析失败") {
                PredictMsg::Book(b) => {
                    self.s.feed_msg(self.now, Feed::Predict);
                    self.s.on_book(self.now, b, &mut self.rl)
                }
                _ => vec![],
            },
            "bn" => {
                let t = parse_binance_book_ticker(raw).expect("bn 解析失败");
                self.s.on_binance(self.now, t.mid(), &mut self.rl)
            }
            _ => unreachable!(),
        }
    }
    fn tick(&mut self) -> Vec<Action> {
        self.s.feed_msg(self.now, Feed::Binance);
        self.s.on_tick(self.now, &mut self.rl)
    }
    /// 把动作里的挂单/撤单全部回报成功(模拟交易所 35ms / 18ms 回报)
    fn ack_all(&mut self, acts: &[Action]) -> Vec<Action> {
        let mut out = vec![];
        for a in acts {
            match a {
                Action::Place { cid, .. } => {
                    self.next_oid += 1;
                    let oid = self.next_oid.to_string();
                    out.extend(self.s.on_place_ok(self.now, *cid, oid, None, &mut self.rl));
                }
                Action::Cancel { order_id, .. } => out.extend(self.s.on_cancel_ok(self.now, order_id, &mut self.rl)),
                Action::CancelAll { known, .. } => {
                    for k in known.clone() {
                        out.extend(self.s.on_cancel_ok(self.now, &k, &mut self.rl));
                    }
                }
                _ => {}
            }
        }
        out
    }
    fn live_order(&self, side: QSide) -> Option<(String, u32)> {
        self.s.orders.values().filter(|o| o.qside == side && !o.closed && o.order_id.is_some()).map(|o| (o.order_id.clone().unwrap(), o.price_cents)).max()
    }
}

fn places(a: &[Action]) -> Vec<(QSide, u32, u64)> {
    a.iter().filter_map(|x| if let Action::Place { qside, price_cents, shares, .. } = x { Some((*qside, *price_cents, *shares)) } else { None }).collect()
}
fn cancels(a: &[Action]) -> Vec<(QSide, String)> {
    a.iter().filter_map(|x| if let Action::Cancel { qside, reason, .. } = x { Some((*qside, reason.clone())) } else { None }).collect()
}
fn has_cancel_all(a: &[Action]) -> bool {
    a.iter().any(|x| matches!(x, Action::CancelAll { .. }))
}
fn has_halt(a: &[Action]) -> bool {
    a.iter().any(|x| matches!(x, Action::Halt { .. }))
}

/// 开盘、盘口 0.50/0.55、两侧挂上并回报
fn quoted() -> Sim {
    let mut sim = Sim::default();
    sim.line("bn", &bn(80880.0));
    let a = sim.line("pf", &pf(&[(0.5, 80.0), (0.4, 10.0)], &[(0.55, 30.0), (0.6, 5.0)]));
    let p = places(&a);
    assert_eq!(p, vec![(QSide::UpBid, 50, 2), (QSide::DownBid, 45, 3)], "{a:?}");
    // 排队位置写进决策上下文
    let q: Vec<_> = a.iter().filter_map(|x| if let Action::Place { ctx, .. } = x { ctx.queue_ahead } else { None }).collect();
    assert_eq!(q, vec![80.0, 30.0]);
    sim.advance(35);
    sim.ack_all(&a);
    sim
}

#[test]
fn reprice_after_best_moves_and_one_second() {
    let mut sim = quoted();
    // 有人抢到 0.51 买一;挂单才 0.3s,不改
    sim.advance(300);
    let a = sim.line("pf", &pf(&[(0.51, 5.0), (0.5, 82.0)], &[(0.55, 30.0)]));
    assert!(cancels(&a).is_empty(), "{a:?}");
    // 满 1s:撤旧(只撤 Up 买侧,Down 侧价没变)
    sim.advance(700);
    let a = sim.tick();
    let c = cancels(&a);
    assert_eq!(c.len(), 1);
    assert_eq!(c[0].0, QSide::UpBid);
    assert!(c[0].1.contains("改价 50→51"));
    assert!(places(&a).is_empty(), "撤单确认前不挂新:{a:?}");
    // 撤单确认 → 挂新价
    sim.advance(18);
    let a = sim.ack_all(&a);
    assert_eq!(places(&a), vec![(QSide::UpBid, 51, 2)]);
    // 卖一移动:Down 侧目标 = 1 − 卖一
    sim.advance(35);
    sim.ack_all(&a);
    sim.advance(1000);
    let a = sim.line("pf", &pf(&[(0.51, 5.0)], &[(0.53, 7.0)]));
    let c = cancels(&a);
    assert_eq!(c.len(), 1);
    assert_eq!(c[0].0, QSide::DownBid);
    let a = sim.ack_all(&a);
    assert_eq!(places(&a), vec![(QSide::DownBid, 47, 3)]);
}

#[test]
fn binance_jump_cancels_adverse_side_then_requotes() {
    let mut sim = quoted();
    let up0 = sim.live_order(QSide::UpBid).unwrap();
    // 0.1bp 下跌:不触发
    sim.advance(100);
    let a = sim.line("bn", &bn(80880.0 * (1.0 - 0.1e-4)));
    assert!(cancels(&a).is_empty(), "{a:?}");
    // 300ms 内累计跌 0.25bp:立刻撤 Up 买(不管挂了多久),Down 买不动
    sim.advance(100);
    let a = sim.line("bn", &bn(80880.0 * (1.0 - 0.25e-4)));
    let c = cancels(&a);
    assert_eq!(c.len(), 1, "{a:?}");
    assert_eq!(c[0].0, QSide::UpBid);
    assert!(c[0].1.contains("币安急动"));
    let a = sim.ack_all(&a);
    assert!(places(&a).is_empty(), "急动期间不重挂");
    assert!(sim.s.order_by_id(&up0.0).unwrap().closed);
    // 价格停住,未满 300ms 安静期:仍不挂
    sim.advance(200);
    let a = sim.line("bn", &bn(80880.0 * (1.0 - 0.25e-4)));
    assert!(places(&a).is_empty(), "{a:?}");
    // 安静满 300ms:按最优价重挂
    sim.advance(150);
    let a = sim.tick();
    assert_eq!(places(&a), vec![(QSide::UpBid, 50, 2)]);
    sim.ack_all(&a);
    // 上涨 0.3bp:撤 Down 买
    sim.advance(400);
    sim.line("bn", &bn(80880.0));
    sim.advance(50);
    let a = sim.line("bn", &bn(80880.0 * (1.0 + 0.3e-4)));
    let c = cancels(&a);
    assert!(c.iter().any(|x| x.0 == QSide::DownBid), "{a:?}");
    assert!(!c.iter().any(|x| x.0 == QSide::UpBid && x.1.contains("急动")), "{a:?}");
}

#[test]
fn inventory_cap_blocks_adding_side() {
    let mut sim = quoted();
    // Up 买单连续成交;每张 2 股 @0.50。中间价 0.525
    for k in 0..6 {
        let (oid, _) = sim.live_order(QSide::UpBid).expect("应有 Up 买单");
        sim.advance(500);
        let (d, a) = sim.s.on_fill(sim.now, &oid, 2.0, &mut sim.rl);
        assert_eq!(d, 2.0);
        // 重复上报同一累计量不重复记账
        let (d2, _) = sim.s.on_fill(sim.now, &oid, 2.0, &mut sim.rl);
        assert_eq!(d2, 0.0);
        let p = places(&a);
        let net = 2.0 * (k + 1) as f64;
        if net * 0.525 >= 6.0 {
            assert!(p.iter().all(|x| x.0 != QSide::UpBid), "净 {net} 股 ≥ $6 不应再挂 Up 买: {a:?}");
        } else {
            assert_eq!(p, vec![(QSide::UpBid, 50, 2)], "k={k} {a:?}");
            sim.advance(35);
            sim.ack_all(&a);
        }
    }
    let r = sim.s.ledger.round(MKT).unwrap();
    assert_eq!(r.up_shares, 12.0);
    assert!((r.cost_usd - 6.0).abs() < 1e-9);
    // Down 买侧(减仓侧)照常挂着
    assert!(sim.live_order(QSide::DownBid).is_some());
    // Down 成交 3 股 → 净 9 股 × 0.525 < 6 → 恢复 Up 买
    let (doid, _) = sim.live_order(QSide::DownBid).unwrap();
    let (_, a) = sim.s.on_fill(sim.now, &doid, 3.0, &mut sim.rl);
    assert!(places(&a).iter().any(|x| x.0 == QSide::UpBid), "{a:?}");
}

#[test]
fn stop_quoting_in_last_30_seconds() {
    let mut sim = quoted();
    sim.now.wall_ms = END_MS - 30_001;
    let a = sim.tick();
    assert!(cancels(&a).is_empty());
    sim.advance(1);
    let a = sim.tick();
    let c = cancels(&a);
    assert_eq!(c.len(), 2, "{a:?}");
    assert!(c.iter().all(|x| x.1.contains("剩余")));
    let a = sim.ack_all(&a);
    assert!(places(&a).is_empty());
    sim.advance(5_000);
    let a = sim.line("pf", &pf(&[(0.6, 1.0)], &[(0.62, 1.0)]));
    assert!(places(&a).is_empty(), "最后 30 秒不挂: {a:?}");
}

#[test]
fn no_quote_before_open() {
    let mut sim = Sim::default();
    sim.now.wall_ms = START_MS - 5_000;
    sim.s.set_round(sim.now, RoundCtx { market_id: MKT + 1, start_ms: START_MS + 300_000, end_ms: END_MS + 300_000 }, &mut sim.rl);
    let raw = pf(&[(0.5, 1.0)], &[(0.55, 1.0)]).replace(&MKT.to_string(), &(MKT + 1).to_string());
    let a = sim.line("pf", &raw);
    assert!(places(&a).is_empty(), "{a:?}");
}

#[test]
fn daily_loss_halts_and_cancels_all() {
    let mut l = Ledger::default();
    l.realized_by_day.insert(predict_mm_probe::risk::day_key(START_MS, 0), -29.1);
    l.realized_total = -29.1;
    let mut sim = Sim::new(Params::default(), l);
    sim.line("bn", &bn(80880.0));
    let a = sim.line("pf", &pf(&[(0.5, 80.0)], &[(0.55, 30.0)]));
    sim.ack_all(&a);
    let (oid, _) = sim.live_order(QSide::UpBid).unwrap();
    let (_, a) = sim.s.on_fill(sim.now, &oid, 2.0, &mut sim.rl);
    assert!(!has_halt(&a));
    // 此时 2 股按中间价 0.525 估值 → 当日 −29.05,不停。盘口崩到 0.01/0.02:市值 0.03,浮亏 0.97 → 当日 −30.07 → 停机
    sim.advance(10);
    let a = sim.line("pf", &pf(&[(0.01, 80.0)], &[(0.02, 30.0)]));
    assert!(has_halt(&a), "{a:?}");
    assert!(has_cancel_all(&a));
    assert!(sim.s.halted().unwrap().contains("当日亏损"));
    assert!(sim.s.ledger.halted.is_some(), "停机写进账本(落盘后重启也不会恢复)");
    // 停机后不再挂
    sim.ack_all(&a);
    sim.advance(1000);
    let a = sim.line("pf", &pf(&[(0.5, 80.0)], &[(0.55, 30.0)]));
    assert!(places(&a).is_empty());
}

#[test]
fn total_loss_halts_and_halted_ledger_refuses_to_quote() {
    let mut l = Ledger::default();
    l.realized_by_day.insert("2026-10-01".into(), -100.0);
    l.realized_total = -100.0;
    let mut sim = Sim::new(Params::default(), l);
    let a = sim.line("pf", &pf(&[(0.5, 80.0)], &[(0.55, 30.0)]));
    assert!(places(&a).is_empty());
    assert!(sim.s.halted().unwrap().contains("累计亏损"));
    // 重启:账本带着停机标记 → 一开始就不挂
    let l2 = sim.s.ledger.clone();
    let mut sim2 = Sim::new(Params::default(), l2);
    let a = sim2.line("pf", &pf(&[(0.5, 80.0)], &[(0.55, 30.0)]));
    assert!(places(&a).is_empty());
}

#[test]
fn rate_limit_429_cancels_all_and_cools_down() {
    let mut sim = quoted();
    sim.advance(10);
    let a = sim.s.on_error(sim.now, ErrorKind::RateLimited, "429", &mut sim.rl);
    assert!(has_cancel_all(&a));
    let a2 = sim.ack_all(&a);
    assert!(places(&a2).is_empty());
    sim.advance(30_000);
    assert!(places(&sim.tick()).is_empty(), "冷却中");
    sim.advance(30_001);
    let a = sim.tick();
    assert_eq!(places(&a).len(), 2, "冷却结束恢复: {a:?}");
}

#[test]
fn place_error_and_ack_timeout_cool_down() {
    let mut sim = Sim::default();
    sim.line("bn", &bn(80880.0));
    let a = sim.line("pf", &pf(&[(0.5, 80.0)], &[(0.55, 30.0)]));
    let cids: Vec<u64> = a.iter().filter_map(|x| if let Action::Place { cid, .. } = x { Some(*cid) } else { None }).collect();
    let r = sim.s.on_place_err(sim.now, cids[0], ErrorKind::PlaceRejected, "400 post-only would cross", &mut sim.rl);
    assert!(has_cancel_all(&r));
    assert!(sim.s.in_cooldown(sim.now));
    // 另一张的回报一直没来:超时也进冷却(已在冷却,不重复计数)
    sim.advance(5_001);
    sim.tick();
    // 迟到回报 → 立刻撤
    let r = sim.s.on_place_ok(sim.now, cids[1], "late-1".into(), None, &mut sim.rl);
    assert!(cancels(&r).iter().any(|c| c.1.contains("迟到")), "{r:?}");
}

#[test]
fn feed_down_over_3s_cancels_all() {
    let mut sim = quoted();
    sim.advance(10);
    let a = sim.s.on_feed_state(sim.now, Feed::Predict, false, &mut sim.rl);
    assert!(!has_cancel_all(&a), "刚断开不全撤");
    // 断开期间不挂新单
    sim.now = sim.now.plus_ms(2_000);
    sim.s.feed_msg(sim.now, Feed::Binance);
    let a = sim.s.on_tick(sim.now, &mut sim.rl);
    assert!(places(&a).is_empty() && !has_cancel_all(&a));
    sim.now = sim.now.plus_ms(1_001);
    sim.s.feed_msg(sim.now, Feed::Binance);
    let a = sim.s.on_tick(sim.now, &mut sim.rl);
    assert!(has_cancel_all(&a), "断开 > 3s 全撤: {a:?}");
    assert!(sim.s.in_cooldown(sim.now));
}

#[test]
fn binance_silence_blocks_new_orders() {
    let mut sim = Sim::default();
    sim.line("bn", &bn(80880.0));
    sim.now = sim.now.plus_ms(1_500);
    sim.s.feed_msg(sim.now, Feed::Predict);
    let a = sim.line("pf", &pf(&[(0.5, 80.0)], &[(0.55, 30.0)]));
    assert!(places(&a).is_empty(), "币安 1.5s 没消息不挂新单: {a:?}");
}

#[test]
fn too_many_cooldowns_halt() {
    let mut sim = quoted();
    for k in 0..6 {
        sim.advance(61_000);
        let a = sim.s.on_error(sim.now, ErrorKind::PlaceRejected, "x", &mut sim.rl);
        sim.ack_all(&a);
        assert_eq!(has_halt(&a), k == 5, "第 {} 次", k + 1);
    }
}

#[test]
fn exhausted_budget_blocks_place_but_not_cancel() {
    let mut sim = quoted();
    // 把每分钟预算用到只剩撤单保留额度
    let (_, rm) = sim.rl.remaining(sim.now.mono_ns);
    let cfg = RateLimitConfig::default();
    let mut used = 0;
    while sim.rl.remaining(sim.now.mono_ns).1 > cfg.place_reserve_min {
        // 分散在多秒里,避开每秒上限
        if !sim.rl.try_acquire(sim.now.mono_ns, Priority::Cancel) {
            sim.now = sim.now.plus_ms(1000);
            sim.s.feed_msg(sim.now, Feed::Predict);
            continue;
        }
        used += 1;
    }
    assert!(used > 0 && used < rm);
    sim.s.feed_msg(sim.now, Feed::Binance);
    // 改价:撤单能发,新挂被限额挡住
    let a = sim.line("pf", &pf(&[(0.52, 1.0)], &[(0.55, 30.0)]));
    let c = cancels(&a);
    assert_eq!(c.len(), 1, "{a:?}");
    let a = sim.ack_all(&a);
    assert!(places(&a).is_empty(), "{a:?}");
    assert!(a.iter().any(|x| matches!(x, Action::Note { msg, .. } if msg.contains("限额"))), "{a:?}");
}

#[test]
fn fills_are_booked_to_right_outcome() {
    let mut sim = quoted();
    let (d, _) = sim.live_order(QSide::DownBid).unwrap();
    let (x, _) = sim.s.on_fill(sim.now, &d, 1.5, &mut sim.rl);
    assert_eq!(x, 1.5);
    let r = sim.s.ledger.round(MKT).unwrap();
    assert_eq!((r.up_shares, r.down_shares), (0.0, 1.5));
    assert!((r.cost_usd - 1.5 * 0.45).abs() < 1e-9);
    // 部分成交后单子还在
    assert!(sim.live_order(QSide::DownBid).is_some());
    let _ = Outcome::Up;
}
