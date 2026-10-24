//! 策略单测:把"录制格式"的原始消息(pf = Predict 盘口推送原文,bn = 币安 bookTicker 原文)
//! 逐条过解析器喂给状态机,检查动作。不连网。
use predict_mm_probe::msgs::{parse_binance_book_ticker, parse_predict, PredictMsg};
use predict_mm_probe::ratelimit::{Priority, RateLimitConfig, RateLimiter};
use predict_mm_probe::risk::{Ledger, Outcome, RoundPos};
use predict_mm_probe::strategy::*;

const MKT: u64 = 3698227;
const START_MS: i64 = 1_792_818_000_000;
const END_MS: i64 = START_MS + 300_000;

/// 旧做法(两边各挂各的)的参数:下面的老测试都用它
fn indep() -> Params {
    Params { aligned: false, ..Params::default() }
}

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
        Self::new(indep(), Ledger::default())
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
    fn tick_actions(&mut self) -> Vec<Action> {
        self.tick()
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
    // Up 买单连续成交;每张 2 股 @0.50 = $1。最坏亏损(Down 赢)= 成本 − min(Up, Down) = 成本
    for k in 0..6 {
        let (oid, _) = sim.live_order(QSide::UpBid).expect("应有 Up 买单");
        sim.advance(500);
        let (d, a) = sim.s.on_fill(sim.now, &oid, 2.0, &mut sim.rl);
        assert_eq!(d, 2.0);
        // 重复上报同一累计量不重复记账
        let (d2, _) = sim.s.on_fill(sim.now, &oid, 2.0, &mut sim.rl);
        assert_eq!(d2, 0.0);
        let p = places(&a);
        let worst = (k + 1) as f64;
        if worst >= 6.0 {
            assert!(p.iter().all(|x| x.0 != QSide::UpBid), "最坏亏损 ${worst} ≥ $6 不应再挂 Up 买: {a:?}");
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
    // Down 成交 3 股 @0.45 → 成本 7.35 − min(12, 3) = 4.35 < 6 → 恢复 Up 买
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
    let mut sim = Sim::new(indep(), l);
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
    let mut sim = Sim::new(indep(), l);
    let a = sim.line("pf", &pf(&[(0.5, 80.0)], &[(0.55, 30.0)]));
    assert!(places(&a).is_empty());
    assert!(sim.s.halted().unwrap().contains("累计亏损"));
    // 重启:账本带着停机标记 → 一开始就不挂
    let l2 = sim.s.ledger.clone();
    let mut sim2 = Sim::new(indep(), l2);
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

#[test]
fn late_ack_cancel_retried_when_budget_returns() {
    let mut sim = Sim::default();
    sim.line("bn", &bn(80880.0));
    let a = sim.line("pf", &pf(&[(0.5, 80.0)], &[(0.55, 30.0)]));
    let cid = a.iter().find_map(|x| if let Action::Place { cid, qside: QSide::UpBid, .. } = x { Some(*cid) } else { None }).unwrap();
    // 回报一直不来 → 超时放弃该槽位,进冷却
    sim.advance(5_001);
    sim.tick();
    assert!(sim.s.in_cooldown(sim.now));
    // 把每秒撤单额度用光,再来迟到回报:这一刻撤不了
    while sim.rl.try_acquire(sim.now.mono_ns, Priority::Cancel) {}
    let r = sim.s.on_place_ok(sim.now, cid, "late-9".into(), None, &mut sim.rl);
    assert!(cancels(&r).is_empty());
    // 额度恢复后的下一个 tick 必须补撤
    sim.advance(1_000);
    let r = sim.tick();
    assert!(r.iter().any(|x| matches!(x, Action::Cancel { order_id, .. } if order_id == "late-9")), "{r:?}");
    // 撤单确认后不再重复
    sim.ack_all(&r);
    sim.advance(1_000);
    assert!(!sim.tick().iter().any(|x| matches!(x, Action::Cancel { order_id, .. } if order_id == "late-9")));
}

#[test]
fn gross_cap_blocks_new_orders_when_both_sides_fill() {
    // 合计上限 $5:两边交替成交,成本 + 在挂金额 + 新单 > $5 时不再挂(10-24 实盘两边各成交 ~$45 把余额押光)
    let mut sim = Sim::new(Params { gross_cap_usd: 5.0, ..indep() }, Ledger::default());
    sim.line("bn", &bn(80880.0));
    let a = sim.line("pf", &pf(&[(0.5, 80.0), (0.4, 10.0)], &[(0.55, 30.0), (0.6, 5.0)]));
    assert_eq!(places(&a), vec![(QSide::UpBid, 50, 2), (QSide::DownBid, 45, 3)], "{a:?}");
    sim.advance(35);
    sim.ack_all(&a);
    // Up 成交 2 股 → 成本 1.00,在挂 Down 1.35,新 Up 1.00 → 3.35 ≤ 5 照挂
    let (oid, _) = sim.live_order(QSide::UpBid).unwrap();
    sim.advance(500);
    let (_, a) = sim.s.on_fill(sim.now, &oid, 2.0, &mut sim.rl);
    assert_eq!(places(&a), vec![(QSide::UpBid, 50, 2)], "{a:?}");
    sim.advance(35);
    sim.ack_all(&a);
    // Down 成交 3 股 → 成本 2.35,在挂 Up 1.00,新 Down 1.35 → 4.70 ≤ 5 照挂
    let (doid, _) = sim.live_order(QSide::DownBid).unwrap();
    sim.advance(500);
    let (_, a) = sim.s.on_fill(sim.now, &doid, 3.0, &mut sim.rl);
    assert_eq!(places(&a), vec![(QSide::DownBid, 45, 3)], "{a:?}");
    sim.advance(35);
    sim.ack_all(&a);
    // Up 再成交 → 成本 3.35,在挂 Down 1.35,新 Up 1.00 → 5.70 > 5 不挂
    let (oid, _) = sim.live_order(QSide::UpBid).unwrap();
    sim.advance(500);
    let (_, a) = sim.s.on_fill(sim.now, &oid, 2.0, &mut sim.rl);
    assert!(places(&a).is_empty(), "超合计上限不应再挂: {a:?}");
    // 在挂的 Down 单不因合计上限被撤
    assert!(sim.live_order(QSide::DownBid).is_some());
    assert!(cancels(&a).is_empty(), "{a:?}");
}

#[test]
fn gross_per_minute_paces_quoting() {
    // 匀速 $2/分钟:开盘第 1 分钟只够挂 Up($1),Down($1.35)要等到第 2 分钟
    let mut sim = Sim::new(Params { gross_per_min_usd: 2.0, ..indep() }, Ledger::default());
    sim.line("bn", &bn(80880.0));
    let a = sim.line("pf", &pf(&[(0.5, 80.0), (0.4, 10.0)], &[(0.55, 30.0), (0.6, 5.0)]));
    assert_eq!(places(&a), vec![(QSide::UpBid, 50, 2)], "{a:?}");
    sim.advance(35);
    sim.ack_all(&a);
    sim.advance(30_000);
    assert!(places(&sim.tick()).is_empty());
    sim.advance(30_000);
    let a = sim.tick();
    assert_eq!(places(&a), vec![(QSide::DownBid, 45, 3)], "{a:?}");
}

#[test]
fn capital_cap_counts_unsettled_previous_round() {
    // 上一轮还没结算(成本 $79,两边各 80 股),未结算合计上限 $81:本轮只够挂 Up($1),Down($1.35)被拦
    let prev = RoundPos { market_id: MKT - 1, start_ms: START_MS - 300_000, end_ms: START_MS, up_shares: 80.0, down_shares: 80.0, cost_usd: 79.0, fills: 60 };
    let ledger = Ledger { open: vec![prev], ..Ledger::default() };
    let mut sim = Sim::new(Params { capital_cap_usd: 81.0, ..indep() }, ledger);
    sim.line("bn", &bn(80880.0));
    let a = sim.line("pf", &pf(&[(0.5, 80.0), (0.4, 10.0)], &[(0.55, 30.0), (0.6, 5.0)]));
    assert_eq!(places(&a), vec![(QSide::UpBid, 50, 2)], "{a:?}");
    // 上一轮结算(平台自动领回)后放开
    sim.s.on_resolution(MKT - 1, 1.0, 0.0);
    sim.advance(100);
    let a = sim.tick();
    assert_eq!(places(&a), vec![(QSide::DownBid, 45, 3)], "{a:?}");
}

#[test]
fn insufficient_collateral_pauses_without_cooldown() {
    let mut sim = quoted();
    let (oid, _) = sim.live_order(QSide::UpBid).unwrap();
    sim.advance(500);
    let (_, a) = sim.s.on_fill(sim.now, &oid, 2.0, &mut sim.rl);
    let cid = a.iter().find_map(|x| if let Action::Place { cid, qside: QSide::UpBid, .. } = x { Some(*cid) } else { None }).expect("应补挂 Up");
    sim.advance(35);
    let a = sim.s.on_place_err(sim.now, cid, ErrorKind::PlaceRejected, r#"400 {"error":"create_order_insufficient_collateral_balance"}"#, &mut sim.rl);
    assert!(!has_cancel_all(&a) && !has_halt(&a), "保证金不足不应全撤/停机: {a:?}");
    assert!(!sim.s.in_cooldown(sim.now));
    // Down 单照常挂着;5 秒内不补挂 Up
    assert!(sim.live_order(QSide::DownBid).is_some());
    sim.advance(2_000);
    assert!(places(&sim.tick()).is_empty());
    sim.advance(3_500);
    assert_eq!(places(&sim.tick()), vec![(QSide::UpBid, 50, 2)]);
}

#[test]
fn insufficient_collateral_over_60s_becomes_cooldown() {
    let mut sim = quoted();
    let msg = r#"400 {"error":"create_order_insufficient_collateral_balance"}"#;
    let a = sim.s.on_place_err(sim.now, 9_999, ErrorKind::PlaceRejected, msg, &mut sim.rl);
    assert!(!has_cancel_all(&a) && !sim.s.in_cooldown(sim.now));
    sim.advance(30_000);
    let a = sim.s.on_place_err(sim.now, 9_998, ErrorKind::PlaceRejected, msg, &mut sim.rl);
    assert!(!has_cancel_all(&a) && !sim.s.in_cooldown(sim.now), "30 秒内仍只暂停");
    sim.advance(31_000);
    let a = sim.s.on_place_err(sim.now, 9_997, ErrorKind::PlaceRejected, msg, &mut sim.rl);
    assert!(has_cancel_all(&a) || sim.s.in_cooldown(sim.now), "连续 60 秒以上保证金不足应转为冷却: {a:?}");
}

/// 10-24 13:35 那一轮的毛病:价格一路下跌时一直接 Up。旧上限按"净股数 × 中间价"算,
/// 早先高价买的股票按现价只值几分钱,挡不住;新上限按最坏亏损(成本 − min(Up, Down))算,花满 $6 就停
#[test]
fn falling_price_cannot_accumulate_beyond_worst_case_cap() {
    let mut sim = Sim::default();
    sim.line("bn", &bn(80880.0));
    let mut spent = 0.0;
    let mut last: Vec<Action>;
    // 把动作里的挂/撤全部回报,直到没有新动作
    fn settle(sim: &mut Sim, mut a: Vec<Action>) -> Vec<Action> {
        let mut all = a.clone();
        for _ in 0..5 {
            sim.advance(20);
            a = sim.ack_all(&a);
            if a.is_empty() {
                break;
            }
            all.extend(a.clone());
        }
        all
    }
    let mut fills = 0;
    for bid in [0.50, 0.40, 0.30, 0.20, 0.10, 0.05, 0.04, 0.03] {
        sim.advance(1_100);
        let a = sim.line("pf", &pf(&[(bid, 50.0)], &[(bid + 0.02, 50.0)]));
        last = settle(&mut sim, a);
        sim.advance(1_100);
        let t = sim.tick_actions();
        last.extend(settle(&mut sim, t));
        let Some((oid, price)) = sim.live_order(QSide::UpBid) else { continue };
        if price != (bid * 100.0_f64).round() as u32 {
            continue;
        }
        let shares = sim.s.order_by_id(&oid).unwrap().shares as f64;
        let (d, a) = sim.s.on_fill(sim.now, &oid, shares, &mut sim.rl);
        assert_eq!(d, shares);
        fills += 1;
        spent += shares * bid;
        last = settle(&mut sim, a);
        let r = sim.s.ledger.round(MKT).unwrap();
        // 旧口径(净股数 × 中间价)在价格跌下去以后一直小于 $6,挡不住继续买
        if bid <= 0.05 {
            assert!(r.net_up() * (bid + 0.01) < 6.0, "这里旧上限还挡不住:{}", r.net_up() * (bid + 0.01));
        }
    }
    let r = sim.s.ledger.round(MKT).unwrap();
    assert!(fills >= 6, "成交 {fills} 次");
    assert!((r.cost_usd - spent).abs() < 1e-9);
    assert!(r.cost_usd >= 6.0 && r.cost_usd < 7.5, "最坏亏损停在 $6 + 最后一张单以内,实际成本 {}", r.cost_usd);
    assert!(sim.live_order(QSide::UpBid).is_none(), "到上限后不再挂 Up 买(已挂的也撤)");
    sim.advance(2_000);
    let a = sim.line("pf", &pf(&[(0.02, 50.0)], &[(0.04, 50.0)]));
    assert!(!places(&a).iter().any(|x| x.0 == QSide::UpBid), "{a:?}");
    let _ = last;
}

#[test]
fn balanced_book_at_cap_blocks_both_sides() {
    let mut sim = quoted();
    let (u, _) = sim.live_order(QSide::UpBid).unwrap();
    let (d, _) = sim.live_order(QSide::DownBid).unwrap();
    // 先各成交 2 股:Up 2 股 @0.50 + Down 2 股 @0.45 → 成本 1.9,min = 2 → 最坏亏损为负,不挡
    sim.s.on_fill(sim.now, &u, 2.0, &mut sim.rl);
    let (_, a) = sim.s.on_fill(sim.now, &d, 2.0, &mut sim.rl);
    assert!(a.iter().all(|x| !matches!(x, Action::Note { msg, .. } if msg.contains("最坏亏损"))), "{a:?}");
}

// ======================= 对齐做法(默认) =======================

fn aligned() -> Sim {
    Sim::new(Params::default(), Ledger::default())
}

fn place_of(a: &[Action], side: QSide) -> Option<(u64, u32, u64)> {
    a.iter().find_map(|x| if let Action::Place { cid, qside, price_cents, shares, .. } = x { (*qside == side).then_some((*cid, *price_cents, *shares)) } else { None })
}

#[test]
fn aligned_both_sides_same_shares() {
    let mut sim = aligned();
    sim.line("bn", &bn(80880.0));
    let a = sim.line("pf", &pf(&[(0.5, 80.0)], &[(0.55, 30.0)]));
    // Up 0.50 最少 2 股,Down 0.45 最少 3 股 → 两边都 3 股
    assert_eq!(places(&a), vec![(QSide::UpBid, 50, 3), (QSide::DownBid, 45, 3)], "{a:?}");
}

#[test]
fn aligned_one_leg_fills_then_only_complete_other_leg_capped() {
    let mut sim = aligned();
    sim.line("bn", &bn(80880.0));
    let a = sim.line("pf", &pf(&[(0.5, 80.0)], &[(0.55, 30.0)]));
    sim.advance(35);
    sim.ack_all(&a);
    let (up, _) = sim.live_order(QSide::UpBid).unwrap();
    // Up 3 股 @0.50 成交 → 多 3 股 Up,均价 0.50:Up 不再挂,只挂 Down,Down 价 ≤ 1 − 0.50
    let (_, a) = sim.s.on_fill(sim.now, &up, 3.0, &mut sim.rl);
    assert!(place_of(&a, QSide::UpBid).is_none(), "{a:?}");
    assert!(a.iter().any(|x| matches!(x, Action::Note { qside: Some(QSide::UpBid), msg, .. } if msg.contains("等另一边补齐"))), "{a:?}");
    assert_eq!(sim.s.imbalance().map(|x| (x.0, x.1, x.2)), Some((QSide::UpBid, 3.0, 0.5)));
    // 行情上涨:Down 最优价 1 − 0.45 = 0.55 > 上限 0.50 → 改挂到 0.50(满 1 秒后)
    sim.advance(1_100);
    let a = sim.line("pf", &pf(&[(0.4, 80.0)], &[(0.45, 30.0)]));
    assert!(cancels(&a).iter().any(|c| c.0 == QSide::DownBid), "{a:?}");
    let a = sim.ack_all(&a);
    assert_eq!(place_of(&a, QSide::DownBid).map(|x| (x.1, x.2)), Some((50, 3)), "补齐价被封顶在 0.50,补 3 股: {a:?}");
    assert!(place_of(&a, QSide::UpBid).is_none());
    sim.advance(35);
    sim.ack_all(&a);
    // Down 3 股成交 → 两边各 3 股,重新两边同股数挂
    let (dn, _) = sim.live_order(QSide::DownBid).unwrap();
    let (_, a) = sim.s.on_fill(sim.now, &dn, 3.0, &mut sim.rl);
    assert!(sim.s.imbalance().is_none());
    let p = places(&a);
    assert_eq!(p.len(), 2, "{a:?}");
    assert_eq!(p[0].2, p[1].2, "两边同股数");
    let r = sim.s.ledger.round(MKT).unwrap();
    assert!((r.cost_usd - 3.0) < 1e-9, "这一对成本 = 3 × (0.50 + 0.50) = $3");
}

#[test]
fn aligned_partial_fill_cancels_rest_of_heavy_leg() {
    let mut sim = aligned();
    sim.line("bn", &bn(80880.0));
    let a = sim.line("pf", &pf(&[(0.5, 80.0)], &[(0.55, 30.0)]));
    sim.advance(35);
    sim.ack_all(&a);
    let (up, _) = sim.live_order(QSide::UpBid).unwrap();
    let (_, a) = sim.s.on_fill(sim.now, &up, 1.2, &mut sim.rl);
    // Up 部分成交 1.2 股:Up 剩下的 1.8 股要撤(否则可能再多出来)
    assert!(cancels(&a).iter().any(|c| c.0 == QSide::UpBid && c.1.contains("补齐")), "{a:?}");
    // Down 继续挂着补
    assert!(sim.live_order(QSide::DownBid).is_some());
}

#[test]
fn failed_settlement_reverts_fill_and_cancels_possibly_relisted_order() {
    let mut sim = aligned();
    sim.line("bn", &bn(80880.0));
    let a = sim.line("pf", &pf(&[(0.5, 80.0)], &[(0.55, 30.0)]));
    sim.advance(35);
    sim.ack_all(&a);
    let (up, _) = sim.live_order(QSide::UpBid).unwrap();
    sim.s.on_fill_set(sim.now, &up, 3.0, true, &mut sim.rl);
    assert_eq!(sim.s.ledger.round(MKT).unwrap().up_shares, 3.0);
    // 上链失败:扣回;这张单在平台上可能重新挂着 → 撤掉
    let (d, a) = sim.s.on_fill_set(sim.now, &up, 0.0, true, &mut sim.rl);
    assert_eq!(d, -3.0);
    let r = sim.s.ledger.round(MKT).unwrap();
    assert_eq!((r.up_shares, r.cost_usd), (0.0, 0.0));
    assert!(sim.s.imbalance().is_none());
    assert!(a.iter().any(|x| matches!(x, Action::Cancel { order_id, .. } if *order_id == up)), "{a:?}");
    // 只增不减的来源(REST)不能把它扣回
    let (d, _) = sim.s.on_fill(sim.now, &up, 0.0, &mut sim.rl);
    assert_eq!(d, 0.0);
}

/// 随机行情 + 随机成交(含部分成交、上链失败扣回):任何时刻两边股数差不超过一张单;
/// 按成交先后配成的每一对,成本不超过 $1
#[test]
fn aligned_invariants_under_random_markets() {
    let mut rng = 0x2545F4914F6CDD1Du64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        (rng >> 11) as f64 / (1u64 << 53) as f64
    };
    for _case in 0..200 {
        let mut sim = aligned();
        sim.line("bn", &bn(80880.0));
        let mut mid = 30 + (next() * 40.0) as i64;
        let mut max_order = 0u64;
        let mut fills: Vec<(u64, QSide, f64, f64)> = vec![]; // (序号, 侧, 股, 价)
        let mut seq = 0u64;
        for _step in 0..60 {
            mid = (mid + ((next() * 7.0) as i64 - 3)).clamp(3, 97);
            let spread = 1 + (next() * 3.0) as i64;
            let (b, a) = ((mid - 1).max(1), (mid - 1 + spread).min(99));
            sim.advance(400 + (next() * 900.0) as u64);
            let acts = sim.line("pf", &pf(&[(b as f64 / 100.0, 50.0)], &[(a as f64 / 100.0, 50.0)]));
            for x in &acts {
                if let Action::Place { shares, .. } = x {
                    max_order = max_order.max(*shares);
                }
            }
            sim.advance(35);
            let more = sim.ack_all(&acts);
            sim.ack_all(&more);
            // 随机成交一侧(可能部分)
            for side in QSide::ALL {
                if next() < 0.35 {
                    // 只成交真正挂着的单(撤单在路上被成交的情况另测)
                    if let Some((oid, price)) = sim.live_order(side).filter(|(o, _)| !sim.s.is_cancelling(o)) {
                        let o = sim.s.order_by_id(&oid).unwrap().clone();
                        let q = if next() < 0.3 { (o.shares as f64) * next() } else { o.shares as f64 - o.filled };
                        let cum = (o.filled + q).min(o.shares as f64);
                        let (d, acts) = sim.s.on_fill_set(sim.now, &oid, cum, true, &mut sim.rl);
                        if d > 0.0 {
                            seq += 1;
                            fills.push((seq, side, d, price as f64 / 100.0));
                        }
                        let more = sim.ack_all(&acts);
                        sim.ack_all(&more);
                    }
                }
            }
            // 两边股数差 ≤ 一张单(本轮挂过的最大一张)
            max_order = max_order.max(sim.s.orders.values().map(|o| o.shares).max().unwrap_or(0));
            let r = sim.s.ledger.round(MKT).unwrap();
            if (r.up_shares - r.down_shares).abs() > max_order as f64 + 1e-6 {
                let mut os: Vec<_> = sim.s.orders.values().cloned().collect();
                os.sort_by_key(|o| o.cid);
                for o in &os {
                    eprintln!(
                        "cid {} {:?} 价 {} 股 {} 成交 {:.3} 关 {} 撤中 {}",
                        o.cid,
                        o.qside,
                        o.price_cents,
                        o.shares,
                        o.filled,
                        o.closed,
                        o.order_id.as_deref().is_some_and(|x| sim.s.is_cancelling(x))
                    );
                }
                panic!("股数差 {} > 一张单 {max_order};Up {} Down {}", (r.up_shares - r.down_shares).abs(), r.up_shares, r.down_shares);
            }
        }
        // 按先后配对:每对成本 ≤ $1
        let mut ups: Vec<(f64, f64)> = fills.iter().filter(|f| f.1 == QSide::UpBid).map(|f| (f.2, f.3)).collect();
        let mut dns: Vec<(f64, f64)> = fills.iter().filter(|f| f.1 == QSide::DownBid).map(|f| (f.2, f.3)).collect();
        let (mut i, mut j) = (0, 0);
        while i < ups.len() && j < dns.len() {
            let q = ups[i].0.min(dns[j].0);
            assert!(ups[i].1 + dns[j].1 <= 1.0 + 1e-9, "配成的一对成本 {} + {} > $1", ups[i].1, dns[j].1);
            ups[i].0 -= q;
            dns[j].0 -= q;
            if ups[i].0 <= 1e-9 {
                i += 1;
            }
            if dns[j].0 <= 1e-9 {
                j += 1;
            }
        }
    }
}

/// 撤单在路上时被成交(10-24 实测 1.6%):多出的部分最多是被撤那张单剩下的股数;之后照常只挂另一边补齐
#[test]
fn aligned_fill_during_cancel_in_flight_is_bounded_and_completed() {
    let mut sim = aligned();
    sim.line("bn", &bn(80880.0));
    let a = sim.line("pf", &pf(&[(0.5, 80.0)], &[(0.55, 30.0)]));
    sim.advance(35);
    sim.ack_all(&a);
    let (up, _) = sim.live_order(QSide::UpBid).unwrap();
    let (_, a) = sim.s.on_fill(sim.now, &up, 1.0, &mut sim.rl);
    assert!(sim.s.is_cancelling(&up), "部分成交后剩余在撤: {a:?}");
    // 撤单还没确认,剩下的 2 股也成交了
    let (d, a) = sim.s.on_fill(sim.now, &up, 3.0, &mut sim.rl);
    assert_eq!(d, 2.0);
    assert_eq!(sim.s.imbalance().map(|x| x.1), Some(3.0), "多出 3 股 = 整张单,没超过这张单本身");
    assert!(place_of(&a, QSide::UpBid).is_none());
    // Down 挂着的那张(3 股)正好补齐
    assert!(sim.live_order(QSide::DownBid).is_some());
}
