//! 探针策略:纯状态机。输入事件(盘口、币安、下单/撤单回报、成交、连接状态、定时),输出动作(挂、撤、全撤、停机、决策记录)。
//! 不做任何 IO,时间全部由调用方传入,所以可以用录好的消息逐条回放做单测。
//!
//! 两个"挂单侧"(都只做挂单 isPostOnly,都是 BUY):
//! - `UpBid`  :买 Up,挂在 Up 买一。
//! - `DownBid`:买 Down,挂在 1 − Up 卖一(等价于卖 Up 挂在卖一;不持有 Up 也能做)。
//!
//! 规则(PROBE_SPEC.md 第 2 节,参数见 `Params`):
//! 1. 只做当前轮;开盘前不挂;剩余 ≤ 30 秒全撤停挂。
//! 2. 单量 = ceil(最小金额 / 价) 股(默认 $1)。
//! 3. 改价:我们的价 ≠ 当前最优价 且 挂单已存在 ≥ 1s → 先撤旧,撤单确认后再挂新(同一侧永远最多 1 张)。
//! 4. 币安保护:bookTicker 中间价 300ms 内对该侧不利方向变动 ≥ 0.2bp → 立刻撤该侧;
//!    此后连续 300ms 不再触发才算"回到正常",按规则 3 重挂。
//! 5. 库存上限:净持仓等值 ≥ $6 → 加仓那一侧不挂(已挂的也撤)。
//! 6. 风控:当日亏损 ≥ $30 或累计 ≥ $100 → 全撤停机;连接断开 > 3s、下单/撤单/签名报错、429、回报超时
//!    → 全撤、冷却 60s;1 小时内冷却次数 > 5 → 停机。
//! 7. 限额:每个请求都要从 RateLimiter 取令牌;先撤后挂。

use crate::msgs::BookSnapshot;
use crate::ratelimit::{Priority, RateLimiter};
use crate::risk::{Ledger, Outcome};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Now {
    pub mono_ns: u64,
    pub wall_ms: i64,
}

impl Now {
    pub fn plus_ms(self, ms: u64) -> Now {
        Now { mono_ns: self.mono_ns + ms * 1_000_000, wall_ms: self.wall_ms + ms as i64 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum QSide {
    UpBid,
    DownBid,
}

impl QSide {
    pub const ALL: [QSide; 2] = [QSide::UpBid, QSide::DownBid];
    pub fn outcome(self) -> Outcome {
        match self {
            QSide::UpBid => Outcome::Up,
            QSide::DownBid => Outcome::Down,
        }
    }
    fn idx(self) -> usize {
        self as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum Feed {
    Binance,
    Predict,
}

#[derive(Debug, Clone)]
pub struct Params {
    pub stop_before_end_ms: i64,
    pub min_notional_usd: f64,
    pub reprice_min_age_ms: u64,
    pub bn_window_ms: u64,
    pub bn_move_bp: f64,
    pub bn_quiet_ms: u64,
    pub inventory_cap_usd: f64,
    /// 对齐做法(10-24 Luke 定,默认开):两边同股数;一边多出来就只挂另一边补齐,补齐价不让这一对超过 $1。
    /// false = 旧做法(两边各挂各的,每张 ceil($1/价) 股)
    pub aligned: bool,
    pub gross_cap_usd: f64,
    pub gross_per_min_usd: f64,
    pub capital_cap_usd: f64,
    pub max_daily_loss_usd: f64,
    pub max_total_loss_usd: f64,
    pub feed_down_ms: u64,
    pub binance_stale_ms: u64,
    pub predict_stale_ms: u64,
    pub cooldown_ms: u64,
    pub ack_timeout_ms: u64,
    pub max_cooldowns_per_hour: usize,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            stop_before_end_ms: 30_000,
            min_notional_usd: 1.0,
            reprice_min_age_ms: 0,
            bn_window_ms: 300,
            bn_move_bp: 0.2,
            bn_quiet_ms: 300,
            inventory_cap_usd: 6.0,
            aligned: true,
            gross_cap_usd: 150.0,
            gross_per_min_usd: 0.0,
            capital_cap_usd: 1_000.0,
            max_daily_loss_usd: 30.0,
            max_total_loss_usd: 100.0,
            feed_down_ms: 3_000,
            binance_stale_ms: 1_000,
            predict_stale_ms: 15_000,
            cooldown_ms: 60_000,
            ack_timeout_ms: 5_000,
            max_cooldowns_per_hour: 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoundCtx {
    pub market_id: u64,
    pub start_ms: i64,
    pub end_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
enum Slot {
    Empty,
    Placing { cid: u64, price: u32, shares: u64, sent_ns: u64 },
    Live { cid: u64, order_id: String, price: u32, shares: u64, placed_ns: u64, want_cancel: Option<String> },
    Cancelling { cid: u64, order_id: String, since_ns: u64 },
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderRec {
    pub cid: u64,
    pub qside: QSide,
    pub market_id: u64,
    pub price_cents: u32,
    pub shares: u64,
    pub order_id: Option<String>,
    pub hash: Option<String>,
    pub filled: f64,
    pub closed: bool,
    /// 下单回报到达的单调时钟
    pub acked_ns: Option<u64>,
    /// 第一次记到成交的单调时钟(凑对按先后配)
    pub first_fill_ns: Option<u64>,
}

/// 决策时的盘面快照(写进日志,和回放逐笔对比用)
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct DecisionCtx {
    pub best_bid: Option<u32>,
    pub best_ask: Option<u32>,
    pub bn_mid: Option<f64>,
    pub target: Option<u32>,
    pub ms_left: Option<i64>,
    /// 挂单时该价位已有的量(股),即我们前面排了多少
    pub queue_ahead: Option<f64>,
    pub net_up_shares: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "action")]
pub enum Action {
    Place {
        cid: u64,
        qside: QSide,
        market_id: u64,
        price_cents: u32,
        shares: u64,
        reason: String,
        ctx: DecisionCtx,
    },
    Cancel {
        cid: u64,
        qside: QSide,
        order_id: String,
        reason: String,
        ctx: DecisionCtx,
    },
    /// 撤掉账户所有挂单(执行层还会用 REST 列一遍挂单兜底);known = 状态机已知的挂单
    CancelAll {
        reason: String,
        known: Vec<String>,
    },
    Halt {
        reason: String,
    },
    /// 不发请求的决策(为什么不挂),同一侧原因变化时才记一次
    Note {
        qside: Option<QSide>,
        msg: String,
        ctx: DecisionCtx,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ErrorKind {
    RateLimited,
    PlaceRejected,
    CancelFailed,
    Sign,
    Auth,
    Timeout,
    Other,
}

#[derive(Debug, Clone, Default)]
struct FeedState {
    connected: bool,
    down_since_ns: Option<u64>,
    last_msg_ns: Option<u64>,
}

#[derive(Debug)]
pub struct Strategy {
    pub p: Params,
    pub ledger: Ledger,
    round: Option<RoundCtx>,
    book: Option<BookSnapshot>,
    bn: VecDeque<(u64, f64)>,
    bn_guard_until: [u64; 2],
    /// 平台回"保证金不足"后暂停新挂到此刻(mono_ns);上一轮的钱由平台自动领回前会短暂不够,不算故障、不冷却
    collateral_wait_until: u64,
    /// 连续"保证金不足"的第一次时刻(mono_ns,0=没有);下单成功清零
    collateral_first_reject: u64,
    slots: [Slot; 2],
    pub orders: HashMap<u64, OrderRec>,
    by_order_id: HashMap<String, u64>,
    next_cid: u64,
    feeds: HashMap<Feed, FeedState>,
    ready: bool,
    cooldown_until_ns: u64,
    cooldowns: VecDeque<u64>,
    halted: Option<String>,
    last_note: [Option<String>; 2],
    /// 账本有变化(成交、结算),需要落盘
    pub ledger_dirty: bool,
    /// 不在槽位里、又必须撤的单(迟到回报),限额不够时留到下次重试
    orphans: Vec<(u64, QSide, String, String)>,
}

impl Strategy {
    pub fn new(p: Params, ledger: Ledger) -> Self {
        let mut feeds = HashMap::new();
        feeds.insert(Feed::Binance, FeedState::default());
        feeds.insert(Feed::Predict, FeedState::default());
        let halted = ledger.halted.clone();
        Self {
            p,
            ledger,
            round: None,
            book: None,
            bn: VecDeque::new(),
            bn_guard_until: [0; 2],
            collateral_wait_until: 0,
            collateral_first_reject: 0,
            slots: [Slot::Empty, Slot::Empty],
            orders: HashMap::new(),
            by_order_id: HashMap::new(),
            next_cid: 1,
            feeds,
            ready: false,
            cooldown_until_ns: 0,
            cooldowns: VecDeque::new(),
            halted,
            last_note: [None, None],
            ledger_dirty: false,
            orphans: vec![],
        }
    }

    // ---------- 查询 ----------
    pub fn halted(&self) -> Option<&str> {
        self.halted.as_deref()
    }
    pub fn round(&self) -> Option<&RoundCtx> {
        self.round.as_ref()
    }
    pub fn in_cooldown(&self, now: Now) -> bool {
        now.mono_ns < self.cooldown_until_ns
    }
    pub fn order_by_id(&self, order_id: &str) -> Option<&OrderRec> {
        self.by_order_id.get(order_id).and_then(|c| self.orders.get(c))
    }
    /// 所有已知、未关闭、已拿到 orderId 的单
    pub fn known_open_order_ids(&self) -> Vec<String> {
        let mut v: Vec<String> = self.orders.values().filter(|o| !o.closed).filter_map(|o| o.order_id.clone()).collect();
        v.sort();
        v
    }
    /// 该单是否正在被我们撤(撤单请求已发、结果未回)
    pub fn is_cancelling(&self, order_id: &str) -> bool {
        self.slots.iter().any(|s| matches!(s, Slot::Cancelling { order_id: o, .. } if o == order_id))
    }
    pub fn live_slot_summary(&self) -> Vec<(QSide, &'static str)> {
        QSide::ALL
            .iter()
            .map(|&s| {
                (
                    s,
                    match &self.slots[s.idx()] {
                        Slot::Empty => "empty",
                        Slot::Placing { .. } => "placing",
                        Slot::Live { .. } => "live",
                        Slot::Cancelling { .. } => "cancelling",
                    },
                )
            })
            .collect()
    }
    pub fn up_mid(&self) -> Option<f64> {
        let b = self.book.as_ref()?;
        Some((b.best_bid()? + b.best_ask()?) / 2.0)
    }
    pub fn bn_mid(&self) -> Option<f64> {
        self.bn.back().map(|x| x.1)
    }

    // ---------- 事件 ----------
    /// 启动时的 cancel-all 完成后才允许挂单
    pub fn set_ready(&mut self, now: Now, rl: &mut RateLimiter) -> Vec<Action> {
        self.ready = true;
        self.decide(now, rl)
    }

    pub fn set_round(&mut self, now: Now, ctx: RoundCtx, rl: &mut RateLimiter) -> Vec<Action> {
        if self.round.as_ref() == Some(&ctx) {
            return vec![];
        }
        self.ledger.round_mut(ctx.market_id, ctx.start_ms, ctx.end_ms);
        self.ledger_dirty = true;
        self.round = Some(ctx);
        self.book = None;
        self.last_note = [None, None];
        self.decide(now, rl)
    }

    pub fn on_book(&mut self, now: Now, book: BookSnapshot, rl: &mut RateLimiter) -> Vec<Action> {
        if self.round.as_ref().map(|r| r.market_id) != Some(book.market_id) {
            return vec![];
        }
        self.book = Some(book);
        self.decide(now, rl)
    }

    pub fn on_binance(&mut self, now: Now, mid: f64, rl: &mut RateLimiter) -> Vec<Action> {
        self.feed_msg(now, Feed::Binance);
        let w = self.p.bn_window_ms * 1_000_000;
        self.bn.push_back((now.mono_ns, mid));
        while let Some(&(t, _)) = self.bn.front() {
            if t + w < now.mono_ns {
                self.bn.pop_front();
            } else {
                break;
            }
        }
        let hi = self.bn.iter().map(|x| x.1).fold(f64::MIN, f64::max);
        let lo = self.bn.iter().map(|x| x.1).fold(f64::MAX, f64::min);
        let thr = self.p.bn_move_bp - 1e-9;
        let quiet = self.p.bn_quiet_ms * 1_000_000;
        // 跌:对买 Up 不利;涨:对买 Down 不利
        if (hi - mid) / hi * 1e4 >= thr {
            self.bn_guard_until[QSide::UpBid.idx()] = now.mono_ns + quiet;
        }
        if (mid - lo) / lo * 1e4 >= thr {
            self.bn_guard_until[QSide::DownBid.idx()] = now.mono_ns + quiet;
        }
        self.decide(now, rl)
    }

    pub fn feed_msg(&mut self, now: Now, feed: Feed) {
        let f = self.feeds.get_mut(&feed).unwrap();
        f.last_msg_ns = Some(now.mono_ns);
        if !f.connected {
            f.connected = true;
            f.down_since_ns = None;
        }
    }

    pub fn on_feed_state(&mut self, now: Now, feed: Feed, connected: bool, rl: &mut RateLimiter) -> Vec<Action> {
        let f = self.feeds.get_mut(&feed).unwrap();
        if connected {
            f.connected = true;
            f.down_since_ns = None;
            f.last_msg_ns = Some(now.mono_ns);
        } else if f.connected || f.down_since_ns.is_none() {
            f.connected = false;
            f.down_since_ns = Some(now.mono_ns);
        }
        self.decide(now, rl)
    }

    pub fn on_tick(&mut self, now: Now, rl: &mut RateLimiter) -> Vec<Action> {
        self.decide(now, rl)
    }

    pub fn on_place_ok(&mut self, now: Now, cid: u64, order_id: String, hash: Option<String>, rl: &mut RateLimiter) -> Vec<Action> {
        self.collateral_first_reject = 0;
        let Some(rec) = self.orders.get_mut(&cid) else { return vec![] };
        rec.order_id = Some(order_id.clone());
        rec.hash = hash;
        rec.acked_ns = Some(now.mono_ns);
        self.by_order_id.insert(order_id.clone(), cid);
        let qside = rec.qside;
        let (price, shares) = (rec.price_cents, rec.shares);
        let i = qside.idx();
        match &self.slots[i] {
            Slot::Placing { cid: c, .. } if *c == cid => {
                self.slots[i] = Slot::Live { cid, order_id, price, shares, placed_ns: now.mono_ns, want_cancel: None };
                self.decide(now, rl)
            }
            _ => {
                // 晚到的回报(已超时放弃的单):立即撤掉
                let mut acts = self.issue_cancel(now, cid, qside, order_id, "迟到回报,撤".into(), rl);
                acts.extend(self.decide(now, rl));
                acts
            }
        }
    }

    pub fn on_place_err(&mut self, now: Now, cid: u64, kind: ErrorKind, msg: &str, rl: &mut RateLimiter) -> Vec<Action> {
        if let Some(rec) = self.orders.get_mut(&cid) {
            rec.closed = true;
            let i = rec.qside.idx();
            if matches!(&self.slots[i], Slot::Placing { cid: c, .. } if *c == cid) {
                self.slots[i] = Slot::Empty;
            }
        }
        if kind == ErrorKind::PlaceRejected && msg.contains("insufficient_collateral") {
            if self.collateral_first_reject == 0 {
                self.collateral_first_reject = now.mono_ns;
            }
            // 连续被拒不到 60 秒:多半是上一轮的钱还没被平台自动领回,暂停新挂 5 秒,不全撤、不计冷却次数;
            // 超过 60 秒仍被拒 = 账本和链上对不上(孤儿单/漏记成交),按故障冷却(计入每小时冷却次数)
            if now.mono_ns < self.collateral_first_reject + 60_000_000_000 {
                self.collateral_wait_until = now.mono_ns + 5_000_000_000;
                return vec![];
            }
            self.collateral_first_reject = 0;
        }
        self.on_error(now, kind, &format!("下单失败: {msg}"), rl)
    }

    pub fn on_cancel_ok(&mut self, now: Now, order_id: &str, rl: &mut RateLimiter) -> Vec<Action> {
        if let Some(&cid) = self.by_order_id.get(order_id) {
            if let Some(rec) = self.orders.get_mut(&cid) {
                rec.closed = true;
                let i = rec.qside.idx();
                if self.slot_holds(i, cid) {
                    self.slots[i] = Slot::Empty;
                }
            }
        }
        self.decide(now, rl)
    }

    /// 撤单失败(执行层已先查过单,确认订单仍未关闭或状态未知)
    pub fn on_cancel_err(&mut self, now: Now, order_id: &str, msg: &str, rl: &mut RateLimiter) -> Vec<Action> {
        if let Some(&cid) = self.by_order_id.get(order_id) {
            if let Some(rec) = self.orders.get(&cid) {
                let i = rec.qside.idx();
                if let Slot::Cancelling { cid: c, order_id, .. } = &self.slots[i] {
                    if *c == cid {
                        let (price, shares) = (rec.price_cents, rec.shares);
                        self.slots[i] = Slot::Live { cid, order_id: order_id.clone(), price, shares, placed_ns: now.mono_ns, want_cancel: Some("撤单失败重试".into()) };
                    }
                }
            }
        }
        self.on_error(now, ErrorKind::CancelFailed, &format!("撤单失败 {order_id}: {msg}"), rl)
    }

    /// 成交(累计成交股数,来源任意;重复上报只按增量记账)。返回 (增量, 动作)
    pub fn on_fill(&mut self, now: Now, order_id: &str, cum_filled: f64, rl: &mut RateLimiter) -> (f64, Vec<Action>) {
        self.on_fill_set(now, order_id, cum_filled, false, rl)
    }

    /// 按累计成交股数记账;allow_decrease = true 时累计变少(撮合后上链失败)会撤回记账。
    /// 撤回后单子在平台上可能重新挂着:如果我们已当它成交完、槽位已空,就把它撤掉(同一侧不留两张单)
    pub fn on_fill_set(&mut self, now: Now, order_id: &str, cum_filled: f64, allow_decrease: bool, rl: &mut RateLimiter) -> (f64, Vec<Action>) {
        let Some(&cid) = self.by_order_id.get(order_id) else { return (0.0, vec![]) };
        let rec = self.orders.get_mut(&cid).unwrap();
        let cum = cum_filled.clamp(0.0, rec.shares as f64);
        let delta = cum - rec.filled;
        if delta < -1e-9 && allow_decrease {
            rec.filled = cum;
            let (market_id, qside, price) = (rec.market_id, rec.qside, rec.price_cents);
            let was_closed = rec.closed;
            rec.closed = false;
            self.ledger.revert_fill(market_id, qside.outcome(), -delta, price as f64 / 100.0);
            self.ledger_dirty = true;
            let mut acts = vec![];
            if was_closed && !self.slot_holds(qside.idx(), cid) {
                acts.extend(self.issue_cancel(now, cid, qside, order_id.to_string(), "撮合上链失败,单子可能重新挂着:撤".into(), rl));
            }
            acts.extend(self.decide(now, rl));
            return (delta, acts);
        }
        if delta <= 1e-9 {
            return (0.0, vec![]);
        }
        rec.filled = cum;
        if rec.first_fill_ns.is_none() {
            rec.first_fill_ns = Some(now.mono_ns);
        }
        let (market_id, qside, price, shares) = (rec.market_id, rec.qside, rec.price_cents, rec.shares);
        if cum >= shares as f64 - 1e-9 {
            rec.closed = true;
            let i = qside.idx();
            if self.slot_holds(i, cid) {
                self.slots[i] = Slot::Empty;
            }
        }
        self.ledger.apply_fill(market_id, qside.outcome(), delta, price as f64 / 100.0);
        self.ledger_dirty = true;
        (delta, self.decide(now, rl))
    }

    /// REST 显示订单已不在挂单列表(且不是我们撤的):关单
    pub fn on_order_closed(&mut self, now: Now, order_id: &str, rl: &mut RateLimiter) -> Vec<Action> {
        if let Some(&cid) = self.by_order_id.get(order_id) {
            if let Some(rec) = self.orders.get_mut(&cid) {
                rec.closed = true;
                let i = rec.qside.idx();
                if self.slot_holds(i, cid) {
                    self.slots[i] = Slot::Empty;
                }
            }
        }
        self.decide(now, rl)
    }

    pub fn on_resolution(&mut self, market_id: u64, payout_up: f64, payout_down: f64) -> Option<f64> {
        let r = self.ledger.resolve(market_id, payout_up, payout_down);
        if r.is_some() {
            self.ledger_dirty = true;
        }
        r
    }

    pub fn on_error(&mut self, now: Now, kind: ErrorKind, msg: &str, rl: &mut RateLimiter) -> Vec<Action> {
        let mut acts = vec![];
        if kind == ErrorKind::RateLimited {
            rl.pause_non_cancel(now.mono_ns + self.p.cooldown_ms * 1_000_000);
        }
        acts.extend(self.enter_cooldown(now, &format!("{kind:?}: {msg}")));
        acts.extend(self.decide(now, rl));
        acts
    }

    /// 人工/信号退出:全撤,不再挂
    pub fn shutdown(&mut self, reason: &str) -> Vec<Action> {
        if self.halted.is_none() {
            self.halted = Some(format!("退出: {reason}"));
        }
        self.cancel_all_action(reason)
    }

    // ---------- 内部 ----------
    fn slot_holds(&self, i: usize, cid: u64) -> bool {
        match &self.slots[i] {
            Slot::Empty => false,
            Slot::Placing { cid: c, .. } | Slot::Live { cid: c, .. } | Slot::Cancelling { cid: c, .. } => *c == cid,
        }
    }

    fn cancel_all_action(&mut self, reason: &str) -> Vec<Action> {
        // 已知的挂单全部转"撤单中",执行层的全撤会对每个 id 回报结果
        for i in 0..2 {
            if let Slot::Live { cid, order_id, .. } = &self.slots[i] {
                self.slots[i] = Slot::Cancelling { cid: *cid, order_id: order_id.clone(), since_ns: u64::MAX };
            }
        }
        vec![Action::CancelAll { reason: reason.to_string(), known: self.known_open_order_ids() }]
    }

    fn enter_cooldown(&mut self, now: Now, reason: &str) -> Vec<Action> {
        let hour = 3_600_000_000_000u64;
        // 冷却期间的连带错误不重复计数
        if !self.in_cooldown(now) {
            self.cooldowns.push_back(now.mono_ns);
        }
        while let Some(&t) = self.cooldowns.front() {
            if t + hour < now.mono_ns {
                self.cooldowns.pop_front();
            } else {
                break;
            }
        }
        if self.cooldowns.len() > self.p.max_cooldowns_per_hour {
            return self.halt(&format!("1 小时内冷却 {} 次 > {}(最后一次:{reason})", self.cooldowns.len(), self.p.max_cooldowns_per_hour));
        }
        self.cooldown_until_ns = self.cooldown_until_ns.max(now.mono_ns + self.p.cooldown_ms * 1_000_000);
        let mut acts = self.cancel_all_action(&format!("冷却: {reason}"));
        acts.push(Action::Note { qside: None, msg: format!("进入冷却 {}s: {reason}", self.p.cooldown_ms / 1000), ctx: self.ctx(None, now) });
        acts
    }

    fn halt(&mut self, reason: &str) -> Vec<Action> {
        if self.halted.is_some() {
            return vec![];
        }
        self.halted = Some(reason.to_string());
        self.ledger.halted = Some(reason.to_string());
        self.ledger_dirty = true;
        let mut acts = self.cancel_all_action(&format!("停机: {reason}"));
        acts.push(Action::Halt { reason: reason.to_string() });
        acts
    }

    fn ms_left(&self, now: Now) -> Option<i64> {
        self.round.as_ref().map(|r| r.end_ms - now.wall_ms)
    }

    fn target(&self, side: QSide) -> Option<u32> {
        let b = self.book.as_ref()?;
        match side {
            QSide::UpBid => b.best_bid_cents(),
            QSide::DownBid => b.best_ask_cents().map(|a| 100 - a).filter(|&p| (1..=99).contains(&p)),
        }
    }

    fn ctx(&self, side: Option<QSide>, now: Now) -> DecisionCtx {
        let b = self.book.as_ref();
        DecisionCtx {
            best_bid: b.and_then(|b| b.best_bid_cents()),
            best_ask: b.and_then(|b| b.best_ask_cents()),
            bn_mid: self.bn_mid(),
            target: side.and_then(|s| self.target(s)),
            ms_left: self.ms_left(now),
            queue_ahead: None,
            net_up_shares: self.round.as_ref().and_then(|r| self.ledger.round(r.market_id)).map_or(0.0, |r| r.net_up()),
        }
    }

    /// 本轮两边合计投入:已成交成本 + 本轮未关闭订单的未成交金额
    fn gross_used(&self, market_id: u64) -> f64 {
        let cost = self.ledger.round(market_id).map_or(0.0, |r| r.cost_usd);
        let pending: f64 =
            self.orders.values().filter(|o| o.market_id == market_id && !o.closed).map(|o| (o.shares as f64 - o.filled).max(0.0) * o.price_cents as f64 / 100.0).sum();
        cost + pending
    }

    /// 所有未结算轮的投入:账本里未结算轮的成本 + 全部未关闭订单的未成交金额
    fn capital_used(&self) -> f64 {
        let cost: f64 = self.ledger.open.iter().map(|r| r.cost_usd).sum();
        let pending: f64 = self.orders.values().filter(|o| !o.closed).map(|o| (o.shares as f64 - o.filled).max(0.0) * o.price_cents as f64 / 100.0).sum();
        cost + pending
    }

    /// 新挂一张 add 美元的单是否超出合计投入上限/匀速额度/未结算资金上限,超出返回原因
    fn gross_blocked(&self, now: Now, add: f64) -> Option<String> {
        let r = self.round.as_ref()?;
        let cap_used = self.capital_used();
        if cap_used + add > self.p.capital_cap_usd + 1e-9 {
            return Some(format!("未结算合计投入 {:.2}+{:.2} > ${}", cap_used, add, self.p.capital_cap_usd));
        }
        let used = self.gross_used(r.market_id);
        if used + add > self.p.gross_cap_usd + 1e-9 {
            return Some(format!("两边合计投入 {:.2}+{:.2} > ${}", used, add, self.p.gross_cap_usd));
        }
        if self.p.gross_per_min_usd > 0.0 {
            let k = ((now.wall_ms - r.start_ms).max(0) / 60_000 + 1) as f64;
            let allow = k * self.p.gross_per_min_usd;
            if used + add > allow + 1e-9 {
                return Some(format!("匀速额度 {:.2}+{:.2} > {:.2}", used, add, allow));
            }
        }
        None
    }

    /// 库存上限(10-24 改):本轮最坏亏损 = 已投入成本 − min(Up 股数, Down 股数)(押错边时整轮亏多少)≥ 上限 →
    /// 股数多的那一侧不再加仓(两边股数相等时两侧都不挂:加哪边都会让最坏亏损变大)。
    /// 旧版按"净股数 × 中间价"算:价格一路跌时,早先高价买进的股票按现价只值几分钱,上限挡不住继续接货
    /// (10-24 13:35 那一轮买进 442.9 股 Up / 125.6 股 Down,Down 赢,一轮亏 $20.8)。返回 [挡 UpBid, 挡 DownBid]
    fn inventory_blocked(&self) -> [bool; 2] {
        let Some(r) = self.round.as_ref().and_then(|r| self.ledger.round(r.market_id)) else { return [false, false] };
        let worst = r.cost_usd - r.up_shares.min(r.down_shares);
        if worst < self.p.inventory_cap_usd - 1e-9 {
            return [false, false];
        }
        let d = r.up_shares - r.down_shares;
        [d >= -1e-9, d <= 1e-9]
    }

    /// 平台最低金额要求的最少股数
    fn min_shares(&self, cents: u32) -> u64 {
        let min_cents = (self.p.min_notional_usd * 100.0).round() as u64;
        min_cents.div_ceil(cents.max(1) as u64).max(1)
    }

    /// 本轮两边股数差:按第一次成交先后把 Up、Down 一股股配成对,剩下没配上的那一边 = 多的一边。
    /// 返回 (多的一边, 多出的股数, 多出部分的平均成本 0~1);两边相等返回 None
    pub fn imbalance(&self) -> Option<(QSide, f64, f64)> {
        let m = self.round.as_ref()?.market_id;
        let mut lots: [Vec<(u64, f64, f64)>; 2] = [vec![], vec![]];
        for o in self.orders.values().filter(|o| o.market_id == m && o.filled > 1e-12) {
            lots[o.qside.idx()].push((o.first_fill_ns.unwrap_or(0), o.filled, o.price_cents as f64 / 100.0));
        }
        for l in lots.iter_mut() {
            l.sort_by(|a, b| a.0.cmp(&b.0).then(a.2.total_cmp(&b.2)));
        }
        let (mut i, mut j) = (0, 0);
        let (mut ru, mut rd) = (lots[0].first().map_or(0.0, |x| x.1), lots[1].first().map_or(0.0, |x| x.1));
        while i < lots[0].len() && j < lots[1].len() {
            let q = ru.min(rd);
            ru -= q;
            rd -= q;
            if ru <= 1e-9 {
                i += 1;
                ru = lots[0].get(i).map_or(0.0, |x| x.1);
            }
            if rd <= 1e-9 {
                j += 1;
                rd = lots[1].get(j).map_or(0.0, |x| x.1);
            }
        }
        let (side, k, first_rem) = if i < lots[0].len() {
            (QSide::UpBid, i, ru)
        } else if j < lots[1].len() {
            (QSide::DownBid, j, rd)
        } else {
            return None;
        };
        let l = &lots[side.idx()];
        let mut sh = first_rem;
        let mut cost = first_rem * l[k].2;
        for x in &l[k + 1..] {
            sh += x.1;
            cost += x.1 * x.2;
        }
        (sh > 1e-6).then(|| (side, sh, cost / sh))
    }

    /// 对齐做法下某一侧的出价:补齐的一侧不超过 1 − 多出部分平均成本(这一对不超过 $1)
    fn quote_target(&self, side: QSide, imb: Option<(QSide, f64, f64)>) -> Option<u32> {
        let t = self.target(side)?;
        if !self.p.aligned {
            return Some(t);
        }
        match imb {
            Some((heavy, _, avg)) if heavy != side => {
                let cap = ((1.0 - avg) * 100.0 + 1e-6).floor() as i64;
                if cap < 1 {
                    return None;
                }
                Some(t.min(cap as u32))
            }
            _ => Some(t),
        }
    }

    /// 连接异常:返回 (是否禁止新挂, 是否已断 > feed_down_ms)
    fn feed_health(&self, now: Now) -> (bool, Option<String>) {
        let mut bad = false;
        let mut long: Option<String> = None;
        for (feed, f) in &self.feeds {
            let stale = match feed {
                Feed::Binance => self.p.binance_stale_ms,
                Feed::Predict => self.p.predict_stale_ms,
            } * 1_000_000;
            let since = if !f.connected {
                Some(f.down_since_ns.unwrap_or(0))
            } else {
                match f.last_msg_ns {
                    Some(t) if t + stale < now.mono_ns => Some(t),
                    None => Some(0),
                    _ => None,
                }
            };
            if let Some(s) = since {
                bad = true;
                // 从未连上(启动阶段)不算"断开",只禁止新挂
                if s > 0 && now.mono_ns.saturating_sub(s) > self.p.feed_down_ms * 1_000_000 && f.last_msg_ns.is_some() {
                    long = Some(format!("{feed:?} 断开/无消息 {} ms", (now.mono_ns - s) / 1_000_000));
                }
            }
        }
        (bad, long)
    }

    fn note(&mut self, side: QSide, msg: String, now: Now) -> Option<Action> {
        if self.last_note[side.idx()].as_deref() == Some(msg.as_str()) {
            return None;
        }
        self.last_note[side.idx()] = Some(msg.clone());
        Some(Action::Note { qside: Some(side), msg, ctx: self.ctx(Some(side), now) })
    }

    fn issue_cancel(&mut self, now: Now, cid: u64, qside: QSide, order_id: String, reason: String, rl: &mut RateLimiter) -> Vec<Action> {
        let i = qside.idx();
        let holds = self.slot_holds(i, cid);
        if rl.try_acquire(now.mono_ns, Priority::Cancel) {
            if holds {
                self.slots[i] = Slot::Cancelling { cid, order_id: order_id.clone(), since_ns: now.mono_ns };
            }
            let ctx = self.ctx(Some(qside), now);
            vec![Action::Cancel { cid, qside, order_id, reason, ctx }]
        } else {
            if holds {
                if let Slot::Live { want_cancel, .. } = &mut self.slots[i] {
                    *want_cancel = Some(reason);
                }
            } else if !self.orphans.iter().any(|o| o.2 == order_id) {
                self.orphans.push((cid, qside, order_id, reason));
            }
            vec![]
        }
    }

    fn decide(&mut self, now: Now, rl: &mut RateLimiter) -> Vec<Action> {
        let mut acts = vec![];
        // 先重试孤儿单的撤单(撤单优先)
        for (cid, qside, order_id, reason) in std::mem::take(&mut self.orphans) {
            if self.orders.get(&cid).is_some_and(|o| !o.closed) {
                acts.extend(self.issue_cancel(now, cid, qside, order_id, reason, rl));
            }
        }
        if self.halted.is_some() {
            // 停机后仍然要撤掉任何还活着的单(例如停机后才到的下单回报)
            for side in QSide::ALL {
                if let Slot::Live { cid, order_id, .. } = self.slots[side.idx()].clone() {
                    acts.extend(self.issue_cancel(now, cid, side, order_id, "停机".into(), rl));
                }
            }
            return acts;
        }
        // 风控:亏损上限
        let live = self.round.as_ref().map(|r| (r.market_id, self.up_mid()));
        if let Some(reason) = self.ledger.check_limits(now.wall_ms, live, self.p.max_daily_loss_usd, self.p.max_total_loss_usd) {
            return self.halt(&reason);
        }
        // 连接
        let (feed_bad, feed_long) = self.feed_health(now);
        if let Some(why) = feed_long {
            if self.ready && !self.in_cooldown(now) {
                acts.extend(self.enter_cooldown(now, &why));
                if self.halted.is_some() {
                    return acts;
                }
            }
        }
        // 回报超时
        for i in 0..2 {
            let timeout = self.p.ack_timeout_ms * 1_000_000;
            let stuck = match &self.slots[i] {
                Slot::Placing { cid, sent_ns, .. } if sent_ns + timeout < now.mono_ns => Some((*cid, "下单回报超时")),
                Slot::Cancelling { cid, since_ns, .. } if *since_ns != u64::MAX && since_ns + timeout < now.mono_ns => Some((*cid, "撤单回报超时")),
                _ => None,
            };
            if let Some((cid, why)) = stuck {
                // 放弃跟踪该槽位;订单若实际存在,全撤会兜底,迟到回报会被立刻撤掉
                self.slots[i] = Slot::Empty;
                acts.extend(self.enter_cooldown(now, &format!("{why} cid={cid}")));
                if self.halted.is_some() {
                    return acts;
                }
            }
        }

        let cooldown = self.in_cooldown(now);
        let ms_left = self.ms_left(now);
        let inv_block = self.inventory_blocked();
        let imb = if self.p.aligned { self.imbalance() } else { None };
        // 两边都要挂时用同一个股数:两边各自最低金额要求的股数取大
        let both_shares = QSide::ALL.iter().filter_map(|&s| self.quote_target(s, imb)).map(|c| self.min_shares(c)).max();
        let started = self.round.as_ref().is_some_and(|r| now.wall_ms >= r.start_ms);
        let end_window = ms_left.is_some_and(|l| l <= self.p.stop_before_end_ms);

        // 先撤
        for side in QSide::ALL {
            let i = side.idx();
            let guard = now.mono_ns < self.bn_guard_until[i];
            let keep_reason: Option<String> = if !self.ready {
                Some("未就绪".into())
            } else if cooldown {
                Some("冷却中".into())
            } else if self.round.is_none() || !started {
                Some("无当前轮/未开盘".into())
            } else if end_window {
                Some(format!("剩余 ≤ {}s", self.p.stop_before_end_ms / 1000))
            } else if guard {
                Some("币安急动".into())
            } else if inv_block[side.idx()] {
                Some(format!("本轮最坏亏损 ≥ ${}", self.p.inventory_cap_usd))
            } else if let Some((_, sh, _)) = imb.filter(|x| x.0 == side) {
                Some(format!("等另一边补齐 {sh:.3} 股"))
            } else {
                None
            };
            let target = self.quote_target(side, imb);
            if let Slot::Live { cid, order_id, price, placed_ns, want_cancel, .. } = self.slots[i].clone() {
                let reason = if let Some(r) = &keep_reason {
                    Some(r.clone())
                } else if let Some(r) = want_cancel {
                    Some(r)
                } else if self.p.aligned && imb.is_some_and(|x| x.0 != side) && target.is_none_or(|t| price > t) {
                    // 补齐那一腿:挂价高于补齐价上限(会让这一对超过 $1)立刻撤,不等 1 秒
                    Some(format!("补齐价上限 {price}→{}", target.map_or("无".into(), |t| t.to_string())))
                } else if target.is_some_and(|t| t != price) && now.mono_ns >= placed_ns + self.p.reprice_min_age_ms * 1_000_000 {
                    Some(format!("改价 {price}→{}", target.unwrap()))
                } else {
                    None
                };
                if let Some(r) = reason {
                    acts.extend(self.issue_cancel(now, cid, side, order_id, r, rl));
                }
            }
            // 再挂
            if self.slots[i] != Slot::Empty {
                continue;
            }
            let block = keep_reason.or_else(|| {
                if feed_bad {
                    Some("行情连接异常".into())
                } else if target.is_none() {
                    Some("该侧无最优价".into())
                } else {
                    None
                }
            });
            if let Some(b) = block {
                acts.extend(self.note(side, b, now));
                continue;
            }
            let price = target.unwrap();
            let shares = if !self.p.aligned {
                self.min_shares(price)
            } else if let Some((_, sh, _)) = imb {
                // 补齐:补多出的股数(取整),但不少于平台最低金额要求
                ((sh - 1e-6).ceil() as u64).max(self.min_shares(price))
            } else {
                both_shares.unwrap_or_else(|| self.min_shares(price)).max(self.min_shares(price))
            };
            let gb = if now.mono_ns < self.collateral_wait_until {
                Some("保证金不足,等上一轮自动领回".to_string())
            } else {
                self.gross_blocked(now, shares as f64 * price as f64 / 100.0)
            };
            if let Some(b) = gb {
                acts.extend(self.note(side, b, now));
                continue;
            }
            if !rl.try_acquire(now.mono_ns, Priority::Place) {
                acts.extend(self.note(side, "限额预算不足,暂缓新挂".into(), now));
                continue;
            }
            let cid = self.next_cid;
            self.next_cid += 1;
            let market_id = self.round.as_ref().unwrap().market_id;
            self.orders.insert(
                cid,
                OrderRec { cid, qside: side, market_id, price_cents: price, shares, order_id: None, hash: None, filled: 0.0, closed: false, acked_ns: None, first_fill_ns: None },
            );
            self.slots[i] = Slot::Placing { cid, price, shares, sent_ns: now.mono_ns };
            let mut ctx = self.ctx(Some(side), now);
            ctx.queue_ahead = self.book.as_ref().map(|b| match side {
                QSide::UpBid => b.bid_size_at(price),
                QSide::DownBid => b.ask_size_at(100 - price),
            });
            self.last_note[i] = None;
            let reason = match imb {
                Some((h, sh, avg)) if self.p.aligned && h != side => format!("补齐 {sh:.3} 股(多出部分均价 {avg:.3})"),
                _ if self.p.aligned => "两边同股数挂最优价".into(),
                _ => "挂最优价".into(),
            };
            acts.push(Action::Place { cid, qside: side, market_id, price_cents: price, shares, reason, ctx });
        }
        acts
    }
}
