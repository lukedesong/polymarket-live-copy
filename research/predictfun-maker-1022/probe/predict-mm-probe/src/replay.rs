//! 回测引擎:把录制的盘口 / 币安 / 链上挂单成交按时间喂给实盘的同一份策略代码(Strategy),
//! 模拟交易所:挂单延迟、撤单延迟、排队与成交(analysis/fill_model.py 校准过的规则)、"已撮合"推送延迟、只挂单被拒。
//!
//! 排队规则(和 fill_model.py 一致):挂单生效时前面的量 = 当时盘口这一档的量;之后链上这一队每笔挂单方成交先吃前面的量,
//! 吃穿了才轮到我们;盘口变多 = 有人排在后面,变少且不是成交 = 撤单,按 CancelMode 记在前面或后面。
//! 回测里我们的单不在录制的盘口里,所以不用扣自己。
use crate::msgs::BookSnapshot;
use crate::ratelimit::{RateLimitConfig, RateLimiter};
use crate::risk::Ledger;
use crate::strategy::{Action, Feed, Now, Params, QSide, RoundCtx, Strategy};
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum CancelMode {
    /// 撤单全记在后面
    Pess,
    /// 先记后面,后面撤完剩下的一半记前面
    Mixed,
    /// 全记在前面(10-24 校准:张数比 1.03、股数比 1.00)
    Opt,
    /// 按撤单那一刻前后的量按比例分摊(先挂先成 + 新单只排队尾推出,无参数;10-24 校准股数比 0.88,偏保守)
    Prop,
}

#[derive(Debug, Clone)]
pub struct ReplayCfg {
    /// 下单发出 → 在交易所生效(开始排队)
    pub place_live_ms: i64,
    /// 下单发出 → 收到回报
    pub place_ack_ms: i64,
    /// 撤单发出 → 生效(之后不会再成交)
    pub cancel_eff_ms: i64,
    /// 撤单发出 → 收到确认
    pub cancel_ack_ms: i64,
    /// 撮合 → 策略收到成交通知("已撮合"推送约 270ms;旧版等"上链成功"约 2300ms)
    pub fill_notify_ms: i64,
    /// 只挂单被拒:回报之后多久收到"未接受"并当关闭处理
    pub reject_close_ms: i64,
    /// 收盘后多久拿到结果
    pub resolve_after_ms: i64,
    pub tick_ms: i64,
    pub cancel_mode: CancelMode,
    pub rate: RateLimitConfig,
}

impl Default for ReplayCfg {
    fn default() -> Self {
        // 10-24 实盘实测:接受 22ms、下单往返 42ms、撤单生效 11ms、撤单往返 29ms、"已撮合"推送 0.27s
        Self {
            place_live_ms: 22,
            place_ack_ms: 42,
            cancel_eff_ms: 11,
            cancel_ack_ms: 29,
            fill_notify_ms: 270,
            reject_close_ms: 300,
            resolve_after_ms: 60_000,
            tick_ms: 50,
            cancel_mode: CancelMode::Opt,
            rate: RateLimitConfig::default(),
        }
    }
}

/// 一条录制的事件(本机毫秒)
#[derive(Debug, Clone)]
pub enum DataEv {
    Book(i64, BookSnapshot),
    Binance(i64, f64),
    /// 链上挂单方成交:撮合时刻、市场、队、档位(分)、股数
    Fill(i64, u64, QSide, u32, f64),
}

impl DataEv {
    pub fn t(&self) -> i64 {
        match self {
            DataEv::Book(t, _) | DataEv::Binance(t, _) | DataEv::Fill(t, ..) => *t,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RoundData {
    pub market_id: u64,
    pub start_ms: i64,
    pub end_ms: i64,
    /// 结果:Up 每股兑付 1 / 0 / 0.5(平局);None = 不知道(不结算,只记持仓)
    pub payout_up: Option<f64>,
    pub events: Vec<DataEv>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RoundResult {
    pub market_id: u64,
    pub start_ms: i64,
    pub payout_up: Option<f64>,
    pub places: u64,
    pub cancels: u64,
    pub rejects: u64,
    pub fills: u64,
    pub up_shares: f64,
    pub down_shares: f64,
    pub cost: f64,
    /// 配成对的股数(按成交先后一股股配)与这些对的平均成本(每对 Up+Down)
    pub pairs: f64,
    pub pair_cost: Option<f64>,
    pub unpaired: f64,
    pub pnl: Option<f64>,
    pub halted: Option<String>,
}

#[derive(Debug, Clone)]
struct VOrder {
    cid: u64,
    market_id: u64,
    qside: QSide,
    level: u32,
    rem: f64,
    filled: f64,
    /// 已在交易所生效、还没撤掉
    live: bool,
    live_at: i64,
    ahead: f64,
    behind: f64,
    prev: Option<f64>,
    fsince: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Sched {
    PlaceLive(String),
    PlaceAck(String),
    RejectClose(String),
    CancelEff(String),
    CancelAck(String),
    FillNotify(String, u64),
    Resolve(usize),
}

fn level_size(b: &BookSnapshot, qside: QSide, level: u32) -> f64 {
    match qside {
        QSide::UpBid => b.bid_size_at(level),
        QSide::DownBid => b.ask_size_at(100 - level),
    }
}

/// 会和对面直接成交(只挂单被拒):买 Up 价 ≥ Up 卖一;买 Down 价 ≥ 1 − Up 买一
fn crosses(b: &BookSnapshot, qside: QSide, level: u32) -> bool {
    match qside {
        QSide::UpBid => b.best_ask_cents().is_some_and(|a| level >= a),
        QSide::DownBid => b.best_bid_cents().is_some_and(|bb| level >= 100 - bb),
    }
}

pub struct Engine {
    pub cfg: ReplayCfg,
    pub strat: Strategy,
    rl: RateLimiter,
    now_ms: i64,
    orders: HashMap<String, VOrder>,
    next_oid: u64,
    sched: BinaryHeap<Reverse<(i64, u64, Sched)>>,
    seq: u64,
    books: HashMap<u64, BookSnapshot>,
    rounds: Vec<RoundData>,
    results: Vec<RoundResult>,
    /// 每轮我们的成交(时刻, 队, 价, 股数),配对用
    our_fills: HashMap<u64, Vec<(i64, QSide, f64, f64)>>,
    cur_round: Option<usize>,
    next_tick: i64,
}

impl Engine {
    pub fn new(params: Params, cfg: ReplayCfg) -> Self {
        let rl = RateLimiter::new(cfg.rate.clone());
        Self {
            strat: Strategy::new(params, Ledger::default()),
            cfg,
            rl,
            now_ms: 0,
            orders: HashMap::new(),
            next_oid: 1,
            sched: BinaryHeap::new(),
            seq: 0,
            books: HashMap::new(),
            rounds: vec![],
            results: vec![],
            our_fills: HashMap::new(),
            cur_round: None,
            next_tick: 0,
        }
    }

    fn now(&self) -> Now {
        Now { mono_ns: (self.now_ms as u64) * 1_000_000, wall_ms: self.now_ms }
    }

    fn at(&mut self, t: i64, s: Sched) {
        self.seq += 1;
        self.sched.push(Reverse((t, self.seq, s)));
    }

    /// 跑一串按开盘时间排好、相邻的轮。返回每轮结果
    pub fn run(mut self, rounds: Vec<RoundData>) -> Vec<RoundResult> {
        let n = rounds.len();
        self.rounds = rounds;
        self.results = self.rounds.iter().map(|r| RoundResult { market_id: r.market_id, start_ms: r.start_ms, payout_up: r.payout_up, ..Default::default() }).collect();
        let Some(first) = self.rounds.first() else { return vec![] };
        self.now_ms = first.start_ms - 90_000;
        self.next_tick = self.now_ms;
        let now = self.now();
        let mut rl = std::mem::replace(&mut self.rl, RateLimiter::new(RateLimitConfig::default()));
        self.strat.on_feed_state(now, Feed::Binance, true, &mut rl);
        self.strat.on_feed_state(now, Feed::Predict, true, &mut rl);
        let acts = self.strat.set_ready(now, &mut rl);
        self.rl = rl;
        self.exec(acts);
        // 第 k 段 = [开盘_k − 90 秒, 开盘_{k+1} − 90 秒):第 k 轮文件里落在这段的事件 + 上一轮文件里还没处理的(上一轮的尾巴)。
        // 币安:两轮文件重叠的那段是同一批数据,第 k 轮文件超出本段的币安丢掉(下一轮文件里有)
        let mut carry: Vec<DataEv> = vec![];
        for k in 0..n {
            let seg1 = if k + 1 < n { self.rounds[k + 1].start_ms - 90_000 } else { i64::MAX };
            let mine = std::mem::take(&mut self.rounds[k].events);
            let (mut evs, later): (Vec<DataEv>, Vec<DataEv>) = mine.into_iter().partition(|e| e.t() < seg1);
            let (now_c, later_c): (Vec<DataEv>, Vec<DataEv>) = std::mem::take(&mut carry).into_iter().partition(|e| e.t() < seg1);
            evs.extend(now_c);
            carry = later.into_iter().filter(|e| !matches!(e, DataEv::Binance(..))).chain(later_c).collect();
            evs.sort_by_key(|e| (e.t(), matches!(e, DataEv::Fill(..)) as u8));
            for e in evs {
                self.advance_to(e.t());
                self.on_data(e);
            }
        }
        let end = self.rounds[n - 1].end_ms + self.cfg.resolve_after_ms + 1;
        self.advance_to(end);
        self.finish()
    }

    fn finish(mut self) -> Vec<RoundResult> {
        for (i, r) in self.results.iter_mut().enumerate() {
            let m = self.rounds[i].market_id;
            let fills = self.our_fills.remove(&m).unwrap_or_default();
            let (pairs, pair_cost, unpaired) = pair_up(&fills);
            r.pairs = pairs;
            r.pair_cost = pair_cost;
            r.unpaired = unpaired;
            r.fills = fills.len() as u64;
            r.up_shares = fills.iter().filter(|f| f.1 == QSide::UpBid).map(|f| f.3).sum();
            r.down_shares = fills.iter().filter(|f| f.1 == QSide::DownBid).map(|f| f.3).sum();
            r.cost = fills.iter().map(|f| f.2 * f.3).sum();
            if r.pnl.is_none() {
                if let Some(pu) = r.payout_up {
                    r.pnl = Some(r.up_shares * pu + r.down_shares * (1.0 - pu) - r.cost);
                }
            }
        }
        self.results
    }

    /// 把时间推进到 t:先处理 t 之前到期的内部事件(回报、生效、成交通知、结算)和 tick、换轮
    fn advance_to(&mut self, t: i64) {
        loop {
            let next_sched = self.sched.peek().map(|Reverse((ts, _, _))| *ts);
            let next_round = self.next_round_switch();
            let mut cand = vec![(self.next_tick, 0u8)];
            if let Some(ts) = next_sched {
                cand.push((ts, 1));
            }
            if let Some(ts) = next_round {
                cand.push((ts, 2));
            }
            let (ts, which) = *cand.iter().min().unwrap();
            if ts > t {
                break;
            }
            self.now_ms = ts.max(self.now_ms);
            match which {
                0 => {
                    self.next_tick = ts + self.cfg.tick_ms;
                    let now = self.now();
                    let acts = self.with_rl(|s, rl| s.on_tick(now, rl));
                    self.exec(acts);
                }
                1 => {
                    let Reverse((_, _, s)) = self.sched.pop().unwrap();
                    self.on_sched(s);
                }
                _ => self.switch_round(),
            }
        }
        self.now_ms = t.max(self.now_ms);
    }

    fn next_round_switch(&self) -> Option<i64> {
        let k = self.cur_round.map_or(0, |k| k + 1);
        self.rounds.get(k).map(|r| r.start_ms)
    }

    fn switch_round(&mut self) {
        let k = self.cur_round.map_or(0, |k| k + 1);
        self.cur_round = Some(k);
        let (m, s, e) = (self.rounds[k].market_id, self.rounds[k].start_ms, self.rounds[k].end_ms);
        let now = self.now();
        let acts = self.with_rl(|st, rl| st.set_round(now, RoundCtx { market_id: m, start_ms: s, end_ms: e }, rl));
        self.exec(acts);
        if let Some(b) = self.books.get(&m).cloned() {
            let acts = self.with_rl(|st, rl| st.on_book(now, b, rl));
            self.exec(acts);
        }
        self.at(e + self.cfg.resolve_after_ms, Sched::Resolve(k));
    }

    fn with_rl<T>(&mut self, f: impl FnOnce(&mut Strategy, &mut RateLimiter) -> T) -> T {
        f(&mut self.strat, &mut self.rl)
    }

    fn on_data(&mut self, e: DataEv) {
        let now = self.now();
        match e {
            DataEv::Book(_, b) => {
                let m = b.market_id;
                self.update_queues_on_book(&b);
                self.books.insert(m, b.clone());
                self.strat.feed_msg(now, Feed::Predict);
                let acts = self.with_rl(|s, rl| s.on_book(now, b, rl));
                self.exec(acts);
            }
            DataEv::Binance(_, mid) => {
                let acts = self.with_rl(|s, rl| s.on_binance(now, mid, rl));
                self.exec(acts);
            }
            DataEv::Fill(t, m, qside, level, q) => self.on_chain_fill(t, m, qside, level, q),
        }
    }

    fn update_queues_on_book(&mut self, b: &BookSnapshot) {
        let mode = self.cfg.cancel_mode;
        for o in self.orders.values_mut().filter(|o| o.live && o.market_id == b.market_id && o.rem > 1e-9) {
            let x = level_size(b, o.qside, o.level);
            if let Some(prev) = o.prev {
                let d = x - prev;
                if d > 0.0 {
                    o.behind += d;
                } else if d < 0.0 {
                    let c = (-d - o.fsince).max(0.0);
                    match mode {
                        CancelMode::Pess => o.behind = (o.behind - c).max(0.0),
                        CancelMode::Mixed => {
                            let cb = o.behind.min(c);
                            o.behind -= cb;
                            o.ahead = (o.ahead - 0.5 * (c - cb)).max(0.0);
                        }
                        CancelMode::Opt => {
                            let ca = o.ahead.min(c);
                            o.ahead -= ca;
                            o.behind = (o.behind - (c - ca)).max(0.0);
                        }
                        CancelMode::Prop => {
                            let tot = o.ahead + o.behind;
                            let ca = if tot > 1e-12 { c * o.ahead / tot } else { 0.0 };
                            o.ahead = (o.ahead - ca).max(0.0);
                            o.behind = (o.behind - (c - ca)).max(0.0);
                        }
                    }
                }
                o.ahead = o.ahead.min(x.max(0.0));
            }
            o.prev = Some(x);
            o.fsince = 0.0;
        }
    }

    fn on_chain_fill(&mut self, t: i64, m: u64, qside: QSide, level: u32, q: f64) {
        let mut ids: Vec<(i64, String)> =
            self.orders.iter().filter(|(_, o)| o.live && o.market_id == m && o.qside == qside && o.level == level && o.rem > 1e-9).map(|(k, o)| (o.live_at, k.clone())).collect();
        ids.sort();
        let mut left = q;
        let mut notify = vec![];
        for (_, oid) in ids {
            let o = self.orders.get_mut(&oid).unwrap();
            o.fsince += q;
            if left <= 1e-9 {
                continue;
            }
            let take = left.min(o.ahead);
            o.ahead -= take;
            let rest = left - take;
            if rest > 1e-9 {
                let f = rest.min(o.rem);
                o.rem -= f;
                o.filled += f;
                left -= f;
                let price = o.level as f64 / 100.0;
                self.our_fills.entry(m).or_default().push((t, qside, price, f));
                notify.push((oid.clone(), (o.filled * 1e9).round() as u64));
            }
        }
        for (oid, cum) in notify {
            self.at(t + self.cfg.fill_notify_ms, Sched::FillNotify(oid, cum));
        }
    }

    fn on_sched(&mut self, s: Sched) {
        let now = self.now();
        match s {
            Sched::PlaceLive(oid) => {
                let Some(o) = self.orders.get(&oid) else { return };
                let (m, qside, level) = (o.market_id, o.qside, o.level);
                let book = self.books.get(&m).cloned();
                if book.as_ref().is_some_and(|b| crosses(b, qside, level)) {
                    // 只挂单:会直接成交就被拒(回报照常 OK,之后收到"未接受")
                    self.orders.get_mut(&oid).unwrap().rem = 0.0;
                    if let Some(i) = self.round_idx(m) {
                        self.results[i].rejects += 1;
                    }
                    self.at(self.now_ms + self.cfg.reject_close_ms, Sched::RejectClose(oid));
                    return;
                }
                let o = self.orders.get_mut(&oid).unwrap();
                o.live = true;
                o.live_at = self.now_ms;
                let x = book.as_ref().map_or(0.0, |b| level_size(b, qside, level));
                o.ahead = x;
                o.prev = Some(x);
            }
            Sched::PlaceAck(oid) => {
                let Some(o) = self.orders.get(&oid) else { return };
                let cid = o.cid;
                let acts = self.with_rl(|st, rl| st.on_place_ok(now, cid, oid.clone(), None, rl));
                self.exec(acts);
            }
            Sched::RejectClose(oid) => {
                let acts = self.with_rl(|st, rl| st.on_order_closed(now, &oid, rl));
                self.exec(acts);
            }
            Sched::CancelEff(oid) => {
                if let Some(o) = self.orders.get_mut(&oid) {
                    o.live = false;
                }
            }
            Sched::CancelAck(oid) => {
                let acts = self.with_rl(|st, rl| st.on_cancel_ok(now, &oid, rl));
                self.exec(acts);
            }
            Sched::FillNotify(oid, cum) => {
                let cum = cum as f64 / 1e9;
                let (_, acts) = self.with_rl(|st, rl| st.on_fill_set(now, &oid, cum, false, rl));
                self.exec(acts);
            }
            Sched::Resolve(k) => {
                let (m, pu) = (self.rounds[k].market_id, self.rounds[k].payout_up);
                if let Some(pu) = pu {
                    let pnl = self.strat.on_resolution(m, pu, 1.0 - pu);
                    self.results[k].pnl = pnl;
                }
            }
        }
    }

    fn round_idx(&self, m: u64) -> Option<usize> {
        self.rounds.iter().position(|r| r.market_id == m)
    }

    fn exec(&mut self, acts: Vec<Action>) {
        for a in acts {
            match a {
                Action::Place { cid, qside, market_id, price_cents, shares, .. } => {
                    let oid = format!("r{}", self.next_oid);
                    self.next_oid += 1;
                    self.orders.insert(
                        oid.clone(),
                        VOrder {
                            cid,
                            market_id,
                            qside,
                            level: price_cents,
                            rem: shares as f64,
                            filled: 0.0,
                            live: false,
                            live_at: 0,
                            ahead: 0.0,
                            behind: 0.0,
                            prev: None,
                            fsince: 0.0,
                        },
                    );
                    if let Some(i) = self.round_idx(market_id) {
                        self.results[i].places += 1;
                    }
                    self.at(self.now_ms + self.cfg.place_live_ms, Sched::PlaceLive(oid.clone()));
                    self.at(self.now_ms + self.cfg.place_ack_ms, Sched::PlaceAck(oid));
                }
                Action::Cancel { order_id, .. } => self.cancel(order_id),
                Action::CancelAll { known, .. } => {
                    for oid in known {
                        self.cancel(oid);
                    }
                }
                Action::Halt { reason } => {
                    if let Some(k) = self.cur_round {
                        self.results[k].halted.get_or_insert(reason);
                    }
                }
                Action::Note { .. } => {}
            }
        }
    }

    fn cancel(&mut self, oid: String) {
        if let Some(o) = self.orders.get(&oid) {
            if let Some(i) = self.round_idx(o.market_id) {
                self.results[i].cancels += 1;
            }
        }
        self.at(self.now_ms + self.cfg.cancel_eff_ms, Sched::CancelEff(oid.clone()));
        self.at(self.now_ms + self.cfg.cancel_ack_ms, Sched::CancelAck(oid));
    }
}

/// 按成交先后把 Up、Down 一股股配成对:(配成的股数, 每对平均成本, 没配上的股数)
pub fn pair_up(fills: &[(i64, QSide, f64, f64)]) -> (f64, Option<f64>, f64) {
    let mut q: [Vec<(f64, f64)>; 2] = [vec![], vec![]];
    let mut sorted = fills.to_vec();
    sorted.sort_by_key(|f| f.0);
    for f in &sorted {
        q[(f.1 == QSide::DownBid) as usize].push((f.3, f.2));
    }
    let (mut i, mut j, mut pairs, mut cost) = (0, 0, 0.0, 0.0);
    let (mut a, mut b) = (q[0].first().map_or(0.0, |x| x.0), q[1].first().map_or(0.0, |x| x.0));
    while i < q[0].len() && j < q[1].len() {
        let k = a.min(b);
        pairs += k;
        cost += k * (q[0][i].1 + q[1][j].1);
        a -= k;
        b -= k;
        if a <= 1e-9 {
            i += 1;
            a = q[0].get(i).map_or(0.0, |x| x.0);
        }
        if b <= 1e-9 {
            j += 1;
            b = q[1].get(j).map_or(0.0, |x| x.0);
        }
    }
    let total: f64 = fills.iter().map(|f| f.3).sum();
    (pairs, (pairs > 1e-9).then(|| cost / pairs), total - 2.0 * pairs)
}

/// 每轮结果汇总(打印用)
pub fn summary(rs: &[RoundResult]) -> BTreeMap<&'static str, f64> {
    let done: Vec<&RoundResult> = rs.iter().filter(|r| r.pnl.is_some()).collect();
    let n = done.len() as f64;
    let pnl: Vec<f64> = done.iter().map(|r| r.pnl.unwrap()).collect();
    let mean = pnl.iter().sum::<f64>() / n.max(1.0);
    let sd = (pnl.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0).max(1.0)).sqrt();
    let mut m = BTreeMap::new();
    m.insert("rounds", n);
    m.insert("pnl_total", pnl.iter().sum());
    m.insert("pnl_mean", mean);
    m.insert("pnl_t", if sd > 0.0 { mean / (sd / n.sqrt()) } else { 0.0 });
    m.insert("pnl_worst", pnl.iter().cloned().fold(f64::INFINITY, f64::min));
    m.insert("win_rate", pnl.iter().filter(|x| **x > 0.0).count() as f64 / n.max(1.0));
    m.insert("fills_mean", done.iter().map(|r| r.fills as f64).sum::<f64>() / n.max(1.0));
    m.insert("unpaired_mean", done.iter().map(|r| r.unpaired).sum::<f64>() / n.max(1.0));
    let (pq, pc): (f64, f64) = done.iter().filter_map(|r| r.pair_cost.map(|c| (r.pairs, r.pairs * c))).fold((0.0, 0.0), |a, b| (a.0 + b.0, a.1 + b.1));
    m.insert("pair_cost_avg", if pq > 0.0 { pc / pq } else { 0.0 });
    m.insert("pairs_mean", pq / n.max(1.0));
    m
}
