//! 本地限额预算。平台按接口类别分开限额(10-24 Luke 截图,平台后台"速率限制"页):
//! - 交易(下单、撤单):每分钟 500,另有每秒 40 的突发上限;
//! - 通用(市场、订单簿、账户读取、查单等,交易和搜索以外的所有请求):每分钟 250;
//! - 搜索:每分钟 120(探针不用)。
//!
//! 两类各用一个滑动窗口计数:保证**任意** 1 秒窗口内 ≤ 每秒预算、**任意** 60 秒窗口内 ≤ 每分钟预算
//! (普通令牌桶在 60 秒里最多能放出 容量 + 速率·60 个,会超过平台的分钟限额,所以不用)。
//! 预算 = 平台限额 × (1 − 余量),默认余量 20%。
//! 交易额度内:撤单 > 新挂,新挂必须给撤单留出保留额度,保证任何时候都撤得动。
//! 通用额度内:紧急查询(全撤前列挂单、鉴权)> 普通查询 > 尽力查询(额度剩一半以上才发,否则跳过)。

use std::collections::VecDeque;

/// 平台限额(提额批下来后改这里;--send 时不允许超过)
pub const PLATFORM_TRADE_PER_SEC: u32 = 40;
pub const PLATFORM_TRADE_PER_MIN: u32 = 500;
pub const PLATFORM_GENERAL_PER_MIN: u32 = 250;
/// 通用类平台没写每秒上限,按交易的每秒上限保守处理
pub const PLATFORM_GENERAL_PER_SEC: u32 = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    /// 撤单(交易额度)
    Cancel = 0,
    /// 新挂(交易额度)
    Place = 1,
    /// 紧急查询:全撤前列挂单、鉴权(通用额度)
    QueryUrgent = 2,
    /// 普通查询(通用额度)
    Query = 3,
    /// 尽力查询:只在通用额度剩一半以上时发,拿不到就跳过(通用额度)
    Background = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bucket {
    Trade,
    General,
}

impl Priority {
    pub fn bucket(self) -> Bucket {
        match self {
            Priority::Cancel | Priority::Place => Bucket::Trade,
            _ => Bucket::General,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// 交易类(下单、撤单)
    pub per_sec: u32,
    pub per_min: u32,
    /// 通用类(查询)
    pub general_per_sec: u32,
    pub general_per_min: u32,
    pub margin: f64,
    /// 新挂单时交易额度在两个窗口里各必须剩下多少给撤单
    pub place_reserve_sec: u32,
    pub place_reserve_min: u32,
    /// 普通查询时通用额度必须剩下多少给紧急查询
    pub query_reserve_sec: u32,
    pub query_reserve_min: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            per_sec: PLATFORM_TRADE_PER_SEC,
            per_min: PLATFORM_TRADE_PER_MIN,
            general_per_sec: PLATFORM_GENERAL_PER_SEC,
            general_per_min: PLATFORM_GENERAL_PER_MIN,
            margin: 0.2,
            place_reserve_sec: 4,
            place_reserve_min: 40,
            query_reserve_sec: 3,
            query_reserve_min: 20,
        }
    }
}

/// 一类额度的滑动窗口
#[derive(Debug, Clone)]
struct Window {
    cap_sec: u32,
    cap_min: u32,
    /// 最近 60 秒内已发请求的单调时钟纳秒
    sent: VecDeque<u64>,
}

const SEC: u64 = 1_000_000_000;
const MIN: u64 = 60 * SEC;

impl Window {
    fn new(per_sec: u32, per_min: u32, margin: f64) -> Self {
        let cap = |x: u32| ((x as f64) * (1.0 - margin)).floor().max(1.0) as u32;
        Self { cap_sec: cap(per_sec), cap_min: cap(per_min), sent: VecDeque::new() }
    }

    fn prune(&mut self, now: u64) {
        while let Some(&t) = self.sent.front() {
            if t + MIN <= now {
                self.sent.pop_front();
            } else {
                break;
            }
        }
    }

    fn used(&mut self, now: u64) -> (u32, u32) {
        self.prune(now);
        let in_sec = self.sent.iter().rev().take_while(|&&t| t + SEC > now).count() as u32;
        (in_sec, self.sent.len() as u32)
    }

    fn remaining(&mut self, now: u64) -> (u32, u32) {
        let (s, m) = self.used(now);
        (self.cap_sec.saturating_sub(s), self.cap_min.saturating_sub(m))
    }

    fn take(&mut self, now: u64, need_s: u32, need_m: u32) -> bool {
        let (rs, rm) = self.remaining(now);
        if rs >= need_s && rm >= need_m {
            self.sent.push_back(now);
            true
        } else {
            false
        }
    }

    /// 最早何时(单调纳秒)能再拿到一个
    fn next_slot(&mut self, now: u64) -> u64 {
        let (rs, rm) = self.remaining(now);
        if rs >= 1 && rm >= 1 {
            return now;
        }
        let mut t = now;
        if rs == 0 {
            // 最近 1 秒里最早的一条过期
            let k = self.sent.len() - self.sent.iter().rev().take_while(|&&x| x + SEC > now).count();
            t = t.max(self.sent[k] + SEC);
        }
        if rm == 0 {
            t = t.max(self.sent[0] + MIN);
        }
        t
    }
}

#[derive(Debug, Clone)]
pub struct RateLimiter {
    cfg: RateLimitConfig,
    trade: Window,
    general: Window,
    /// 服务端 429 或限额头提示后,在此之前只允许撤单和紧急查询
    pause_until_ns: u64,
}

impl RateLimiter {
    pub fn new(cfg: RateLimitConfig) -> Self {
        let trade = Window::new(cfg.per_sec, cfg.per_min, cfg.margin);
        let general = Window::new(cfg.general_per_sec, cfg.general_per_min, cfg.margin);
        Self { cfg, trade, general, pause_until_ns: 0 }
    }

    /// 交易额度的 (每秒, 每分钟) 预算
    pub fn caps(&self) -> (u32, u32) {
        (self.trade.cap_sec, self.trade.cap_min)
    }

    /// 通用额度的 (每秒, 每分钟) 预算
    pub fn general_caps(&self) -> (u32, u32) {
        (self.general.cap_sec, self.general.cap_min)
    }

    /// 交易额度:(过去 1 秒已用, 过去 60 秒已用)
    pub fn used(&mut self, now: u64) -> (u32, u32) {
        self.trade.used(now)
    }

    /// 交易额度剩余 (每秒, 每分钟)
    pub fn remaining(&mut self, now: u64) -> (u32, u32) {
        self.trade.remaining(now)
    }

    /// 通用额度剩余 (每秒, 每分钟)
    pub fn general_remaining(&mut self, now: u64) -> (u32, u32) {
        self.general.remaining(now)
    }

    pub fn try_acquire(&mut self, now: u64, prio: Priority) -> bool {
        if !matches!(prio, Priority::Cancel | Priority::QueryUrgent) && now < self.pause_until_ns {
            return false;
        }
        let c = &self.cfg;
        match prio {
            Priority::Cancel => self.trade.take(now, 1, 1),
            Priority::Place => self.trade.take(now, 1 + c.place_reserve_sec, 1 + c.place_reserve_min),
            Priority::QueryUrgent => self.general.take(now, 1, 1),
            Priority::Query => self.general.take(now, 1 + c.query_reserve_sec, 1 + c.query_reserve_min),
            Priority::Background => {
                let (cs, cm) = (self.general.cap_sec, self.general.cap_min);
                self.general.take(now, (cs / 2).max(1 + c.query_reserve_sec), (cm / 2).max(1 + c.query_reserve_min))
            }
        }
    }

    /// 最早何时(单调纳秒)能再拿到一个撤单令牌
    pub fn next_cancel_slot(&mut self, now: u64) -> u64 {
        self.trade.next_slot(now)
    }

    /// 收到 429 或限额头显示快用完:在 until 之前只放撤单和紧急查询
    pub fn pause_non_cancel(&mut self, until_ns: u64) {
        self.pause_until_ns = self.pause_until_ns.max(until_ns);
    }
}

/// 从响应头 `ratelimit` 里容错解析剩余量。兼容 IETF 草案两种写法:
/// `limit=40, remaining=12, reset=1` 与 `"default";r=12;t=1`。多条策略取最小值。
pub fn parse_ratelimit_remaining(header: &str) -> Option<u64> {
    let mut best: Option<u64> = None;
    for part in header.split([',', ';']) {
        let p = part.trim();
        let val = p.strip_prefix("remaining=").or_else(|| p.strip_prefix("r="));
        if let Some(v) = val.and_then(|v| v.trim().trim_matches('"').parse::<u64>().ok()) {
            best = Some(best.map_or(v, |b| b.min(v)));
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_parse() {
        assert_eq!(parse_ratelimit_remaining("limit=40, remaining=12, reset=1"), Some(12));
        assert_eq!(parse_ratelimit_remaining("\"sec\";r=30;t=1, \"min\";r=480;t=60"), Some(30));
        assert_eq!(parse_ratelimit_remaining("garbage"), None);
    }
}
