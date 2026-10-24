//! 本地限额预算(实测:每个 API key 每秒 40、每分钟 500,全站所有接口共用)。
//!
//! 按"令牌桶"语义对外(try_acquire 取令牌),内部用滑动窗口计数实现:保证**任意** 1 秒窗口内
//! ≤ 每秒预算、**任意** 60 秒窗口内 ≤ 每分钟预算。普通令牌桶(容量 C、速率 r)在 60 秒里最多能放出
//! C + r·60 个,会超过平台的分钟限额,所以不用。
//!
//! 预算 = 平台限额 × (1 − 余量),默认余量 20% → 每秒 32、每分钟 400。
//! 优先级:撤单 > 新挂 > 查询。新挂和查询必须给撤单留出保留额度,保证任何时候都撤得动。

use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Cancel = 0,
    Place = 1,
    Query = 2,
}

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub per_sec: u32,
    pub per_min: u32,
    pub margin: f64,
    /// 新挂单时必须在两个窗口里各剩下多少给撤单
    pub place_reserve_sec: u32,
    pub place_reserve_min: u32,
    /// 查询时必须剩下多少(比新挂更多)
    pub query_reserve_sec: u32,
    pub query_reserve_min: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self { per_sec: 40, per_min: 500, margin: 0.2, place_reserve_sec: 4, place_reserve_min: 40, query_reserve_sec: 10, query_reserve_min: 120 }
    }
}

#[derive(Debug, Clone)]
pub struct RateLimiter {
    cap_sec: u32,
    cap_min: u32,
    cfg: RateLimitConfig,
    /// 最近 60 秒内已发请求的单调时钟纳秒
    sent: VecDeque<u64>,
    /// 服务端 429 或限额头提示后,在此之前只允许撤单
    pause_until_ns: u64,
}

const SEC: u64 = 1_000_000_000;
const MIN: u64 = 60 * SEC;

impl RateLimiter {
    pub fn new(cfg: RateLimitConfig) -> Self {
        let cap_sec = ((cfg.per_sec as f64) * (1.0 - cfg.margin)).floor().max(1.0) as u32;
        let cap_min = ((cfg.per_min as f64) * (1.0 - cfg.margin)).floor().max(1.0) as u32;
        Self { cap_sec, cap_min, cfg, sent: VecDeque::new(), pause_until_ns: 0 }
    }

    pub fn caps(&self) -> (u32, u32) {
        (self.cap_sec, self.cap_min)
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

    /// (过去 1 秒已用, 过去 60 秒已用)
    pub fn used(&mut self, now: u64) -> (u32, u32) {
        self.prune(now);
        let in_sec = self.sent.iter().rev().take_while(|&&t| t + SEC > now).count() as u32;
        (in_sec, self.sent.len() as u32)
    }

    /// 剩余 (每秒, 每分钟)
    pub fn remaining(&mut self, now: u64) -> (u32, u32) {
        let (s, m) = self.used(now);
        (self.cap_sec.saturating_sub(s), self.cap_min.saturating_sub(m))
    }

    pub fn try_acquire(&mut self, now: u64, prio: Priority) -> bool {
        if prio != Priority::Cancel && now < self.pause_until_ns {
            return false;
        }
        let (rs, rm) = self.remaining(now);
        let (need_s, need_m) = match prio {
            Priority::Cancel => (1, 1),
            Priority::Place => (1 + self.cfg.place_reserve_sec, 1 + self.cfg.place_reserve_min),
            Priority::Query => (1 + self.cfg.query_reserve_sec, 1 + self.cfg.query_reserve_min),
        };
        if rs >= need_s && rm >= need_m {
            self.sent.push_back(now);
            true
        } else {
            false
        }
    }

    /// 最早何时(单调纳秒)能再拿到一个撤单令牌
    pub fn next_cancel_slot(&mut self, now: u64) -> u64 {
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

    /// 收到 429 或限额头显示快用完:在 until 之前只放撤单
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
