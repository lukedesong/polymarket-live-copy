//! 风控账本:当日 / 累计 已实现 + 未实现 盈亏。落盘(state 文件),重启不清零。
//!
//! 估值口径(保守):
//! - 当前轮(还在交易):持仓按 Predict 盘口中间价估值;拿不到中间价按最坏结果估值。
//! - 已收盘但结果未知:按最坏结果估值 = min(Up 股数, Down 股数)(Up 赢得 Up 股、Down 赢得 Down 股、平局各 0.5)。
//! - 结果已知:Up/Down 每股兑付(赢 1、输 0、平局各 0.5),计入已实现,按该轮收盘时间归日。
//! - 挂单 0 手续费;探针不吃单,不计吃单费。
//! - "当日" = 按 day_offset_hours 平移后的 UTC 日期(默认 0 = UTC)。

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Outcome {
    Up,
    Down,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RoundPos {
    pub market_id: u64,
    pub start_ms: i64,
    pub end_ms: i64,
    pub up_shares: f64,
    pub down_shares: f64,
    pub cost_usd: f64,
    pub fills: u32,
}

impl RoundPos {
    pub fn worst_value(&self) -> f64 {
        self.up_shares.min(self.down_shares)
    }
    /// 盘口中间价(Up 的价格,0~1)估值
    pub fn mark_value(&self, up_mid: Option<f64>) -> f64 {
        match up_mid {
            Some(m) => self.up_shares * m + self.down_shares * (1.0 - m),
            None => self.worst_value(),
        }
    }
    pub fn net_up(&self) -> f64 {
        self.up_shares - self.down_shares
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Ledger {
    /// 还没出结果的轮(含当前轮)
    pub open: Vec<RoundPos>,
    /// 日期 → 已实现盈亏
    pub realized_by_day: BTreeMap<String, f64>,
    pub realized_total: f64,
    /// 已结算的轮(只留摘要,便于核对)
    pub settled: Vec<(u64, f64)>,
    /// 停机原因(非空时拒绝下单,需人工 --clear-halt)
    pub halted: Option<String>,
    #[serde(default)]
    pub day_offset_hours: i64,
}

pub fn day_key(wall_ms: i64, offset_hours: i64) -> String {
    let secs = wall_ms.div_euclid(1000) + offset_hours * 3600;
    let days = secs.div_euclid(86400);
    // 1970-01-01 起的天数 → 公历日期(Howard Hinnant civil_from_days)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

impl Ledger {
    pub fn round_mut(&mut self, market_id: u64, start_ms: i64, end_ms: i64) -> &mut RoundPos {
        if let Some(i) = self.open.iter().position(|r| r.market_id == market_id) {
            return &mut self.open[i];
        }
        self.open.push(RoundPos { market_id, start_ms, end_ms, ..Default::default() });
        self.open.last_mut().unwrap()
    }

    pub fn round(&self, market_id: u64) -> Option<&RoundPos> {
        self.open.iter().find(|r| r.market_id == market_id)
    }

    pub fn apply_fill(&mut self, market_id: u64, outcome: Outcome, shares: f64, price: f64) {
        if let Some(r) = self.open.iter_mut().find(|r| r.market_id == market_id) {
            match outcome {
                Outcome::Up => r.up_shares += shares,
                Outcome::Down => r.down_shares += shares,
            }
            r.cost_usd += shares * price;
            r.fills += 1;
        }
    }

    /// 已收盘且一股没成交的轮直接删掉(不影响盈亏,免得账本无限变长)
    pub fn prune_empty(&mut self, now_ms: i64) {
        self.open.retain(|r| !(now_ms >= r.end_ms && r.fills == 0 && r.up_shares == 0.0 && r.down_shares == 0.0));
    }

    /// 撤回一笔已记的成交(撮合后上链失败)
    pub fn revert_fill(&mut self, market_id: u64, outcome: Outcome, shares: f64, price: f64) {
        if let Some(r) = self.open.iter_mut().find(|r| r.market_id == market_id) {
            match outcome {
                Outcome::Up => r.up_shares = (r.up_shares - shares).max(0.0),
                Outcome::Down => r.down_shares = (r.down_shares - shares).max(0.0),
            }
            r.cost_usd = (r.cost_usd - shares * price).max(0.0);
        }
    }

    /// 结果出来:payout_up/payout_down = 每股兑付(1/0/0.5)。返回该轮已实现盈亏。
    pub fn resolve(&mut self, market_id: u64, payout_up: f64, payout_down: f64) -> Option<f64> {
        let i = self.open.iter().position(|r| r.market_id == market_id)?;
        let r = self.open.remove(i);
        let pnl = r.up_shares * payout_up + r.down_shares * payout_down - r.cost_usd;
        *self.realized_by_day.entry(day_key(r.end_ms, self.day_offset_hours)).or_insert(0.0) += pnl;
        self.realized_total += pnl;
        self.settled.push((market_id, pnl));
        Some(pnl)
    }

    /// 未结的轮估值后的盈亏。live_market + up_mid:当前轮按中间价;其余按最坏结果。
    fn open_pnl(&self, r: &RoundPos, live: Option<(u64, Option<f64>)>, now_ms: i64) -> f64 {
        let v = match live {
            Some((id, mid)) if id == r.market_id && now_ms < r.end_ms => r.mark_value(mid),
            _ => r.worst_value(),
        };
        v - r.cost_usd
    }

    pub fn day_pnl(&self, now_ms: i64, live: Option<(u64, Option<f64>)>) -> f64 {
        let today = day_key(now_ms, self.day_offset_hours);
        let realized = self.realized_by_day.get(&today).copied().unwrap_or(0.0);
        // 未结的轮全部算进"当日"(保守:哪怕是昨天收盘、今天还没出结果的)
        realized + self.open.iter().map(|r| self.open_pnl(r, live, now_ms)).sum::<f64>()
    }

    pub fn total_pnl(&self, now_ms: i64, live: Option<(u64, Option<f64>)>) -> f64 {
        self.realized_total + self.open.iter().map(|r| self.open_pnl(r, live, now_ms)).sum::<f64>()
    }

    /// 亏损超限返回停机原因
    pub fn check_limits(&self, now_ms: i64, live: Option<(u64, Option<f64>)>, max_daily_loss: f64, max_total_loss: f64) -> Option<String> {
        let d = self.day_pnl(now_ms, live);
        let t = self.total_pnl(now_ms, live);
        if -d >= max_daily_loss {
            return Some(format!("当日亏损 {:.2} ≥ {:.2}", -d, max_daily_loss));
        }
        if -t >= max_total_loss {
            return Some(format!("累计亏损 {:.2} ≥ {:.2}", -t, max_total_loss));
        }
        None
    }

    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        Ok(serde_json::from_str(&std::fs::read_to_string(path)?)?)
    }

    /// 先写临时文件再改名,断电不留半个文件
    pub fn save(&self, path: &std::path::Path) -> anyhow::Result<()> {
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(tmp, path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn days() {
        assert_eq!(day_key(0, 0), "1970-01-01");
        assert_eq!(day_key(1_792_818_000_000, 0), "2026-10-24");
        assert_eq!(day_key(1_792_818_000_000 + 19 * 3600 * 1000, 0), "2026-10-25");
        assert_eq!(day_key(1_792_818_000_000 + 19 * 3600 * 1000, -8), "2026-10-24");
    }

    #[test]
    fn worst_case_and_resolve() {
        let mut l = Ledger::default();
        l.round_mut(1, 0, 300_000);
        l.apply_fill(1, Outcome::Up, 2.0, 0.5);
        l.apply_fill(1, Outcome::Down, 3.0, 0.4);
        // 成本 1.0 + 1.2 = 2.2;最坏 min(2,3) = 2 → −0.2
        assert!((l.total_pnl(400_000, None) + 0.2).abs() < 1e-9);
        // 当前轮按中间价 0.5:2×0.5 + 3×0.5 = 2.5 → +0.3
        assert!((l.total_pnl(100_000, Some((1, Some(0.5)))) - 0.3).abs() < 1e-9);
        // 平局:各 0.5 → 2.5 − 2.2
        assert!((l.resolve(1, 0.5, 0.5).unwrap() - 0.3).abs() < 1e-9);
        assert!(l.open.is_empty());
        assert!((l.realized_total - 0.3).abs() < 1e-9);
    }

    #[test]
    fn prune_only_ended_empty_rounds() {
        let mut l = Ledger::default();
        l.round_mut(1, 0, 300_000);
        l.round_mut(2, 0, 300_000);
        l.apply_fill(2, Outcome::Down, 1.0, 0.4);
        l.round_mut(3, 300_000, 600_000);
        l.prune_empty(300_000);
        let ids: Vec<u64> = l.open.iter().map(|r| r.market_id).collect();
        assert_eq!(ids, vec![2, 3], "有成交的、还没收盘的都留着");
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("state.json");
        let mut l = Ledger::default();
        l.round_mut(7, 1, 2);
        l.apply_fill(7, Outcome::Up, 1.0, 0.3);
        l.halted = Some("x".into());
        l.save(&p).unwrap();
        let m = Ledger::load(&p).unwrap();
        assert_eq!(m.open, l.open);
        assert_eq!(m.halted.as_deref(), Some("x"));
    }
}
