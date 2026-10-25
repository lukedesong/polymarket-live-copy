//! 回测:用录制的数据跑实盘那份策略代码。数据由 analysis/backtest_data.py 准备(<data>/rounds.json + parts/)。
//!
//!   replay --data <目录> [--from <开盘秒>] [--to <开盘秒>] [--exclude <开盘秒,...>] [策略参数] [--out 每轮结果.tsv]
//!
//! 连续的轮一起跑(上一轮没结算的钱占额度,和实盘一样);中间缺轮就断开、分段各跑一次。
use clap::Parser;
use flate2::read::GzDecoder;
use predict_mm_probe::msgs::{parse_binance_book_ticker, parse_predict, PredictMsg};
use predict_mm_probe::replay::{summary, CancelMode, DataEv, Engine, ReplayCfg, RoundData, RoundResult};
use predict_mm_probe::strategy::{Params, QSide};
use serde_json::Value;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(about = "Predict BTC 5m 策略回测(实盘同一份策略代码 + 校准过的成交模型)")]
struct Args {
    #[arg(long)]
    data: PathBuf,
    #[arg(long)]
    from: Option<i64>,
    #[arg(long)]
    to: Option<i64>,
    /// 不跑的轮(开盘秒,逗号分隔),例如已经看过结果的
    #[arg(long, value_delimiter = ',')]
    exclude: Vec<i64>,
    #[arg(long)]
    out: Option<PathBuf>,
    /// 币安永续 bookTicker 文件(tsv.gz:本机收到 ns、买一、卖一);v7 用
    #[arg(long)]
    perp: Option<PathBuf>,
    /// aligned | independent | v7
    #[arg(long, default_value = "aligned")]
    mode: String,
    #[arg(long, default_value_t = 0)]
    reprice_min_age_ms: u64,
    #[arg(long, default_value_t = 6.0)]
    inventory_cap_usd: f64,
    #[arg(long, default_value_t = 150.0)]
    gross_cap_usd: f64,
    #[arg(long, default_value_t = 1000.0)]
    capital_cap_usd: f64,
    /// 回测默认不按亏损停机(要看策略本身);实盘对照时设成实盘的值
    #[arg(long, default_value_t = 1e9)]
    max_daily_loss_usd: f64,
    #[arg(long, default_value_t = 1e9)]
    max_total_loss_usd: f64,
    #[arg(long, default_value_t = 30)]
    stop_before_end_s: i64,
    /// 撮合 → 策略收到成交(新版"已撮合" 270;旧版"上链成功"约 2300)
    #[arg(long, default_value_t = 270)]
    fill_notify_ms: i64,
    /// 撤单记在前面还是后面:opt | mixed | pess | prop
    #[arg(long, default_value = "opt")]
    cancel_mode: String,
}

fn load_perp(path: &std::path::Path) -> anyhow::Result<Vec<(i64, f64)>> {
    let f = std::fs::File::open(path)?;
    let mut v = vec![];
    for line in BufReader::new(GzDecoder::new(f)).lines() {
        let line = line?;
        let mut it = line.split('\t');
        let (Some(ns), Some(b), Some(a)) = (it.next(), it.next(), it.next()) else { continue };
        let (Ok(ns), Ok(b), Ok(a)) = (ns.parse::<i64>(), b.parse::<f64>(), a.parse::<f64>()) else { continue };
        v.push((ns / 1_000_000, (b + a) / 2.0));
    }
    v.sort_by_key(|x| x.0);
    Ok(v)
}

fn load_round(dir: &std::path::Path, r: &Value, perp: &[(i64, f64)]) -> Option<RoundData> {
    let mut events = vec![];
    for p in r["parts"].as_array()? {
        let f = std::fs::File::open(dir.join(p.as_str()?)).ok()?;
        for (i, line) in BufReader::new(GzDecoder::new(f)).lines().enumerate() {
            let line = line.ok()?;
            if i == 0 {
                continue;
            }
            let v: Value = serde_json::from_str(&line).ok()?;
            let t = v[1].as_i64()?;
            match v[0].as_str()? {
                "pf" => {
                    if let Some(PredictMsg::Book(b)) = parse_predict(v[2].as_str()?) {
                        events.push(DataEv::Book(t, b));
                    }
                }
                "bn" => {
                    if let Some(bt) = parse_binance_book_ticker(v[2].as_str()?) {
                        events.push(DataEv::Binance(t, bt.mid()));
                    }
                }
                "fill" => {
                    let q = if v[2].as_str()? == "UpBid" { QSide::UpBid } else { QSide::DownBid };
                    events.push(DataEv::Fill(t, r["market_id"].as_u64()?, q, v[3].as_u64()? as u32, v[4].as_f64()?));
                }
                _ => {}
            }
        }
    }
    let s = r["start_s"].as_i64()?;
    // 永续:本轮 [开盘, 收盘) 内的(每条只进一轮,避免重复)
    let (t0, t1) = (s * 1000, (s + 300) * 1000);
    let i0 = perp.partition_point(|x| x.0 < t0);
    let i1 = perp.partition_point(|x| x.0 < t1);
    // 永续覆盖检查:有永续数据时,本轮内最大空档 > 10 秒(含开盘/收盘两端)就跳过这一轮并报出(不悄悄退回只用现货)
    if !perp.is_empty() {
        let mut last = t0;
        let mut gap = 0;
        for &(t, _) in &perp[i0..i1] {
            gap = gap.max(t - last);
            last = t;
        }
        gap = gap.max(t1 - last);
        if gap > 10_000 {
            eprintln!("跳过 {s}:永续最大空档 {gap}ms");
            return None;
        }
    }
    events.extend(perp[i0..i1].iter().map(|&(t, m)| DataEv::Perp(t, m)));
    events.sort_by_key(|e| e.t());
    Some(RoundData { market_id: r["market_id"].as_u64()?, start_ms: t0, end_ms: t1, payout_up: r["payout_up"].as_f64(), strike: r["start_price"].as_f64(), events })
}

fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    let perp = match &a.perp {
        Some(p) => load_perp(p)?,
        None => vec![],
    };
    if a.mode == "v7" && perp.is_empty() {
        anyhow::bail!("v7 需要 --perp 永续数据");
    }
    let rounds: Vec<Value> = serde_json::from_reader(std::fs::File::open(a.data.join("rounds.json"))?)?;
    let sel: Vec<&Value> = rounds
        .iter()
        .filter(|r| {
            let s = r["start_s"].as_i64().unwrap_or(0);
            a.from.is_none_or(|f| s >= f) && a.to.is_none_or(|t| s <= t) && !a.exclude.contains(&s)
        })
        .collect();
    let params = Params {
        aligned: a.mode == "aligned",
        v7: a.mode == "v7",
        reprice_min_age_ms: a.reprice_min_age_ms,
        inventory_cap_usd: a.inventory_cap_usd,
        gross_cap_usd: a.gross_cap_usd,
        capital_cap_usd: a.capital_cap_usd,
        max_daily_loss_usd: a.max_daily_loss_usd,
        max_total_loss_usd: a.max_total_loss_usd,
        stop_before_end_ms: a.stop_before_end_s * 1000,
        ..Params::default()
    };
    let cfg = ReplayCfg {
        fill_notify_ms: a.fill_notify_ms,
        cancel_mode: match a.cancel_mode.as_str() {
            "pess" => CancelMode::Pess,
            "mixed" => CancelMode::Mixed,
            "prop" => CancelMode::Prop,
            _ => CancelMode::Opt,
        },
        ..ReplayCfg::default()
    };
    // 连续的轮分成段
    let mut segs: Vec<Vec<&Value>> = vec![];
    for r in sel {
        let s = r["start_s"].as_i64().unwrap_or(0);
        match segs.last_mut() {
            Some(g) if g.last().and_then(|x| x["start_s"].as_i64()) == Some(s - 300) => g.push(r),
            _ => segs.push(vec![r]),
        }
    }
    let mut all: Vec<RoundResult> = vec![];
    for g in segs {
        let data: Vec<RoundData> = g.iter().filter_map(|r| load_round(&a.data, r, &perp)).collect();
        all.extend(Engine::new(params.clone(), cfg.clone()).run(data));
    }
    let mut w: Box<dyn Write> = match &a.out {
        Some(p) => Box::new(std::fs::File::create(p)?),
        None => Box::new(std::io::sink()),
    };
    writeln!(w, "start_s\tmarket_id\tpayout_up\tplaces\tcancels\trejects\tfills\tup_shares\tdown_shares\tcost\tpairs\tpair_cost\tunpaired\tpnl\thalted\trl_deferred_cum\tr8_violations_cum")?;
    for r in &all {
        writeln!(
            w,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.4}\t{:.4}\t{:.4}\t{:.4}\t{}\t{:.4}\t{}\t{}\t{}\t{}",
            r.start_ms / 1000,
            r.market_id,
            r.payout_up.map_or(String::new(), |x| x.to_string()),
            r.places,
            r.cancels,
            r.rejects,
            r.fills,
            r.up_shares,
            r.down_shares,
            r.cost,
            r.pairs,
            r.pair_cost.map_or(String::new(), |x| format!("{x:.4}")),
            r.unpaired,
            r.pnl.map_or(String::new(), |x| format!("{x:.4}")),
            r.halted.clone().unwrap_or_default(),
            r.rl_deferred_cum,
            r.r8_violations_cum
        )?;
        println!(
            "{} 挂 {:4} 撤 {:4} 拒 {:3} 成交 {:4} 笔 Up {:7.2} Down {:7.2} 成本 {:7.2} 配对 {:7.2}(每对 {}) 没配上 {:6.2} 盈亏 {}",
            r.start_ms / 1000,
            r.places,
            r.cancels,
            r.rejects,
            r.fills,
            r.up_shares,
            r.down_shares,
            r.cost,
            r.pairs,
            r.pair_cost.map_or("-".into(), |x| format!("{x:.3}")),
            r.unpaired,
            r.pnl.map_or("-".into(), |x| format!("{x:+.2}"))
        );
    }
    println!("汇总:{:?}", summary(&all));
    Ok(())
}
