//! 回测引擎:配对计算、排队成交、挂单 / 撤单延迟、只挂单被拒、结算。数据全部手工构造。
use predict_mm_probe::msgs::{parse_predict, PredictMsg};
use predict_mm_probe::replay::{pair_up, CancelMode, DataEv, Engine, ReplayCfg, RoundData};
use predict_mm_probe::strategy::{Params, QSide};

const MKT: u64 = 4242;
const S0: i64 = 1_800_000_000_000;

fn book(bids: &[(f64, f64)], asks: &[(f64, f64)]) -> predict_mm_probe::msgs::BookSnapshot {
    let raw = serde_json::json!({"type":"M","topic":format!("predictOrderbook/{MKT}"),"data":{
        "asks": asks.iter().map(|l| [l.0, l.1]).collect::<Vec<_>>(), "bids": bids.iter().map(|l| [l.0, l.1]).collect::<Vec<_>>(),
        "marketId": MKT, "updateTimestampMs": 0, "version": 1}})
    .to_string();
    match parse_predict(&raw) {
        Some(PredictMsg::Book(b)) => b,
        _ => panic!(),
    }
}

/// 一轮:盘口 0.50(80 股)/ 0.55(30 股)不动,币安不动;开盘 10 秒后 UpBid 0.50 先后成交 79 + 5 股
fn round(payout_up: f64) -> RoundData {
    let mut ev = vec![];
    let mut t = S0 - 90_000;
    while t < S0 + 300_000 {
        ev.push(DataEv::Book(t, book(&[(0.50, 80.0)], &[(0.55, 30.0)])));
        ev.push(DataEv::Binance(t, 80_000.0));
        t += 100;
    }
    ev.push(DataEv::Fill(S0 + 10_000, MKT, QSide::UpBid, 50, 79.0));
    ev.push(DataEv::Fill(S0 + 10_500, MKT, QSide::UpBid, 50, 5.0));
    ev.sort_by_key(|e| e.t());
    RoundData { market_id: MKT, start_ms: S0, end_ms: S0 + 300_000, payout_up: Some(payout_up), events: ev }
}

#[test]
fn pairs_fifo() {
    let f = vec![(1, QSide::UpBid, 0.40, 3.0), (2, QSide::DownBid, 0.55, 2.0), (3, QSide::DownBid, 0.50, 2.0)];
    let (pairs, cost, unpaired) = pair_up(&f);
    assert!((pairs - 3.0).abs() < 1e-9);
    // 2 对 0.40+0.55,1 对 0.40+0.50
    assert!((cost.unwrap() - (2.0 * 0.95 + 0.90) / 3.0).abs() < 1e-9);
    assert!((unpaired - 1.0).abs() < 1e-9);
}

#[test]
fn queue_then_fill_and_settle() {
    // 旧做法(两边各挂 ceil($1/价) 股):Up 买 0.50 × 2 股,前面 80 股;成交 79 → 还差 1,再 5 → 我们 2 股满
    let p = Params { aligned: false, ..Params::default() };
    let rs = Engine::new(p, ReplayCfg::default()).run(vec![round(1.0)]);
    let r = &rs[0];
    assert!(r.places >= 2, "两边都挂: {r:?}");
    assert_eq!(r.rejects, 0);
    assert!((r.up_shares - 2.0).abs() < 1e-9, "{r:?}");
    assert_eq!(r.down_shares, 0.0);
    assert!((r.cost - 1.0).abs() < 1e-9);
    assert!((r.pnl.unwrap() - 1.0).abs() < 1e-9, "Up 赢:2 − 1 = 1");
}

#[test]
fn pess_mode_and_settle_loss() {
    let p = Params { aligned: false, ..Params::default() };
    let cfg = ReplayCfg { cancel_mode: CancelMode::Pess, ..ReplayCfg::default() };
    let rs = Engine::new(p, cfg).run(vec![round(0.0)]);
    assert!((rs[0].pnl.unwrap() + 1.0).abs() < 1e-9, "Down 赢:0 − 1");
}

#[test]
fn late_join_is_behind_the_queue() {
    // 撤单延迟:成交前 1ms 才挂上的单排在 80 股后面,成交 79 吃不到;没有成交就不结算出亏损
    let p = Params { aligned: false, ..Params::default() };
    let mut r = round(1.0);
    r.events.retain(|e| !matches!(e, DataEv::Fill(_, _, _, _, q) if (*q - 5.0).abs() < 1e-9));
    let rs = Engine::new(p, ReplayCfg::default()).run(vec![r]);
    assert_eq!(rs[0].up_shares, 0.0, "{:?}", rs[0]);
    assert_eq!(rs[0].pnl, Some(0.0));
}

#[test]
fn post_only_cross_is_rejected() {
    // 开盘时按 0.50/0.55 挂 Up 买 0.50;10ms 后卖一掉到 0.50(买一退到 0.49):22ms 生效时会直接成交 → 被拒
    let p = Params { aligned: false, ..Params::default() };
    let mut ev = vec![];
    let mut t = S0 - 90_000;
    while t < S0 + 20_000 {
        ev.push(DataEv::Book(t, book(&[(0.50, 80.0)], &[(0.55, 30.0)])));
        ev.push(DataEv::Binance(t, 80_000.0));
        t += 100;
    }
    ev.retain(|e| e.t() < S0 + 10);
    ev.push(DataEv::Book(S0 + 10, book(&[(0.49, 80.0)], &[(0.50, 30.0)])));
    let mut r = round(1.0);
    r.events = ev;
    let rs = Engine::new(p, ReplayCfg::default()).run(vec![r]);
    assert!(rs[0].rejects >= 1, "{:?}", rs[0]);
    assert_eq!(rs[0].up_shares, 0.0);
}
