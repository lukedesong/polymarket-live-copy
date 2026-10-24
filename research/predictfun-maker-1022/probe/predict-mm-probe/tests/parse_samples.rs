//! 用 ref/ws_samples.tsv 的真实消息做解析测试(副本在 tests/data/,格式:本机纳秒 \t 来源 \t 原始 JSON)。
use predict_mm_probe::msgs::*;
use serde_json::json;
use std::path::Path;

fn samples() -> Vec<(u64, String, String)> {
    let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/ws_samples.tsv");
    std::fs::read_to_string(p)
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| {
            let mut it = l.splitn(3, '\t');
            (it.next().unwrap().parse().unwrap(), it.next().unwrap().to_string(), it.next().unwrap().to_string())
        })
        .collect()
}

#[test]
fn every_binance_line_classified() {
    let mut tickers = 0;
    let mut trades = 0;
    for (_, src, raw) in samples().iter().filter(|s| s.1 == "bn" || s.1 == "bf") {
        match parse_binance_book_ticker(raw) {
            Some(t) => {
                tickers += 1;
                assert!(t.bid > 0.0 && t.ask >= t.bid, "{src} {raw}");
                assert!(t.symbol.ends_with("USDT"));
            }
            None => {
                assert!(raw.contains("@trade"), "只有 trade 消息可以不是 bookTicker: {raw}");
                trades += 1;
            }
        }
    }
    assert!(tickers >= 8 && trades >= 4, "{tickers} {trades}");
}

#[test]
fn binance_spot_first_ticker_values() {
    let s = samples();
    let first = s.iter().find(|x| x.1 == "bn").unwrap();
    let t = parse_binance_book_ticker(&first.2).unwrap();
    assert_eq!(t.symbol, "BTCUSDT");
    assert_eq!(t.update_id, 101845374984);
    assert_eq!(t.bid, 80880.0);
    assert_eq!(t.ask, 80880.01);
    assert!((t.mid() - 80880.005).abs() < 1e-6);
    assert_eq!(t.bid_qty, 13.4033);
    // 单流格式(无 stream 包裹)也认
    let t2 = parse_binance_book_ticker(r#"{"u":1,"s":"BTCUSDT","b":"1.0","B":"2","a":"1.5","A":"3"}"#).unwrap();
    assert_eq!(t2.mid(), 1.25);
}

#[test]
fn every_predict_line_is_a_book() {
    let s = samples();
    let pf: Vec<_> = s.iter().filter(|x| x.1 == "pf").collect();
    assert_eq!(pf.len(), 8);
    for (_, _, raw) in pf {
        match parse_predict(raw).unwrap() {
            PredictMsg::Book(b) => {
                assert!(b.market_id > 3_000_000);
                assert_eq!(b.version, Some(1));
                assert!(b.update_ts_ms.unwrap() > 1_792_817_000_000);
                if let (Some(bb), Some(ba)) = (b.best_bid(), b.best_ask()) {
                    assert!(bb < ba, "{raw}");
                }
            }
            other => panic!("应为盘口: {other:?}"),
        }
    }
}

#[test]
fn predict_book_details_15m_market() {
    let s = samples();
    let raw = &s.iter().find(|x| x.2.contains("predictOrderbook/3698218")).unwrap().2;
    let PredictMsg::Book(b) = parse_predict(raw).unwrap() else { panic!() };
    assert_eq!(b.market_id, 3698218);
    assert_eq!(b.best_bid(), Some(0.5));
    assert_eq!(b.best_ask(), Some(0.6));
    assert_eq!(b.best_bid_cents(), Some(50));
    assert_eq!(b.best_ask_cents(), Some(60));
    assert_eq!(b.bid_size_at(50), 80.0);
    assert_eq!(b.ask_size_at(60), 137.0);
    assert_eq!(b.bids.len(), 13);
    assert_eq!(b.asks.len(), 12);
    assert_eq!(b.settlements_pending_bids, vec![(0.5, 7.0)]);
    assert!(b.settlements_pending_asks.is_empty());
    assert_eq!(b.order_count, Some(41));
}

#[test]
fn predict_book_one_sided_and_odd_sizes() {
    let s = samples();
    // 3698001:没有买单,卖一 0.01,挂量带长小数
    let raw = &s.iter().find(|x| x.2.contains("predictOrderbook/3698001")).unwrap().2;
    let PredictMsg::Book(b) = parse_predict(raw).unwrap() else { panic!() };
    assert_eq!(b.best_bid(), None);
    assert_eq!(b.best_ask_cents(), Some(1));
    assert!((b.asks[1].1 - 14.32878843537414).abs() < 1e-12);
    assert_eq!(b.settlements_pending_bids, vec![(0.01, 5.6504)]);
}

#[test]
fn heartbeat_subscribe_and_wallet_messages() {
    let hb = r#"{"type":"M","topic":"heartbeat","data":1792818000123}"#;
    let PredictMsg::Heartbeat(d) = parse_predict(hb).unwrap() else { panic!() };
    assert_eq!(heartbeat_reply(&d), r#"{"method":"heartbeat","data":1792818000123}"#);
    assert_eq!(subscribe_msg(3, "predictOrderbook/1"), r#"{"requestId":3,"method":"subscribe","params":["predictOrderbook/1"]}"#);
    // 订阅回包:无 topic
    assert!(matches!(parse_predict(r#"{"type":"R","requestId":1,"success":true}"#).unwrap(), PredictMsg::Response(_)));
    // 钱包事件:格式未录,按容错解析
    let w = r#"{"type":"M","topic":"predictWalletEvents/xxx.yyy.zzz","data":{"type":"ORDER_FILLED","order":{"id":"6165626687","hash":"0x7d0b25d89060c493b334dca0a634ecdf2485d0240e143de1969f0ddf0c55a356","amountFilled":"2000000000000000000","price":"0.5"}}}"#;
    let PredictMsg::Wallet(d) = parse_predict(w).unwrap() else { panic!() };
    let h = wallet_event_hint(&d);
    assert_eq!(h.kind.as_deref(), Some("ORDER_FILLED"));
    assert_eq!(h.order_ids, vec!["6165626687"]);
    assert_eq!(h.order_hashes, vec!["0x7d0b25d89060c493b334dca0a634ecdf2485d0240e143de1969f0ddf0c55a356"]);
    assert_eq!(h.amount_filled, Some(2.0));
    assert_eq!(h.price, Some(0.5));
    // 完全陌生的结构:不报错,什么也抠不出
    let h = wallet_event_hint(&json!({"foo": [1, 2, {"bar": null}]}));
    assert_eq!(h, WalletEventHint::default());
}

#[test]
fn non_json_and_garbage_do_not_panic() {
    for s in samples() {
        let _ = parse_predict(&s.2);
        let _ = parse_binance_book_ticker(&s.2);
    }
    assert!(parse_predict("not json").is_none());
    assert!(parse_binance_book_ticker("{}").is_none());
}
