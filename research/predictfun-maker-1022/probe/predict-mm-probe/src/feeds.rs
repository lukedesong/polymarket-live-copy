//! WebSocket 连接:币安 bookTicker、Predict(盘口 + 钱包事件,同一条连接)。断线自动重连,状态变化通知主循环。
//! Predict 的心跳在这里直接回(和录制器 pfrec5 一样),不经过主循环,避免主循环忙时漏回心跳。

use crate::jsonlog::{now, Log};
use crate::msgs::{heartbeat_reply, subscribe_msg, unsubscribe_msg};
use crate::strategy::{Feed, Now};
use futures_util::{SinkExt, StreamExt};
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug)]
pub enum FeedEv {
    Raw {
        feed: Feed,
        now: Now,
        raw: String,
    },
    /// 收到 pong:连接活着(不记日志)
    Alive {
        feed: Feed,
        now: Now,
    },
    State {
        feed: Feed,
        now: Now,
        up: bool,
        why: String,
    },
}

#[derive(Debug)]
pub enum PfCtrl {
    Subscribe(String),
    Unsubscribe(String),
    /// 断开重连(例如 JWT 换了)
    Reconnect,
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn binance_task(url: String, tx: mpsc::UnboundedSender<FeedEv>, log: Log) {
    let mut backoff = 1u64;
    loop {
        match tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(url.as_str())).await {
            Ok(Ok((ws, _))) => {
                backoff = 1;
                let _ = tx.send(FeedEv::State { feed: Feed::Binance, now: now(), up: true, why: "connected".into() });
                let (mut w, mut r) = ws.split();
                let why = loop {
                    match tokio::time::timeout(Duration::from_secs(30), r.next()).await {
                        Ok(Some(Ok(Message::Text(t)))) => {
                            let _ = tx.send(FeedEv::Raw { feed: Feed::Binance, now: now(), raw: t.to_string() });
                        }
                        Ok(Some(Ok(Message::Ping(p)))) => {
                            let _ = w.send(Message::Pong(p)).await;
                        }
                        Ok(Some(Ok(Message::Close(c)))) => break format!("close {c:?}"),
                        Ok(Some(Ok(_))) => {}
                        Ok(Some(Err(e))) => break format!("error {e}"),
                        Ok(None) => break "eof".into(),
                        Err(_) => break "30s 无消息".into(),
                    }
                };
                let _ = tx.send(FeedEv::State { feed: Feed::Binance, now: now(), up: false, why });
            }
            Ok(Err(e)) => {
                log.ev("feed_error", serde_json::json!({"feed": "binance", "error": e.to_string()}));
                let _ = tx.send(FeedEv::State { feed: Feed::Binance, now: now(), up: false, why: format!("connect: {e}") });
            }
            Err(_) => {
                let _ = tx.send(FeedEv::State { feed: Feed::Binance, now: now(), up: false, why: "connect timeout".into() });
            }
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(8);
    }
}

/// url 里可能带 apiKey:绝不记日志
pub async fn predict_task(url: String, origin: Option<String>, mut ctrl: mpsc::UnboundedReceiver<PfCtrl>, tx: mpsc::UnboundedSender<FeedEv>, log: Log) {
    let mut topics: BTreeSet<String> = BTreeSet::new();
    let mut rid: u64 = 0;
    let mut backoff = 1u64;
    loop {
        // 先把排队的订阅变化收下(断线期间也要记住)
        while let Ok(c) = ctrl.try_recv() {
            match c {
                PfCtrl::Subscribe(t) => {
                    topics.insert(t);
                }
                PfCtrl::Unsubscribe(t) => {
                    topics.remove(&t);
                }
                PfCtrl::Reconnect => {}
            }
        }
        let mut req = match url.as_str().into_client_request() {
            Ok(r) => r,
            Err(e) => {
                log.ev("feed_error", serde_json::json!({"feed": "predict", "error": format!("bad url: {e}")}));
                return;
            }
        };
        if let Some(o) = &origin {
            if let Ok(v) = o.parse() {
                req.headers_mut().insert("origin", v);
            }
        }
        match tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(req)).await {
            Ok(Ok((ws, _))) => {
                backoff = 1;
                let (mut w, mut r) = ws.split();
                let mut ok = true;
                for t in &topics {
                    rid += 1;
                    let m = subscribe_msg(rid, t);
                    log.ev("ws_send", serde_json::json!({"feed": "predict", "msg": m}));
                    if w.send(Message::Text(m.into())).await.is_err() {
                        ok = false;
                    }
                }
                if ok {
                    let _ = tx.send(FeedEv::State { feed: Feed::Predict, now: now(), up: true, why: "connected".into() });
                }
                // 每 5 秒发一次 WS ping;pong 也算存活,不完全依赖应用层心跳(心跳间隔未实测)
                let mut ping = tokio::time::interval(Duration::from_secs(5));
                ping.tick().await;
                let why = loop {
                    if !ok {
                        break "subscribe send failed".to_string();
                    }
                    tokio::select! {
                        _ = ping.tick() => {
                            if w.send(Message::Ping(Vec::new().into())).await.is_err() { break "ping send failed".into(); }
                        }
                        m = tokio::time::timeout(Duration::from_secs(60), r.next()) => match m {
                            Ok(Some(Ok(Message::Text(t)))) => {
                                let t = t.to_string();
                                let n = now();
                                if t.starts_with(r#"{"type":"M","topic":"heartbeat""#) || t.contains(r#""topic":"heartbeat""#) {
                                    if let Some(crate::msgs::PredictMsg::Heartbeat(d)) = crate::msgs::parse_predict(&t) {
                                        if w.send(Message::Text(heartbeat_reply(&d).into())).await.is_err() {
                                            break "heartbeat reply failed".into();
                                        }
                                    }
                                }
                                let _ = tx.send(FeedEv::Raw { feed: Feed::Predict, now: n, raw: t });
                            }
                            Ok(Some(Ok(Message::Ping(p)))) => { let _ = w.send(Message::Pong(p)).await; }
                            Ok(Some(Ok(Message::Pong(_)))) => { let _ = tx.send(FeedEv::Alive { feed: Feed::Predict, now: now() }); }
                            Ok(Some(Ok(Message::Close(c)))) => break format!("close {c:?}"),
                            Ok(Some(Ok(_))) => {}
                            Ok(Some(Err(e))) => break format!("error {e}"),
                            Ok(None) => break "eof".into(),
                            Err(_) => break "60s 无消息".into(),
                        },
                        c = ctrl.recv() => match c {
                            Some(PfCtrl::Subscribe(t)) => {
                                if topics.insert(t.clone()) {
                                    rid += 1;
                                    let m = subscribe_msg(rid, &t);
                                    log.ev("ws_send", serde_json::json!({"feed": "predict", "msg": m}));
                                    if w.send(Message::Text(m.into())).await.is_err() { break "send failed".into(); }
                                }
                            }
                            Some(PfCtrl::Unsubscribe(t)) => {
                                if topics.remove(&t) {
                                    rid += 1;
                                    let m = unsubscribe_msg(rid, &t);
                                    log.ev("ws_send", serde_json::json!({"feed": "predict", "msg": m}));
                                    if w.send(Message::Text(m.into())).await.is_err() { break "send failed".into(); }
                                }
                            }
                            Some(PfCtrl::Reconnect) => { let _ = w.send(Message::Close(None)).await; break "reconnect requested".into(); }
                            None => return,
                        }
                    }
                };
                let _ = tx.send(FeedEv::State { feed: Feed::Predict, now: now(), up: false, why });
            }
            Ok(Err(e)) => {
                log.ev("feed_error", serde_json::json!({"feed": "predict", "error": e.to_string()}));
                let _ = tx.send(FeedEv::State { feed: Feed::Predict, now: now(), up: false, why: "connect error".into() });
            }
            Err(_) => {
                let _ = tx.send(FeedEv::State { feed: Feed::Predict, now: now(), up: false, why: "connect timeout".into() });
            }
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(8);
    }
}
