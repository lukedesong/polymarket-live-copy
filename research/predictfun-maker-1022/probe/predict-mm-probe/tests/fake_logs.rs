//! 仿真场景的模拟器:生成"像实盘"的探针日志,给 probe/analysis/ 的分析工具做测试数据。
//! 全部在 127.0.0.1,不连外网;凭证全是现场随机生成的。
//!
//! 场景:
//! - 币安 BTCUSDT 中间价每 100ms 随机游走(σ = SIGMA /√秒),偶发 ±4bp 跳动。
//! - Predict 盘口跟着公允价走:Up 公允 = Φ(ln(S/K)/(σ√τ)),K = 本轮开盘时的币安价;买一 = ceil(公允×100) − 1,卖一 = floor(公允×100) + 1。
//! - 成交:每 150ms 一拍。我们的买单所在价位被市场穿过("through",逆向成交)→ 全部成交;
//!   价位仍是最优价时,以一定概率来一笔随机对手单,先吃掉我们前面的排队(挂单时该价位的显示量),再吃我们。
//! - 收盘按币安收盘价与 K 比较出结果,收盘 30 秒后市场接口给出 WON/LOST。
//!
//! 模拟器把"真值"(K、σ、每笔成交是逆向还是随机、挂单时前面排了多少、结算结果)写进 mock_truth.json。
//!
//! 生成测试数据(约 7 分钟,跨一次收盘与结算):
//!   PROBE_FAKE_LOG_DIR=../analysis/tests/data/fake_run cargo test --release --test fake_logs generate -- --ignored --nocapture
use futures_util::{SinkExt, StreamExt};
use predict_mm_probe::signing::OrderSigner;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

const JWT: &str = "simjwt.header.payload-signature-456";
const API_KEY: &str = "sim-api-key-0123456789";
/// 扩散项:每 √秒 的波动
const SIGMA: f64 = 4e-5;
/// 跳动:每 100ms 一步的概率与幅度
const JUMP_P: f64 = 0.004;
const JUMP: f64 = 4e-4;

/// 含跳动的有效波动(每 √秒),公允价用它:√(σ² + 每秒跳动次数 × 幅度²)
fn sigma_eff() -> f64 {
    (SIGMA * SIGMA + 10.0 * JUMP_P * JUMP * JUMP).sqrt()
}

fn now_ms() -> i64 {
    predict_mm_probe::jsonlog::wall_ms()
}

fn market_id(start_s: i64) -> u64 {
    2_000_000 + ((start_s / 300) % 100_000) as u64
}

fn phi(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

/// Abramowitz-Stegun 7.1.26(误差 < 1.5e-7,模拟器够用)
fn erf(x: f64) -> f64 {
    let s = x.signum();
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * x);
    let y = 1.0 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * (-x * x).exp();
    s * y
}

struct SimOrder {
    id: String,
    hash: String,
    market: u64,
    is_up: bool,
    price_c: u32,
    shares: f64,
    filled: f64,
    ahead: f64,
    status: String,
}

#[derive(Default)]
struct Sim {
    owner: String,
    rng: u64,
    btc: f64,
    starts: HashMap<u64, i64>,
    k: HashMap<u64, f64>,
    close: HashMap<u64, f64>,
    orders: Vec<SimOrder>,
    places: usize,
    subscribed: Vec<String>,
    pf_out: Vec<String>,
    bn_out: Vec<String>,
    truth_fills: Vec<Value>,
    truth_orders: Vec<Value>,
    last_book: HashMap<u64, (u32, u32)>,
    settle_seq: u64,
}

type S = Arc<Mutex<Sim>>;

impl Sim {
    fn rand(&mut self) -> f64 {
        self.rng = self.rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((self.rng >> 11) as f64) / ((1u64 << 53) as f64)
    }
    fn gauss(&mut self) -> f64 {
        let (u, v) = (self.rand().max(1e-12), self.rand());
        (-2.0 * u.ln()).sqrt() * (2.0 * std::f64::consts::PI * v).cos()
    }
    fn up_fair(&self, m: u64, t: i64) -> f64 {
        let Some(&st) = self.starts.get(&m) else { return 0.5 };
        let (start, end) = (st * 1000, st * 1000 + 300_000);
        let Some(&k) = self.k.get(&m) else { return 0.5 };
        if t < start {
            return 0.5;
        }
        let s = if t >= end { self.close.get(&m).copied().unwrap_or(self.btc) } else { self.btc };
        let tau = ((end - t) as f64 / 1000.0).max(0.0);
        if tau < 1e-3 {
            return if s > k {
                1.0
            } else if s < k {
                0.0
            } else {
                0.5
            };
        }
        phi((s / k).ln() / (sigma_eff() * tau.sqrt()))
    }
    /// 买一 = ceil(公允×100) − 1,卖一 = floor(公允×100) + 1:公允价严格夹在买一卖一之间。
    /// 于是"被穿过"的成交价一定在公允价不利一侧(逆向),"最优价上被随机吃到"的一定在有利一侧
    fn book(&self, m: u64, t: i64) -> (u32, u32) {
        let f = self.up_fair(m, t) * 100.0;
        let bid = ((f.ceil() as i64) - 1).clamp(1, 98) as u32;
        let ask = ((f.floor() as i64) + 1).clamp(bid as i64 + 1, 99) as u32;
        (bid, ask)
    }
    fn level_size(m: u64, c: u32, t: i64) -> f64 {
        let h = (m.wrapping_mul(31) ^ (c as u64).wrapping_mul(1_000_003) ^ ((t / 20_000) as u64).wrapping_mul(2_654_435_761)) % 56;
        5.0 + h as f64
    }
    /// 跨过开盘 / 收盘时刻:K / 收盘价 = 此刻之前最后一个已推送的币安价
    fn mark_open_close(&mut self, t: i64) {
        for (&m, &st) in self.starts.clone().iter() {
            if t >= st * 1000 && !self.k.contains_key(&m) {
                self.k.insert(m, self.btc);
            }
            if t >= st * 1000 + 300_000 && !self.close.contains_key(&m) && self.k.contains_key(&m) {
                self.close.insert(m, self.btc);
            }
        }
    }

    /// 实盘格式:每笔撮合一个 settlementId,先"已撮合",再"上链成功";executedSizeWei 是这一笔的股数
    fn wallet_event(&mut self, i: usize, qty: f64) {
        self.settle_seq += 1;
        let o = &self.orders[i];
        let fill = json!({"executedPriceWei": format!("{}", o.price_c as u128 * 10_000_000_000_000_000u128), "executedSizeWei": format!("{}", (qty * 1e18).round() as u128), "executedValueWei": "0"});
        for typ in ["orderTransactionSubmitted", "orderTransactionSuccess"] {
            let ev = json!({"type": "M", "topic": format!("predictWalletEvents/{JWT}"), "data": {"type": typ, "orderId": o.id, "orderHash": o.hash,
                "settlementId": format!("sim-{}", self.settle_seq), "fill": fill, "isMaker": true, "timestamp": now_ms()}});
            self.pf_out.push(ev.to_string());
        }
    }
    fn fill(&mut self, i: usize, qty: f64, kind: &str, t: i64) {
        let qty = qty.min(self.orders[i].shares - self.orders[i].filled);
        if qty <= 1e-9 {
            return;
        }
        self.orders[i].filled += qty;
        if self.orders[i].filled >= self.orders[i].shares - 1e-9 {
            self.orders[i].status = "FILLED".into();
        }
        let o = &self.orders[i];
        let fair = self.up_fair(o.market, t);
        self.truth_fills.push(json!({"order_id": o.id, "wall_ms": t, "shares": qty, "price": o.price_c as f64 / 100.0, "is_up": o.is_up, "kind": kind,
            "market_id": o.market, "up_fair": fair, "btc": self.btc}));
        self.wallet_event(i, qty);
    }
    /// 一拍:盘口更新、处理成交、推送
    fn tick(&mut self) {
        let t = now_ms();
        self.mark_open_close(t);
        let subs: Vec<u64> = self.subscribed.iter().filter_map(|t| t.strip_prefix("predictOrderbook/")).filter_map(|x| x.parse().ok()).collect();
        for m in subs {
            let (bid, ask) = self.book(m, t);
            // 成交
            for i in 0..self.orders.len() {
                if self.orders[i].market != m || self.orders[i].status != "OPEN" {
                    continue;
                }
                let o = &self.orders[i];
                let level = if o.is_up { o.price_c } else { 100 - o.price_c };
                let (through, at_best) = if o.is_up { (bid < level, bid == level) } else { (ask > level, ask == level) };
                if through {
                    let q = o.shares - o.filled;
                    self.fill(i, q, "through", t);
                } else if at_best && self.rand() < 0.30 {
                    let vol = 1.0 + (self.rand() * 9.0).floor();
                    let o = &mut self.orders[i];
                    let eat = vol - o.ahead;
                    o.ahead = (o.ahead - vol).max(0.0);
                    if eat > 0.0 {
                        self.fill(i, eat, "random", t);
                    }
                }
            }
            self.last_book.insert(m, (bid, ask));
            let mut bids = vec![[bid as f64 / 100.0, Self::level_size(m, bid, t)]];
            let mut asks = vec![[ask as f64 / 100.0, Self::level_size(m, ask, t)]];
            for d in 1..4u32 {
                if bid > d {
                    bids.push([(bid - d) as f64 / 100.0, Self::level_size(m, bid - d, t)]);
                }
                if ask + d < 100 {
                    asks.push([(ask + d) as f64 / 100.0, Self::level_size(m, ask + d, t)]);
                }
            }
            let msg = json!({"type": "M", "topic": format!("predictOrderbook/{m}"), "data": {"asks": asks, "bids": bids, "marketId": m, "orderCount": 8,
                "settlementsPending": {"asks": [], "bids": []}, "updateTimestampMs": t, "version": 1}});
            self.pf_out.push(msg.to_string());
        }
    }
}

fn market_json(st: &Sim, id: u64) -> Value {
    let start = st.starts.get(&id).copied();
    let t = now_ms();
    let resolved = start.filter(|s| t > s * 1000 + 300_000 + 30_000).and_then(|_| Some((st.k.get(&id)?, st.close.get(&id)?)));
    let (su, sd) = match resolved {
        Some((k, c)) if c > k => (json!("WON"), json!("LOST")),
        Some((k, c)) if c < k => (json!("LOST"), json!("WON")),
        Some(_) => (json!("WON"), json!("WON")),
        None => (Value::Null, Value::Null),
    };
    json!({"id": id, "feeRateBps": 200, "isNegRisk": false, "isYieldBearing": false, "status": "REGISTERED", "tradingStatus": "OPEN",
        "conditionId": format!("0x{:064x}", id),
        "outcomes": [{"name": "Up", "indexSet": 1, "onChainId": (id * 2).to_string(), "status": su},
                     {"name": "Down", "indexSet": 2, "onChainId": (id * 2 + 1).to_string(), "status": sd}]})
}

fn route(s: &S, method: &str, path: &str, headers: &[(String, String)], body: &str) -> (u16, Value) {
    let mut st = s.lock().unwrap();
    let authed = headers.iter().any(|(k, v)| k == "authorization" && v == &format!("Bearer {JWT}"));
    if path.starts_with("/v1/orders") && !authed {
        return (401, json!({"success": false}));
    }
    let p = path.split('?').next().unwrap();
    match (method, p) {
        ("POST", "/rpc") => {
            let req: Value = serde_json::from_str(body).unwrap();
            let res = match req["method"].as_str().unwrap() {
                "eth_call" => json!(format!("0x{:0>64}", st.owner.trim_start_matches("0x").to_lowercase())),
                _ => json!(null),
            };
            (200, json!({"jsonrpc": "2.0", "id": req["id"], "result": res}))
        }
        ("GET", "/v1/auth/message") => (200, json!({"data": {"message": "sim sign in\nNonce: 7"}})),
        ("POST", "/v1/auth") => (200, json!({"data": {"token": JWT}})),
        ("GET", p) if p.starts_with("/v1/categories/btc-updown-5m-") => {
            let s: i64 = p.rsplit('-').next().unwrap().parse().unwrap();
            st.starts.insert(market_id(s), s);
            (200, json!({"data": {"markets": [{"id": market_id(s)}]}}))
        }
        ("GET", p) if p.starts_with("/v1/markets/") && p.ends_with("/orderbook") => {
            let id: u64 = p.trim_start_matches("/v1/markets/").trim_end_matches("/orderbook").parse().unwrap();
            let (b, a) = st.book(id, now_ms());
            (200, json!({"data": {"bids": [[b as f64 / 100.0, 20.0]], "asks": [[a as f64 / 100.0, 20.0]]}}))
        }
        ("GET", p) if p.starts_with("/v1/markets/") => {
            let id: u64 = p.trim_start_matches("/v1/markets/").parse().unwrap();
            (200, json!({"data": market_json(&st, id)}))
        }
        ("POST", "/v1/orders") => {
            st.places += 1;
            let b: Value = serde_json::from_str(body).unwrap();
            let o = &b["data"]["order"];
            let tok: u64 = o["tokenId"].as_str().unwrap().parse().unwrap();
            let (market, is_up) = (tok / 2, tok.is_multiple_of(2));
            let shares = o["takerAmount"].as_str().unwrap().parse::<f64>().unwrap() / 1e18;
            let price_c = (b["data"]["pricePerShare"].as_str().unwrap().parse::<f64>().unwrap() / 1e16).round() as u32;
            let t = now_ms();
            let (bid, ask) = st.last_book.get(&market).copied().unwrap_or_else(|| st.book(market, t));
            let level = if is_up { price_c } else { 100 - price_c };
            let at_best = if is_up { level == bid } else { level == ask };
            let ahead = if at_best { Sim::level_size(market, level, t) } else { 0.0 };
            let id = (7_000_000_000u64 + st.places as u64).to_string();
            let hash = o["hash"].as_str().unwrap().to_string();
            st.truth_orders.push(json!({"order_id": id, "wall_ms": t, "market_id": market, "is_up": is_up, "price": price_c as f64 / 100.0, "shares": shares, "ahead": ahead}));
            st.orders.push(SimOrder { id: id.clone(), hash: hash.clone(), market, is_up, price_c, shares, filled: 0.0, ahead, status: "OPEN".into() });
            (200, json!({"success": true, "data": {"orderId": id, "orderHash": hash}}))
        }
        ("POST", "/v1/orders/remove") => {
            let b: Value = serde_json::from_str(body).unwrap();
            for id in b["data"]["ids"].as_array().unwrap() {
                if let Some(o) = st.orders.iter_mut().find(|o| Some(o.id.as_str()) == id.as_str()) {
                    if o.status == "OPEN" {
                        o.status = "CANCELLED".into();
                    }
                }
            }
            (200, json!({"success": true}))
        }
        ("GET", "/v1/orders") => {
            let list: Vec<Value> = st
                .orders
                .iter()
                .filter(|o| o.status == "OPEN")
                .map(|o| json!({"id": o.id, "status": "OPEN", "amountFilled": format!("{}", (o.filled * 1e18).round() as u128), "order": {"hash": o.hash}}))
                .collect();
            (200, json!({"success": true, "data": list}))
        }
        ("GET", p) if p.starts_with("/v1/orders/") => {
            let h = p.trim_start_matches("/v1/orders/");
            match st.orders.iter().find(|o| o.hash == h) {
                Some(o) => (200, json!({"data": {"id": o.id, "status": o.status, "amountFilled": format!("{}", (o.filled * 1e18).round() as u128), "order": {"hash": o.hash}}})),
                None => (404, json!({})),
            }
        }
        _ => (404, json!({"path": path})),
    }
}

async fn http_server(s: S) -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (sock, _) = l.accept().await.unwrap();
            let s = s.clone();
            tokio::spawn(async move {
                let (r, mut w) = sock.into_split();
                let mut r = BufReader::new(r);
                loop {
                    let mut line = String::new();
                    if r.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    let mut parts = line.split_whitespace();
                    let (method, path) = (parts.next().unwrap_or("").to_string(), parts.next().unwrap_or("").to_string());
                    let mut headers = vec![];
                    let mut len = 0usize;
                    loop {
                        let mut h = String::new();
                        r.read_line(&mut h).await.unwrap();
                        let h = h.trim_end();
                        if h.is_empty() {
                            break;
                        }
                        let (k, v) = h.split_once(':').unwrap();
                        let (k, v) = (k.trim().to_ascii_lowercase(), v.trim().to_string());
                        if k == "content-length" {
                            len = v.parse().unwrap();
                        }
                        headers.push((k, v));
                    }
                    let mut body = vec![0u8; len];
                    r.read_exact(&mut body).await.unwrap();
                    let (status, v) = route(&s, &method, &path, &headers, &String::from_utf8_lossy(&body));
                    let b = v.to_string();
                    let resp =
                        format!("HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nratelimit: limit=40, remaining=35, reset=1\r\n\r\n{b}", b.len());
                    if w.write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    port
}

/// 通用 WS 推送:从 outbox(取 f 选出的那个 Vec)按顺序推给每个连接;Predict 连接还处理订阅与心跳
async fn ws_server(s: S, predict: bool) -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (sock, _) = l.accept().await.unwrap();
            let s = s.clone();
            tokio::spawn(async move {
                let ws = tokio_tungstenite::accept_async(sock).await.unwrap();
                let (mut w, mut r) = ws.split();
                let mut sent = {
                    let st = s.lock().unwrap();
                    if predict {
                        st.pf_out.len()
                    } else {
                        st.bn_out.len()
                    }
                };
                let mut tick = tokio::time::interval(Duration::from_millis(25));
                let mut n = 0u64;
                loop {
                    tokio::select! {
                        m = r.next() => match m {
                            Some(Ok(Message::Text(t))) => {
                                let v: Value = serde_json::from_str(&t).unwrap();
                                let mut st = s.lock().unwrap();
                                match v["method"].as_str() {
                                    Some("subscribe") => st.subscribed.push(v["params"][0].as_str().unwrap().to_string()),
                                    Some("unsubscribe") => { let p = v["params"][0].as_str().unwrap().to_string(); st.subscribed.retain(|x| x != &p); }
                                    _ => {}
                                }
                            }
                            Some(Ok(_)) => {}
                            _ => return,
                        },
                        _ = tick.tick() => {
                            n += 1;
                            let out: Vec<String> = {
                                let st = s.lock().unwrap();
                                let v = if predict { &st.pf_out } else { &st.bn_out };
                                v[sent.min(v.len())..].to_vec()
                            };
                            sent += out.len();
                            for m in out {
                                // 钱包事件只推给订阅了钱包频道的连接(这里只有一条连接,直接推)
                                if w.send(Message::Text(m.into())).await.is_err() { return; }
                            }
                            if predict && n.is_multiple_of(40) {
                                let hb = json!({"type": "M", "topic": "heartbeat", "data": n}).to_string();
                                if w.send(Message::Text(hb.into())).await.is_err() { return; }
                            }
                        }
                    }
                }
            });
        }
    });
    port
}

fn spawn_sim(s: S) {
    // 币安:每 100ms 一步
    let s1 = s.clone();
    tokio::spawn(async move {
        let mut u = 0u64;
        loop {
            {
                let mut st = s1.lock().unwrap();
                st.mark_open_close(now_ms());
                let z = st.gauss();
                let mut r = SIGMA * 0.1f64.sqrt() * z;
                if st.rand() < JUMP_P {
                    r += if st.rand() < 0.5 { JUMP } else { -JUMP };
                }
                st.btc *= r.exp();
                u += 1;
                let (b, a) = (st.btc - 0.005, st.btc + 0.005);
                let m = format!(r#"{{"stream":"btcusdt@bookTicker","data":{{"u":{u},"s":"BTCUSDT","b":"{b:.3}","B":"1.0","a":"{a:.3}","A":"1.0"}}}}"#);
                st.bn_out.push(m);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    // Predict:每 150ms 一拍
    tokio::spawn(async move {
        loop {
            s.lock().unwrap().tick();
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    });
}

async fn run(secs: u64, out: &std::path::Path) -> (i32, String) {
    let signer = OrderSigner::random(None).unwrap();
    let account = OrderSigner::random(None).unwrap().signer_address().to_checksum(None);
    let s: S = Arc::new(Mutex::new(Sim { owner: signer.signer_address().to_checksum(None), rng: 0x9E3779B97F4A7C15, btc: 80_000.0, ..Default::default() }));
    spawn_sim(s.clone());
    let http = http_server(s.clone()).await;
    let pws = ws_server(s.clone(), true).await;
    let bws = ws_server(s.clone(), false).await;
    std::fs::create_dir_all(out).unwrap();
    for e in std::fs::read_dir(out).unwrap() {
        let p = e.unwrap().path();
        if p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("probe_")) || p.file_name().is_some_and(|n| n == "mock_truth.json") {
            std::fs::remove_file(p).unwrap();
        }
    }
    let o = tokio::process::Command::new(env!("CARGO_BIN_EXE_predict-mm-probe"))
        .args(["--send", "--no-claim", "--quiet", "--log-dir", out.to_str().unwrap()])
        .args(["--api-base", &format!("http://127.0.0.1:{http}"), "--predict-ws", &format!("ws://127.0.0.1:{pws}/ws")])
        .args(["--binance-ws", &format!("ws://127.0.0.1:{bws}/stream"), "--rpc-url", &format!("http://127.0.0.1:{http}/rpc")])
        .args(["--duration-s", &secs.to_string(), "--poll-ms", "1000"])
        .env("PREDICT_API_KEY", API_KEY)
        .env("PREDICT_PRIVATE_KEY", signer.private_key_hex_for_tests())
        .env("PREDICT_ACCOUNT_ADDRESS", &account)
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .output();
    let o = tokio::time::timeout(Duration::from_secs(secs + 60), o).await.expect("探针没按时退出").unwrap();
    let st = s.lock().unwrap();
    let res: HashMap<String, Value> = st
        .close
        .iter()
        .filter_map(|(m, c)| {
            let k = st.k.get(m)?;
            let (pu, pd) = if c > k {
                (1.0, 0.0)
            } else if c < k {
                (0.0, 1.0)
            } else {
                (0.5, 0.5)
            };
            Some((m.to_string(), json!({"k": k, "close": c, "payout_up": pu, "payout_down": pd})))
        })
        .collect();
    let truth = json!({"sigma_per_sqrt_s": sigma_eff(), "sigma_diffusion": SIGMA, "k": st.k.iter().map(|(m, k)| (m.to_string(), json!(k))).collect::<HashMap<_, _>>(),
        "starts": st.starts.iter().map(|(m, s)| (m.to_string(), json!(s))).collect::<HashMap<_, _>>(),
        "resolutions": res, "fills": st.truth_fills, "orders": st.truth_orders});
    std::fs::write(out.join("mock_truth.json"), serde_json::to_string_pretty(&truth).unwrap()).unwrap();
    // state.json 里只有模拟账本,不要
    let _ = std::fs::remove_file(out.join("state.json"));
    (o.status.code().unwrap_or(-1), String::from_utf8_lossy(&o.stderr).to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sim_smoke_produces_fills() {
    let dir = tempfile::tempdir().unwrap();
    let (code, err) = run(15, dir.path()).await;
    assert_eq!(code, 0, "{err}");
    let truth: Value = serde_json::from_str(&std::fs::read_to_string(dir.path().join("mock_truth.json")).unwrap()).unwrap();
    // 若恰好处在最后 30 秒不挂单,就没有成交;否则应有成交
    let orders = truth["orders"].as_array().unwrap().len();
    let fills = truth["fills"].as_array().unwrap().len();
    if orders > 0 {
        assert!(fills > 0, "挂了 {orders} 张单却没有成交");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn generate() {
    let secs: u64 = std::env::var("PROBE_E2E_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(420);
    let out = std::path::PathBuf::from(std::env::var("PROBE_FAKE_LOG_DIR").expect("设 PROBE_FAKE_LOG_DIR"));
    let (code, err) = run(secs, &out).await;
    assert_eq!(code, 0, "{err}");
    let truth: Value = serde_json::from_str(&std::fs::read_to_string(out.join("mock_truth.json")).unwrap()).unwrap();
    eprintln!(
        "生成完成:{out:?};订单 {} 成交 {} 已结算轮 {}",
        truth["orders"].as_array().unwrap().len(),
        truth["fills"].as_array().unwrap().len(),
        truth["resolutions"].as_object().unwrap().len()
    );
}
