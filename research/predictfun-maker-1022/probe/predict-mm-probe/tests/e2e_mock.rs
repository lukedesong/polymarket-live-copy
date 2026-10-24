//! 端到端:本机起一个假的 Predict(REST + WS)、币安 WS、BNB RPC,把编译好的可执行文件整个跑起来。
//! 全部在 127.0.0.1,不连外网;私钥 / 存款地址 / API key 都是测试里现场随机生成的。
//! 覆盖:dry-run 不发任何写请求;--send 鉴权 → 启动全撤 → 挂单(请求体结构)→ 改价撤单 → 钱包事件成交 → 退出全撤;
//! 429 → 全撤 + 冷却;日志里不出现任何凭证。
use futures_util::{SinkExt, StreamExt};
use predict_mm_probe::signing::OrderSigner;
use serde_json::{json, Value};
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

#[derive(Default)]
struct MockOrder {
    id: String,
    hash: String,
    shares: f64,
    status: String,
    filled: f64,
}

#[derive(Default)]
struct State {
    owner: String,
    requests: Vec<(String, String)>,
    posts: Vec<Value>,
    cancels: Vec<Vec<String>>,
    orders: Vec<MockOrder>,
    wallet_events: Vec<String>,
    heartbeat_replies: usize,
    subscribed: Vec<String>,
    fail_429_on_place: Option<usize>,
    places: usize,
    auth_headers_ok: bool,
    auth_headers_bad: usize,
    market_start: std::collections::HashMap<u64, i64>,
}

type S = Arc<Mutex<State>>;
const JWT: &str = "mockjwt.header.payload-signature-123";
const API_KEY: &str = "mock-api-key-0123456789";

fn market_id(start_s: i64) -> u64 {
    1_000_000 + ((start_s / 300) % 100_000) as u64
}

/// 收盘 30 秒后出结果:Up 赢
fn market_json(id: u64, start_s: Option<i64>) -> Value {
    let resolved = start_s.is_some_and(|s| predict_mm_probe::jsonlog::wall_ms() / 1000 > s + 300 + 30);
    let (su, sd) = if resolved { (json!("WON"), json!("LOST")) } else { (Value::Null, Value::Null) };
    json!({"id": id, "feeRateBps": 200, "isNegRisk": false, "isYieldBearing": false, "status": "REGISTERED", "tradingStatus": "OPEN",
        "conditionId": format!("0x{:064x}", id),
        "outcomes": [{"name": "Up", "indexSet": 1, "onChainId": (id * 2).to_string(), "status": su},
                     {"name": "Down", "indexSet": 2, "onChainId": (id * 2 + 1).to_string(), "status": sd}]})
}

fn route(s: &S, method: &str, path: &str, headers: &[(String, String)], body: &str) -> (u16, Value) {
    let mut st = s.lock().unwrap();
    st.requests.push((method.to_string(), path.to_string()));
    let authed = headers.iter().any(|(k, v)| k == "authorization" && v == &format!("Bearer {JWT}"));
    let keyed = headers.iter().any(|(k, v)| k == "x-api-key" && v == API_KEY);
    let needs_auth = path.starts_with("/v1/orders");
    if needs_auth {
        if authed && keyed {
            st.auth_headers_ok = true;
        } else {
            st.auth_headers_bad += 1;
            return (401, json!({"success": false}));
        }
    }
    let p = path.split('?').next().unwrap();
    match (method, p) {
        ("POST", "/rpc") => {
            let req: Value = serde_json::from_str(body).unwrap();
            let res = match req["method"].as_str().unwrap() {
                "eth_call" => json!(format!("0x{:0>64}", st.owner.trim_start_matches("0x").to_lowercase())),
                "eth_chainId" => json!("0x38"),
                _ => json!(null),
            };
            (200, json!({"jsonrpc": "2.0", "id": req["id"], "result": res}))
        }
        ("GET", "/v1/auth/message") => (200, json!({"data": {"message": "Sign in to mock predict\nNonce: 42"}})),
        ("POST", "/v1/auth") => {
            let b: Value = serde_json::from_str(body).unwrap();
            assert!(b["signature"].as_str().unwrap().len() == 2 + 86 * 2, "鉴权签名应为 86 字节");
            (200, json!({"data": {"token": JWT}}))
        }
        ("GET", p) if p.starts_with("/v1/categories/btc-updown-5m-") => {
            let s: i64 = p.rsplit('-').next().unwrap().parse().unwrap();
            st.market_start.insert(market_id(s), s);
            (200, json!({"data": {"markets": [{"id": market_id(s)}]}}))
        }
        ("GET", p) if p.starts_with("/v1/markets/") && p.ends_with("/orderbook") => (200, json!({"data": {"bids": [[0.5, 80.0]], "asks": [[0.55, 30.0]], "version": 1}})),
        ("GET", p) if p.starts_with("/v1/markets/") => {
            let id: u64 = p.trim_start_matches("/v1/markets/").parse().unwrap();
            (200, json!({"data": market_json(id, st.market_start.get(&id).copied())}))
        }
        ("POST", "/v1/orders") => {
            st.places += 1;
            if st.fail_429_on_place == Some(st.places) {
                return (429, json!({"success": false, "message": "Too Many Requests"}));
            }
            let b: Value = serde_json::from_str(body).unwrap();
            let o = &b["data"]["order"];
            let hash = o["hash"].as_str().unwrap().to_string();
            let shares = o["takerAmount"].as_str().unwrap().parse::<f64>().unwrap() / 1e18;
            let id = (6_000_000_000u64 + st.places as u64).to_string();
            st.posts.push(b.clone());
            st.orders.push(MockOrder { id: id.clone(), hash: hash.clone(), shares, status: "OPEN".into(), filled: 0.0 });
            // 第一张单 300ms 后全部成交,推钱包事件
            if st.places == 1 {
                let s2 = s.clone();
                let (id2, h2) = (id.clone(), hash.clone());
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    let mut st = s2.lock().unwrap();
                    if let Some(o) = st.orders.iter_mut().find(|o| o.id == id2) {
                        if o.status == "OPEN" {
                            o.status = "FILLED".into();
                            o.filled = o.shares;
                            // 实盘录到的格式:先"已撮合"(按 settlementId),再"上链成功"(同一 settlementId,不能重复记账)
                            let wei = format!("{}000000000000000000", o.shares as u64);
                            let fill = json!({"executedPriceWei": "500000000000000000", "executedSizeWei": wei, "executedValueWei": "0"});
                            for typ in ["orderTransactionSubmitted", "orderTransactionSuccess"] {
                                let ev = json!({"type": "M", "topic": format!("predictWalletEvents/{JWT}"), "data": {"type": typ, "orderId": id2, "orderHash": h2,
                                    "settlementId": "s-1", "fill": fill, "isMaker": true, "timestamp": 1792848900367i64}});
                                st.wallet_events.push(ev.to_string());
                            }
                        }
                    }
                });
            }
            (200, json!({"success": true, "data": {"orderId": id, "orderHash": hash}}))
        }
        ("POST", "/v1/orders/remove") => {
            let b: Value = serde_json::from_str(body).unwrap();
            let ids: Vec<String> = b["data"]["ids"].as_array().unwrap().iter().map(|x| x.as_str().unwrap().to_string()).collect();
            assert!(ids.len() <= 100);
            for o in st.orders.iter_mut() {
                if ids.contains(&o.id) && o.status == "OPEN" {
                    o.status = "CANCELLED".into();
                }
            }
            st.cancels.push(ids);
            (200, json!({"success": true}))
        }
        ("GET", "/v1/orders") => {
            let list: Vec<Value> =
                st.orders.iter().filter(|o| o.status == "OPEN").map(|o| json!({"id": o.id, "status": "OPEN", "amountFilled": "0", "order": {"hash": o.hash}})).collect();
            (200, json!({"success": true, "data": list}))
        }
        ("GET", p) if p.starts_with("/v1/orders/") => {
            let h = p.trim_start_matches("/v1/orders/");
            match st.orders.iter().find(|o| o.hash == h) {
                Some(o) => (200, json!({"data": {"id": o.id, "status": o.status, "amountFilled": format!("{}", (o.filled * 1e18) as u128), "order": {"hash": o.hash}}})),
                None => (404, json!({"success": false})),
            }
        }
        _ => (404, json!({"success": false, "path": path})),
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
                    let resp = format!(
                        "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nratelimit-policy: \"sec\";q=40;w=1\r\nratelimit: limit=40, remaining=35, reset=1\r\n\r\n{b}",
                        b.len()
                    );
                    if w.write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    port
}

async fn predict_ws(s: S) -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (sock, _) = l.accept().await.unwrap();
            let s = s.clone();
            tokio::spawn(async move {
                let ws = tokio_tungstenite::accept_async(sock).await.unwrap();
                let (mut w, mut r) = ws.split();
                let mut tick = tokio::time::interval(Duration::from_millis(150));
                let mut n = 0u64;
                let mut sent_wallet = 0usize;
                loop {
                    tokio::select! {
                        m = r.next() => match m {
                            Some(Ok(Message::Text(t))) => {
                                let v: Value = serde_json::from_str(&t).unwrap();
                                let mut st = s.lock().unwrap();
                                match v["method"].as_str() {
                                    Some("subscribe") => st.subscribed.push(v["params"][0].as_str().unwrap().to_string()),
                                    Some("unsubscribe") => { let p = v["params"][0].as_str().unwrap().to_string(); st.subscribed.retain(|x| x != &p); }
                                    Some("heartbeat") => st.heartbeat_replies += 1,
                                    _ => {}
                                }
                            }
                            Some(Ok(_)) => {}
                            _ => return,
                        },
                        _ = tick.tick() => {
                            n += 1;
                            let (books, wallet): (Vec<String>, Vec<String>) = {
                                let st = s.lock().unwrap();
                                // 买一在 0.50 / 0.51 之间每 1.5 秒切换一次 → 触发改价
                                let bid = if (n / 10).is_multiple_of(2) { 0.5 } else { 0.51 };
                                let books = st.subscribed.iter().filter_map(|t| t.strip_prefix("predictOrderbook/")).map(|id| {
                                    json!({"type": "M", "topic": format!("predictOrderbook/{id}"), "data": {"asks": [[0.55, 30.0], [0.6, 5.0]], "bids": [[bid, 80.0], [0.4, 10.0]],
                                        "marketId": id.parse::<u64>().unwrap(), "orderCount": 3, "settlementsPending": {"asks": [], "bids": []}, "updateTimestampMs": 0, "version": n}}).to_string()
                                }).collect();
                                let wallet = if st.subscribed.iter().any(|t| t.starts_with("predictWalletEvents/")) { st.wallet_events[sent_wallet.min(st.wallet_events.len())..].to_vec() } else { vec![] };
                                (books, wallet)
                            };
                            sent_wallet += wallet.len();
                            for b in books.into_iter().chain(wallet) {
                                if w.send(Message::Text(b.into())).await.is_err() { return; }
                            }
                            if n.is_multiple_of(7) {
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

async fn binance_ws() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (sock, _) = l.accept().await.unwrap();
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(sock).await.unwrap();
                let mut u = 0u64;
                loop {
                    u += 1;
                    let m = format!(r#"{{"stream":"btcusdt@bookTicker","data":{{"u":{u},"s":"BTCUSDT","b":"80880.00","B":"1.0","a":"80880.01","A":"1.0"}}}}"#);
                    if ws.send(Message::Text(m.into())).await.is_err() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            });
        }
    });
    port
}

fn read_logs(dir: &std::path::Path) -> (String, Vec<Value>) {
    let mut all = String::new();
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.extension().is_some_and(|x| x == "gz") {
            let mut s = String::new();
            flate2::read::GzDecoder::new(std::fs::File::open(&p).unwrap()).read_to_string(&mut s).unwrap();
            all.push_str(&s);
        }
    }
    let lines = all.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    (all, lines)
}

/// 离本轮收盘不足 45 秒时等到下一轮开盘(否则最后 30 秒不挂单,测不到)
async fn wait_for_quotable_window() {
    let now = predict_mm_probe::jsonlog::wall_ms();
    let left = 300_000 - now.rem_euclid(300_000);
    if left < 45_000 {
        tokio::time::sleep(Duration::from_millis(left as u64 + 1_500)).await;
    }
}

struct Run {
    code: i32,
    stderr: String,
    all_logs: String,
    logs: Vec<Value>,
}

async fn run_probe(s: &S, send: bool, extra: &[&str], secs: u64, signer: &OrderSigner, account: &str) -> Run {
    let http = http_server(s.clone()).await;
    let pws = predict_ws(s.clone()).await;
    let bws = binance_ws().await;
    let dir = tempfile::tempdir().unwrap();
    let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_predict-mm-probe"));
    cmd.args([
        "--log-dir",
        dir.path().to_str().unwrap(),
        "--api-base",
        &format!("http://127.0.0.1:{http}"),
        "--predict-ws",
        &format!("ws://127.0.0.1:{pws}/ws"),
        "--binance-ws",
        &format!("ws://127.0.0.1:{bws}/stream?streams=btcusdt@bookTicker"),
        "--rpc-url",
        &format!("http://127.0.0.1:{http}/rpc"),
        "--duration-s",
        &secs.to_string(),
        "--poll-ms",
        "500",
        "--no-claim",
    ]);
    cmd.args(extra);
    if send {
        cmd.arg("--send");
        cmd.env("PREDICT_API_KEY", API_KEY).env("PREDICT_PRIVATE_KEY", signer.private_key_hex_for_tests()).env("PREDICT_ACCOUNT_ADDRESS", account);
    } else {
        cmd.env_remove("PREDICT_API_KEY").env_remove("PREDICT_PRIVATE_KEY").env_remove("PREDICT_ACCOUNT_ADDRESS");
    }
    cmd.env("NO_PROXY", "127.0.0.1,localhost").env("no_proxy", "127.0.0.1,localhost");
    let out = tokio::time::timeout(Duration::from_secs(secs + 40), cmd.output()).await.expect("探针没按时退出").unwrap();
    let (all_logs, logs) = read_logs(dir.path());
    if let Ok(d) = std::env::var("PROBE_E2E_DUMP") {
        std::fs::write(format!("{d}/{}.jsonl", if send { "send" } else { "dry" }), &all_logs).unwrap();
    }
    Run { code: out.status.code().unwrap_or(-1), stderr: String::from_utf8_lossy(&out.stderr).to_string(), all_logs, logs }
}

fn kinds(logs: &[Value], k: &str) -> Vec<Value> {
    logs.iter().filter(|l| l["kind"] == k).cloned().collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_dry_run_sends_no_writes() {
    wait_for_quotable_window().await;
    let s: S = Arc::new(Mutex::new(State::default()));
    let signer = OrderSigner::random(None).unwrap();
    let r = run_probe(&s, false, &[], 8, &signer, "").await;
    assert_eq!(r.code, 0, "{}", r.stderr);
    let st = s.lock().unwrap();
    assert!(st.requests.iter().all(|(m, _)| m == "GET"), "dry-run 只能有 GET: {:?}", st.requests);
    assert!(st.requests.iter().all(|(_, p)| !p.starts_with("/v1/orders") && !p.starts_with("/v1/auth")));
    let dry = kinds(&r.logs, "dry_request");
    assert!(dry.iter().any(|d| d["path"] == "/v1/orders"), "应打印将要发的下单请求");
    // 打印的请求体里签名已隐藏
    let body = &dry.iter().find(|d| d["path"] == "/v1/orders").unwrap()["body"];
    assert!(body["data"]["order"]["signature"].as_str().unwrap().contains("已隐藏"));
    assert_eq!(body["data"]["isPostOnly"], true);
    assert!(!kinds(&r.logs, "bn").is_empty() && !kinds(&r.logs, "pf").is_empty());
    assert!(st.heartbeat_replies >= 1, "要回心跳");
    // 改价在 dry-run 里也会走一遍(模拟回报)
    assert!(kinds(&r.logs, "decision").iter().any(|d| d["action"] == "cancel" && d["reason"].as_str().unwrap().contains("改价")));
    assert!(kinds(&r.logs, "exit").len() == 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_send_against_mock() {
    wait_for_quotable_window().await;
    let signer = OrderSigner::random(None).unwrap();
    let account = OrderSigner::random(None).unwrap().signer_address().to_checksum(None);
    let s: S = Arc::new(Mutex::new(State { owner: signer.signer_address().to_checksum(None), ..Default::default() }));
    let r = run_probe(&s, true, &[], 10, &signer, &account).await;
    assert_eq!(r.code, 0, "退出码 {}:{}", r.code, r.stderr);
    let st = s.lock().unwrap();
    assert!(st.auth_headers_ok && st.auth_headers_bad == 0, "订单接口都要带 x-api-key 与 JWT");
    // 启动全撤:鉴权之后、第一次下单之前就列了挂单
    let idx = |pred: &dyn Fn(&(String, String)) -> bool| st.requests.iter().position(pred);
    let auth = idx(&|r| r.1 == "/v1/auth").unwrap();
    let first_list = idx(&|r| r.0 == "GET" && r.1.starts_with("/v1/orders?")).unwrap();
    let first_post = idx(&|r| r.0 == "POST" && r.1 == "/v1/orders").unwrap();
    assert!(auth < first_list && first_list < first_post, "{:?}", st.requests);
    // 请求体结构
    // 对齐做法(默认):开盘两边同股数各一张;Up 成交后 Up 不再挂,只挂 Down 补齐(模拟器不会成交 Down)
    assert!(st.posts.len() >= 2, "下单 {} 次", st.posts.len());
    let sizes: Vec<String> = st.posts.iter().take(2).map(|b| b["data"]["order"]["takerAmount"].as_str().unwrap().to_string()).collect();
    assert_eq!(sizes[0], sizes[1], "两边同股数");
    for b in &st.posts {
        let d = &b["data"];
        assert_eq!(d["strategy"], "LIMIT");
        assert_eq!(d["isPostOnly"], true);
        let o = &d["order"];
        assert_eq!(o["maker"].as_str().unwrap(), account);
        assert_eq!(o["signer"].as_str().unwrap(), account, "predictAccount 模式 maker = signer = 存款地址");
        assert_eq!(o["side"], 0);
        assert_eq!(o["expiration"], 4102444800u64);
        assert_eq!(o["feeRateBps"], "200");
        assert_eq!(o["signature"].as_str().unwrap().len(), 2 + 86 * 2);
        assert!(o["signature"].as_str().unwrap().starts_with("0x01845adb2c711129d4f3966735ed98a9f09fc4ce57"));
        let price: u128 = d["pricePerShare"].as_str().unwrap().parse().unwrap();
        assert!(price == 500_000_000_000_000_000 || price == 510_000_000_000_000_000 || price == 450_000_000_000_000_000, "{price}");
    }
    // 改价撤单发生过;退出时没有残留挂单
    assert!(!st.cancels.is_empty());
    assert!(st.orders.iter().all(|o| o.status != "OPEN"), "退出后不应有挂单");
    drop(st);
    // 成交被记录(钱包事件 + REST 核实),账本有持仓
    let fills = kinds(&r.logs, "fill");
    assert!(fills.iter().any(|f| f["source"] == "wallet_submitted"), "收到'已撮合'就记成交: {fills:?}");
    assert!(!fills.iter().any(|f| f["source"] == "wallet_success"), "'上链成功'是同一笔,不能再记一次: {fills:?}");

    let exit = kinds(&r.logs, "exit");
    assert_eq!(exit.len(), 1);
    let open = exit[0]["ledger"]["open"].as_array().unwrap();
    assert!(open.iter().any(|r| r["up_shares"].as_f64().unwrap() >= 2.0 || r["down_shares"].as_f64().unwrap() >= 2.0), "{open:?}");
    // 每个请求都有发出/返回记录与限额头
    let resps = kinds(&r.logs, "resp");
    assert!(resps.iter().filter(|x| x["tag"] == "place").all(|x| x["status"] == 200 && x["ratelimit"].is_string() && x["rtt_ms"].as_f64().is_some()));
    // 决策日志带盘口与币安价
    assert!(kinds(&r.logs, "decision").iter().any(|d| d["action"] == "place" && d["ctx"]["bn_mid"].as_f64().is_some() && d["ctx"]["queue_ahead"].as_f64().is_some()));
    // 凭证绝不出现在日志 / 输出里
    for secret in [API_KEY, JWT, signer.private_key_hex_for_tests().trim_start_matches("0x")] {
        assert!(!r.all_logs.contains(secret), "日志泄露凭证");
        assert!(!r.stderr.contains(secret), "输出泄露凭证");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_429_cancels_all_and_cools_down() {
    wait_for_quotable_window().await;
    let signer = OrderSigner::random(None).unwrap();
    let account = OrderSigner::random(None).unwrap().signer_address().to_checksum(None);
    let s: S = Arc::new(Mutex::new(State { owner: signer.signer_address().to_checksum(None), fail_429_on_place: Some(3), ..Default::default() }));
    let r = run_probe(&s, true, &["--cooldown-s", "60", "--mode", "independent"], 8, &signer, &account).await;
    assert_eq!(r.code, 0, "{}", r.stderr);
    let st = s.lock().unwrap();
    // 第 3 次下单 429 → 之后冷却 60 秒,8 秒内不再下单
    assert_eq!(st.places, 3, "429 后不应再下单: {}", st.places);
    assert!(st.orders.iter().all(|o| o.status != "OPEN"));
    drop(st);
    assert!(kinds(&r.logs, "decision").iter().any(|d| d["action"] == "cancel_all" && d["reason"].as_str().unwrap().contains("RateLimited")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_send_refuses_wrong_owner() {
    let signer = OrderSigner::random(None).unwrap();
    let account = OrderSigner::random(None).unwrap().signer_address().to_checksum(None);
    // RPC 说存款地址的 owner 是别人
    let s: S = Arc::new(Mutex::new(State { owner: account.clone(), ..Default::default() }));
    let r = run_probe(&s, true, &[], 5, &signer, &account).await;
    assert_ne!(r.code, 0);
    assert!(r.stderr.contains("owner"), "{}", r.stderr);
    assert!(s.lock().unwrap().posts.is_empty());
}

/// 长跑(默认 600 秒,PROBE_E2E_SECS 可改):跨轮次切换、最后 30 秒停挂、收盘、结算。默认不跑:
///   cargo test --release --test e2e_mock long_run -- --ignored --nocapture
async fn long_run(send: bool) {
    let secs: u64 = std::env::var("PROBE_E2E_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(600);
    let signer = OrderSigner::random(None).unwrap();
    let account = OrderSigner::random(None).unwrap().signer_address().to_checksum(None);
    let s: S = Arc::new(Mutex::new(State { owner: signer.signer_address().to_checksum(None), ..Default::default() }));
    let t0 = std::time::Instant::now();
    let r = run_probe(&s, send, &[], secs, &signer, &account).await;
    let el = t0.elapsed().as_secs_f64();
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert!(kinds(&r.logs, "fatal").is_empty());
    let rs = kinds(&r.logs, "round_start");
    let re = kinds(&r.logs, "round_end");
    let st = kinds(&r.logs, "settle");
    let places: Vec<Value> = kinds(&r.logs, "decision").into_iter().filter(|d| d["action"] == "place").collect();
    // 最后 30 秒、开盘前都不能挂
    for p in &places {
        let left = p["ctx"]["ms_left"].as_i64().unwrap();
        assert!(left > 30_000 && left <= 300_000, "剩 {left}ms 时挂单");
    }
    let stm = s.lock().unwrap();
    assert!(stm.orders.iter().all(|o| o.status != "OPEN"));
    let mode = if send { "send" } else { "dry-run" };
    eprintln!(
        "[{mode}] {el:.0}s:round_start {} round_end {} settle {} 挂单决策 {} 下单请求 {} 撤单请求 {} 心跳回复 {} 日志行 {}",
        rs.len(),
        re.len(),
        st.len(),
        places.len(),
        stm.posts.len(),
        stm.cancels.len(),
        stm.heartbeat_replies,
        r.logs.len()
    );
    if secs >= 400 {
        assert!(rs.len() >= 2 && !re.is_empty(), "应跨过轮次");
        assert!(!st.is_empty(), "应有结算");
    }
    if !send {
        assert!(stm.requests.iter().all(|(m, _)| m == "GET"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn long_run_dry() {
    long_run(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn long_run_send() {
    long_run(true).await;
}
