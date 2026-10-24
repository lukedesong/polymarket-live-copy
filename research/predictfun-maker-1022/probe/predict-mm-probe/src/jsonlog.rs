//! 日志:JSON Lines,gzip,按 UTC 小时滚动。每行都带本机单调时钟纳秒 `mono_ns` 与墙钟毫秒 `wall_ms`。
//! 文件名:<dir>/probe_<YYYYMMDDHH>_<启动时刻HHMMSS>.jsonl.gz(每次启动写新文件,绝不追加别的进程的 gz)。
//! 写入前对整行做脱敏:API key、JWT 等登记过的秘密字符串一律替换成 "<redacted>"。

use crate::strategy::Now;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::{json, Map, Value};
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// CLOCK_MONOTONIC 纳秒(Linux/macOS);跨进程可比,与录制器 time.time_ns() 不同(那是墙钟)
pub fn mono_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: clock_gettime 只写入传入的 timespec
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

pub fn wall_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

pub fn now() -> Now {
    Now { mono_ns: mono_ns(), wall_ms: wall_ms() }
}

/// "YYYYMMDDHH"(UTC)
pub fn utc_hour_key(wall_ms: i64) -> String {
    let d = crate::risk::day_key(wall_ms, 0).replace('-', "");
    format!("{d}{:02}", (wall_ms.div_euclid(1000) % 86400) / 3600)
}

fn hhmmss(wall_ms: i64) -> String {
    let s = wall_ms.div_euclid(1000) % 86400;
    format!("{:02}{:02}{:02}", s / 3600, (s / 60) % 60, s % 60)
}

enum Msg {
    Line(i64, String),
    Close(mpsc::Sender<()>),
}

#[derive(Clone)]
pub struct Log {
    tx: mpsc::Sender<Msg>,
    secrets: Arc<RwLock<Vec<String>>>,
    echo: bool,
}

pub fn redact(line: &str, secrets: &[String]) -> String {
    let mut s = line.to_string();
    for k in secrets {
        if k.len() >= 6 && s.contains(k.as_str()) {
            s = s.replace(k.as_str(), "<redacted>");
        }
    }
    s
}

impl Log {
    /// echo:同时把非行情类日志打印到 stderr
    pub fn start(dir: PathBuf, prefix: &str, echo: bool) -> anyhow::Result<(Log, std::thread::JoinHandle<()>)> {
        std::fs::create_dir_all(&dir)?;
        let (tx, rx) = mpsc::channel::<Msg>();
        let start = hhmmss(wall_ms());
        let prefix = prefix.to_string();
        let h = std::thread::spawn(move || {
            let mut cur: Option<(String, GzEncoder<std::fs::File>)> = None;
            let mut last_flush = std::time::Instant::now();
            loop {
                let m = rx.recv_timeout(Duration::from_secs(1));
                match m {
                    Ok(Msg::Line(wall, line)) => {
                        let key = utc_hour_key(wall);
                        if cur.as_ref().map(|c| &c.0) != Some(&key) {
                            if let Some((_, enc)) = cur.take() {
                                let _ = enc.finish();
                            }
                            let path = dir.join(format!("{prefix}_{key}_{start}.jsonl.gz"));
                            match std::fs::File::create(&path) {
                                Ok(f) => cur = Some((key, GzEncoder::new(f, Compression::new(3)))),
                                Err(e) => eprintln!("日志文件打不开 {path:?}: {e}"),
                            }
                        }
                        if let Some((_, enc)) = cur.as_mut() {
                            let _ = enc.write_all(line.as_bytes());
                            let _ = enc.write_all(b"\n");
                        }
                    }
                    Ok(Msg::Close(done)) => {
                        if let Some((_, enc)) = cur.take() {
                            let _ = enc.finish();
                        }
                        let _ = done.send(());
                        return;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        if let Some((_, enc)) = cur.take() {
                            let _ = enc.finish();
                        }
                        return;
                    }
                }
                // 每 5 秒同步刷新一次,正在写的文件也能 zcat 读到
                if last_flush.elapsed() > Duration::from_secs(5) {
                    if let Some((_, enc)) = cur.as_mut() {
                        let _ = enc.flush();
                    }
                    last_flush = std::time::Instant::now();
                }
            }
        });
        Ok((Log { tx, secrets: Arc::new(RwLock::new(vec![])), echo }, h))
    }

    /// 登记需要脱敏的字符串(API key、JWT、私钥…)
    pub fn add_secret(&self, s: &str) {
        let s = s.trim();
        if s.len() >= 6 {
            self.secrets.write().unwrap().push(s.to_string());
        }
    }

    pub fn redact_str(&self, s: &str) -> String {
        redact(s, &self.secrets.read().unwrap())
    }

    fn write(&self, wall: i64, line: String, echo: bool) {
        let line = redact(&line, &self.secrets.read().unwrap());
        if echo && self.echo {
            eprintln!("{line}");
        }
        let _ = self.tx.send(Msg::Line(wall, line));
    }

    /// 普通事件:{"mono_ns","wall_ms","kind", ...fields}
    pub fn ev(&self, kind: &str, fields: Value) {
        self.ev_at(now(), kind, fields)
    }

    pub fn ev_at(&self, t: Now, kind: &str, fields: Value) {
        let mut m = Map::new();
        m.insert("mono_ns".into(), json!(t.mono_ns));
        m.insert("wall_ms".into(), json!(t.wall_ms));
        m.insert("kind".into(), json!(kind));
        if let Value::Object(f) = fields {
            m.extend(f);
        } else if !fields.is_null() {
            m.insert("data".into(), fields);
        }
        let echo = !matches!(kind, "bn" | "pf" | "pf_rest");
        self.write(t.wall_ms, Value::Object(m).to_string(), echo);
    }

    /// 行情原文(收到即记,原样保留为字符串,和录制器的原文可直接对比)
    pub fn raw(&self, t: Now, kind: &str, raw: &str) {
        let line = format!(r#"{{"mono_ns":{},"wall_ms":{},"kind":"{}","raw":{}}}"#, t.mono_ns, t.wall_ms, kind, Value::String(raw.to_string()));
        self.write(t.wall_ms, line, false);
    }

    /// 刷盘并关闭(gzip 收尾)
    pub fn close(&self) {
        let (tx, rx) = mpsc::channel();
        if self.tx.send(Msg::Close(tx)).is_ok() {
            let _ = rx.recv_timeout(Duration::from_secs(10));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn writes_gzip_jsonl_and_redacts() {
        let dir = tempfile::tempdir().unwrap();
        let (log, h) = Log::start(dir.path().to_path_buf(), "probe", false).unwrap();
        log.add_secret("supersecretjwt.abc.def");
        log.ev("info", json!({"msg": "hello", "topic": "predictWalletEvents/supersecretjwt.abc.def"}));
        log.raw(now(), "bn", r#"{"stream":"btcusdt@bookTicker","data":{"b":"1"}}"#);
        log.close();
        h.join().unwrap();
        let files: Vec<_> = std::fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().path()).collect();
        assert_eq!(files.len(), 1);
        let name = files[0].file_name().unwrap().to_string_lossy().to_string();
        assert!(name.starts_with("probe_") && name.ends_with(".jsonl.gz"), "{name}");
        let mut s = String::new();
        flate2::read::GzDecoder::new(std::fs::File::open(&files[0]).unwrap()).read_to_string(&mut s).unwrap();
        let lines: Vec<Value> = s.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        assert_eq!(lines.len(), 2);
        assert!(!s.contains("supersecretjwt"));
        assert_eq!(lines[0]["topic"], "predictWalletEvents/<redacted>");
        assert!(lines[0]["mono_ns"].as_u64().unwrap() > 0);
        assert!(lines[0]["wall_ms"].as_i64().unwrap() > 1_700_000_000_000);
        let raw: Value = serde_json::from_str(lines[1]["raw"].as_str().unwrap()).unwrap();
        assert_eq!(raw["data"]["b"], "1");
    }

    #[test]
    fn hour_key() {
        assert_eq!(utc_hour_key(1_792_818_000_000), "2026102405");
    }
}
