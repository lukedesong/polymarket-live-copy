//! 配置:命令行参数(都有对应环境变量 PROBE_*),凭证只从环境变量读(不进命令行、不进 --help、不打印)。

use crate::ratelimit::RateLimitConfig;
use crate::strategy::Params;
use anyhow::{bail, Result};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(name = "predict-mm-probe", version, about = "Predict.fun BTC 5m 实盘探针(校准回放器成交模型)。默认 --dry-run:连真实行情、做全部决策,只打印将要发的请求。")]
pub struct Cli {
    /// 真发单(需要 PREDICT_API_KEY / PREDICT_PRIVATE_KEY / PREDICT_ACCOUNT_ADDRESS)
    #[arg(long, conflicts_with = "dry_run")]
    pub send: bool,
    /// 只打印将要发的请求(默认)
    #[arg(long)]
    pub dry_run: bool,
    /// 清除账本里的停机标记(亏损超限仍会立刻再次停机)
    #[arg(long)]
    pub clear_halt: bool,
    /// 不把日志同时打印到 stderr(文件照写)
    #[arg(long, env = "PROBE_QUIET")]
    pub quiet: bool,

    #[arg(long, env = "PROBE_LOG_DIR", default_value = "logs", hide_env_values = true)]
    pub log_dir: PathBuf,
    /// 账本文件(默认 <log_dir>/state.json;dry-run 用 state-dry.json,互不影响)
    #[arg(long, env = "PROBE_STATE_FILE", hide_env_values = true)]
    pub state_file: Option<PathBuf>,
    /// 运行多少秒后自动全撤退出(0 = 一直跑)
    #[arg(long, env = "PROBE_DURATION_S", default_value_t = 0)]
    pub duration_s: u64,
    /// 跑完多少轮后自动全撤退出(0 = 不限)
    #[arg(long, env = "PROBE_MAX_ROUNDS", default_value_t = 0)]
    pub max_rounds: u32,

    #[arg(long, env = "PROBE_API_BASE", default_value = "https://api.predict.fun")]
    pub api_base: String,
    #[arg(long, env = "PROBE_PREDICT_WS", default_value = "wss://ws.predict.fun/ws")]
    pub predict_ws: String,
    #[arg(long, env = "PROBE_BINANCE_WS", default_value = "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker")]
    pub binance_ws: String,
    /// BNB 链 RPC(claim/merge、核对存款地址 owner)
    #[arg(long, env = "PREDICT_RPC_URL", default_value = "https://bsc-rpc.publicnode.com", hide_env_values = true)]
    pub rpc_url: String,
    /// 不做结算后的自动 claim/merge
    #[arg(long, env = "PROBE_NO_CLAIM")]
    pub no_claim: bool,

    // ---- 策略参数(默认值 = PROBE_SPEC.md 第 2 节)----
    #[arg(long, env = "PROBE_STOP_BEFORE_END_S", default_value_t = 30)]
    pub stop_before_end_s: i64,
    #[arg(long, env = "PROBE_MIN_NOTIONAL_USD", default_value_t = 1.0)]
    pub min_notional_usd: f64,
    /// 挂单满这么久才允许改价。默认 0 = 最优价一变就跟(10-24 Luke 定:排队按时间,等就往后排)
    #[arg(long, env = "PROBE_REPRICE_MIN_AGE_MS", default_value_t = 0)]
    pub reprice_min_age_ms: u64,
    #[arg(long, env = "PROBE_BN_WINDOW_MS", default_value_t = 300)]
    pub bn_window_ms: u64,
    #[arg(long, env = "PROBE_BN_MOVE_BP", default_value_t = 0.2)]
    pub bn_move_bp: f64,
    #[arg(long, env = "PROBE_BN_QUIET_MS", default_value_t = 300)]
    pub bn_quiet_ms: u64,
    /// 挂单做法:aligned = 两边同股数、多出来只挂另一边补齐且这一对不超过 $1(默认);independent = 旧做法两边各挂各的
    #[arg(long, env = "PROBE_MODE", default_value = "aligned", value_parser = ["aligned", "independent"])]
    pub mode: String,
    #[arg(long, env = "PROBE_INVENTORY_CAP_USD", default_value_t = 6.0)]
    pub inventory_cap_usd: f64,
    /// 每轮两边合计投入上限(已成交成本 + 在挂单金额);只限净持仓会把余额全押进一轮(10-24 实盘)
    #[arg(long, env = "PROBE_GROSS_CAP_USD", default_value_t = 150.0)]
    pub gross_cap_usd: f64,
    /// 按分钟匀速:开盘后第 k 分钟内合计投入 ≤ k × 此值;0 = 不限
    #[arg(long, env = "PROBE_GROSS_PER_MIN_USD", default_value_t = 0.0)]
    pub gross_per_min_usd: f64,
    /// 所有未结算轮合计投入上限(下一轮开盘时上一轮的钱要 10~70 秒后才由平台自动领回);应 ≤ 账户余额
    #[arg(long, env = "PROBE_CAPITAL_CAP_USD", default_value_t = 80.0)]
    pub capital_cap_usd: f64,
    #[arg(long, env = "PROBE_MAX_DAILY_LOSS_USD", default_value_t = 30.0)]
    pub max_daily_loss_usd: f64,
    #[arg(long, env = "PROBE_MAX_TOTAL_LOSS_USD", default_value_t = 100.0)]
    pub max_total_loss_usd: f64,
    #[arg(long, env = "PROBE_FEED_DOWN_MS", default_value_t = 3000)]
    pub feed_down_ms: u64,
    #[arg(long, env = "PROBE_BINANCE_STALE_MS", default_value_t = 1000)]
    pub binance_stale_ms: u64,
    #[arg(long, env = "PROBE_PREDICT_STALE_MS", default_value_t = 15000)]
    pub predict_stale_ms: u64,
    #[arg(long, env = "PROBE_COOLDOWN_S", default_value_t = 60)]
    pub cooldown_s: u64,
    #[arg(long, env = "PROBE_ACK_TIMEOUT_MS", default_value_t = 5000)]
    pub ack_timeout_ms: u64,
    #[arg(long, env = "PROBE_MAX_COOLDOWNS_PER_HOUR", default_value_t = 5)]
    pub max_cooldowns_per_hour: usize,
    /// "当日"的时区偏移小时(0 = UTC,8 = 北京时间)
    #[arg(long, env = "PROBE_DAY_OFFSET_HOURS", default_value_t = 0)]
    pub day_offset_hours: i64,

    // ---- 限额 ----
    #[arg(long, env = "PROBE_RATE_PER_SEC", default_value_t = 40)]
    pub rate_per_sec: u32,
    #[arg(long, env = "PROBE_RATE_PER_MIN", default_value_t = 500)]
    pub rate_per_min: u32,
    #[arg(long, env = "PROBE_RATE_MARGIN", default_value_t = 0.2)]
    pub rate_margin: f64,
    /// REST 查挂单兜底的间隔
    #[arg(long, env = "PROBE_POLL_MS", default_value_t = 3000)]
    pub poll_ms: u64,
}

/// 凭证:只从环境变量读。Debug 不输出内容。
#[derive(Clone, Default)]
pub struct Creds {
    pub api_key: Option<String>,
    pub private_key: Option<String>,
    pub account: Option<String>,
}

impl std::fmt::Debug for Creds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Creds")
            .field("api_key", &self.api_key.as_ref().map(|_| "<set>"))
            .field("private_key", &self.private_key.as_ref().map(|_| "<set>"))
            .field("account", &self.account)
            .finish()
    }
}

impl Creds {
    pub fn from_env() -> Self {
        let get = |k: &str| std::env::var(k).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
        Self { api_key: get("PREDICT_API_KEY"), private_key: get("PREDICT_PRIVATE_KEY"), account: get("PREDICT_ACCOUNT_ADDRESS") }
    }
}

impl Cli {
    pub fn is_send(&self) -> bool {
        self.send
    }

    pub fn state_path(&self) -> PathBuf {
        self.state_file.clone().unwrap_or_else(|| self.log_dir.join(if self.send { "state.json" } else { "state-dry.json" }))
    }

    pub fn params(&self) -> Params {
        Params {
            stop_before_end_ms: self.stop_before_end_s * 1000,
            min_notional_usd: self.min_notional_usd,
            reprice_min_age_ms: self.reprice_min_age_ms,
            bn_window_ms: self.bn_window_ms,
            bn_move_bp: self.bn_move_bp,
            bn_quiet_ms: self.bn_quiet_ms,
            inventory_cap_usd: self.inventory_cap_usd,
            aligned: self.mode == "aligned",
            gross_cap_usd: self.gross_cap_usd,
            gross_per_min_usd: self.gross_per_min_usd,
            capital_cap_usd: self.capital_cap_usd,
            max_daily_loss_usd: self.max_daily_loss_usd,
            max_total_loss_usd: self.max_total_loss_usd,
            feed_down_ms: self.feed_down_ms,
            binance_stale_ms: self.binance_stale_ms,
            predict_stale_ms: self.predict_stale_ms,
            cooldown_ms: self.cooldown_s * 1000,
            ack_timeout_ms: self.ack_timeout_ms,
            max_cooldowns_per_hour: self.max_cooldowns_per_hour,
        }
    }

    pub fn rate(&self) -> RateLimitConfig {
        RateLimitConfig { per_sec: self.rate_per_sec, per_min: self.rate_per_min, margin: self.rate_margin, ..Default::default() }
    }

    /// 参数合法性 + --send 时的硬上限(防手滑把探针变成真做市)
    pub fn validate(&self, creds: &Creds) -> Result<()> {
        if !(0.0..1.0).contains(&self.rate_margin) || self.rate_per_sec == 0 || self.rate_per_min == 0 {
            bail!("限额参数不合法");
        }
        if self.stop_before_end_s < 0
            || self.min_notional_usd <= 0.0
            || self.bn_move_bp <= 0.0
            || !(self.min_notional_usd.is_finite()
                && self.bn_move_bp.is_finite()
                && self.gross_cap_usd.is_finite()
                && self.gross_per_min_usd.is_finite()
                && self.capital_cap_usd.is_finite()
                && self.inventory_cap_usd.is_finite()
                && self.max_daily_loss_usd.is_finite()
                && self.max_total_loss_usd.is_finite())
            || self.gross_cap_usd < 5.0 * self.min_notional_usd
            || self.capital_cap_usd < self.gross_cap_usd.min(5.0 * self.min_notional_usd)
            || self.gross_per_min_usd < 0.0
            || (self.gross_per_min_usd > 0.0 && self.gross_per_min_usd < 5.0 * self.min_notional_usd)
        {
            bail!("策略参数不合法");
        }
        if self.send {
            for (k, v) in [("PREDICT_API_KEY", &creds.api_key), ("PREDICT_PRIVATE_KEY", &creds.private_key), ("PREDICT_ACCOUNT_ADDRESS", &creds.account)] {
                if v.is_none() {
                    bail!("--send 需要环境变量 {k}");
                }
            }
            if self.min_notional_usd > 5.0
                || self.inventory_cap_usd > 30.0
                || self.max_daily_loss_usd > 100.0
                || self.max_total_loss_usd > 100.0
                || self.gross_cap_usd > 300.0
                || self.capital_cap_usd > 500.0
            {
                bail!("--send 下单量/库存/亏损/合计投入上限超过探针的硬上限(单量 ≤ $5、库存 ≤ $30、日亏 ≤ $100、累计 ≤ $100、每轮合计 ≤ $300、未结算合计 ≤ $500)");
            }
            if self.rate_margin < 0.1 || self.rate_per_sec > 40 || self.rate_per_min > 500 {
                bail!("--send 限额不能超过实测值(40/秒、500/分钟)且余量 ≥ 10%");
            }
        }
        Ok(())
    }
}
