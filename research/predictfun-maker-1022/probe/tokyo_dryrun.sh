#!/usr/bin/env bash
# 东京机:编译探针 → 后台 dry-run(真实行情;不发任何写请求、不鉴权、不碰私钥)→ 跑完自动出分析。
#
# 在本机(有东京机 ssh 私钥、有本仓库的那台电脑)执行这一条命令:
#   git fetch origin predict-probe && git archive origin/predict-probe research/predictfun-maker-1022/probe | ssh -i ~/.ssh/lightsail_eu_north_1.pem ubuntu@54.95.203.70 'rm -rf ~/probe-src && mkdir -p ~/probe-src && tar -x -C ~/probe-src && bash ~/probe-src/research/predictfun-maker-1022/probe/tokyo_dryrun.sh'
#
# 可选环境变量:PROBE_DRY_SECS(默认 3600)、PROBE_NO_API_KEY=1(连 API key 也不用)。
# 凭证:只从 ~/pflat/.env 读 PREDICT_API_KEY 一项(公开 GET 请求带上,和实盘同样的请求头),
#       私钥、账户地址一律不读、不传给子进程;key 不出现在命令行、ps 或日志里(日志写入前脱敏)。
set -euo pipefail

DUR="${PROBE_DRY_SECS:-3600}"
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="$HOME/probe_logs/dry_$(date -u +%Y%m%d_%H%M%S)"
BIN_DIR="$HOME/probe-bin"
say() { echo "[$(date -u +%H:%M:%S)] $*"; }

# 0. 不重复启动(也绝不去动正在跑的 --send 进程)
if pgrep -f "predict-mm-probe .*--log-dir" >/dev/null 2>&1; then
  say "已有 predict-mm-probe 在运行,不再启动:"; pgrep -af "predict-mm-probe .*--log-dir" | sed 's/^/    /'
  exit 1
fi

# 1. 工具链
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
if ! command -v cc >/dev/null 2>&1; then
  say "安装 C 编译器(aws-lc-sys 需要)…"; sudo apt-get update -qq && sudo apt-get install -y -qq build-essential >/dev/null
fi
if ! command -v cargo >/dev/null 2>&1 || ! command -v rustup >/dev/null 2>&1; then
  say "安装 Rust(rustup,minimal)…"; curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal >/dev/null 2>&1
  . "$HOME/.cargo/env"
else
  rustup update stable --no-self-update >/dev/null 2>&1 || true
fi
say "$(cargo --version)"

# 2. 编译(关 LTO、2 个并行任务:少占内存,不挤录制器)
cd "$SRC/predict-mm-probe"
say "编译中(首次约 5~15 分钟)…"
CARGO_PROFILE_RELEASE_LTO=false nice -n 10 cargo build --release -j 2 --quiet
mkdir -p "$BIN_DIR" "$OUT"
cp target/release/predict-mm-probe "$BIN_DIR/predict-mm-probe"
cp "$SRC/analyze_probe.py" "$OUT/"   # 下次部署会删 ~/probe-src,分析脚本留一份在日志目录
BIN="$BIN_DIR/predict-mm-probe"
say "编译完成:$BIN"

# 3. 只取 API key 一项
KEY=""
if [ "${PROBE_NO_API_KEY:-0}" != "1" ] && [ -f "$HOME/pflat/.env" ]; then
  KEY="$(grep -E '^[[:space:]]*(export[[:space:]]+)?PREDICT_API_KEY=' "$HOME/pflat/.env" | tail -1 | sed -E 's/^[[:space:]]*(export[[:space:]]+)?PREDICT_API_KEY=//; s/^["'\'']//; s/["'\'']$//' | tr -d '\r')"
fi
[ -n "$KEY" ] && say "使用 PREDICT_API_KEY(只用于公开 GET;不读私钥)" || say "不使用 API key"

# 4. 后台启动:先 dry-run,结束后自动跑分析
(
  unset PREDICT_PRIVATE_KEY PREDICT_ACCOUNT_ADDRESS PREDICT_API_KEY
  [ -n "$KEY" ] && export PREDICT_API_KEY="$KEY"
  nohup bash -c '"$0" --dry-run --log-dir "$1" --duration-s "$2" --quiet; echo "exit=$?" >> "$1/run.out"; python3 "$1/analyze_probe.py" "$1" --out "$1" > "$1/summary.txt" 2>&1' \
    "$BIN" "$OUT" "$DUR" >> "$OUT/run.out" 2>&1 &
  echo $! > "$OUT/pid"
)
KEY=""
say "dry-run 已在后台启动:$DUR 秒,日志 $OUT"

# 5. 90 秒后体检一次
sleep 90
cnt() { zcat "$OUT"/probe_*.jsonl.gz 2>/dev/null | grep -c "\"kind\":\"$1\"" || true; }
say "体检(启动 90 秒):"
echo "    币安 bookTicker 条数 bn=$(cnt bn)   Predict 推送 pf=$(cnt pf)   市场 market=$(cnt market)   轮次 round_start=$(cnt round_start)"
echo "    挂单决策 place=$(zcat "$OUT"/probe_*.jsonl.gz 2>/dev/null | grep '"kind":"decision"' | grep -c '"action":"place"' || true)   将发请求 dry_request=$(cnt dry_request)   连接状态 feed_state=$(cnt feed_state)   错误 error=$(cnt error) fatal=$(cnt fatal)"
echo "    REST 返回状态:$(zcat "$OUT"/probe_*.jsonl.gz 2>/dev/null | grep '"kind":"resp"' | grep -o '"status":[0-9]*' | sort | uniq -c | tr '\n' ' ')"
zcat "$OUT"/probe_*.jsonl.gz 2>/dev/null | grep -E '"kind":"(error|fatal|feed_error)"' | tail -3 | cut -c1-300 | sed 's/^/    最近错误: /' || true
if pgrep -f "predict-mm-probe --dry-run --log-dir $OUT" >/dev/null 2>&1; then
  say "运行中。提前停止:pkill -TERM -f 'predict-mm-probe --dry-run --log-dir $OUT'(正常退出并出分析);跑完后看 $OUT/summary.txt"
else
  say "进程已退出,见 $OUT/run.out"; tail -5 "$OUT/run.out" | sed 's/^/    /'
fi
