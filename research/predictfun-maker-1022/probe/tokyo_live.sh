#!/usr/bin/env bash
# 东京机:实盘探针一次运行(Luke 执行一次,之后全自动;不自动重启)。
# 停止条件,先到先停:① 时间到(默认 24 小时)② 跑满 PROBE_ROUNDS 个完整轮次(默认 0 = 不限;启动时当前轮已开始则这半轮另算一轮)
#   ③ 亏损到上限(默认 $30,未结算持仓按最坏结果计入)④ 手动停止命令。
# 启动时读存款地址链上 USDT 余额,按 可用余额 = 链上余额(PROBE_BALANCE_CAP > 0 时再取 min;默认 0 = 不压缩)自动定上限:
#   未结算合计上限 = min(可用余额 − 5, 500);
#   可用余额 ≥ $195 → 每轮两边合计 = min(150, 未结算上限 / 2)、不匀速(整轮持续挂单)。不超过一半:上一轮的钱在平台结算领回、
#     探针查到结果之前(收盘后约 1~2 分钟)一直占着未结算额度,下一轮仍有完整一轮的额度,开盘头几分钟不被卡。
#   不要为了"少押钱"压缩额度:每轮合计被卡住会停挂,后半轮的数据就收不到(10-24 Luke:亏的钱是买数据,数据不全等于白花);
#   最多亏多少由亏损上限管(未结算持仓按最坏结果计入),与余额无关。
#   否则每轮合计 = 未结算上限 / 1.3,按分钟匀速 = 每轮合计 / 4.5。
# 日志:~/probe_live3/(探针 jsonl.gz、state.json、supervisor.log)。
# 手动停止(撤掉所有挂单后退出):pkill -INT -f '^/home/ubuntu/probe/predict-mm-probe'
set -uo pipefail
HOURS="${PROBE_HOURS:-24}"
LOSS="${PROBE_LOSS:-30}"
ROUNDS="${PROBE_ROUNDS:-0}"
BAL_CAP="${PROBE_BALANCE_CAP:-0}"
H="${PROBE_HOME_DIR:-/home/ubuntu}"
BIN="${PROBE_BIN:-$H/probe/predict-mm-probe.v3}"
OUT=$H/probe_live3_$(date -u +%m%d_%H%M)
ACCOUNT=35fb7e1ac51a9c4504c1d6ffc64f3d45a420f651
RPC="${PROBE_RPC:-https://bsc-dataseed.bnbchain.org}"
for v in "$HOURS" "$LOSS" "$ROUNDS" "$BAL_CAP"; do [[ "$v" =~ ^[0-9]+$ ]] || { echo "参数必须是非负整数:$v"; exit 1; }; done
# 先查有没有探针在跑,再建目录、改软链(否则被拒的启动会把 probe_live3 指到空目录)
if pgrep -f "^$H/probe/predict-mm-probe" >/dev/null; then echo "已有探针(实盘或空跑)在跑,不启动:"; pgrep -af "^$H/probe/predict-mm-probe" | cut -c1-120; exit 1; fi
mkdir -p "$OUT"; ln -sfn "$OUT" $H/probe_live3
say() { echo "[$(date -u +%F' '%T)Z] $*" | tee -a "$OUT/supervisor.log"; }

BAL=""
for i in 1 2 3 4 5; do
  BAL=$(curl -s -m 10 -X POST "$RPC" -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_call\",\"params\":[{\"to\":\"0x55d398326f99059fF775485246999027B3197955\",\"data\":\"0x70a08231000000000000000000000000$ACCOUNT\"},\"latest\"]}" \
    | python3 -c 'import sys,json; print(int(json.load(sys.stdin)["result"],16)/1e18)' 2>/dev/null) && [ -n "$BAL" ] && break
  sleep 3
done
[ -n "$BAL" ] || { say "读链上余额失败,不启动"; exit 1; }
read USE CAP GROSS PERMIN < <(python3 -c "
b=$BAL if $BAL_CAP <= 0 else min($BAL, $BAL_CAP); cap=min(int(b-5),500)
if b>=195: g,pm=min(150,cap//2),0
else: g=int(cap/1.3); pm=max(5,-(-g*2//9))
print(int(b),cap,g,pm)")
for v in "$USE" "$CAP" "$GROSS" "$PERMIN"; do [[ "$v" =~ ^[0-9]+$ ]] || { say "参数计算失败,不启动"; exit 1; }; done
MAXR=0
if [ "$ROUNDS" -gt 0 ]; then
  INTO=$(( $(date -u +%s) % 300 ))
  MAXR=$ROUNDS; [ "$INTO" -gt 10 ] && MAXR=$(( ROUNDS + 1 ))   # 当前轮已开始:这半轮另算一轮,保证跑满 ROUNDS 个完整轮
fi
[ "$GROSS" -ge 10 ] || { say "余额 \$$BAL 太少(每轮合计 < \$10),不启动"; exit 1; }
say "启动:链上余额 \$$BAL,按 \$$USE 计 → 未结算上限 \$$CAP、每轮两边合计 \$$GROSS、匀速 \$$PERMIN/分钟(0=不限);时间上限 ${HOURS} 小时、轮数上限 $MAXR(0=不限)、亏损上限 \$$LOSS"
say "手动停止:ssh 到东京后执行 pkill -INT -f '^$H/probe/predict-mm-probe'"
(
  set -a; . $H/pflat/.env; set +a
  unset PROBE_STATE_FILE PROBE_DAY_OFFSET_HOURS PROBE_MIN_NOTIONAL_USD PROBE_INVENTORY_CAP_USD
  exec setsid nohup "$BIN" --send --quiet --no-claim --log-dir "$OUT" --state-file "$OUT/state.json" \
    --day-offset-hours 0 --min-notional-usd 1 \
    --capital-cap-usd "$CAP" --gross-cap-usd "$GROSS" --gross-per-min-usd "$PERMIN" \
    --max-daily-loss-usd "$LOSS" --max-total-loss-usd "$LOSS" \
    --max-rounds "$MAXR" --duration-s $(( HOURS * 3600 ))
) >> "$OUT/probe.out" 2>&1 < /dev/null &
sleep 5
if pgrep -f "^$H/probe/predict-mm-probe" >/dev/null; then say "已在后台运行(断开 ssh 不影响)"; else say "启动失败,看 $OUT/probe.out"; tail -5 "$OUT/probe.out"; exit 1; fi
