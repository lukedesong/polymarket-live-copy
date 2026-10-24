# 交接卡:Predict 实盘探针(给云端会话,10-24 20:45 CST)

用简体中文和 Luke 交流。先读完本文件,再读同目录 `PROBE_SPEC.md`、`analysis/README.md`、`predict-mm-probe/README.md`。

## 0. 目的和现状
- **目的**:Predict.fun(BNB 链)BTC 5 分钟涨跌盘做市。在历史回放上挑参数失败了三次,根因是回放器的成交模型不准(复现老师打法时成交量只有实际的 1/4~1/16)。探针用小额实盘**测量**真实成交:成交率、排队位置、成交后 1/5/30 秒走向、同一批单回放 vs 实盘。目的不是赚钱。
- **已完成**:
  - 10-24 09:54 UTC 实盘跑了 2 轮。成交识别正常(78 笔,钱包推送 76 笔)。发现只限净持仓会把余额押进一轮,已修。
  - 改好的 v3 已部署东京:每轮两边合计上限、所有未结算轮合计上限、按分钟匀速、"保证金不足"暂停 5 秒(连续超过 60 秒按故障冷却)、每轮开盘记链上 USDT 余额。55 个单测全过,Opus 审了两轮,东京空跑 11 分钟无异常。代码在本分支 commit dc3a95949f。
  - 2 轮的体检结果(未审):`analysis/check_live_1024.py`,结果在本机 `analysis/results/`。
    - 成交都在每轮前 2.5 分钟,两边合计花钱约 $31/分钟。
    - 51% 的挂单有成交;挂单时前方排队中位 91 股;从挂上到首次成交中位 2.7 秒。
    - 成交后每股走向:盘口中间价 1 秒 −0.9 分、5 秒 −1.2 分;币安公允价 30 秒 −3.1 分。
    - 这一轮结算 −$2.13。
- **现在**:Luke 已充值,存款地址链上余额 **$391.9**(20:41 CST 读数)。探针**没在跑**。

## 1. 东京服务器
- 地址 `54.95.203.70`,用户 `ubuntu`,AWS Lightsail `pf-tokyo-main`(ap-northeast-1)。防火墙 22 端口对全网开放。
- 云端专用私钥:Luke 会放进云端环境的密钥/环境变量,建议名 `TOKYO_SSH_KEY`(完整私钥文本)。用法:
  ```bash
  mkdir -p ~/.ssh && printf '%s\n' "$TOKYO_SSH_KEY" > ~/.ssh/tokyo && chmod 600 ~/.ssh/tokyo
  ssh -i ~/.ssh/tokyo -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 ubuntu@54.95.203.70 'echo ok'
  ```
  连不上时,先确认云端环境的网络权限允许出站 SSH 到 54.95.203.70:22(默认的受限网络会拦)。连不上就直接告诉 Luke,不要绕路。
- 这把钥匙只用于云端,注释是 `claude-cloud-tokyo-1024`。要吊销,删掉东京 `~/.ssh/authorized_keys` 里这一行即可。
- **实测(10-24 云端会话):云端连不了 SSH。** 即使环境网络改成"完全访问",出站也只能走 HTTPS 代理;代理会断开 SSH 这类原始 TCP 流(握手即被重置),WebSocket 也不支持(代理文档列为"不支持、报告即可")。
  改用 **东京 HTTPS 网关**(`gateway/`,README 有说明):只认白名单操作(状态、启动、停止、空跑、列出/下载探针日志与录制器数据),口令保护,经 Cloudflare 隧道。云端用 `gateway/tokyo_client.py` 调。

## 2. 东京上的东西(⛔ 标的不准动)
- ⛔ **录制器 `pfrec5.py`**(PID 会变,`pgrep -af pfrec5`):一直录 Predict 盘口,Luke 明确不允许停,也不准重启或改它。开机自启单元 `pfrec5-boot`。参数 14 天,约 11-06 06:45 UTC 正常退出,到时需要重启。
- ⛔ **凭证 `~/pflat/.env`**(PREDICT_API_KEY / PREDICT_PRIVATE_KEY / PREDICT_ACCOUNT_ADDRESS,chmod 600):永远不 cat、不打印、不复制出机器、不写进日志或命令行。探针由启动脚本从这里读。
- 探针程序:`~/probe/predict-mm-probe.v3`(sha256 前 16 位 5a75bb9ec088990a)。旧版 `predict-mm-probe`、`predict-mm-probe.v1` 不再用。
  - 10-24 晚起:v5(静态链接;v4 未装)经网关 `/deploy/install` 安装,`~/probe/predict-mm-probe.current` 指向当前版本,`tokyo_live.sh` 与空跑默认用它。
    校验码登记在 `probe/bin/SHA256SUMS`。亏损上限改为跨次累计,账本 `~/probe/state_live.json`(首次记入之前已亏 $37.01)。
- 启动脚本:`~/probe/tokyo_live.sh`(本分支 `probe/tokyo_live.sh` 同一份)。
- 日志:
  - 本次运行:`~/probe_live3` → 软链到 `~/probe_live3_<月日_时分>/`,里面有 `probe_*.jsonl.gz`、`state.json`、`supervisor.log`、`probe.out`。
  - 10-24 两轮实盘:`~/probe_live/`。
  - 空跑:`~/probe_logs_dry*`。
- 东京是交易机:只跑探针和录制器,不在上面跑重分析(日志拉回来分析)。东京 1 核小机,别占满 CPU。

## 3. 运行(启停实盘必须 Luke 在对话里明确同意,问题不等于授权)
- **启动**(脚本读链上余额自动定参数。余额 ≥ $195 时:每轮两边合计 $150、整轮持续挂单、未结算合计上限 = min(余额 − 5, 500)):
  ```bash
  ssh -i ~/.ssh/tokyo ubuntu@54.95.203.70 'bash ~/probe/tokyo_live.sh'
  ```
  可选:`PROBE_HOURS=24`(时间上限,默认 24)、`PROBE_LOSS=30`(亏损上限,默认 30,探针代码硬上限也是 30;**要改成 $100 得先改 `config.rs` 的硬上限并交 Opus 审,再经 Luke 同意**)。写法:`ssh ... 'PROBE_LOSS=30 bash ~/probe/tokyo_live.sh'`。
- **停止条件**,先到先停:① 时间到;② 亏损到上限(未结算持仓按最坏结果计入);③ 手动停止:
  ```bash
  ssh -i ~/.ssh/tokyo ubuntu@54.95.203.70 "pkill -INT -f '^/home/ubuntu/probe/predict-mm-probe'"
  ```
  探针收到 SIGINT 会先全撤再退出。确认:`pgrep -af predict-mm-probe` 为空,并且 `probe.out`/日志里有 `cancel_all_done` 和 `exit`。
- **状态**:
  ```bash
  ssh -i ~/.ssh/tokyo ubuntu@54.95.203.70 'pgrep -af "^/home/ubuntu/probe/predict-mm-probe" | cut -c1-120; tail -5 ~/probe_live3/supervisor.log; zcat ~/probe_live3/probe_*.jsonl.gz | grep -cE "\"kind\":\"fill\""; zcat ~/probe_live3/probe_*.jsonl.gz | grep -E "\"kind\":\"(halt|exit|balance)\"" | tail -3 | cut -c1-250'
  ```
- 不自动重启。停机后要不要再跑,问 Luke。

## 4. 最大亏损(先告诉 Luke 再启动)
- 亏损上限 $30 时,最多约 $40,极端约 $45。上限之外可能多亏的部分:
  - 当前轮按中间价估值、没按最坏结果算,最多差约 $6;
  - 停机时在途的单,约 $3~4;
  - 下单时网络报错、实际已挂上的孤儿单,每张约 $1.5,每小时最多 5 次,超过就停机。
- 亏损上限 $100 时,约 $110~115。
- 预计每轮亏 $3~9(只从一轮推算,误差很大)。所以 $30 上限大约 3~10 轮就停,$100 上限大约 11~33 轮。

## 5. 平台事实(都已核实,不要再猜)
- **结算后平台每轮自动批量领取(redeem)并代付 gas**:中继地址 0x6a694de8… 调用合约 0xfca64d34…,在收盘后 10~70 秒到账。探针用 `--no-claim`,**不需要 BNB,不需要手动领取**。签名钱包 0xf4E6…468d 的 BNB 余额是 0,这是正常的。
- 存款地址(Kernel 智能钱包)0x35FB7E1ac51a9c4504C1d6ffC64F3d45a420F651。USDT(BSC)0x55d398326f99059fF775485246999027B3197955,18 位小数。
- 限额:每个 key 40 次/秒、500 次/分钟,全站接口共用。挂单 35ms、撤单 18ms。没有撤单锁定期。
- 最小单 ≈ $1(股数 = ceil(1/价))。吃单费 = 0.02·min(p,1−p)·股数;挂单 0 费,返佣比例未核实(估 25%)。
- **⛔ 凡是要 Luke 花钱、转账或手动操作的,先用链上记录或实测坐实**(10-24 我凭 SDK 臆断"要转 BNB、要手动领",是很严重的错误)。

## 6. 跑完之后做什么
1. 把日志拉回云端,用 `analysis/` 的三个工具跑:`markouts.py`、`queue_position.py`、`replay_compare.py`(`mm_grid_adapter.py` 待完成,格式见 `REPLAY_FORMATS.md`)。回放用的盘口数据来自东京录制器(导出脚本见本机 `model/export_book.py`)。
2. 回答:回放器预测的成交率和成交时刻,比实盘偏多少、偏向哪边;成交后走向按剩余时间、排队位置分组。
3. 每个数字标"未审";下结论前交独立 agent(Opus)审。
4. 用实盘数据改回放器的成交模型,再在新数据上重新写预注册,检验做市参数(不要拿看过的数据挑参数)。
5. 另一个待做项:Predict 积分和返佣到底值多少钱。

## 7. Luke 的规矩(摘要)
- 结论先行,用中文,数字标明 未审/已快检/已深审。
- 必须 Luke 亲自做的事,只给他一条命令。脚本包办一切、自动选参数,结果写到你能读的地方;不让他填参数、不让他贴结果。
- 启停实盘、部署、花钱:先问 Luke,并给出最大亏损。
- 他的质疑是待检验的假设:用数据判,不顺着说。
- 不擅自加过滤器或改规则;额外的想法单独提。
