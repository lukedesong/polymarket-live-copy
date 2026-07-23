# 三钱包纸面跟单

本机后台观察三个公开钱包。每个钱包有独立的 **100 USD** 纸面账户；这是用户指定值，不代表最优资金量。

## 固定口径

- `russell110320` → `0x118689b24aead1d6e9507b8068d056b2ec4f051b`
- `ZorroDeLaVega` → `0xaae9b2c5ad90e82b5068c7f8a4b491997633d661`
- `sabsabinxz` → `0xd3ecb2aee0d65622da559ff356b00e8c2e626603`
- 首次启动只建立历史水位，不回补旧仓位。
- 每个新 BUY/SELL 信号只跟一个当时官方 `min_order_size`。
- BUY 走当前卖盘，SELL 走当前买盘；资金、深度或持仓不足就跳过。
- maker 与 taker 成交都读取（`takerOnly=false`）。
- 费用使用市场当时公开的 CLOB 费率和指数；结算使用 Gamma 的公开已解决结果。
- 纸面成交永远不等于真实订单。

## 文件

- `runtime/paper.sqlite3`：唯一账本。
- `runtime/status.html`：自动刷新的本地状态页。
- `runtime/status.json`：机器可读状态。
- `runtime/ledger.csv`：逐笔源信号和纸面结果。

状态页始终展示：

```text
paper_only: true
real_order_submitted: false
```

开放持仓按最近一次可执行买盘深度估值；正式结算前不是最终输赢。结算后按官方获胜 token 兑付，写回现金和已实现盈亏。

## 操作

```bash
./start.sh
./status.sh
./stop.sh
```

`start.sh` 安装并启动当前 macOS 用户的 LaunchAgent，同时打开状态页。后台默认每秒检查一次；这个间隔是纸面监控的暂定估算值，不是已证明最优参数。三个钱包的基础请求频率低于 Polymarket Data API 公布的限制，遇到接口错误时本轮不制造成交。

启动脚本会把当前终端实际使用的 HTTP(S) 代理写入本次 LaunchAgent，避免沿用 macOS 登录会话中已经失效的旧代理；不把具体代理端口写死在仓库里。

程序只包含公开 GET 白名单，不加载私钥、API key、secret 或 passphrase，也没有创建、取消订单的代码路径。
