# Tian-Wen 链上快速影子通道

这是一个独立、只读的前瞻测量通道。它不会改写
`tian_wen_speech_runtime/paper.sqlite3`，不会记入原纸面账户，也没有签名、
提交订单或撤单代码。

## 它测量什么

- `block_timestamp`：源成交所在 Polygon 区块时间；
- `chain_seen_at_ms`：本进程第一次从 `OrderFilled` 日志看到该成交的本机时间；
- `data_api_seen_at_ms`：本进程第一次从公开 Data API 看到同一成交的本机时间；
- `book_request_started_at_ms` / `book_request_finished_at_ms`：各通道取得完整
  CLOB 盘口的本机请求区间；
- `book_timestamp`：CLOB 返回字段，只作盘口来源字段，不冒充观察时间。

这些时间是**实证值**，只描述本进程实际看见数据的时点。盘口快照证明当时公开可见的
深度，不证明我方一定能按该价真实成交。

首次启动从当前区块设置水位，不把历史回放伪装成前瞻样本。进程中断后补齐的区块标为
`catchup=true`，不进入实时延迟中位数。

## 安全边界

- `paper_only=true`
- `real_order_submitted=false`
- 公共 JSON-RPC 只允许读方法；
- 公共 Polymarket 只允许 `GET /trades` 与 `GET /book`；
- 旧纸面 SQLite 通过 `mode=ro` 打开；
- 新证据只写入 `tian_wen_chain_shadow_runtime/shadow.sqlite3`。

## 控制

```bash
./start_tian_wen_chain_shadow.sh
./status_tian_wen_chain_shadow.sh
./stop_tian_wen_chain_shadow.sh
```

实时页：

`file:///Users/luke/Documents/polymarket/work/wallet_copy_paper/tian_wen_chain_shadow_runtime/status.html`

历史回执只读核验：

```bash
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/python \
  tian_wen_chain_shadow.py --verify-tx 0x交易哈希
```
