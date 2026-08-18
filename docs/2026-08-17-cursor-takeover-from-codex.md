# Cursor 接管清单（从 Codex 迁运行层，不是再拷一份代码）

生成时间：2026-08-17 04:10 Asia/Shanghai。

## 代码仓库已经在这里

Codex 的现网跟单仓库就是 `/Users/luke/Documents/polymarket`，当前分支 `codex/source-action-fidelity`。  
`/Users/luke/Documents/Codex/2026-08-17/new-chat` 只是当天聊天沙盒，里面没有项目源码。

没有再 clone，也没有动 `.git`。本地未提交的 3.18 补丁和 CD90 删除改动必须保留；禁止 `git reset --hard`。

## 已迁进本仓库的 Codex 运行层

| 来源 | 落到 |
|---|---|
| `~/.codex/skills/polymarket-closed-loop-recovery/` | `.cursor/skills/polymarket-closed-loop-recovery/` |
| `~/.codex/AGENTS.md` 的 Polymarket 版本 Hook | `AGENTS.md` 新增章节 + `.cursor/rules/polymarket-version-authority.mdc` |
| 本机 Cursor 起立授权 | `.cursor/rules/polymarket-standing-authorization.mdc` |
| Codex 交接正文 | `docs/2026-08-17-polymarket-complete-project-handoff.md`（原已在仓库） |

## 扫过、故意不迁的

| 路径 | 原因 |
|---|---|
| `Documents/Codex/2026-08-17/new-chat` | 只有 Obsidian 记忆中枢说明，不是仓库 |
| `Documents/Codex/2026-07-05/polymarket-btc-5min-v1` | 另一个 BTC 5 分钟项目 |
| `~/.agents/skills/polymarket/` 及其 `.venv` | 本地 Gamma/CLOB 桥，不是现网跟单栈 |
| `Documents/Codex/2026-06-03/.../polymarket/.venv` | 旧技能库依赖，禁止当生产代码 |
| `~/.codex/auth.json`、服务器 `*.env` | 密钥，禁止复制进 Git 或对话 |
| `~/.codex/sessions/**/*.jsonl` | 聊天记录，不是项目 |
| `~/.codex/memories/**` | 记忆线索；现网事实必须重新 SSH 核验 |
| Codex automation「每日磁盘清理」 | 服务器已有 `daily-safe-gc` systemd；未改成 Cursor Automation |
| 本地 `.git`（约 11 GiB） | 已经是本仓库 |

## 当前现网（迁入时核验，会变）

以服务器固定索引为准，不以本文件为准。迁入时读数为：版本 3.18、`VERIFIED_FIXED`、release `20260816T180805Z-active-cancel-still-open-v3.18`。CD90 跟单执行器已 masked，Zockdo 与 wallet9506 仍在跑。
