## Why

当前 duct 的认证凭据仅能通过 CLI 参数 `--user` / `--passwd` 传入，作为 systemd 系统服务部署时，密码会以明文出现在 `ps -ef` 和 `/proc/PID/cmdline` 中，构成安全风险。需要支持从环境变量读取凭据，以便 systemd 通过 `EnvironmentFile` 安全注入——环境变量不在 argv 中，`ps` 不可见。

同时借此机会将 CLI 参数从 `--username` / `--password` 简化为 `--user` / `--passwd`，利用 clap 内置的 `env` + `requires` 机制消除手写的配对验证和环境变量 fallback 逻辑。

## What Changes

- **新增**: clap `env` 属性支持环境变量 `DUCT_USER` / `DUCT_PASSWD` 自动读取
- **变更**: `--username` → `--user`，`--password` → `--passwd`
- **变更**: 利用 clap 的 `requires` 声明式配对验证，删除手写的 match 分支
- **变更**: AuthConfig 构造改为 `cli.user.zip(cli.passwd).map(...)` 
- **变更**: 更新 `docs/deploy.md` 使用环境变量方式部署

## Capabilities

### New Capabilities
- `env-var-auth`: 支持通过 `DUCT_USER` / `DUCT_PASSWD` 环境变量配置 HTTP Basic 认证凭据

### Modified Capabilities
- `proxy-auth`: CLI 参数名从 `--username`/`--password` 改为 `--user`/`--passwd`；增加环境变量 fallback

## Impact

- **修改文件**: `src/main.rs` — CLI 参数名变更 + `env` + `requires` + `zip()` 构造
- **修改文件**: `docs/deploy.md` — 更新为环境变量部署方案
- **无新增依赖**: 全部使用 clap 内置功能
- **无新增文件**: 纯修改已有代码
- **行为变更**: CLI 参数名变化（BREAKING，但此功能刚引入未正式发布）
