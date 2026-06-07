## Why

duct 已从专用 WSL VPN 桥重构为通用 HTTP/HTTPS 代理服务，但核心缺陷影响了通用场景的使用：

1. **无认证保护** — 任何人都能连接代理，无法安全地暴露在局域网或公网
2. **伪装默认开启** — 纯 CLI 工具场景下无端 re-exec 自身，增加启动延迟和心智负担
3. **伪装参数冗余** — 既有 `--disguise` 又有 `--no-disguise`，语义矛盾

本次改动补齐认证能力，同时清理伪装的行为模式，让 duct 真正准备好作为通用代理部署。

## What Changes

- **新增**: `--username` / `--password` CLI 参数，启用 HTTP Basic 认证
- **变更**: `--disguise` 从默认启用改为可选传入才启用，删除 `--no-disguise`
- **移除**: `ALLOWED_NAMES` 常量列表（不再需要自动检查）
- **新增**: `auth.rs` 模块，封装认证检查逻辑
- **修改**: `server.rs` 中 CONNECT 和 HTTP Proxy 路径均检查 `Proxy-Authorization` header
- **不引入**: 配置文件、用户数据库、SOCKS5 协议——保持极简单二进制

## Capabilities

### New Capabilities
- `proxy-auth`: HTTP Basic 认证，通过 CLI 参数传入单组用户名/密码，检查每个请求的 `Proxy-Authorization` header

### Modified Capabilities

（无 — 当前没有正式 spec，这是首次建立）

## Impact

- **新增文件**: `src/auth.rs`
- **修改文件**: `src/main.rs`, `src/server.rs`, `src/lib.rs`
- **新增依赖**: 无（`base64` 解码手动实现或使用标准库）
- **CLI 变更**: `--disguise` 变为 `Option<String>`；新增 `--username`/`--password`
- **行为变更**: 未传 `--disguise` 时不做任何进程名改造；传入 `--username`/`--password` 时强制认证
