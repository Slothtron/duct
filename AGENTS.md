# AGENTS.md

`duct` 是一个 **Rust + tokio** 轻量级 HTTP/HTTPS 网络代理，单二进制、单端口提供：

- HTTP CONNECT 隧道代理
- HTTP absolute-form 正向代理
- 可选 HTTP Basic 代理认证
- `/healthz` 进程存活探测

Before editing: `README.md` is the user-facing contract. Keep behavior and documentation aligned.

## Commands

```bash
cargo build --release
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check

cargo run -- -v
RUST_LOG=debug cargo run -- -v
```

## Repository layout

```
Cargo.toml       # tokio/clap/tracing stack
src/
  main.rs        # CLI → process-name disguise → tracing → listener
  lib.rs         # public module exports
  server.rs      # TCP accept loop, healthz, CONNECT/HTTP dispatch and auth
  connect.rs     # CONNECT tunnel and upstream connection timeout
  auth.rs        # HTTP Basic auth parsing and checking
tests/
  integration.rs # CONNECT, HTTP forward proxy and authentication coverage
docs/
  deploy.md      # systemd deployment and proxy verification
```

## Core behavior

`server.rs::handle_connection` reads one request line:

```
GET /healthz                        → 200 ok
CONNECT host:port                   → optional auth → connect::handle_connect
absolute-form HTTP request          → optional auth → rewrite path → upstream forward
other origin-form relative request  → 400 Bad Request
```

Basic authentication applies to CONNECT and HTTP forward-proxy requests. Keep `/healthz` available without proxy authentication for process-level liveness checks. The service is a forward proxy, not a reverse proxy; do not add application-specific routes or upstream configuration.

## Conventions

- Use structured `tracing` fields and keep comments concise and consistent with nearby code.
- Keep unit tests co-located in `src/` and end-to-end socket tests in `tests/integration.rs`.
- Preserve request-line/header size limits and test CONNECT, absolute-form forwarding, auth success/failure, health checks, and unsupported relative paths.
- Required gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test`.
