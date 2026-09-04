# AGENTS.md

`duct` 是一个**Rust + tokio** 的轻量级网络中转服务：单二进制、单端口复用两类功能。

- **通用代理**：HTTP CONNECT 隧道 + HTTP 正向代理（浏览器插件 / SwitchyOmega）
- **AI 转发反向代理**：`/aiproxy/{provider}/*` 前缀转发至预配置上游，SSE 流式透传 + 请求轨迹
- **MCP 转发**：`/mcp/{server}/*` 前缀透明转发至预配置 MCP 上游（Streamable HTTP），会话/流式全透传 + 请求轨迹

Before editing: `README.md` is the user-facing contract; `docs/*.md` carries the design decisions that this code implements. When these disagree, the code + docs are the source of truth.

## Commands

```bash
cargo build --release          # build
cargo test                     # all tests (unit + integration)
cargo test --test aiproxy      # one integration suite
cargo clippy --all-targets -- -D warnings   # must pass clean
cargo fmt --check              # must pass clean

# run locally
cargo run -- -v
RUST_LOG=debug cargo run -- -v
```

## Repository layout

```
Cargo.toml       # lib + bin; axum/hyper/reqwest/tokio/clap stack, edition 2024
src/
  main.rs        # CLI parse → process-name disguise re-exec → tracing → config → trace sink → run (dual state)
  lib.rs         # pub mod re-exports (x10)
  server.rs      # TCP accept loop + six-branch dispatch + auth check  ← central scheduler
  connect.rs     # CONNECT tunnel + HTTP forward proxy (absolute-form rewrite)
  bridge.rs      # serve_conn_from_prelude 公共桥（aiproxy / mcp 复用，Router 入参）
  aiproxy.rs     # /aiproxy reverse proxy: routing/header/fwd/trace hooks + conn bridge
  mcp.rs         # /mcp reverse proxy: McpState / router / 透明转发 + origin_policy + 轨迹
  trace.rs       # request trace: JSONL event sourcing + sink + redaction + stream tap
  sse_normalize.rs # SSE normalization: stream field inject + tool-name dedupe + compressed re-encode
  config.rs      # config.yaml loading (3-tier semantics) + ProviderConfig + McpServerConfig
  error.rs       # OpenAI-compatible error responses
  auth.rs        # HTTP Basic auth check + base64 decode
tests/           # aiproxy / dispatch / integration / mcp / trace_e2e integration suites
docs/            # aiproxy-trace / deploy / mcp-forwarding / sse-tool-stream-normalization
```

## Core architecture

### Six-branch dispatch (`server.rs::handle_connection`)

One request line decides the branch; per-connection state is `(stream, auth, AppState, McpState)`.

```
read request line
 ├─ origin-form relative path (method != CONNECT):
 │    ├─ GET /healthz              → 200 ok (process liveness, config-independent)
 │    ├─ /aiproxy/{provider}/*     → aiproxy::serve_conn_from_prelude (bridge into axum)
 │    ├─ /mcp/…                    → mcp::serve_conn_from_prelude (bare /mcp → 404 列表)
 │    └─ other relative paths      → 400 (duct is not a general reverse proxy)
 ├─ CONNECT host:port              → auth check → connect::handle_connect (copy_bidirectional)
 └─ absolute-form GET http://..    → auth check → rewrite to relative → forward → copy
```

**Invariant:** HTTP Basic auth (`--user`/`--passwd`) applies **only** to the CONNECT and forward-proxy branches. It must **not** be hoisted up to the shared dispatch layer. The `/aiproxy`, `/mcp`, and `/healthz` branches are unauthenticated by design (trust boundary = internal network).

### MCP forwarding data flow (`mcp.rs`)

```
client ── /mcp/{server}/{rest}?query ──> axum forward handler
   ├─ resolve target = {server.url}/{rest}?query
   ├─ blacklist header strip (hop-by-hop + Proxy-*; MCP-* / Authorization pass verbatim)
   ├─ origin_policy transforms Origin (keep | strip | upstream) — §5.5
   ├─ body streamed byte-for-byte (LimitedBody, --max-body cap); MCP DELETE 默认无体
   ├─ response streamed chunk-by-chunk back (SSE passthrough, no line rewrite this phase)
   └─ all traced via Arc<RequestTrace> with branch:"mcp" (TracedBody sse:false)
```

`McpState` mirrors `AppState`: `Arc<Config>` (mcp server list) + `max_body` + `Arc<TraceSink>` + `trace_body`. It shares the same config/trace sinks as aiproxy (single `config.yaml`, single `trace.jsonl`).

### AI forwarding data flow (`aiproxy.rs`)

```
client ── /aiproxy/{provider}/{rest}?query ──> axum forward handler
   ├─ resolve target = {provider.base_url}/{rest}?query
   ├─ blacklist header strip (hop-by-hop + Proxy-*, Authorization passes verbatim)
   ├─ body streamed byte-for-byte (LimitedBody, --max-body cap)
   ├─ response streamed chunk-by-chunk back (SSE passthrough)
   └─ all traced via Arc<RequestTrace> (JSONL event chain)
```

`AppState` holds `Arc<Config>` (read-only provider list) + `max_body` + `Arc<TraceSink>` + `trace_body` budget. It is cheaply `Clone`d into each connection.

## Design invariants (respect these — they are load-bearing)

1. **Credential zero-touch.** duct never stores or injects upstream API keys. `Authorization` / `x-api-key` etc. are relayed verbatim, never read. The trace layer additionally never records their values (only `name:***`). Do not "helpfully" log or persist these.
2. **Header handling is blacklist, not allowlist.** `forward_allowed` strips only hop-by-hop headers and `Proxy-*`; everything else passes through. Do not add an allowlist.
3. **Body is streamed, not buffered.** Request body and response are forwarded chunk-by-chunk. Avoid `.bytes()`/full-buffer aggregations in the hot path. The `--max-body` cap is enforced incrementally via `LimitedBody`.
4. **Trace is append-only event sourcing.** One request = one `trace` id, monotonic `seq` from 0, envelope `{v, time, trace, seq, type, severity, data}`. `request/end` is the **only** terminator; a crash leaves a truncated chain (detected by missing `request/end`). Drop must synthesize `interrupted` on un-consumed streams.
5. **Trace must not read prompts/completions unless opted-in.** Default only derived facts (model/stream/usage/finish_reason/error preview). Content capture is gated solely by `--trace-body > 0`.
6. **Observing must not harm the hot path.** Trace events go through a bounded channel to a background writer; overflow drops lines and increments a counter (WARN), never blocks forwarding.
7. **`normalize_sse` is per-provider opt-in.** Never enable it globally or by default.
8. **Upstream errors are relayed, not re-synthesized.** Gateways return OpenAI-shaped errors only for *their own* failures (404 provider / 413 too large 502/504); upstream 4xx/5xx bodies pass through verbatim.

## CLI reference (`main.rs::Cli`)

| Option | Default | Meaning |
|---|---|---|
| `-p, --port` | `11088` | listen port (was `10999`; note bump for existing deploys) |
| `-b, --bind` | `0.0.0.0` | listen address |
| `-v, --verbose` | off | debug-level logs |
| `--disguise <NAME>` | off | re-exec with argv[0] set (process-name filtering) |
| `--user` / `--passwd` | off | Basic auth; **must be used as a pair** (clap `requires`); also `DUCT_USER`/`DUCT_PASSWD` |
| `--config-file <PATH>` | `~/.config/duct/config.yaml` | aiproxy + mcp server YAML (`providers` / `mcp.servers`) |
| `--max-body <BYTES>` | `16777216` | aiproxy/mcp request body cap |
| `--trace-file <PATH>` | `$XDG_STATE_HOME/duct/trace.jsonl` | trace JSONL; empty string disables file sink |
| `--trace-body <BYTES>` | `0` | opt-in content capture (grants access to prompts/completions) |

`--trace-file` `~` expansion uses `$HOME`; a missing default config **disables** aiproxy/mcp (not fatal). An unopenable trace file degrades to tracing-only (WARN, not fatal).

## Config formats

### providers (config.yaml)

```yaml
providers:
  openai:    { url: https://api.openai.com/v1 }
  ollama:    { url: http://ollama:11434 }
  anthropic: { url: https://api.anthropic.com }
```

Only `id + url` fields; **no credentials** (clients authenticate upstream themselves). `url` may carry a `normalize_sse` flag to enable SSE normalization for that provider.

Three-tier loading semantics (`config.rs`):
- Explicit `--config-file`: corrupt file → **fatal** (with `--config-file` context).
- Default path: exists → enable; missing → **disable** aiproxy (no error).
- Individual broken entries are skipped (entry-level tolerance).

`providers` 与 `mcp` 均变为可选段，但**至少要有一个**；两者皆缺 → 文件级错误。

### mcp.servers (config.yaml)

```yaml
mcp:
  servers:
    github:     { url: https://api.githubcopilot.com/mcp }
    filesystem: { url: http://127.0.0.1:9100/mcp, origin_policy: strip }
    internal:   { url: http://mcp.corp:9000/mcp, origin_policy: upstream }
```

- server id 复用 provider 校验（`[a-z0-9][a-z0-9_-]*`）；`url` 复用 base url 归一化且**禁止 query/fragment**（凭证不进配置文件）。
- `origin_policy`: `keep`（默认，透传）| `strip` | `upstream`。
- 本期无 `transport` 字段；未知键（如二期预留 `transport`）静默忽略。上游若有遗留 SSE server，用 supergateway 转 Streamable HTTP 再接入（不内建，见 `docs/mcp-forwarding-design.md` §6）。

### trace events

Event vocabulary (`type`): `request/start` → `request/body` → `upstream/request` → `upstream/response` → `upstream/error` → `request/end`. See `docs/aiproxy-trace.md` for the field table.

`request/end.outcome` values & default severity:
`completed` (info/warn when `status>=400`), `rejected` (error), `upstream_error` (error), `stream_error` (error), `interrupted` (warn), `client_disconnected` (reserved, currently covered by `interrupted`).

mcp 分支与 aiproxy 共用同一 `trace.jsonl`，事件 `data.branch:"mcp"` + `data.server`；`TracedBody` 固定 `sse:false`（`sse.done` 等 OpenAI 语义字段不进 mcp 事件）。

## Conventions & gotchas

- Use `tracing::info!/debug!/warn!` with structured fields. Target for trace-fallback is `duct::trace`.
- Chinese doc-comments are used throughout; keep them when editing existing `//!` module docs.
- Keep `#[cfg(test)] mod tests` co-located in each `src/` module; integration suites under `tests/` must start a real `TcpListener` on a random port and drive it via `run_from_listener` / `run_with_aiproxy_from_listener` / `run_with_states_from_listener`. `tests/mcp.rs` covers the `/mcp` suite (socket dispatch-only + oneshot forward/trace).
- `cargo clippy --all-targets -- -D warnings` and `cargo fmt --check` are the repo gates.
- `MAX_REQUEST_LINE_BYTES` (8192) and `MAX_HTTP_REQUEST_BYTES` (65536) cap request-line/header buffering; keep bridging long-header tests in `tests/`.
- `tests/trace_e2e.rs` asserts the full trace chain (prefix scan / full parse / stream facts). When adding trace fields, extend both the emission and the e2e assertions.

## Docs pointers

- `docs/aiproxy-trace.md` — event vocabulary, severity mapping, redaction contract, jq recipes.
- `docs/sse-tool-stream-normalization.md` — the normalize_sse behavior and compressed-stream re-encode path.
- `docs/deploy.md` — systemd unit, env-file credential injection, trace rotation.
- `README.md` — user-facing usage + troubleshooting tables.
