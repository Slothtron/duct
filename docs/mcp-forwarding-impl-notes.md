# MCP 转发实现任务拆解与落地记录

> 对应设计 [docs/mcp-forwarding-design.md](mcp-forwarding-design.md)（v3，本期仅 A 方案：Streamable HTTP 透明转发）。
> 分支：`feat/mcp-forwarding`。基线：`cargo test` **129 全绿**（82 单元 + 47 集成，HEAD `d13a3e4`）。

## 任务拆解（映射设计 §10 T1–T7）

| 任务 | 内容 | 落地要点 | 验收 |
|---|---|---|---|
| **T1** config.rs | 双段可选 + `McpServerConfig` 装载校验（本期仅 url + origin_policy） | `providers`/`mcp` 均可选但至少一个；`is_valid_provider_id`/`normalize_base_url` 复用并**新增禁止 query/fragment**；未知键（如 `transport`）静默忽略 | C1–C6 ✅ |
| **T2** bridge.rs | `serve_conn_from_prelude` 泛化为 `Router` 入参（行为不变重构） | 新公共模块 `bridge.rs`；`aiproxy::serve_conn_from_prelude` 改为薄再导出；`sse_normalize.rs` 零改动 | 现有测试全绿 ✅ |
| **T3** mcp.rs | `McpState`（config/client/max_body/trace/trace_body）/ router / 透传转发 + origin_policy + **轨迹埋点** | 同构 `AppState`；`branch:"mcp"` + `server` + `origin_policy`；`TracedBody` 固定 `sse:false`；MCP DELETE 默认无体；不做请求体扫描 | M1–M3, I1, I4, I5 ✅ |
| **T4** server.rs + main.rs | 六分支分流 + 双 state 装配 | server 增 `/mcp` 分支（含裸 `/mcp` 入 router 回 404）；main 构建 `AppState`+`McpState` 共享同一 `Arc<Config>` 与 trace sink | I4, I7 ✅ |
| **T5** 流式与语记忆集成测试 | 会话/流式语义 + 轨迹链断言 | `tests/mcp.rs`：I1 握手/session 头透传、I1b 三方法、I2 增量、I3 长挂、I4 404、I5 origin、I8 轨迹+脱敏、I8b interrupted、上游错误透传 | I1–I8 ✅ |
| **T6** 文档同步 | README / deploy.md / **AGENTS.md** | 新增 MCP 特性/用法/六分支架构图/supergateway 组合姿势/安全警示/测试计数；AGENTS.md 的 "Five-branch dispatch" → "Six-branch" | 文档评审 ✅ |
| **T7** clippy + fmt | 仓库门禁 | `cargo clippy --all-targets -- -D warnings` + `cargo fmt --check` 全绿（含修复若干既有 clippy 警告） | CI 绿 ✅ |

二期（B，未做）：`SseEndpointRewriter` + `transport: http_sse` 接线 + 行改写 trait 抽取（设计 §5.4 留档）；stdio 托管（§2.1，外部 supergateway 组合）；聚合网关（§2.4，不做）。

## 关键设计取舍落地

- **透明转发**：仅切一次前缀，剩余路径不解释；方法/query/body/响应逐字节透传，SSE 流不缓冲、无整体超时（仅 connect 超时 30s）。
- **凭证零接触**：头黑名单制（逐跳 + `Proxy-*` + host/content-length/expect），`Authorization` / `MCP-*` 原样透传；`server.url` 禁 query/fragment，避免凭证落盘。
- **origin_policy**（`keep`=透传 / `strip` / `upstream`=改写为 server.url 的 origin）：默认 keep，防上游 DNS-rebinding 校验误杀。
- **轨迹**：与 aiproxy 共用同一 `TraceSink`（单一 `trace.jsonl`），事件 `data.branch:"mcp"` + `data.server` 区分；`TracedBody` 固定 `sse:false`（`sse.done` 等 OpenAI 语义不入 mcp 事件），JSON-RPC 回包仍产出 `body` 事实；`mcp-session-id` 不入 `SAFE_HEADER_VALUES`（会话 id 是可重放凭证，默认只记名字）。
- **不合并 handler**：aiproxy 与 mcp 各自持有头黑名单/限长流组件，避免过早抽象（演进方向不同），仅 `bridge.rs` 公共桥共享。
- **stdio / 遗留 SSE**：不内建托管，文档给出 supergateway 转 Streamable HTTP 的组合接入姿势。

## 规模对比

- 本期实现 ≈ 350 行（`mcp.rs` + `bridge.rs` + config/error/server/main 增量）；测试 ≈ 400 行（`tests/mcp.rs` + config/mcp 单测）。
- 轨迹埋点：`mcp.rs` 内约 60 行（复用现成 sink/脱敏设施），断言在 `tests/mcp.rs` I8。

## 最终测试计数

`cargo test`：**93 单元 + 56 集成 = 149 全绿**（基线 129；新增 20：11 单测 + 9 mcp 集成）。
