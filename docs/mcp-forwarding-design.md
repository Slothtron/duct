# duct MCP 转发（/mcp/*）调研与设计方案

> 状态: **设计 v3（未实现）**，范围已评审圈定（§9）：**本期仅实施 A 方案（Streamable HTTP 透明转发）**，
> B 方案（遗留 HTTP+SSE 改写）**顺延二期**（设计结论留档 §5.4），stdio 不内建，配置取 `mcp.servers` 嵌套段。
> **修订记录**:
> - v1: 初版调研（协议调研、方案对比、底座复用分析）。
> - v2: 按 `d13a3e4`（trace 落地）+ `367e30c`（fmt）重梳理——trace 从"未提交改动"升级为已落地底座，
>   §4/§5.6 接入契约重写；方案 B 改走 `ToolNameRewriter` 行状态机 + `SseRewindStream` 压缩通道复用。
> - v3: **范围收敛评审**——本期只交付 A；B 标注二期（本期不写 `SseEndpointRewriter`、不做
>   `SseRewindStream` trait 抽取、不留相关测试项），`transport` 配置字段随之暂缓（§5.1）。
> 定位: 在 duct 单端口上新增 `/mcp/*` 反向代理分支，把多个 **HTTP 型 MCP server** 汇聚暴露在
> 同一入口下；复用 `config.yaml`，新增 `mcp.servers` 配置段。
> 关联: `aiproxy` 转发语义（路径一次切分、字节流式透传、凭证零接触、头黑名单制）、
> `docs/aiproxy-trace.md`（事件词表/severity 映射/脱敏契约——mcp 轨迹对齐它）、
> 入口分流（现五分支 → 六分支）、`config.rs` 三层装载语义、`sse_normalize.rs` 的 SSE 流内改写先例、
> `AGENTS.md` 设计不变量（#1 凭证零接触、#2 头黑名单、#3 流式不缓冲、#4–#6 轨迹契约、#8 上游错误透传）。
> 协议基线: MCP spec 2024-11-05 / 2025-03-26 / 2025-06-18（联网检索通道暂不可用，
> 关键行为以本地知识为据，实施前建议对照 modelcontextprotocol.io 规范原文复核，见 §9 Q6）。

---

## 1. 背景与目标

### 1.1 需求

- 新增特性：**MCP 转发**，路由前缀 `/mcp/*`。
- **复用 `config.yaml`**（同一个 `--config-file` / 默认路径，三层装载语义不变）。
- 新增 **`mcp.servers` 配置段**：声明若干上游 MCP server（id → url）。
- 目标形态：

```
{method} /mcp/{server}/{剩余路径}?{query}   →   {method} {server.url}/{剩余路径}?{query}
```

客户端（Claude Desktop / Cursor / Cline / OpenViking 等任何支持 remote MCP 的工具）
把 server 的 url 配成 duct 前缀地址即可，一个 duct 端口收敛 N 个 MCP 上游。

### 1.2 与 aiproxy 的关系

MCP 转发与 `/aiproxy` 在工程形态上高度同构：都是**按路径前缀一次切分的反向代理**，
都要求**SSE 长流不被掐断**、**凭证透传（不存 Key、不注入 Key）**、**逐 chunk 流式回传**。
区别在于协议语义：

| 维度 | aiproxy | mcp 转发 |
|---|---|---|
| 协议 | OpenAI Chat/Completions（无会话头） | JSON-RPC over HTTP（有会话头 `Mcp-Session-Id`） |
| 端点形态 | 单端点为主 | POST + GET(SSE) + DELETE 三方法一套端点（Streamable HTTP） |
| 连接生命周期 | 单次请求-响应（可流式） | 会话级：GET 可长挂数小时 |
| 上游类型 | HTTP API | HTTP / SSE / stdio 三类传输，差异大 |

结论：**传输层可整套复用 aiproxy 的底座**（路由/桥接/黑名单/流式/错误），
协议层只需保证「MCP 相关头与会话语义透明穿透」，不需要 duct 理解 JSON-RPC。

---

## 2. MCP 传输协议调研（代理视角）

MCP 客户端与 server 之间的传输有三种，代理含义完全不同：

### 2.1 stdio（本地子进程）

- 客户端 spawn server 进程，通过 stdin/stdout 换行分隔 JSON-RPC 通信。
- **没有 HTTP 面可代理**。要转发 stdio server，代理必须自己实现：进程生命周期管理
  （spawn/守护/重启）、stdio↔HTTP(SSE) 双向桥、initialize 握手代答、会话↔进程映射。
- 这是"另一个量级"的功能（等价内嵌 mcp-proxy / supergateway），**不是转发，是托管**。

### 2.2 HTTP+SSE（2024-11-05，已废弃但仍广泛在野）

两个端点、一条长流：

```
GET  {base}/sse        → text/event-stream 长连接（server→client 全部响应走这条流）
     首事件: event: endpoint
             data: /messages?sessionId=xxxx        ← 相对路径！
POST {base}/messages?sessionId=xxxx                → 202 Accepted（请求体=JSON-RPC）
     （真正的响应稍后从 SSE 流下发，按 JSON-RPC id 关联）
```

代理难点：`endpoint` 事件里的 data 是**相对上游自己挂载点的路径**。
经 duct 挂在 `/mcp/{id}` 前缀下后，客户端会照着 `POST /messages?...` 发给 duct → 404/错误分流。
**必须改写该事件为 `data: /mcp/{id}/messages?...`**（对绝对 URL 也要替换 origin 为 duct）。
改写需要感知 SSE 帧边界（duct 已有 `SseToolNormalizer` 的按事件缓冲、低延迟改写先例，可仿写）。

### 2.3 Streamable HTTP（2025-03-26 起现行标准）

单端点（惯例路径 `/mcp`），**天然适合透明反代**：

```
POST   {endpoint}   Accept: application/json, text/event-stream
      · 请求=JSON-RPC；响应三态之一：
        - application/json           （单响应）
        - text/event-stream          （该请求期间的响应+通知流，可持续数小时）
        - 202 Accepted               （通知/无响应请求）
      · initialize 成功后 server 可在响应头下发  Mcp-Session-Id
GET    {endpoint}   Accept: text/event-stream
      · 可选的 server→client 独立通知流；server 不支持时回 405
DELETE {endpoint}   · 终止会话（带 Mcp-Session-Id），server 回 200/204/405
```

对代理的关键要求（逐条对照 duct 现有底座）：

| # | 协议要求 | duct 底座现状 | 结论 |
|---|---|---|---|
| 1 | `Mcp-Session-Id`、`MCP-Protocol-Version`、`Last-Event-ID` 等头双向透传 | 黑名单制：非逐跳/非 Proxy-* 一律透传 | ✅ 零改动满足 |
| 2 | 响应可能是**长挂 SSE**，不得整体缓冲、不得总超时掐断 | reqwest `bytes_stream` 逐 chunk 回传；仅 connect 超时（30s），无整体超时 | ✅ 同 aiproxy 已验证形态 |
| 3 | GET/POST/DELETE 方法透传 | `axum::routing::any` + 方法原样转发 | ✅ |
| 4 | 查询串原样保留（sessionId 常走头，但遗留/变体走 query） | `target_url` 拼 query 原样保留 | ✅ |
| 5 | SSE 断线重连（`Last-Event-ID` 续传） | 头透传 + 流式透传 | ✅ |
| 6 | **Origin 校验**（2025-06-18 安全章节：server SHOULD 校验 Origin 防 DNS rebinding） | 客户端 Origin 透传后与上游 origin 不一致 → 严格 server 回 403 | ⚠️ 需配置项（§5.5） |
| 7 | Host 校验（同上） | reqwest 按上游 Url 自动重写 Host（`host` 在请求黑名单） | ✅ |
| 8 | 凭证：`Authorization` / server 预置 query-key | P5 凭证零接触：header 透传，**不注入** | ✅ 客户端自带凭证 |

### 2.4 聚合网关（更重的另一形态）

把 N 个 server 的 `tools/list` 合并成**一个** MCP 端点（protocol-aware gateway，如
mcp-gateway / LiteLLM 的 MCP 聚合）：duct 必须实现 MCP server 端（握手、会话表、
工具改名与回路由、资源/提示转发）。**与 duct「轻量网络中转、不理解协议载荷」的定位冲突，本方案不做**，
列入非目标。

---

## 3. 方案对比

| 方案 | 内容 | 规模 | 判定 |
|---|---|---|---|
| **A. 透明转发（Streamable HTTP）** | `/mcp/{id}/*` 前缀反代 + `mcp.servers` 配置 + 会话/流式全透传 | ~350 行（含测试另计） | ✅ **本期主交付** |
| **B. 遗留 HTTP+SSE endpoint 改写** | 配置 `transport: http_sse` 时挂行改写器，仅改写首帧 `event: endpoint` 的 data 补前缀 | +150~250 行（复用 `ToolNameRewriter` 行状态机 + `SseRewindStream` 压缩通道） | ◯ **二期**（v3 评审：本期不做，设计留档 §5.4；遗留 server 先经 supergateway 转 Streamable HTTP 接入，见 §6） |
| **C. stdio 托管桥** | duct spawn stdio server 进程并实现 stdio↔HTTP 桥 | +800~1500 行，进程管理/会话映射，爆炸半径大 | ❌ 不做。外部一行命令解决：`supergateway --stdio "npx -y <pkg>" --port P`（或 mcp-proxy），再在 duct 配 `url: http://127.0.0.1:P/mcp`。写进 README 作为标准搭配 |
| **D. 聚合网关** | 单端点合并多 server 工具面 | 需实现完整 MCP server 协议栈 | ❌ 不做（定位冲突，见 §2.4） |

交付节奏（v3）：**本期 A，B 二期**。与 aiproxy 同构、风险集中在传输层、协议语义零侵入；
stdio 与遗留 SSE 场景本期均由外部桥工具（supergateway 输出 Streamable HTTP）+ duct 转发的
**组合**覆盖，不阻塞任何存量接入。

---

## 4. 现有底座复用分析（代码级）

| 复用点 | 现状 | mcp 侧动作 |
|---|---|---|
| 入口分流 | `server.rs::handle_connection` origin-form 先行判定 `/healthz`、`/aiproxy/{p}/...`，兜底 400 | 新增序 2 分支：`segments[1] == "mcp"` 且其后有段 → 桥接给 mcp router；**五分支 → 六分支** |
| 连接桥接 | `aiproxy::serve_conn_from_prelude`（预读请求行 → duplex → hyper），签名仍绑定 aiproxy `AppState` | 提公共模块 `bridge.rs`，签名收敛为接收 `axum::Router`；mcp 直接复用（行为不变，dispatch 测试兜底） |
| 路由/转发 | `aiproxy::router` + `respond_forwarded`（黑名单/限长/流式/日志/轨迹埋点） | 新 `mcp.rs`，同构实现（不强行合并两个 handler：演进方向不同，见 §10） |
| 头策略 | `REQUEST/RESPONSE_HEADER_BLACKLIST` + `is_blacklisted`（黑名单制，AGENTS.md 不变量 #2） | mcp 侧共享常量（挪到公共位置或各自持有，倾向先各自持有，避免过早抽象） |
| 请求轨迹 | **已落地**（d13a3e4）：每请求 `RequestTrace::new(state.trace)`，事件链 `request/start → upstream/request → request/body → upstream/response → request/end`；`TracedBody`/`ScannedBody` 旁路观测 tap（Drop 兜底合成 `interrupted`）；脱敏契约 `header_summary`/`url_display`/`query_keys` | `McpState` 持同款 `Arc<TraceSink>` + `trace_body`；事件链整套沿用，仅 data 词汇 mcp 化（`branch:"mcp"` + `server`，§5.6）；**共用同一 trace.jsonl**（单一排查入口），不新增 CLI |
| 配置装载 | `config.rs` 三层语义（缺省禁用/损坏致命/条目跳过）、`is_valid_provider_id`、`normalize_base_url` | `Config` 扩展 `mcp.servers` 段；id/url 校验函数直接复用 |
| 错误模型 | `error.rs` OpenAI 兼容 JSON（404 列表提示 / 413 / 502 / 504）+ `trace_identity` | 新增 `AppError::ServerNotFound` 等变体或复用 message 形态；错误体格式对 MCP 客户端不敏感（状态码才是语义），维持 JSON `{error:{message,type}}` 即可 |
| SSE 行改写 | `sse_normalize.rs` 已有完整通道：`ToolNameRewriter`（`ingest/finish` 分块行状态机）+ `SseRewindStream`（gzip/deflate 解压→改写→明文重发、剥 content-encoding；br 退透传+WARN） | **本期零改动**（A 方案纯透传）；二期 B 的 `SseEndpointRewriter` 实现同款 `ingest/finish` 形态，`SseRewindStream` 届时抽一个行改写 trait（形态即现有签名，零语义变更）即可复用压缩流通道（§5.4） |
| CLI | `--config-file` / `--max-body` / `--trace-file` / `--trace-body` | **不新增 CLI 参数**：同一 config.yaml、同一 max_body、同一 trace sink |

✅ trace 底座已在 `d13a3e4` 落地（此前"工作区未提交改动"的提示作废）：`AppState` 现含
`trace: Arc<TraceSink>` + `trace_body: usize`，构造链 `new → with_trace → with_trace_body`。
mcp 分支按 AGENTS.md 不变量 #4–#6 从第一天就接入轨迹（事件成对、观测不反噬热路径、
正文默认不落盘），具体契约见 §5.6。

---

## 5. 推荐设计（本期 = 方案 A；§5.4 为二期留档设计）

### 5.1 配置 schema

```yaml
# ~/.config/duct/config.yaml
providers:            # 现有段，语义不变
  openai:
    url: https://api.openai.com/v1

mcp:                  # 新增段，可选；本期上游一律按 Streamable HTTP 转发
  servers:
    github:
      url: https://api.githubcopilot.com/mcp      # 上游完整端点，不含 query
    filesystem:
      url: http://127.0.0.1:9100/mcp              # stdio server 经 supergateway 转换后接入（§6）
    internal:
      url: http://mcp.corp:9000/mcp
      origin_policy: strip                        # 可选：upstream|strip|keep（默认 keep）
```

装载规则（在既有三层语义上放宽，保持向后兼容）：

1. `providers` 与 `mcp` **均变为可选段，但至少要有一个**；两者皆缺 → 文件级错误
   （现行为是"缺 providers 即致命"，对已部署无影响——合法旧文件仍合法）。
2. server id 复用 `is_valid_provider_id`（`[a-z0-9][a-z0-9_-]*`），路由段大小写敏感。
3. `url` 复用 `normalize_base_url`（http/https、去尾 `/`、host 非空），**新增禁止
   query/fragment**——凭证不进配置文件（P5 一致性；amap 式 key-in-query 的 server 请由
   客户端侧配置或前置桥处理）。
4. **本期无 `transport` 字段**：`mcp.servers` 条目仅 `url + origin_policy` 两键；
   `transport` 为二期 B 方案预留（§5.4），本期若配置中出现，按现行解析惯例**未知键静默忽略**。
   运维判据：上游若是遗留 SSE 端点（特征 `GET /sse` 下发 `event: endpoint`），
   透明转发不生效（客户端会照原始路径 POST `/messages` 打偏）——用 supergateway 转换接入（§6）。
5. 个别条目非法：跳过 + WARN 指名；与 provider 条目互不影响。

### 5.2 路由与分流

```
/mcp                       → 404 JSON（提示用法 + 已配置 server 列表）
/mcp/                      → 同上
/mcp/{id}                  → 转发到 {server.url}（端点本身，Streamable HTTP 主用法）
/mcp/{id}/{rest...}?query  → 转发到 {server.url}/{rest}?query（一次切分，剩余不解释）
```

- 判定加在 `handle_connection` origin-form 层（aiproxy 之后、400 兜底之前），
  `segments.len() >= 2 && segments[1] == "mcp"`（含裸 `/mcp`，由 router 回 404 列表）。
- CONNECT / absolute-form 正向代理 / healthz 分支**零改动**；`GET http://host/mcp/x HTTP/1.1`
  仍是正向代理（origin-form 判定不受影响）。
- Basic 认证边界不变：origin-form 分支（含 mcp）不经 `--user/--passwd`（P6 原则延续，
  信任边界=部署面）。

### 5.3 转发语义

- 方法/query/body 逐字节透传；响应逐 chunk 回传（含 `text/event-stream` 长流）；
  仅 connect 超时，无整体超时（GET 长挂合法）。
- 头：复用黑名单制（逐跳 + `Proxy-*` + `host`/`content-length`/`expect`）→
  `Mcp-Session-Id`、`MCP-Protocol-Version`、`Accept`、`Last-Event-ID`、`Authorization` 天然透传。
- `--max-body` 同栈复用（前置 CL 快路径 + LimitedBody 兜底）；MCP 工具大结果在**响应**方向，不受限。
- 小差异：mcp 的 `request_has_body` 建议将 `DELETE` 按"默认无体"处理（MCP DELETE 规范上无体，
  避免给上游挂空 chunked 体）；POST/PUT/PATCH 及带 CL 请求照旧。
- `trace_body > 0` 时同 aiproxy 向 mcp 上游协商 `Accept-Encoding: identity`
  （使内容快照与 JSON 响应事实解析可读；透传语义不变）。上游无视协商回压缩流时，
  A 方案透明透传不受任何影响（压缩流改写属二期 B 方案，§5.4）。

### 5.4 【二期】方案 B：遗留 SSE 的 endpoint 改写（设计留档，本期不实施）

> **v3 范围决议：本期不做**。不写 `SseEndpointRewriter`、不抽行改写 trait、不接 `transport` 字段；
> 以下为留档设计，二期启动时按本节落地（含 §7 风险 #12、§8 M4/M5/I6 测试项一并生效）。

仅当 `transport: http_sse` 且响应 `content-type: text/event-stream` 时，把上游字节流过
`SseEndpointRewriter`——实现与 `ToolNameRewriter` 同款的**分块行状态机**
（`ingest(&[u8]) -> Vec<Bytes>` / `finish() -> Vec<Bytes>`，按帧边界产出、低延迟、不整包缓冲）：

- 只改写**首个** `event: endpoint` 的 `data:` 行：
  - 以 `/` 开头 → 前插挂载前缀：`/messages?sessionId=x` → `/mcp/{id}/messages?sessionId=x`
  - 绝对 URL → 替换 scheme://authority 为 duct 请求侧 origin（path/query 保留）
- 其余帧字节级原样。改写一次性、幂等、非 SSE 响应零开销旁路。
- **压缩上游**（kso 教训：网关可无视 identity 协商恒发 gzip）：把 `SseRewindStream`
  硬绑的 `ToolNameRewriter` 抽为行改写 trait（现有 `ingest/finish` 签名零语义变更），
  endpoint 改写即免费复用「解压→改写→明文重发（剥 content-encoding）」通道；
  br 与 aiproxy 现状一致：退回压缩透传 + WARN 说明改写失效。

### 5.5 Origin 策略（防上游 DNS-rebinding 校验误杀）

`origin_policy`（默认 `keep`，即现状透传）：

| 值 | 行为 | 适用 |
|---|---|---|
| `keep` | 透传客户端 Origin（含无 Origin 时不造） | 上游不校验 Origin（TS SDK 默认关） |
| `strip` | 剥掉 Origin | 上游"存在即校验"的宽松策略 |
| `upstream` | 改写为 server.url 的 origin | 上游严格校验同源（如带 DNS-rebinding 防护的开箱部署） |

### 5.6 轨迹接入（对齐 AGENTS.md 不变量 #4–#6）

trace 已是落地底座（d13a3e4），mcp 分支**从第一天接入同一事件链**，与 aiproxy 共用
同一 `TraceSink`（单一 `trace.jsonl` = 单一排查入口；`--trace-file` / `--trace-body` 语义全局一致）：

| 事件 | mcp 侧 data | 说明 |
|---|---|---|
| `request/start` | `method/path/query_keys/branch:"mcp"/server/origin_policy/request_headers(header_summary)` | 未注册 server 也成对（`known:false` + `available` 列表 + `end{rejected}`），平移 aiproxy 的 provider-miss 先例 |
| `upstream/request` | `url`（经 `url_display`：剥 userinfo 与 query 值） | mcp url 禁 query（§5.1），天然无 key |
| `upstream/response` | `status/ttfb_ms/response_headers(header_summary)` | 与 aiproxy 同构 |
| `request/end` | `outcome/bytes/chunks/…`（TracedBody 收尾） | 词表不变：`completed/rejected/upstream_error/stream_error/interrupted` |

关键取舍（防止把 OpenAI 词汇误读到 mcp 上）：

- **`TracedBody` 以 `sse:false` 包装**：`usage/finish_reason/[DONE]` 是 OpenAI 语义，
  对 MCP 流会产出恒 `done:false` 的假象，且 README 的 `select(.data.sse.done==false)`
  截断流配方会误报——`sse:false` 保留字节/分块计数、内容快照与 Drop 兜底
  `interrupted`（这些与协议无关），只关掉 SSE 帧词汇提取。
  `request/start.branch` 字段供消费端显式区分两分支。
  附注：`sse:false` 下 JSON 响应仍走 `json_body_facts` 预览解析——JSON-RPC 正常回包
  产出近空事实（events 计数；`id` 仅字符串型才记录），而 `{"error":{…}}` 型 JSON-RPC
  错误体会记入 facts.error，是**免费的排查增益**；MCP 的 SSE 回包预览解析失败即静默放弃。
- **请求体扫描不做**（ScannedBody 提取的 model/stream 对 JSON-RPC 无意义）；
  JSON-RPC `method` / tools/call 工具名是天然的 mcp 派生事实，列为后续增强，不入本期。
- **`mcp-session-id` 不进 `SAFE_HEADER_VALUES`**：会话 id 是可重放的能力凭证
  （持有者可冒用会话），默认名单外行为"只记名字"恰好正确；`authorization` 等已被
  `SENSITIVE_HEADERS` 覆盖，无需改动脱敏名单。
- **`--trace-body` 内容采集对 mcp 同样门控**：JSON-RPC 参数可含 prompt/工具入参，
  默认永不落盘；开启后快照行为与 aiproxy 一致（含敏感数据警示进 docs）。
- 访问日志对齐既有格式：`mcp forwarded server=github method=POST path=/mcp/github status=200 elapsed_ms=…`；
  长挂流加 `mcp stream opened/closed`。
- AGENTS.md 约定"加轨迹字段须同步扩 emit 与 e2e 断言"：本期 mcp 不新增共享词表字段
  （branch/server 为 mcp 事件自有 data），e2e 断言落在新增的 mcp 测试套件（§8 T 项）。

### 5.7 模块落位

```
src/
├── mcp.rs             # 新增：McpState（config/client/max_body/trace/trace_body 同构 AppState）
│                      #       + router / forward / 轨迹埋点（B 的改写器挂载属二期增量）
├── bridge.rs          # 新增：serve_conn_from_prelude 提取公共（aiproxy 现函数改为薄再导出）
├── config.rs          # 扩展：mcp.servers 段 + McpServerConfig（本期仅 url + origin_policy）
├── server.rs          # 分流：五分支 → 六分支
├── error.rs           # 变体扩展（ServerNotFound 等，含 trace_identity）
└── main.rs            # 双 state 构建 + 启动日志（mcp servers=N ids=…）
```

二期（B）才追加：`sse_normalize.rs` 行改写 trait 抽取 + `SseEndpointRewriter`。
本期 **`sse_normalize.rs` 零改动**。

---

## 6. 客户端使用示例

```jsonc
// Claude Desktop claude_desktop_config.json（remote 型）
{ "mcpServers": { "github-via-duct": { "url": "http://127.0.0.1:11088/mcp/github" } } }
```

```bash
# Streamable HTTP 冒烟（initialize 握手）
curl -sS -D- http://127.0.0.1:11088/mcp/github \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize",
       "params":{"protocolVersion":"2025-03-26","capabilities":{},
                 "clientInfo":{"name":"curl","version":"0"}}}'
# 期望：200 + mcp-session-id 响应头 + result（协议版本协商）

# 未注册 id
curl http://127.0.0.1:11088/mcp/nope -X POST -d '{}'   # → 404 JSON，列出可用 server

# stdio server 接入（组合而非内建；supergateway 输出 Streamable HTTP，正好落在本期 A 方案能力面内）
npx -y @supercorp/supergateway --stdio "npx -y @modelcontextprotocol/server-filesystem /tmp" \
  --outputTransport httpStreamable --port 9100
# config.yaml:  filesystem: { url: http://127.0.0.1:9100/mcp }

# 遗留 SSE-only server 同理：先转换再挂载（本期 duct 只转发 Streamable HTTP 上游）
npx -y @supercorp/supergateway --sse http://legacy-mcp:8931/sse \
  --outputTransport httpStreamable --port 9200
# config.yaml:  legacy: { url: http://127.0.0.1:9200/mcp }
```

---

## 7. 边界情况与风险清单

| # | 场景 | 分析 | 处置 |
|---|---|---|---|
| 1 | GET 通知流与 POST 请求**共用一条 TCP 连接** | hyper http1 连接内串行，长挂流会饿死后继请求；MCP 客户端 SDK（TS/Python）本就为通知流**另开连接**，直连上游时行为相同 | 与直连语义一致，不设特殊处理；文档标注 |
| 2 | 上游 `initialize` 回 4xx（Origin 被拒） | 透传状态码，客户端可见 | §5.5 origin_policy 消化 |
| 3 | 会话过期：server 回 404 + "重新 initialize" | 透明转发即合规 | 无需处理 |
| 4 | 同一 server 多客户端并发会话 | session id 由上游签发、客户端各持自己的，duct 无状态 | 无需处理 |
| 5 | 大响应（工具返回 MB 级 JSON/SSE） | 流式回传，max_body 只限请求体 | 无需处理 |
| 6 | 上游仅 HTTP/1.1 或经 ALPN 升 HTTP/2 | reqwest 自适应；GET 长流两者均支持 | 集成测试覆盖 h1 上游即可 |
| 7 | `/mcp` 前缀与正向代理 URL 含 `/mcp` 的撞名 | origin-form 才进此分支，absolute-form 不受影响 | dispatch 测试锁定 |
| 8 | `providers` 必填放宽的兼容性 | 旧合法文件不变；旧"缺 providers 致命"用例失去意义属预期演进 | config 单测锁定新语义 |
| 9 | url 含 query 的配置 | 禁止（§5.1），避免凭证落盘与 query 合并歧义 | 校验拒绝 + WARN 跳过 |
| 10 | duct 无鉴权 + MCP server 可执行工具 | 比 aiproxy 暴露面更敏感：转发的是"能执行动作"的端点 | README/部署文档加粗警示：仅限本机/内网，公网必须套鉴权反代 |
| 11 | 桥接预读（长请求行/头） | 与 aiproxy 同款 duplex 桥，已有长头专项测试 | 复用 bridge.rs，测试平移 |
| 12 | 上游是遗留 SSE server（`GET /sse` + `event: endpoint`） | 本期无 endpoint 改写，客户端照原始路径 POST `/messages` 打偏，转发不生效 | **本期边界，文档明示**；接入方式 = supergateway 转 Streamable HTTP（§6）；确有存量诉求再启用 §5.4 二期 |
| 12b | （二期）B 方案上游回压缩 SSE（kso 教训：无视 identity 协商恒发 gzip） | endpoint 事件在压缩字节里不可见，改写空转 | 二期实现即按 §5.4：复用 trait 化 `SseRewindStream`；br 退透传+WARN |
| 13 | OpenAI 轨迹词汇误读 | `TracedBody(sse:true)` 对 MCP 流恒报 `sse.done:false`，README 截断流 jq 配方误报 | mcp 侧固定 `sse:false` + `branch:"mcp"` 区分（§5.6）；不动共享词表 |
| 14 | mcp-session-id 落入轨迹 | 会话 id 是可重放凭证 | 不入 `SAFE_HEADER_VALUES`（默认只记名字，恰正确，§5.6） |

---

## 8. 测试计划（TDD）

基线：`cargo test` **129 全绿**（82 单元 + 47 集成，含 trace_e2e；HEAD `d13a3e4`）。
每步任务合入前基线必须保持全绿。

单测（`config.rs` / `mcp.rs` 内联）：

- C1 纯 providers 旧文件照常装载（向后兼容）；C2 纯 mcp 文件；C3 双段并存；
  C4 两段全缺 → 文件级错误；C5 非法 id / 非法 scheme / url 带 query 或 fragment → 条目跳过其余存活；
  C6 `McpServerConfig` 默认值（origin_policy=keep；未知键如 `transport` 静默忽略）。
- M1 target_url 拼接（root / rest / query 保编码）；M2 origin_policy 三态头效果；
  M3 DELETE 不挂体、POST 挂体、CL=0 边界。
  （M4 `SseEndpointRewriter`、M5 行改写 trait 抽取随 §5.4 移二期，本期不执行。）

Socket 级集成（`tests/mcp.rs` + dispatch 平移）：

- I1 mock MCP 上游（axum 迷你实现：initialize 发 `mcp-session-id` 头、tools/list 校验回传
  的 session、notify→202、DELETE→204、GET→慢滴 event-stream）全链路经 duct 握手成功；
- I2 SSE 分帧增量透传（上游每事件 sleep，断言客户端**非聚合**先后收到）；
- I3 长挂流不被总超时掐断（挂 > connect_timeout 不截，测试用缩短值验证）；
- I4 未注册 server id → 404 含可用列表；裸 `/mcp` → 404；
- I5 origin_policy=upstream 时上游收到改写后的 Origin；
  （I6 endpoint 改写 + messages 路由命中，随 §5.4 移二期，本期无用例。）
- I7 CONNECT / absolute-form / healthz / aiproxy 回归不破（现有 129 测试全绿）；
- I8 轨迹链（仿 `tests/trace_e2e.rs`，用 `TraceSink::capture()`）：
  正常转发 `request/start(branch/server) → upstream/request → upstream/response → request/end{completed}`；
  provider-miss 成对 `rejected`；客户端中断 → `interrupted`；
  **脱敏全文扫描**：trace 行内不出现 `mcp-session-id` 值与 authorization 值；
  `sse.done` 字段在 mcp 事件里不出现（`sse:false` 验证）。

---

## 9. 决策记录

| # | 问题 | 决议 |
|---|---|---|
| Q1 范围 | 遗留 HTTP+SSE 改写（B 方案）的交付节奏 | 🔄 **v3 评审变更：本期只做 A（Streamable HTTP 透明转发），B 顺延二期**（设计留档 §5.4，`transport` 字段暂缓；前轮"A+B 同期"决议作废） |
| Q2 stdio | 不内建子进程托管；README/deploy 给出 supergateway 组合姿势 | ✅ **按推荐执行** |
| Q3 聚合 | 确认不做单端点工具聚合（§2.4） | ✅ 按推荐执行，不做（如未来需要单独立项） |
| Q4 origin_policy 默认值 | `keep`（透传，与 aiproxy 无侵入哲学一致）；`strip`/`upstream` 按需显式配置 | ✅ 按推荐执行 |
| Q5 配置形态 | 取 `mcp.servers.<id>.{url, origin_policy}` 嵌套段（`transport` 键二期随 B 启用） | ✅ **已确认**（v3 微调：本期两键） |
| Q6 规范复核 | MCP 是否有 2025-06-18 之后的新修订影响 §2.3 结论（如 `Mcp-Param-*` 头、轮询长轮询语义） | ⏳ 联网通道恢复后复核一次（预计不改 A 方案结论，仅可能增补透传头清单） |

---

## 10. 实施任务拆解（评审通过后）

| 任务 | 内容 | 验收 |
|---|---|---|
| T1 | config.rs：双段可选 + `McpServerConfig` 装载校验（本期仅 url + origin_policy） | C1–C6 |
| T2 | bridge.rs：`serve_conn_from_prelude` 泛化为 Router 入参（行为不变重构；`sse_normalize.rs` 本期零改动） | 现有 129 测试全绿 |
| T3 | mcp.rs：McpState（含 trace/trace_body）/router/透传转发 + origin_policy + **轨迹埋点**（§5.6 事件链） | M1–M3, I1, I4, I5 |
| T4 | server.rs 六分支 + main.rs 双 state 装配 | I4, I7 |
| T5 | 流式与会话语义集成测试（含长挂）+ 轨迹链断言 | I1–I3, I8 |
| T6 | 文档同步：README（新特性条目/用法/六分支架构图/supergateway 组合姿势/安全警示/测试计数）、docs/deploy.md、**AGENTS.md 的 "Five-branch dispatch" 一节**（README 与 AGENTS.md 是用户契约与代理契约，代码合入须同提交更新） | 文档评审 |
| T7 | `cargo clippy --all-targets -- -D warnings` + `cargo fmt --check` | CI 绿 |

（二期任务位预留：T8 = SseEndpointRewriter + `transport: http_sse` 接线 + 行改写 trait 抽取，
设计见 §5.4，验收 M4/M5、I6。）

规模预估（本期 A）：≈ 350 行实现 + ~400 行测试；轨迹接入 ≈ 60 行埋点 + 100 行断言
（复用现成 sink/脱敏设施，增量小）。二期 B 追加 ≈ 150–250 行实现 + 100 行测试。
