# duct MCP 聚合网关（mcp hub）调研与设计方案

> 状态: **设计 v2（未实现）**。范围建议：**一期只做 tools 聚合（list + call）**，resources/prompts/通知流/反向请求中继进二期。
> **修订记录**:
> - v1: 初版设计稿（端点 `/mcp/hub` + 子集白名单形态）。
> - v2: **评审落地三项决策**——① 形态定为 **duct 内建**（§4 方案 A）；② 端点定为**顶层固定路径 `/mcphub`**
>   （与 `/mcp/*` 转发路由表彻底隔离：分流六分支→七分支，取消 `path` 配置与 "hub 保留 id" 规则）；
>   ③ 聚合纳入粒度定为 **`mcp.servers.<id>.hub: bool` 按 server 启用/禁用、默认启用**
>   （替代 `hub.servers` 子集白名单）；连带把「id 含 `__`」的单射性约束从整条跳过降级为**仅摘出聚合面 + WARN**。
> 定位变化: 这是 duct **第一个需要「理解 MCP 协议 envelope」的组件**——正是 `mcp-forwarding-design.md` §2.4
> 判过「与轻量中转定位冲突、列入非目标、如未来需要单独立项」的 **D 方案（聚合网关）**。本文即该立项的设计稿。
> 关联: `docs/mcp-forwarding-design.md`（A 方案已落地 `feat/mcp-forwarding`，本方案复用其传输底座与配置体系）、
> `AGENTS.md` 不变量（#1 凭证零接触、#2 头黑名单、#3 流式不缓冲、#4–#6 轨迹契约、#8 上游错误透传——
> 本方案对 #3 有两处**受控破例**，见 §2.3 与 §5.6）。
> 协议基线: MCP spec 2024-11-05 / 2025-03-26 / 2025-06-18（联网检索通道仍不可用，关键行为以本地知识为据；
> 实施前对照 modelcontextprotocol.io 规范原文复核——沿用 forwarding §9 Q6 的未决项）。
> 先例参照: Higress `mcp-session` golang-filter + `mcp-servers` wasm 插件（本地有 checkout，REST/MCP 后端统一汇聚
> 到网关单端点，含会话与工具表管理）；docker/mcp-gateway、MetaMCP、mcp.run、LiteLLM MCP 聚合（未联网复核）。

---

## 1. 需求与目标形态

### 1.1 需求（用户原话归纳）

- MCP **发现/聚合服务**：后端配置多个 MCP server，**对外只暴露一个 MCP 端点**；
  把所有后端 server 的**可用工具合并**到统一端点的 `tools/list` 返回，客户端经这一端点调用任一工具。

**已定需求决策（v2 评审输入）**：① 形态为 **duct 内建**（非 sidecar、非开源引入）；
② 端点使用 **`/mcphub`**，与 `/mcp/*` 转发路径避免冲突；③ 配置支持**对指定 mcp server 启用/禁用聚合，默认启用**。
对应设计落点：§4-Q1 / §5.1-Q9 / §5.2-Q2。

### 1.2 目标形态

```
客户端（任何 remote MCP client，只见一个 url）
   │  http://duct:11088/mcphub
   ▼
┌─────────────────────────── duct mcp hub ───────────────────────────┐
│ initialize/握手：hub 自签 Mcp-Session-Id，自报 capabilities         │
│ tools/list ：扇出 N 后端 → 改名合并（github__create_issue …）→ 分页 │
│ tools/call ：按前缀反查路由 → 原样调用后端 → 响应/进度流透传        │
│ 会话表     ：hub session → {server → backend session}（懒建、TTL）  │
└──────────┬───────────────┬───────────────┬─────────────────────────┘
           ▼               ▼               ▼
      github (Streamable  fs (supergateway  internal (…origin_policy
      HTTP)                转出的 HTTP)      per-server 配置复用)
```

与 A 方案（`/mcp/{server}` 透明转发，每 server 一 url）的关系：**并存、互补、共享底座**——
A 面向「客户端知道要连哪个 server」的多 url 场景；hub 面向「客户端要一个工具面」的场景。
hub 的后端连接管理直接复用 A 的 reqwest 栈、头黑名单、`origin_policy`、`--max-body`、trace sink。

### 1.3 为什么这不是 A 方案的顺手延伸

A 的价值恰恰在于**不理解载荷**（§2.4 原判定）。聚合端点必须：签发自己的会话、
合并/改写 `tools/list` 的 result、解析 `tools/call` 的 `params.name` 做反向路由、
维护 hub↔backend 双层会话与 id 关联——**协议状态机不可回避**。设计上的对策是把
「懂协议」限制在 **JSON-RPC envelope 的最小面**（method / id / params.name / params._meta /
result.tools 条目名），其余字节（arguments、content 数组、错误体）全部原样中继（§5.6）。

---

## 2. 需求分析

### 2.1 能力需求（一期）

| # | 能力 | 说明 |
|---|---|---|
| F1 | 单端点握手 | hub 实现 MCP server 侧：`initialize` → 协商 protocolVersion、自报 serverInfo/capabilities、签发**自己的** `Mcp-Session-Id`；受理 `notifications/initialized` |
| F2 | 会话双层管理 | hub session ↔ 每后端一个懒建 backend session；backend session 失效（404）透明重建一次；客户端 DELETE/空闲 TTL 收尾 |
| F3 | tools/list 聚合 | 并发扇出全部**纳入**的 server（per-server `hub` 开关，默认启用）→ 合并工具表（改名 §5.4）→ 统一游标分页 → 单后端失败**降级**不阻塞整体 → 快照 + TTL 缓存 |
| F4 | tools/call 路由 | 反解工具名前缀 → 定位后端与原名 → 透传 arguments/_meta → 原样中继 result（含 JSON 与 SSE 两形态、progress 通知、isError） |
| F5 | 凭证零接触延续 | 客户端 `Authorization` 只在建 backend 会话的那次请求内瞬时使用，不落配置、不进轨迹（不变量 #1） |
| F6 | 与 A 并存零回归 | `/mcp/{server}/*` 透明转发行为不变；hub 挂**独立顶层路径 `/mcphub`**（§5.2），与 mcp 路由表零交集 |
| F7 | 轨迹可观测 | 每个 hub 请求一条 trace：握手、扇出各后端耗时/工具数、路由到的 server、流收尾——复用 trace.jsonl（§5.10） |
| F8 | 部署假设显性化 | 会话表在进程内存 → 单实例假设，README/deploy 标注（§6 风险 #4） |

### 2.2 一期不做（明确非目标，防需求蔓延）

| 项 | 处置 |
|---|---|
| resources/*、prompts/* 聚合 | 二期。后端 capabilities 含 resources/prompts 时，一期**不声明、不聚合**，工具面保持完整即可用；配置 `include_*` 预留 |
| 服务端主动通知流（客户端 GET /mcphub 长挂） | 一期回 **405**（spec 允许，等价于「无 server→client 主动消息能力」）。代价：`tools/list_changed`、跨请求 progress、后端 GET 流事件一期不透传 |
| sampling / elicitation / roots 等 **server→client 反向请求** | 一期不中继：出现在后端 POST 响应流里的反向请求帧 → hub 代答 JSON-RPC error（`-32601`, "gateway does not relay server-to-client requests"）+ WARN，其余帧正常透传（§5.6）。完整中继需 id 翻译表，二期 |
| 多实例/会话粘性/跨进程共享会话 | 不做，文档声明单实例 |
| stdio / 遗留 SSE 后端 | 沿用 A 的结论：经 supergateway 转 Streamable HTTP 再纳入聚合，hub 不内建托管 |
| 跨客户端共享后端会话（省连接的 pooled 模式） | 一期不做（会话语义隔离 + 凭证瞬时性优先），列为演进选项（§5.3 M2） |
| 聚合开关之外的动态注册/服务发现 | 不做，配置静态清单，与 providers/servers 同构（纳入与否由 per-server `hub` 开关表达） |

### 2.3 不变量冲突登记（评审必读）

- **#3 流式不缓冲**：`tools/call` 响应透传仍是逐帧流式；**唯二**的缓冲点是 ① `tools/list`
  聚合需读完整 result（有 `max_tools`/`list_ttl` 约束的有限 JSON）② POST 响应流的帧扫描器
  （行状态机，逐帧产出，不整包缓冲——即 `ToolNameRewriter` 既有形态）。其余路径不破例。
- **轻量中转定位**：hub 是显性的 opt-in 特性（`mcp.hub.enabled`），默认关闭；不启用时 duct 行为与 A 完全一致。

---

## 3. 协议职责调研（聚合器视角，spec 2025-06-18）

| # | 协议点 | 聚合器义务 |
|---|---|---|
| 1 | `initialize` 协商 | 后端集合可能版本不一致：hub 以**自己与客户端**协商的版本对外；对每个后端用自身支持的版本独立握手 |
| 2 | `Mcp-Session-Id` | 双层：hub 签发对外 id，与后端 id 一一映射存会话表；**后端 id 永不外泄**；404 会话→重建（F2） |
| 3 | `MCP-Protocol-Version` 头（2025-06-18） | hub 对外按版本矩阵处理；对后端握手成功后该头照发 |
| 4 | Origin 校验（2025-06-18 安全章节） | 复用 per-server `origin_policy`（A 已落地），hub 对后端建会话同样应用 |
| 5 | `tools/list` cursor | 后端各有 cursor：hub 必须先全量拉平每个后端（循环翻页）再对**合并快照**重分页，对外 cursor 用 `snapshotRev:offset` |
| 6 | 工具名唯一性 | spec 只保证**单 server 内**唯一 → 聚合必须改名（§5.4）；`title`/`_meta` 保留原名 |
| 7 | `notifications/tools/list_changed` | 二期（依赖 F 中 GET 流或长轮询）；一期以 TTL 重取近似 |
| 8 | `tools/call` progressToken | 在 `_meta` 内：hub 不解释、原样透传即可让后端在同一 POST 响应流上发 progress；帧扫描器只放行通知/响应帧（§5.6） |
| 9 | 反向请求（sampling/elicitation/roots） | 后端可能在 POST 流或 GET 流上发 `method` 请求帧：一期代答 error（§2.2），二期 id 翻译中继 |
| 10 | 批量请求 | 2025-06-18 已移除 batching：一期只收单对象 POST，数组直接 400 |
| 11 | 通知类 POST 回 202 | hub 对外同理（`notifications/initialized|cancelled` → 202/204） |
| 12 | `outputSchema`/structuredContent、`annotations` | 不改写、随条目原样合并（envelope-only 的直接收益） |

---

## 4. 方案对比

| 方案 | 内容 | 规模 | 判定 |
|---|---|---|---|
| **A. hub 内建于 duct**（新模块 `mcp_hub`） | 复用 A 方案传输栈/配置/trace/桥；envelope-only 协议层 | ≈ 1500–2200 行实现 + ≈ 1000–1400 行测试 | ✅ **评审已定（Q1）**。单二进制/单端口/零新增运维是 duct 的立身之本，聚合是内网轻量场景的自然延伸，opt-in 门控把「懂协议」的爆炸半径关进一个模块 |
| **B. 独立 sidecar（mcp-hub 单独二进制/仓库）** | duct 保持纯透明；hub 作为独立进程，后端 url 可仍指 duct 的 `/mcp/{id}` | 类似规模 + 独立工程 | ◯ 备选。协议栈边界更纯，但两进程两配置两运维；且 B 的 hub 最终仍要抄一遍 duct 的 mcp client 底座。若团队对「duct 永远不懂协议」有硬契约，选 B |
| **C. 采用现成开源** | Higress mcp-session（envoy 生态、k8s 重）、docker/mcp-gateway、MetaMCP、mcp.run 等 | 0 开发 | ❌ 与「单二进制、内网轻量中转」定位不符；引入运行时/平台依赖。可在有现成平台的环境直接使用（Higress 本地 checkout 可参考其会话过滤器的工程形态） |

> 组合备注：A 与已落地的透明转发共存——`mcp.servers` 一份清单，`/mcp/{id}` 逐个透明暴露，
> `/mcphub` 一个聚合面（per-server `hub` 开关决定谁进聚合）。二者共享 hub 的懒会话都走 §5.11 的同一个后端 client。

---

## 5. 详细设计（方案 A，评审已定；一期 = tools 聚合）

### 5.1 配置 schema

```yaml
mcp:
  servers:                       # 现有段；新增第三键 hub（是否进聚合面；A 转发不受影响）
    github:   { url: https://api.githubcopilot.com/mcp }              # hub 默认启用
    fs:       { url: http://127.0.0.1:9100/mcp }
    debug:    { url: http://127.0.0.1:9200/mcp, hub: false }          # 显式禁用：仅保留 /mcp/debug 透明转发
    internal: { url: http://mcp.corp:9000/mcp, origin_policy: upstream }
  hub:                           # 新增可选段；缺省 = hub 不启用
    enabled: true
    separator: "__"              # 工具改名分隔符；本期仅支持 "__"
    list_ttl: 300                # 秒；合并快照缓存期（客户端请求触发重取）
    backend_timeout: 15          # 秒；扇出/单后端调用无响应超时（区别于 connect 超时）
    max_tools: 512               # 合并表上限：超限条目跳过 + WARN（防巨表）
    name_max_len: 64             # 改名后工具名上限（LLM function-name 现实约束），超限跳过 + WARN
    session_idle_ttl: 86400      # 秒；hub 会话空闲 GC
    max_sessions: 1024           # 会话表上限；超限拒绝新 initialize + WARN
```

装载规则在 A 的三层语义上追加（仍是条目级容错）：
1. `mcp.hub` 未知键静默忽略（与 `transport` 预留惯例一致）；`enabled` 缺省 false（缺段 = 不启用）。
2. **per-server 开关（v2 决策 Q9）**：`mcp.servers.<id>` 第三键 `hub: bool`，**默认 true**。
   `hub: false` 只把该 server 摘出聚合面——它的 `/mcp/{id}/*` 透明转发端点**完整保留**。
   这替代了 v1 的 `hub.servers` 子集白名单（默认启用 + 显式禁用，与 `normalize_sse` 的 per-provider 布尔先例同构）。
3. 单射性约束（v2 精化）：server id 含 `separator`（`__`）会破坏前缀反解（§5.4）→ 该条目**自动降级为
   `hub: false` + WARN**，不再整条跳过——透明转发不受牵连，故障半径与开关语义对齐。
4. 端点固定 `/mcphub`：与 server id 命名空间不相交，v1 的 "path 首段保留" 规则**删除**；`path` 键随之移除（不再可配置）。

### 5.2 端点与分流（六分支 → 七分支）

```
/mcphub             → hub 主端点（Streamable HTTP 单端点：POST JSON-RPC / DELETE 会话 / notifications）
/mcphub/            → 等同
/mcphub/<sub...>    → 404 hub usage JSON（客户端 url 配错兜底）
GET  /mcphub        → 一期 405（无 server→client 主动流；§2.2）
/mcp/{server}/*     → A 方案透明转发（零改动）
/mcp 与 /mcp/       → A 的 404 用法列表，追加 /mcphub 聚合端点提示
```

- **分流新增一臂**：`server.rs` origin-form 判定序 3（mcp 之后、400 兜底之前）加
  `segments.len() >= 2 && segments[1] == "mcphub"` → `mcp_hub::serve_conn_from_prelude(hub, prelude, stream)`
  （复用 `bridge.rs` 公共桥）。判定粒度与 mcp 臂一致（query 后缀行为同款）。**六分支 → 七分支**，
  AGENTS.md/README 契约同步更新（T8）。
- **为什么 `/mcphub`（评审决策 Q2）**：与 `/mcp/*` 路由表完全隔离——
  ① `mcp::router` 与 `mcp.rs` 零改动（v1 需在 mcp router 内赌 matchit static>param 优先级、注册 `/mcp/hub` 静态路由）；
  ② 不再需要把 "hub" 从 server id 保留——有人真配一个叫 `hub` 的 server 时，`/mcp/hub`（转发）与
  `/mcphub`（聚合）各行其道，无劫持/遮蔽面；
  ③ 端点形态自释义（url 层面一眼区分"聚合 hub" vs "单 server 转发"）。代价是分流层多一臂 + 顶层路径占名。
- **hub 未启用时**：`/mcphub*` 由分流臂照常桥给 hub router，router 内以 `HubState=None` 回
  **404 `{"error":{"message":"mcp hub is not enabled",...}}` + trace `rejected`**（对齐 A 的 disabled 先例，
  比落 400 分流兜底更自释义）。CONNECT / absolute-form 正向代理 / healthz / aiproxy 零改动；
  `GET http://host/mcphub HTTP/1.1` 仍是正向代理（origin-form 判定不受影响，风险 #13 由 dispatch 测试锁定）。

### 5.3 会话模型

```
HubSession (hub 签发 sid: uuid-v4, 内存态)
 ├─ protocol_version / client_info / initialized 标志 / created / last_seen
 └─ backends: { server_id → BackendSession {
        state: Idle | Connecting | Ready | Dead,
        backend_sid: Option<Mcp-Session-Id>,
        protocol_version, last_seen } }
```

- **M1（推荐）：每客户端会话一套后端会话**——工具与调用语义按客户端隔离（后端 session 内常含 cwd/授权态），
  且后端 initialize 只发生在**客户端请求的 in-flight 期**，`Authorization` 从当前请求头瞬时取用、
  会话表只存 backend sid 不存凭证（守 P5）。代价：连接数 = 客户端 × 后端（懒建缓解）。
- M2（演进选项，一期不做）：全局共享后端会话池，凭证需在 hub 配置注入——直接违反 P5，需另立评审。
- 生命周期：客户端 `DELETE` → 对所有 Ready 后端转发 DELETE、表内删除；`session_idle_ttl` GC
  （tokio 定时器，同 trace writer 的后台任务形态）；`max_sessions` 上限拒绝新握手。
- 后端会话丢失（请求回 404 会话/明确失效）：同请求内**透明重建一次**（重新 initialize）重试一次，再失败 →
  JSON-RPC error + trace。

### 5.4 工具改名与反解（碰撞处理）

- 对外名 = `{serverId}{sep}{originalName}`（`github__create_issue`）。
- 反解 = **配置内 server id 最长前缀匹配 `{id}+sep`**，剩余部分即后端原名。
  单射性由 §5.1 规则 3 保证（含 `__` 的 id 自动摘出聚合），无需转义；同 server 内工具名唯一由 spec 保证 →
  合并表天然无碰撞。`tools/call` 的 args 与 result 的 content **不重写名字**（后端看到的永远是原名）。
- 每条合并条目附加 `_meta.hub = {server, original}`（客户端展示友好；spec 允许 `_meta` 自由扩展）；
  若后端未给 `title`，一期不改 `title`，避免侵入语义。
- **入站防御**：客户端直连某后端 url（A 路径）不受影响；hub 仅对**本会话 list 快照之外**的调用名按前缀
  规则现算路由（不依赖缓存命中，容忍 TTL 边界的名字漂移；后端回 -32602 unknown tool 原样中继）。
- 巨表/超长名：`max_tools`、`name_max_len` 截断跳过 + WARN（LLM function-name 现实 64 字符约束的规避，
  宁可显性降级不静默改名）。

### 5.5 tools/list 聚合流程

```
POST tools/list (hub session)
 ├─ 快照有效（list_ttl 内，含未翻完页）→ 直接对快照出页
 └─ 否则扇出：
     for each 纳入 server（配置序）:
        ensure BackendSession（懒 initialize，取当前请求凭证头，应用 origin_policy）
        → tools/list 循环翻页拉平（backend_timeout 约束）
     merge（改名/附 _meta/max_tools/name_max_len 过滤）→ 新快照 {rev, tools[], degraded[]}
     失败后端 → 记 degraded，列表照常返回；全失败 → JSON-RPC internal error + trace
 出页：opaque cursor = "rev:offset"，snapshotRev 间页一致（翻页中途不重取）
```

### 5.6 tools/call 转发流程

```
POST tools/call → 前缀反解 → (server, originalName)
 ├─ ensure BackendSession → 改写 name 后原样重发（客户端 jsonrpc id 不变 → 单后端无需 id 翻译）
 ├─ 响应 application/json → 原样回给客户端（含 isError/error）
 └─ 响应 text/event-stream → **帧扫描器**（ingest/finish 行状态机，复用 sse_normalize.rs 既有形态）：
      · data: 帧解析仅看「是否 server→client request 帧」（有 method 且非 notification）：
          一期 → 吞掉该帧、向后端发 error 代答（经后端 POST 流同址不可达 → 走其会话上开一次性
          JSON-RPC POST 答复？——否。规范上后端等待的是**该客户端会话**的 response；一期简化为
          关闭该后端响应流 + 代答错误进 trace + WARN，客户端收到流尾）※评审点 Q5
      · response/notification 帧 → 字节原样透传（progress 通知天然免费：token 在 _meta，后端自管）
      · 非 JSON 帧/注释帧原样透传
```

> 与 A 方案的差异要坦诚：A 里响应流 100% 字节透传；hub 的 tools/call 流要过帧扫描器
> （识别反向请求）。缓冲纪律不破坏：逐帧产出、无整包缓冲；`backend_timeout` 仅约束「流上迟迟
> 无任何字节」的僵死后端。

### 5.7 模块落位与复用

```
src/
├── mcp.rs                 # A 方案：零改动（hub 为独立顶层路径，不挂 McpState、不进 mcp router）
├── mcp_hub/
│   ├── mod.rs             # router() + handlers + HubState（client 池/config/trace sink/store）
│   │                      #   + serve_conn_from_prelude（薄封装 bridge.rs，与 aiproxy/mcp 同款）
│   ├── session.rs         # HubSession 表 + BackendSession 状态机 + GC
│   ├── backend.rs         # MCP 客户端：initialize / tools/list 翻页 / tools/call / DELETE
│   │                      #   （reqwest + mcp.rs 同款头黑名单/origin_policy；凭证取当次请求头）
│   ├── merge.rs           # 快照、改名/反解、cursor、max_tools/name_max_len
│   └── frames.rs          # envelope 解析（jsonrpc method/id/name）+ SSE 帧扫描器
│                          #   （行状态机形态对齐 sse_normalize.rs，反向依赖解除后可提公共）
├── server.rs              # 分流序 3 新增 mcphub 臂（六分支 → 七分支）
├── config.rs              # mcp.hub 段 + HubConfig；servers 条目第三键 hub: bool（默认 true）+ 单射性降级
└── error.rs               # HubDisabled / TooManySessions 等（JSON-RPC 语境：错误体走
                           #   {error:{message,type}} 仅作 fallback；协议内应答用 JSON-RPC error 对象）
```

JSON-RPC 对外错误一律用**协议形态**（`-32601/-32602/-32000` + message），网关级不可用（会话超限、
后端全灭）也回合法 JSON-RPC error——这与 A 的「网关自身故障造 OpenAI 形错误、上游错误透传」的
不变量 #8 精神一致：**只为自己产生的错误造形，后端错误原样中继**。

### 5.8 凭证与安全

- **P5 延续**：凭证不落配置文件（url 禁 query 已有）；hub 会话表**不存** Authorization——
  后端懒建只发生在客户端请求 in-flight 内（M1 的直接好处）；重连/重建同样必须搭客户端请求的车。
- 会话 id：hub 对外 sid 由 hub 自管（随机 uuid，不可猜）；**后端 sid 永不进对外响应/轨迹**
  （沿用 A 的 `mcp-session-id` 不入 SAFE_HEADER_VALUES 判定，hub 侧同理）。
- 攻击面警示升级（A 的 README 警示在 hub 上加倍成立）：聚合端点 = 一把钥匙开 N 个能执行动作的后端；
  合并工具描述来自后端（**prompt injection 面**），部署上仍限内网/防火墙后；per-server `hub: false`
  开关让运维能显性控制哪些后端进聚合面（默认全进，需显式摘除）。
- DoS 界：`max_tools` / `name_max_len` / `max_sessions` / `backend_timeout` / `list_ttl` 五道闸。

### 5.9 错误模型与降级

| 场景 | 对外行为 |
|---|---|
| hub 未启用而访问 `/mcphub*` | HTTP 404 `{error:{message:"mcp hub is not enabled"}}` + trace `rejected`（§5.2；不伪装成 server 转发） |
| 单后端 tools/list 失败/超时 | 该 server 缺席 + 快照 `degraded` 记录；`_meta.hub.warnings` 附 `[{server, reason}]`；不 5xx |
| 全部后端不可达 | tools/list → JSON-RPC error(-32000, "no upstream available")；trace upstream_error |
| tools/call 路由到的后端失败 | 后端 JSON-RPC error / isError 响应**原样中继**（不变量 #8）；连接失败 → -32000 upstream_error |
| 未 initialize / 无会话头 / 未知 sid | HTTP 400/404（按 spec：无会话 404 + 提示重新 initialize；缺版本头 400） |
| initialize 前调用 / 非法 batching | JSON-RPC error / HTTP 400 |

### 5.10 轨迹接入（对齐不变量 #4–#6）

同一条 trace id 贯穿「一个客户端请求 → N 个后端交互」（单链排查）：

| 事件 | data 要点 |
|---|---|
| `request/start` | `branch:"mcp-hub"`, method(path=/mcphub), hub_sid **不记值**（同 sid 不入 SAFE 名单原则）, `init`/`list`/`call{server}` 语义摘要 |
| `hub/fanout`（tools/list） | `servers:[{id, status, tools_count, ms}]`，degraded 列表 |
| `upstream/request`/`upstream/response` | 每后端一次（复用 A 的形态：url_display/header_summary/ttfb） |
| `request/end` | `outcome/bytes/chunks` 收尾；`TracedBody` 固定 `sse:false`（同 A §5.6 全部取舍） |

请求体不经 `ScannedBody`（openai 字段无意义）；envelope 解析失败静默（观测不反噬热路径 #6 优先）。

### 5.11 对 A 模块的复用与最小侵入

- `mcp.rs`：**零改动**。hub 是独立模块 + 独立顶层路径（v2 决策 Q2 的直接收益）；server id `hub`
  可正常配置，`/mcp/hub`（转发）与 `/mcphub`（聚合）互不相干。
- `server.rs`：新增分流序 3（`segments[1] == "mcphub"`）；`run_with_states_from_listener` 保留现签名
  （内部 hub=None），新增全签名 `run_with_all_from_listener(listener, auth, aiproxy, mcp, hub)`；
  既有 dispatch/aiproxy/mcp 测试零扰动。
- `main.rs`：三 state 装配（`AppState` / `McpState` / `Option<HubState>`），共享同一 `Arc<Config>` 与
  trace sink；启动日志追加 `hub=on(servers=k)/off`。
- 后端 client（`backend.rs`）与 A 的 forward 共享同源原语：`REQUEST_HEADER_BLACKLIST`、`forward_allowed`、
  `origin_of/apply_origin_policy`、`target_url`、connect-timeout 常量——第三次出现，**允许**届时抽
  `proxy_util` 小模块或先 `pub(crate)` 直用（独立小重构，不阻塞本特性合入）。
- 桥（`bridge.rs`）/ trace / config 三层语义：全部原样复用。

---

## 6. 边界情况与风险清单

| # | 场景 | 分析 | 处置 |
|---|---|---|---|
| 1 | 后端握手/会话状态各异（同一工具在两家后端） | 改名后天然并存（`a__run` / `b__run`） | 无需处理；文档写明 |
| 2 | 后端 list 很长且翻页 | 快照前全量拉平，客户端只面对 hub 自己的 cursor | `max_tools` 闸 + fan-out 超时 |
| 3 | 客户端把合并列表喂给 function-name 受限 LLM（64 字符/字符集） | 前缀加长、`.` 不在常见字符集 → 选 `__` 而非 `.` | `name_max_len` 截断显性 WARN；二期可上可逆 hash 缩短 |
| 4 | 会话表纯内存 → 重启丢会话、多实例不共享 | duct 单二进制定位，客户端（TS/Python SDK）遇 404 会话会自动重 initialize | 文档声明单实例；重连风暴由 `max_sessions` 兜底 |
| 5 | 客户端长挂 GET 等通知（部分 SDK 会开） | 一期 405：SDK 按规范降级为无通知流，工具调用不受影响 | 二期 GET fan-in |
| 6 | 后端在 tools/call 流上发 sampling 等反向请求 | 一期无法中继到客户端 | §5.6 处置 + trace/WARN；评审点 Q5 |
| 7 | 后端 A 的工具依赖「同客户端刚在别处建的会话态」 | 每客户端一套后端会话保证同一 hub 客户端跨工具调用同后端会话 | M1 天然满足；跨 hub 会话共享不存在（正确默认） |
| 8 | 凭证仅部分后端需要 | 无凭证时后端 initialize 401 → 该 server degraded，其余照常 | 降级语义覆盖 |
| 9 | `tools/list` 与 `tools/call` 竞态（快照过期、后端工具下线） | 路由按前缀现算不依赖快照；后端 -32602 原样回 | 已设计（§5.4 入站防御） |
| 10 | 巨工具描述注入 hub 内存 | 快照上限 `max_tools`；单条目大小随 envelope 解析上限（`--max-body` 只罩请求，后端响应无界 → fan-out 聚合处加 per-backend 响应字节帽） | 二期加 `max_result_bytes`；一期至少 `backend_timeout` 兜时间 |
| 11 | spec 再修订（2025-11+）影响会话/授权语义（如 OAuth resource server 条款） | 转发设计 Q6 未决项在 hub 上权重更高（要握手，不再是透传） | 立项 spike 先复核原文；协议层集中在 `backend.rs`/`mod.rs` 便于跟进 |
| 12 | 聚合面安全暴露 | 一把钥匙 N 个可执行后端 | §5.8 部署警示 + per-server `hub:false` 显性摘除 |
| 13 | `/mcphub` 与正向代理流量撞名 | absolute-form（`GET http://host/mcphub`）不进 origin-form 分流，仍走正向代理 | 与 forwarding §7 #7 同构；dispatch 测试锁定（J7） |
| 14 | per-server 开关配置漂移（转发要、聚合不要，反之亦然） | 开关只表达「是否进聚合面」，与转发存在性解耦（§5.1 规则 2）；启动日志/`/mcp` 用法列表标出纳入集 | 可观测性兜底（T8 文档写明） |

---

## 7. 测试计划（TDD，一期）

基线：A 已落地（149 全绿）；每步合入基线保持全绿；`/mcp/{server}` 的既有 I1–I8 用例**零修改**通过。

单测：
- H1 命名：改名/反解单射（含「id 含 `__` → 自动摘出聚合 + WARN、转发保留」的装载用例，
  per-server `hub: false` 不改变 `get_mcp` 可见性）、`_meta` 附挂、`max_tools`/`name_max_len` 截断；
- H2 快照/cursor：多后端翻页拉平、rev 间页一致、degraded 记录；
- H3 会话表：懒建状态机、404 重建一次、TTL GC、`max_sessions`；
- H4 frames：envelope 识别（response/通知/反向请求三分）、分块喂入跨帧边界。

集成（mock 双后端：工具名故意碰撞 + 一家间歇失败 + 一家带 progress SSE 的 tools/call）：
- J1 initialize→initialized→tools/list（合并断言含两家 + degraded）→tools/call 两路各中；
- J2 后端 tools/call 返回 SSE 带 progress：逐帧透传非聚合（仿 A 的 I2 计时断言）；
- J3 后端会话 404 → 透明重建重试成功；重建再败 → JSON-RPC error；
- J4 GET /mcphub → 405；DELETE → 200/204 且后端会话被转 DELETE；
- J5 未 initialize 直呼 tools/list → spec 错误形态；数组 batching → 400；
- J6 origin_policy 对 hub 懒建会话同样生效（平移 A 的 I5）；
- J7 全 A 回归 + dispatch **七分支**回归：`/mcp/{id}`（含配了 id=`hub` 的 server 时 `/mcp/hub` 正常转发）、
  `/mcphub`（聚合）、`/mcphub/x`（404 usage）、hub 未启用时 `/mcphub`（404 disabled）三线互不扰；
  CONNECT/absolute-form/healthz/aiproxy 不破（含 absolute-form 带 `/mcphub` 路径仍走正向代理）；
- J8 轨迹链：单 trace 贯穿 fan-out，`branch:"mcp-hub"`、`hub_sid`/后端 sid/Authorization 值
  **全文扫描不出现**、`sse:false` 验证（仿 A 的 I8）。

---

## 8. 实施任务拆解（评审通过后）

| 任务 | 内容 | 验收 |
|---|---|---|
| T0 | spike：mock Streamable-HTTP 后端上跑通 hub 侧完整 initialize 握手 + list/call 骨架（验证 §3 假设、定帧扫描器形态） | 原型可演示 |
| T1 | config.rs：`mcp.hub` 段 + HubConfig；`mcp.servers.<id>.hub: bool`（默认 true）+ 单射性自动降级 | H1-装载项 |
| T2 | `mcp_hub/frames.rs` + `backend.rs`：envelope/SSE 帧扫描器 + MCP 后端 client（复用 §5.11 底座抽取） | H4、T0 语义回归为测试 |
| T3 | `mcp_hub/session.rs`：双层会话表 + 懒建 + 重建 + GC + 上限 | H3 |
| T4 | `mcp_hub/merge.rs`：快照/改名/反解/分页/degraded（含 per-server 开关过滤） | H1 H2 |
| T5 | `mcp_hub/mod.rs` handlers：initialize 状态机 / tools/list / tools/call / DELETE / 405/400 边角 / disabled 兜底 + 错误模型（§5.9） | J1–J5 |
| T6 | server 侧接线：分流序 3 `mcphub` 臂（六→七分支）+ `run_with_all_from_listener` + main.rs 三 state 装配 | J7 |
| T7 | 轨迹接入 + 脱敏扫描 | J8 |
| T8 | 文档：README（新特性/配置含 per-server 开关/安全警示/测试计数/**七分支分流图**）、AGENTS.md（模块表/分流图/不变量 #3 破例登记/hub 契约）、deploy.md（单实例声明） | 评审 |
| T9 | clippy + fmt 门禁 | CI |

（二期任务位：T10 GET fan-in 通知流 + listChanged；T11 resources/prompts 聚合；T12 反向请求中继 id 翻译；
T13 后端响应字节帽/可逆短名；设计增量届时补 §9。）

规模预估：一期 ≈ 1500–2200 行实现 + ≈ 1000–1400 行测试（帧扫描器/会话表/envelope 三块是主体）。

---

## 9. 决策记录

| # | 问题 | 决议 |
|---|---|---|
| Q1 | 形态：duct 内建（A）vs sidecar（B）vs 开源（C） | ✅ **评审已定：内建**——`mcp_hub/` 模块，opt-in `mcp.hub.enabled` 默认关；「懂协议」爆炸半径关进门控模块内 |
| Q2 | 端点路径 | ✅ **评审已定：顶层固定 `/mcphub`**——与 `/mcp/*` 转发彻底隔离；分流六→七分支；`path` 不可配、"hub 保留 id" 规则删除 |
| Q3 | 命名方案 | 恒前缀 `{server}__{tool}` + `_meta.hub` 附原值；sep 固定 `__`。v2 精化：id 含 `__` 时**自动摘出聚合 + WARN**（不再整条跳过，转发保留）——待复核 |
| Q4 | 会话模型 | M1 每客户端一套后端会话（P5 凭证瞬时性的前提）；M2 池化模式若需求出现另立评审（涉凭证存储）——待复核 |
| Q5 | 一期反向请求处置 | 帧扫描器吞帧 + trace/WARN 的「断流降级」；若嫌粗，一期 tools/call 仅接 JSON 响应、SSE 响应整包按 A 透传（放弃 progress 识别）——**推荐前者**，待复核 |
| Q6 | capabilities 对外声明 | v1 `{tools:{listChanged:false}}`；resources/prompts/补全类一律不声明（诚实暴露）——待复核 |
| Q7 | 单实例假设 | 接受，文档显性声明（duct 定位即单二进制）——待复核 |
| Q8 | 规范复核 | 联网通道恢复后先复核 2025-06-18+ 修订（尤其授权/OAuth 条款与 notifications 语义）再开工 T0——沿用 forwarding Q6 未决 |
| Q9 | 聚合纳入粒度 | ✅ **评审已定：`mcp.servers.<id>.hub: bool`，默认启用**；`false` 仅摘出聚合面、透明转发保留（v1 `hub.servers` 白名单作废） |
