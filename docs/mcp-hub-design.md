# duct MCP 聚合网关（mcp hub）调研与设计方案

> 状态: **设计 v4（未实现）**。范围建议：**一期只做 tools 聚合（list + call）+ hub 鉴权**，resources/prompts/通知流/反向请求中继进二期。
> **修订记录**:
> - v1: 初版设计稿（端点 `/mcp/hub` + 子集白名单形态）。
> - v2: **评审落地三项决策**——① 形态定为 **duct 内建**（§4 方案 A）；② 端点定为**顶层固定路径 `/mcphub`**
>   （与 `/mcp/*` 转发路由表彻底隔离：分流六分支→七分支，取消 `path` 配置与 "hub 保留 id" 规则）；
>   ③ 聚合纳入粒度定为 **`mcp.servers.<id>.hub: bool` 按 server 启用/禁用、默认启用**
>   （替代 `hub.servers` 子集白名单）；连带把「id 含 `__`」的单射性约束从整条跳过降级为**仅摘出聚合面 + WARN**。
> - v3: **鉴权与凭证模型重划（需求输入）**——后端 MCP server 需要鉴权，透传模型不适用于聚合面：
>   ① 后端鉴权凭证**统一配置在 hub**（`mcp.servers.<id>.auth`，仅 hub 消费）；② 客户端对 hub 用
>   **hub 签发的统一 token**（Bearer）鉴权，**原后端鉴权头被剥离、永不达后端**（凭证置换而非透传）；
>   ③ 由此对不变量 #1（凭证零接触）产生 hub 限定的**受控破例**（§2.3/§5.8 治理登记）；
>   ④ 连带重写：会话模型论据（§5.3，凭证不再依赖客户端 in-flight → 可预热/后台重建）、
>   scope 授权（§5.4，token 级工具面裁剪）、注入/剥离链（§5.6）、启动自检 fail-closed（§5.1 规则 5）。
> - v4: **A 路径鉴权非介入原则成文化 + 配置面彻底分离（评审指令："/mcp/\* 透明转发不需要碰鉴权头"）**——
>   后端凭证从 `mcp.servers.<id>.auth` **迁入 `mcp.hub.credentials.<id>`**：`servers` 段回到
>   v2 的三键形态（url/origin_policy/hub），鉴权在 **A 的配置模型、代码路径、行为语义三个层面都不存在**；
>   同时精确化 A 的契约表述：**不碰 ≠ 剥离**——客户端自带鉴权头在 A 路径仍作为普通头**原样透传**
>   （A 的既有用法就是客户端自带凭证），hub 的剥离链只存在于 hub 自己的请求组装里。
> 定位变化: 这是 duct **第一个需要「理解 MCP 协议 envelope」的组件**——正是 `mcp-forwarding-design.md` §2.4
> 判过「与轻量中转定位冲突、列入非目标、如未来需要单独立项」的 **D 方案（聚合网关）**。本文即该立项的设计稿。
> 关联: `docs/mcp-forwarding-design.md`（A 方案已落地 `feat/mcp-forwarding`，本方案复用其传输底座与配置体系）、
> `AGENTS.md` 不变量（#1 凭证零接触、#2 头黑名单、#3 流式不缓冲、#4–#6 轨迹契约、#8 上游错误透传——
> 本方案对 #3 有两处**受控破例**（§2.3/§5.6），对 #1 有**一处 hub 限定破例**（§2.3/§5.8；A 路径不受影响）。
> 协议基线: MCP spec 2024-11-05 / 2025-03-26 / 2025-06-18（联网检索通道仍不可用，关键行为以本地知识为据；
> 实施前对照 modelcontextprotocol.io 规范原文复核——沿用 forwarding §9 Q6 的未决项）。
> 先例参照: Higress `mcp-session` golang-filter + `mcp-servers` wasm 插件（本地有 checkout，REST/MCP 后端统一汇聚
> 到网关单端点，含会话与工具表管理）；docker/mcp-gateway、MetaMCP、mcp.run、LiteLLM MCP 聚合（未联网复核）。

---

## 1. 需求与目标形态

### 1.1 需求（用户原话归纳）

- MCP **发现/聚合服务**：后端配置多个 MCP server，**对外只暴露一个 MCP 端点**；
  把所有后端 server 的**可用工具合并**到统一端点的 `tools/list` 返回，客户端经这一端点调用任一工具。

**已定需求决策（v2/v3 评审输入）**：① 形态为 **duct 内建**（非 sidecar、非开源引入）；
② 端点使用 **`/mcphub`**，与 `/mcp/*` 转发路径避免冲突；③ 配置支持**对指定 mcp server 启用/禁用聚合，默认启用**；
④ **（v3）鉴权分层**：后端 MCP server 的鉴权凭证统一配置在 hub（**v4：收敛于 `mcp.hub.credentials`，
`mcp.servers` 段无鉴权字段**）；客户端只持有 **hub 签发的唯一鉴权 token**
（替代原 mcp server 的鉴权头——透明透传模型在聚合面不适用）；
⑤ **（v4）边界裁决**：`/mcp/*` 透明转发**不需要也不允许碰鉴权头**——不注入、不剥离、不改写，
客户端凭证头照旧原样透传（A 语义零变化）。
对应设计落点：§4-Q1 / §5.1-Q9 / §5.2-Q2 / §5.8-Q10~Q14。

### 1.2 目标形态

```
客户端（任何 remote MCP client，只见一个 url + 一把 hub token）
   │  http://duct:11088/mcphub   Authorization: Bearer <hub-token>（hub 签发/配置，非后端凭证）
   ▼
┌─────────────────────────── duct mcp hub ───────────────────────────┐
│ 鉴权门 ：Bearer hub-token 校验（token 表 + per-token servers scope）│
│ initialize/握手：hub 自签 Mcp-Session-Id，自报 capabilities         │
│ tools/list ：扇出 N 后端 → 改名合并（github__create_issue …）→ 分页 │
│ tools/call ：按前缀反查路由 → 原样调用后端 → 响应/进度流透传        │
│ 会话表     ：hub session → {server → backend session}（懒建、TTL）  │
│ 凭证置换   ：剥离客户端鉴权头 + 注入 hub.credentials 配置凭证（§5.8）│
└──────────┬───────────────┬───────────────┬─────────────────────────┘
           ▼               ▼               ▼
   github (Bearer ghp_…)  fs (x-api-key…)  internal (custom header…)
   （后端真实凭证 = hub 统一配置，客户端不可见、不经手）
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
| F5 | 鉴权与凭证分层（v3） | **仅 hub 面**：客户端→hub 用 hub 签发的统一 Bearer token（token 表 + scope）；hub→后端用 `hub.credentials` 配置凭证注入，并剥离客户端自带的鉴权头（不后传）。详见 §5.8 |
| F6 | 与 A 并存零回归（v4 成文化） | `/mcp/{server}/*` 透明转发**不碰鉴权**：`mcp.servers` 无鉴权字段、A 代码零引用凭证配置、客户端鉴权头仍作普通头**原样透传**（不碰 = 不介入，非剥离）。hub 挂**独立顶层路径 `/mcphub`**（§5.2），与 mcp 路由表零交集 |
| F7 | 轨迹可观测 | 每个 hub 请求一条 trace：握手、鉴权 label（不记 token 值）、扇出各后端耗时/工具数、路由到的 server、流收尾——复用 trace.jsonl（§5.10） |
| F8 | 部署假设显性化 | 会话表在进程内存 → 单实例假设，README/deploy 标注（§6 风险 #4） |

### 2.2 一期不做（明确非目标，防需求蔓延）

| 项 | 处置 |
|---|---|
| resources/*、prompts/* 聚合 | 二期。后端 capabilities 含 resources/prompts 时，一期**不声明、不聚合**，工具面保持完整即可用；配置 `include_*` 预留 |
| 服务端主动通知流（客户端 GET /mcphub 长挂） | 一期回 **405**（spec 允许，等价于「无 server→client 主动消息能力」）。代价：`tools/list_changed`、跨请求 progress、后端 GET 流事件一期不透传 |
| sampling / elicitation / roots 等 **server→client 反向请求** | 一期不中继：出现在后端 POST 响应流里的反向请求帧 → hub 代答 JSON-RPC error（`-32601`, "gateway does not relay server-to-client requests"）+ WARN，其余帧正常透传（§5.6）。完整中继需 id 翻译表，二期 |
| 多实例/会话粘性/跨进程共享会话 | 不做，文档声明单实例 |
| stdio / 遗留 SSE 后端 | 沿用 A 的结论：经 supergateway 转 Streamable HTTP 再纳入聚合，hub 不内建托管 |
| 跨客户端共享后端会话（省连接的 pooled 模式） | 一期不做（会话语义隔离优先；v3 后凭证模型已不阻碍，属可解锁项），列为演进选项（§5.3 M2） |
| 聚合开关之外的动态注册/服务发现 | 不做，配置静态清单，与 providers/servers 同构（纳入与否由 per-server `hub` 开关表达） |

### 2.3 不变量冲突登记（评审必读）

- **#3 流式不缓冲**：`tools/call` 响应透传仍是逐帧流式；**唯二**的缓冲点是 ① `tools/list`
  聚合需读完整 result（有 `max_tools`/`list_ttl` 约束的有限 JSON）② POST 响应流的帧扫描器
  （行状态机，逐帧产出，不整包缓冲——即 `ToolNameRewriter` 既有形态）。其余路径不破例。
- **#1 凭证零接触（v3 新增破例，hub 限定）**：hub 将**长期保管**后端 MCP server 的鉴权凭证（配置注入）
  并对客户端**签发/校验**自己的 token——这是 duct 第一次存凭证、第一次注入凭证。治理四件套（§5.8）：
  ① 作用域锁死在 hub（v4 强化：凭证配置在 `mcp.hub.credentials` **专属命名空间**，`mcp.servers` 条目
  无鉴权字段；A 透明转发在配置模型与代码路径两层都不接触鉴权，**A 语义零变化**）；
  ② 首选 env 引用注入（`headers_env`/`token_env`），配置明文要求文件 600 + 装载 WARN；
  ③ 后端凭证值与 hub token 值**永不进轨迹/日志**（沿用 SENSITIVE_HEADERS 脱敏 + token 只记 `label`）；
  ④ AGENTS.md 不变量 #1 修订登记（T8 文档任务）："零接触"约束精确到转发路径，hub 凭证枢纽形态单列治理条款。
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
  servers:                       # v4 形态：三键 url / origin_policy / hub —— 无任何鉴权字段
    github:   { url: https://api.githubcopilot.com/mcp }
    fs:       { url: http://127.0.0.1:9100/mcp }
    debug:    { url: http://127.0.0.1:9200/mcp, hub: false }       # 显式禁用聚合：仅 /mcp/debug 转发
    internal: { url: http://mcp.corp:9000/mcp, origin_policy: upstream }
  hub:                           # 新增可选段；缺省 = hub 不启用
    enabled: true
    auth:                        # v3：客户端侧统一鉴权（缺段 = 不鉴权，与 duct 无鉴权传统一致）
      enabled: true
      tokens:
        - label: laptop                 # trace 审计标识（不涉密）
          token_env: DUCT_HUB_TK_LAPTOP # 首选 env 注入（生成建议：openssl rand -hex 32）
          servers: [github, fs]         # 可选 scope；缺省 = 全部 hub 纳入 server
        - label: ci
          token_env: DUCT_HUB_TK_CI     # "唯一 token" = 长度 1 的列表；多 token 供最小权限
    credentials:                 # v4：hub→后端凭证（hub 专属命名空间；未列出的 server 匿名）
      github:   { headers_env: DUCT_CRED_GITHUB }   # JSON env {"authorization":"Bearer ghp_xxx"}（首选）
      internal: { headers: { x-corp-token: "…" } }  # 内联明文：要求文件 600，装载 WARN 引导用 env
    separator: "__"              # 工具改名分隔符；本期仅支持 "__"
    list_ttl: 300                # 秒；合并快照缓存期（客户端请求触发重取）
    backend_timeout: 15          # 秒；扇出/单后端调用无响应超时（区别于 connect 超时）
    max_tools: 512               # 合并表上限：超限条目跳过 + WARN（防巨表）
    name_max_len: 64             # 改名后工具名上限（LLM function-name 现实约束），超限跳过 + WARN
    session_idle_ttl: 86400      # 秒；hub 会话空闲 GC
    max_sessions: 1024           # 会话表上限；超限拒绝新 initialize + WARN
```

装载规则在 A 的三层语义上追加（仍是条目级容错）：
1. `mcp.hub` / `mcp.hub.auth` / `mcp.hub.credentials` 未知键静默忽略（与 `transport` 预留惯例一致）；`enabled` 缺省 false（缺段 = 不启用）。
2. **per-server 开关（v2 决策 Q9）**：`mcp.servers.<id>` 第三键 `hub: bool`，**默认 true**。
   `hub: false` 只把该 server 摘出聚合面——它的 `/mcp/{id}/*` 透明转发端点**完整保留**。
   这替代了 v1 的 `hub.servers` 子集白名单（默认启用 + 显式禁用，与 `normalize_sse` 的 per-provider 布尔先例同构）。
3. 单射性约束（v2 精化）：server id 含 `separator`（`__`）会破坏前缀反解（§5.4）→ 该条目**自动降级为
   `hub: false` + WARN**，不再整条跳过——透明转发不受牵连，故障半径与开关语义对齐。
4. 端点固定 `/mcphub`：与 server id 命名空间不相交，v1 的 "path 首段保留" 规则**删除**；`path` 键随之移除（不再可配置）。
5. **auth fail-closed（v3）**：`hub.auth.enabled: true` 而 `tokens` 缺省/为空/全部无有效值 →
   **文件级错误拒绝启动**（"开了鉴权却无可用凭证"比无鉴权更危险，不静默放行也不静默锁死）。
6. token 条目：`token_env`（首选，环境变量单值）与 `token`（明文兜底，文件 600 WARN）二取一，缺失 → 条目跳过 + WARN；
   `label` 缺省取序号；`servers` 引用不存在或未被 hub 纳入的 id → 该引用忽略 + WARN。
7. **凭证命名空间（v4）**：`hub.credentials.<id>` 的键必须是 `mcp.servers` 中存在且 `hub: true` 的 id，
   否则该条忽略 + WARN（凭证不会静默配到转发面/不存在的 server 上）；条目内 `headers_env`
   （JSON 环境变量，首选）与 `headers`（内联 map 兜底，文件 600 + WARN）二取一；
   未出现在 `credentials` 中的 server = hub 对其匿名请求（fs/supergateway 场景）。
8. **A 零鉴权契约（v4，硬约束）**：`mcp.servers` 段的解析模型（`McpServerConfig`）**没有鉴权字段**；
   凭证只活在 `HubConfig` 里，`mcp.rs` 转发路径对 `hub` 段零读取——分离发生在类型层，
   无需靠"约定不读"维持；A 既有的 I 组测试（凭证透传断言）保持零修改即为本契约的回归锁。

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
- **鉴权门（v3，先于一切 handler）**：`hub.auth.enabled` 时，**所有** `/mcphub*` 请求（含 `initialize`，
  防匿名会话枚举）过 Bearer token 校验（§5.8.1）；失败 → 401 + `WWW-Authenticate: Bearer error="invalid_token"`，
  同样成对落轨迹（`request/end{rejected}`，只记 label/不记值）。
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

- **M1（默认）：每客户端会话一套后端会话**——理由在 v3 后**不再是凭证问题**（后端凭证来自配置，§5.8.2），
  而是**会话语义隔离**：后端 session 常携带使用态（工作目录、授权上下文、任务态），跨客户端共享会造成串扰。
  凭证入配置的直接收益：后端会话**不再受"客户端请求 in-flight"约束**——可以预建、后台重建、
  keep-warm 与 TTL 到期后台刷新（均为可选增强，一期仍懒建，但实现不再被凭证模型掣肘）。
- M2（演进选项，一期不做）：全局共享后端会话池。v3 后技术可行（凭证已中心化），但会话串扰语义风险仍在；
  若出现「后端为无状态服务账号、连接成本主导」的诉求再评。
- 生命周期：客户端 `DELETE` → 对所有 Ready 后端转发 DELETE、表内删除；`session_idle_ttl` GC
  （tokio 定时器，同 trace writer 的后台任务形态）；`max_sessions` 上限拒绝新握手。
- 后端会话丢失（请求回 404 会话/明确失效）：同请求内**透明重建一次**（重新 initialize，凭证取自配置）重试一次，再失败 →
  JSON-RPC error + trace。
- 会话表条目还挂 **`scope`**（来自鉴权命中的 token 条目）：`{server_id → …}` 的可达集与出页过滤都按 scope 走（§5.4）。

### 5.4 工具改名与反解（碰撞处理）

- 对外名 = `{serverId}{sep}{originalName}`（`github__create_issue`）。
- 反解 = **配置内 server id 最长前缀匹配 `{id}+sep`**，剩余部分即后端原名。
  单射性由 §5.1 规则 3 保证（含 `__` 的 id 自动摘出聚合），无需转义；同 server 内工具名唯一由 spec 保证 →
  合并表天然无碰撞。`tools/call` 的 args 与 result 的 content **不重写名字**（后端看到的永远是原名）。
- 每条合并条目附加 `_meta.hub = {server, original}`（客户端展示友好；spec 允许 `_meta` 自由扩展）；
  若后端未给 `title`，一期不改 `title`，避免侵入语义。
- **入站防御**：客户端直连某后端 url（A 路径）不受影响；hub 仅对**本会话 list 快照之外**的调用名按前缀
  规则现算路由（不依赖缓存命中，容忍 TTL 边界的名字漂移；后端回 -32602 unknown tool 原样中继）。
- **scope 授权（v3）**：合并快照存全量（hub 纳入集），**出页与调用按会话 scope 过滤**——
  list 只回 token 可见 server 的工具；call 反解命中 scope 外 server 时，回与"不存在"完全一致的
  `-32602 tool not found`（不泄露"该工具存在但无权限"的事实，未授权语义收口）。
  scope 缺省 = 全部 hub 纳入 server（默认启用、默认全可见，禁用走 server `hub: false` 或 token `servers:` 显性裁剪）。
- 巨表/超长名：`max_tools`、`name_max_len` 截断跳过 + WARN（LLM function-name 现实 64 字符约束的规避，
  宁可显性降级不静默改名）。

### 5.5 tools/list 聚合流程

```
POST tools/list (hub session)
 ├─ 快照有效（list_ttl 内，含未翻完页）→ 直接对快照出页
 └─ 否则扇出：
     for each 纳入 server（配置序，hub:true；scope 外直接跳过不扇出 §5.4）:
        ensure BackendSession（懒 initialize；凭证取 hub.credentials 配置注入 §5.8.2，应用 origin_policy）
        → tools/list 循环翻页拉平（backend_timeout 约束）
     merge（改名/附 _meta/max_tools/name_max_len 过滤）→ 新快照 {rev, tools[], degraded[]}
     失败后端 → 记 degraded，列表照常返回；全失败 → JSON-RPC internal error + trace
 出页：按会话 scope 过滤（§5.4）后出页；opaque cursor = "rev:offset"，snapshotRev 间页一致（翻页中途不重取）
```

### 5.6 tools/call 转发流程

```
POST tools/call → scope 校验（§5.4）→ 前缀反解 → (server, originalName)
 ├─ ensure BackendSession → 改写 name 后原样重发（客户端 jsonrpc id 不变 → 单后端无需 id 翻译）
 │    转发头组装：客户端头 − 剥离集(SENSITIVE_HEADERS 名单 ∪ 该 server hub.credentials 头名) + 配置凭证注入（§5.8.2）
 │    ※ 客户端自带的鉴权类头**永不到达后端**——凭证置换是本模型的核心语义
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
│   │                      #   （reqwest + mcp.rs 同款头黑名单/origin_policy；
│   │                      #    v3：凭证剥离/注入链在此收口，取 hub.credentials §5.8.2——A 路径无此环节）
│   ├── merge.rs           # 快照、改名/反解、cursor、max_tools/name_max_len、scope 出页过滤
│   ├── auth.rs            # v3：Bearer 提取 + 恒定时间校验 + token 表(scope) + 401 应答
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

### 5.8 鉴权与凭证模型（v3 重划信任边界）

需求锚点：后端 MCP server 大多需要鉴权。A 透明转发让客户端自带凭证（P5 无损）；但聚合面**不能**
要求客户端持有 N 份后端凭证，且后端真实凭证对客户端应当不可见。故 v3 把信任边界切成两段：

```
┌─ 客户端 → hub ─────────────────────────┐   Bearer <hub-token>：hub 签发/配置的统一凭证，带 scope
└─ hub → 后端 ──────────────────────────┘   per-server 配置凭证注入；客户端鉴权头被剥离、永不达后端
```

#### 5.8.1 客户端 → hub：hub token

- **载体**：`Authorization: Bearer <token>`（标准形态；Claude/Cursor 等 remote MCP client 均支持自定义
  headers；将来若接 spec 的 OAuth 2.1 资源服务器链路，Bearer 形态天然兼容）。
- **token 签发两段式**：
  - **P1 静态 token 表（一期实现，已满足"hub 签发唯一鉴权 token"）**：管理员以 `openssl rand -hex 32`
    生成、经 env/配置写入 `mcp.hub.auth.tokens`，随配置分发给客户端——"签发"发生在部署面。
    "唯一"表达为长度 1 的列表；多 token 供**按客户端最小权限**（scope）与**定点吊销**（删条目重启）。
  - **P2 动态签发端点（任务位 T14，一期不做）**：`POST /mcphub/token` 以 bootstrap 凭据换取
    HMAC 自签限时 token（`{sub, scope, exp}`，验证免存储、无状态），补齐吊销时效与过期语义。
- **校验**：hub router 最外层（axum middleware/统一入口函数），先于 initialize（防匿名会话枚举）；
  从 Bearer 提取后与有效 token 表做**恒定时间比较**（自实现 ct-compare 小函数，不引新依赖）；
  无 `Authorization` 头/格式错/不命中 → **401** + `WWW-Authenticate: Bearer error="invalid_token"`，
  成对落轨迹后短路（不进会话表、不建任何后端连接）。
- **fail-closed 自检**：`enabled: true` 但 token 表为空/全无效 → **装载致命**（§5.1 规则 5）。
- **缺段 = 不鉴权**：auth 段缺省或 `enabled: false` 时 hub 与 A/aiproxy 同语义（内网信任边界），
  文档强推 hub 场景显式开启（§6 风险 #12）。
- **审计**：命中的 token `label` 进 trace（`request/start.auth`）；**token 值本身永不落**轨迹/日志。

#### 5.8.2 hub → 后端：配置凭证注入（v4：凭证专属 hub 命名空间）

- **配置形态**：`mcp.hub.credentials.<id>.headers_env`（JSON 环境变量，首选，密钥不落盘——对齐
  `DUCT_USER/DUCT_PASSWD` env 先例与 deploy.md env-file 实践）或 `.headers`（内联 map 兜底，
  要求文件 600 + 装载 WARN）。多 header 注入天然支持（`authorization` + 自定义 key 并存）。
  **v4 裁决（评审指令）**：凭证不放 `mcp.servers.<id>`——`servers` 段保持三键纯形态，
  鉴权整体收进 hub 段（`auth` 管进门、`credentials` 管出门），A 的配置模型里根本没有这个概念。
- **注入与剥离（核心语义，只发生在 hub 的请求组装里）**：
  ① 从客户端头集合中**剥离**：该 server `credentials` 出现的头名 ∪ SENSITIVE_HEADERS 名单
  （`authorization`/`x-api-key`/`api-key`/…）——客户端即便误带后端凭证也不会下传；
  ② 注入配置凭证；③ 其余头仍走黑名单制透传（不变量 #2 在"非凭证段"上继续成立）。
  未列入 `credentials` 的 server = 匿名请求（fs/supergateway 场景零配置）。
- **A 路径零鉴权（契约，v4 成文化）**：`/mcp/{id}/*` 透明转发**不碰鉴权头**——不注入、不剥离、不改写，
  客户端的 `Authorization` 等凭证头仍按黑名单制**原样透传**（A 的既有用法就是客户端自带凭证直连后端，
  "不碰" = 不介入，**不是**代客户端剥离）。实现保障：凭证只存在于 `HubConfig` 类型内，
  `McpServerConfig` 无鉴权字段、`mcp.rs` 对 `hub` 段零读取——类型层面即无耦合通道；
  A 的既有 I 组测试（凭证字节透传断言，tests/mcp.rs）零修改通过即回归锁（J7）。
- **凭证获取时机解锁**：后端 initialize/重建不再要求客户端 in-flight（凭证来自配置）→
  预建/keep-warm/后台 list 刷新成为可选增强（一期仍懒建，代码结构不为凭证模型让路）。

#### 5.8.3 治理与安全（P5 破例的四件套）

- **作用域**：保管/注入凭证只存在于 hub 段（`hub.credentials`）与 hub 开关（`mcp.hub.enabled`）内；
  A 的 `McpServerConfig` 类型无鉴权字段，aiproxy 不受波及（v4 类型层锁死）。
- **落盘面**：env 引用首选；内联明文 → 装载 WARN + deploy.md「600 权限 + env-file」指引；
  token 与凭证值**一律不进轨迹**（Authorization 类已被 SENSITIVE_HEADERS 覆盖；若未来启用自定义
  token 头名，须同步加入该名单——写进实现验收）。
- **吊销与轮换**：P1 = 删 token 条目/换 env 重启（重启即全量吊销，粗但可靠）；后端凭证轮换 = 换 env 重启。
  P2 限时 token 补齐分钟级吊销（T14）。
- **DoS/暴力**：一期鉴权失败只计数 + WARN 日志（trace `rejected`），**无限速**（轻量定位）；
  防爆破限速/backoff 列二期（T15）；叠加闸门仍是五道：`max_tools`/`name_max_len`/`max_sessions`/`backend_timeout`/`list_ttl`。
- **会话 id 纪律不变**：hub 对外 sid 随机 uuid 不可猜；后端 sid 永不外泄、不进轨迹（沿用 A 判定）。

### 5.9 错误模型与降级

| 场景 | 对外行为 |
|---|---|
| 鉴权失败（缺头/坏格式/不命中，v3） | HTTP 401 + `WWW-Authenticate: Bearer error="invalid_token"` + trace `rejected`（不建会话、不触后端，§5.8.1） |
| 调用 scope 外工具（v3） | `-32602 tool not found`——与真不存在不可区分（§5.4 未授权收口） |
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
| `request/start` | `branch:"mcp-hub"`, method(path=/mcphub), `auth:"<token label>"`（**不记 token 值**）, hub_sid **不记值**（同 sid 不入 SAFE 名单原则）, `init`/`list`/`call{server}` 语义摘要 |
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
| 12 | 聚合面安全暴露 | 一把钥匙 N 个可执行后端 | v3 主答案 = `hub.auth` 统一 token（部署文档强推开启）；叠加 per-server `hub:false` 显性摘除 + 内网边界（§5.8.1） |
| 15 | 后端凭证集中保管（v3，P5 破例的固有风险） | hub 成为凭证单点蜜罐：一泄全泄 | env 引用首选/明文 600；trace/日志零落值；重启即吊销（P1）+ P2 限时 token；内网边界不变 |
| 16 | P1 静态 token 无过期、可重放 | 网络窃听=长期凭证泄露 | duct 监听 HTTP——传输加密属部署面（反代 TLS/SSH 隧道，deploy.md 增补）；P2 限时 token（T14）补时效 |
| 17 | token 暴力猜测 | 32B 随机 token 熵足够；无限速仍留审计 | 失败计数 + WARN（trace `rejected`）；限速/backoff 列 T15；scope 限制得手半径 |
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

鉴权（v3 新增 K 组）：
- K1 auth 开启：无头/错 token → 401 + `WWW-Authenticate`，**零后端触达**（mock 断言连接数为 0）、
  轨迹成对 `rejected`；对 token → 全链路通；auth 缺省 → 匿名直通（默认语义回归）；
- K2 scope：token-A 仅 `[github]` → list 只见 github 工具；直呼 `fs__x` → 与不存在工具**同文**的 -32602
  （错误消息不区分断言）；label 进 trace、token 值全文扫描不出现；
- K3 凭证置换（hub 面）：后端 mock 断言收到的 `authorization` == `hub.credentials` 配置值；客户端自带
  `authorization` 被剥离；自定义凭证头（x-corp-token）同断言；`credentials` 未列出的 server 匿名可达。
  **A 面对照断言**：同 server 经 `/mcp/{id}/*` 转发时客户端 `authorization` **原样透传**、无注入
  （"透明转发不碰鉴权头"的两侧回归锁——v4 契约测试）；
- K4 装载自检：`auth.enabled` + 空 token 表 → 致命；`credentials.<id>` 引用未知/未纳入 id → 忽略 + WARN；
  `headers_env` 指向缺失变量 → 该 server 聚合降级 + WARN；
- K5 剥离集：客户端带任意 SENSITIVE_HEADERS 名单头 → 后端均不见。

---

## 8. 实施任务拆解（评审通过后）

| 任务 | 内容 | 验收 |
|---|---|---|
| T0 | spike：mock Streamable-HTTP 后端上跑通 hub 侧完整 initialize 握手 + list/call 骨架（验证 §3 假设、定帧扫描器形态） | 原型可演示 |
| T1 | config.rs：`mcp.hub` 段（含 **v3 `auth` 子段 + v4 `credentials` 命名空间**）+ HubConfig；`McpServerConfig` 保持三键（零鉴权字段，v4 契约）；per-server `hub: bool`（默认 true）+ 单射性降级 + **fail-closed 自检（规则 5）** | H1-装载项、K4 |
| T2 | `mcp_hub/frames.rs` + `backend.rs`：envelope/SSE 帧扫描器 + MCP 后端 client（复用 §5.11 底座抽取；**凭证注入/剥离链 §5.8.2 在 client 侧收口**） | H4、K3、T0 语义回归为测试 |
| T3 | `mcp_hub/session.rs`：双层会话表 + 懒建 + 重建 + GC + 上限（会话挂 scope） | H3 |
| T4 | `mcp_hub/merge.rs`：快照/改名/反解/**scope 出页过滤**/分页/degraded（含 per-server 开关过滤） | H1 H2 |
| T5 | `mcp_hub/mod.rs` handlers：initialize 状态机 / tools/list / tools/call / DELETE / 405/400 边角 / disabled 兜底 + 错误模型（§5.9） | J1–J5 |
| TA | **（v3）鉴权层**：Bearer 提取 + 恒定时间校验 + 401/WWW-Authenticate + trace 短路 + scope 装配（§5.8.1） | K1–K2、K5 |
| T6 | server 侧接线：分流序 3 `mcphub` 臂（六→七分支）+ `run_with_all_from_listener` + main.rs 三 state 装配 | J7 |
| T7 | 轨迹接入 + 脱敏扫描（含 **token/凭证值零落盘断言**） | J8、K2 |
| T8 | 文档：README（新特性/配置含 per-server 开关与 auth/token 生成姿势/安全警示/测试计数/**七分支分流图**）、AGENTS.md（模块表/分流图/**不变量 #1、#3 破例登记**/hub 契约）、deploy.md（单实例声明 + **TLS/反代前置与 env-file 凭证注入指引**） | 评审 |
| T9 | clippy + fmt 门禁 | CI |

（二期任务位：T10 GET fan-in 通知流 + listChanged；T11 resources/prompts 聚合；T12 反向请求中继 id 翻译；
T13 后端响应字节帽/可逆短名；**T14 动态签发端点 `POST /mcphub/token`（HMAC 限时 token，§5.8.1 P2）**；
**T15 鉴权失败限速/backoff（§5.8.3）**；设计增量届时补 §9。）

规模预估：一期 ≈ 1700–2400 行实现 + ≈ 1200–1600 行测试（帧扫描器/会话表/envelope 三块是主体；
v3 鉴权层 ≈ +150–250 行——Bearer 提取/恒定时间比较/scope 装配都薄，成本大头在配置形态与断言）。

---

## 9. 决策记录

| # | 问题 | 决议 |
|---|---|---|
| Q1 | 形态：duct 内建（A）vs sidecar（B）vs 开源（C） | ✅ **评审已定：内建**——`mcp_hub/` 模块，opt-in `mcp.hub.enabled` 默认关；「懂协议」爆炸半径关进门控模块内 |
| Q2 | 端点路径 | ✅ **评审已定：顶层固定 `/mcphub`**——与 `/mcp/*` 转发彻底隔离；分流六→七分支；`path` 不可配、"hub 保留 id" 规则删除 |
| Q3 | 命名方案 | 恒前缀 `{server}__{tool}` + `_meta.hub` 附原值；sep 固定 `__`。v2 精化：id 含 `__` 时**自动摘出聚合 + WARN**（不再整条跳过，转发保留）——待复核 |
| Q4 | 会话模型 | M1 每客户端一套后端会话——v3 修正论据：凭证已入配置，M1 不再以"P5 瞬时性"为前提，理由收敛为**会话语义隔离**；M2 池化技术上解锁、语义风险仍在，维持不做——待复核 |
| Q5 | 一期反向请求处置 | 帧扫描器吞帧 + trace/WARN 的「断流降级」；若嫌粗，一期 tools/call 仅接 JSON 响应、SSE 响应整包按 A 透传（放弃 progress 识别）——**推荐前者**，待复核 |
| Q6 | capabilities 对外声明 | v1 `{tools:{listChanged:false}}`；resources/prompts/补全类一律不声明（诚实暴露）——待复核 |
| Q7 | 单实例假设 | 接受，文档显性声明（duct 定位即单二进制）——待复核 |
| Q8 | 规范复核 | 联网通道恢复后先复核 2025-06-18+ 修订（尤其授权/OAuth 条款与 notifications 语义）再开工 T0——沿用 forwarding Q6 未决；**v3 后权重再升：spec OAuth 框架若落地，P1 静态 Bearer 需对齐 resource server 元数据形态** |
| Q9 | 聚合纳入粒度 | ✅ **评审已定：`mcp.servers.<id>.hub: bool`，默认启用**；`false` 仅摘出聚合面、透明转发保留（v1 `hub.servers` 白名单作废） |
| Q10 | 凭证集中保管破例（v3） | ✅ **需求已定**：后端鉴权统一配置在 hub（v4：**专属命名空间 `mcp.hub.credentials`**，`servers` 段无鉴权字段），A 路径零接触 + 零感知（不碰鉴权头：不注不剥，客户端头照旧透传）；治理四件套见 §2.3/§5.8.3，AGENTS.md #1 修订登记进 T8 |
| Q11 | 客户端凭证形态（v3） | ✅ **需求已定**：hub 签发唯一 token 替代原 mcp server 鉴权头；实现取 `Authorization: Bearer`（§5.8.1）；客户端凭证**不向后端透传**（剥离+置换，§5.8.2） |
| Q12 | token 签发机制 | 建议：**P1 静态 token 表**（env 注入、label 审计、per-token scope、fail-closed）即满足需求；P2 `POST /mcphub/token` HMAC 限时 token 列 T14——待复核 |
| Q13 | 凭证配置形状 | ✅ **v4 评审已定**：`mcp.hub.credentials.<id>.{headers_env|headers}`（hub 专属命名空间；`headers_env` JSON env 首选，内联兜底 600 WARN）——`/mcp/*` 转发配置面不存在鉴权概念 |
| Q14 | 鉴权默认姿态 | 建议：`hub.auth` 缺段 = 不鉴权（duct opt-in 传统）；但 README/deploy **强推 hub 场景开启**（聚合面即执行面）——待复核 |
