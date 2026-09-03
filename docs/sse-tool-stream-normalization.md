# duct SSE 工具调用流归一化（兼容层）设计方案

> 状态: 已实现（duct 代码 + 单测 + 集成测试，见 §7）
> 定位: duct `aiproxy` 的**增量兼容层**设计，不改动上行 provider、不改动下游消费者。
> 关联: 设计文档 v3.2 的 §6.1–§6.4（aiproxy 转发语义）、§6.5（错误）、§9（测试）。
> 触发: `providers.<id>.normalize_sse: true`（默认 false）。开启后执行两类归一化：
>   1) **请求侧（新增）**：检测请求 JSON 是否携带 `stream`；缺失则显式注入
>      `"stream": false`，规避「把缺 stream 当默认流式」的网关（如 kso），
>      使非流式请求拿到合规 JSON 而非 `text/event-stream`。
>   2) **响应侧（原方案）**：对 SSE 流式工具调用**重复下发完整 `function.name`** 做归一化。
> 范围: 见 §2（目标 / 非目标）。本文件同时是这两类兼容的来源文档。

---

## 1. 背景与问题

### 1.1 现象

上游（已实测 `kso` 网关的 `ali/qwen3.8-flash`）在 SSE 流式返回工具调用时，**同一个 tool call 的 `function.name` 在每个 chunk 里都被重复下发完整值**：

```
{"delta":{"tool_calls":[{"function":{"name":"list_dir","arguments":""},  "index":0}]}}
{"delta":{"tool_calls":[{"function":{"name":"list_dir","arguments":"{\"path\": "},"index":0}]}}
{"delta":{"tool_calls":[{"function":{"name":"list_dir","arguments":"\"/"},"index":0}]}}
...
{"usage":{...},"finish_reason":"tool_calls"}
```

OpenAI 流式协议规定：`function.name` **只应在一个 tool call 的首个 `delta.tool_calls[]` 出现一次**，后续 chunk 只携带 `arguments` 片段。kso 把完整 name 重复下发是**协议违规**。

### 1.2 为什么会把消费者搞坏

下游消费端对 name 的处理分两种（实测与源码对照）：

| 消费端 | name 处理 | 结果（对 kso 重发） |
|---|---|---|
| DeepSeek Harness（DSH） | **覆盖赋值 `=`**（`assembler.ts:73`、`llm-deepseek/src/translate.ts:170`） | `block.name = 'list_dir'` 恒等覆盖 6 次 → 仍是 `list_dir` ✅ |
| OpenViking | **字符串累加 `+=`**（`bot/vikingbot/providers/base.py:71`） | `'list_dir' * 6` → `list_dirlist_dir…` ❌ |
| Octop | **累加**（`langchain_core/utils/_merge.py:65`、`stream_project.py:174`、`acp/server.py:106`） | 同左，且 LangChain 合并后**执行工具的 `tool_calls` 也被污染** ❌ |

即：**凡是把 name 当"片段"做累加的消费端，都会被"重发完整 name"的上游污染**；凡是把 name 当"完整值"做覆盖的消费端（DSH），则天然免疫。

所以根因是**上游协议违规 + 消费端累加语义**这一对组合。duct 作为中间网关，可以在"上游 → 消费端"这一段，把违规流**改写成合规流**，从而让 OpenViking/Octop 这类累加型消费端也拿到正确输入。

### 1.3 为什么交给 duct 而不是各消费端

- 一处修复，覆盖所有下游（OpenViking / Octop / 未来任何累加型客户端）。DSH 不需要。
- 不改上游、不改下游，只在一个可独立部署的网关里做**透明改写**。
- duct 已有 `/aiproxy/{provider}/*` 的路由与 SSE 流式透传底座，接入成本低。
- 符合 duct 现有 P5（凭证零接触、不注入 Key）、P3/P4（字节流式、路径一次切分）语义。

---

## 2. 目标 / 非目标

### 2.1 目标

1. **请求侧（新增）**：对 `normalize_sse` 的 provider，自动检测请求 JSON 的 `stream` 字段；缺失则显式注入 `"stream": false`，让「缺 stream 即流式」的网关对非流式请求返回合规 JSON。
2. 在 `aiproxy` 响应流上，把"每个 chunk 重复下发完整 `function.name`"改写为标准 OpenAI SSE：**每个 tool-call index 只保留首帧的 name，后续帧去掉 name**。
3. 对所有其他字段（`id`、`arguments` 片段、`reasoning_content`、`content`）**原样透传**，不重新解释、不重排。
4. 兼容性改动**默认关闭、按 provider 显式开启**（`providers.<id>.normalize_sse: true`），未开启的 provider 行为与今天**逐字节一致**。
5. 对合规上游（name 只在首帧出现）开启归一化后**输出不变**（幂等）；对已带 `stream:true`/`stream:false` 的请求**不改写**。
6. 保持流式、低延迟、低内存，不整包缓冲 SSE。

### 2.2 非目标

- **不**对非流式响应做 **SSE→JSON 整体聚合**——本次只做「请求侧 `stream` 注入 + 响应侧工具名归一化」。若某网关连显式 `stream:false` 都无视，其「非流式强制 SSE」的聚合兜底不在本次范围内。
- **不**修复消费端的其它放大器（如 Octop `stream_project.py` 跨轮 `tool_name_buf` 泄漏、`PatchToolCallsMiddleware` 重放去重缺失）。这些是消费端自身 bug，与本方案正交；方案只负责把"上游喂给消费端的流"修正为合规。
- **不**解析/校验 `arguments` JSON 是否合法；保持原样字符串。
- **不**改写 `id`（kso 也重复下发 id，但 id 是常量，无害；为最小改动暂不处理）。

---

## 3. 总体设计

在 `aiproxy` 的响应回传路径插入一个**可选的 SSE 归一化适配器（`SseToolNormalizer`）**。当 provider 开启归一化且上游响应为 `text/event-stream` 时，把 `upstream.bytes_stream()` 包上该适配器；否则维持现有 `Body::from_stream(upstream.bytes_stream())` 逐字节透传。

```
client(sse)
   │  POST /aiproxy/{provider}/chat/completions  (stream:true)
   ▼
respond_forwarded()
   ├─ 上游返回 content-type: text/event-stream
   │    ├─ provider.normalize == true  →  SseToolNormalizer  → 客户端
   │    └─ provider.normalize == false →  现有 bytes_stream 透传
   └─ 上游返回非 SSE（application/json 等）
        └─ 现有 bytes_stream 透传（不归一化）
```

改动点收敛在：

- `src/config.rs`：`ProviderConfig` 增加 `normalize: bool`；YAML 装载逻辑读取 `normalize` 字段。
- `src/aiproxy.rs`：`respond_forwarded()` 在构造响应 body 前，按 provider 配置与响应头决定是否包一层归一化流。
- 新文件 `src/sse_normalize.rs`：行切分器 + 工具名归一化逻辑 + 单测。
- `src/lib.rs`：导出新模块。

---

## 4. 详细设计

### 4.1 配置与结构体

`src/config.rs`：

```rust
#[derive(Debug, Clone)]
pub struct ProviderConfig {
    pub id: String,
    pub base_url: String,
    /// 对上游做 SSE 流兼容归一化。默认 false；开启后：
    ///   1) 请求侧：body 为 JSON 对象且缺 `stream` 时注入 `"stream": false`；
    ///   2) 响应侧：上游为 text/event-stream 时，对"重发完整 name"的工具调用改写为合规流。
    pub normalize_sse: bool,
}
```

YAML 样例：

```yaml
providers:
  kso:
    url: https://ai-kas.kso.net/codeplan/v1
    normalize_sse: true      # 新增，可选，默认 false
  volcengine:
    url: https://ark.cn-beijing.volces.com/api/plan/v3
```

装载逻辑改动（`parse_str`，`src/config.rs`）：

```rust
let normalize_sse = entry
    .get("normalize_sse")
    .and_then(|v| v.as_bool())
    .unwrap_or(false);
index.insert(id.clone(), providers.len());
providers.push(ProviderConfig { id, base_url, normalize_sse });
```

语义保持 v3.2 §6.2 三层装载：未知字段不报错、`normalize` 非布尔则视为 false 且（可选）WARN；条目的 `url` 缺失仍跳过。

### 4.2 响应回传路径改动

`src/aiproxy.rs::respond_forwarded`（现 `src/aiproxy.rs:263-270`）：

```rust
let upstream_ct = upstream
    .headers()
    .get(axum::http::header::CONTENT_TYPE)
    .and_then(|v| v.to_str().ok())
    .unwrap_or("")
    .to_string();

let is_sse = upstream_ct.starts_with("text/event-stream");
let do_normalize = provider_normalize && is_sse;   // provider_normalize 从 config.get(provider_id) 取

let stream = upstream.bytes_stream();
let body = if do_normalize {
    Body::from_stream(SseToolNormalizer::new(stream))
} else {
    Body::from_stream(stream)                        // 既有行为
};
```

> 注意：`respond_forwarded` 目前入参没有 provider 归一化标志，需要从 `uri.path()` 切出的 provider id（`src/aiproxy.rs:206` 已取 `provider_id_log`）查 `state.config.get(id)` 得到 `normalize`。若查不到（理论不可达，路由已保证），视为 `false`。

### 4.3 新模块 `src/sse_normalize.rs`

核心是一个 `Stream` 适配器：逐字节缓冲 → 按 `\n` 切出完整行 → 对 `data: <json>` 行做工具名归一化 → 重新拼回 `data: <json>\n`。

```rust
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

/// 把上行 SSE 流改写为工具名合规的 SSE 流。
pub struct SseToolNormalizer<S> {
    inner: S,
    pending: Vec<u8>,
    seen_names: std::collections::HashMap<usize, String>, // tool-call index -> 已捕获的完整 name
}

impl<S> SseToolNormalizer<S> {
    pub fn new(inner: S) -> Self {
        Self { inner, pending: Vec::new(), seen_names: std::collections::HashMap::new() }
    }
}

impl<S> Stream for SseToolNormalizer<S>
where
    S: Stream<Item = Result<Bytes, axum::Error>> + Unpin,
{
    type Item = Result<Bytes, axum::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        // 1) 尽取 inner 数据，切行处理，产出已归一化的行
        // 2) 每行要么产生一个 Bytes，要么并入 pending 等待下一行
        // 3) 流结束后 flush 残余 pending
        todo!()  // 语义见 §4.3.2 / §4.4
    }
}
```

#### 4.3.1 行切分

SSE 的 `data:` 行以 `\n` 结尾（实际以 `\n\n` 分隔事件，但 chat completion 每帧是单行 `data: {...}` + `\n`）。切分策略：在 `pending` 里找下一个 `\n` 字节；找不到则继续等待累积（处理**一行被拆到多个 TCP chunk**的情况，这是必须的）。找到后把这段（含 `\n`）作为一个完整单元处理并出队。

> 若上游用了 `\r\n`，则先归一化为 `\n` 处理；为保持字节一致，若检测到整段都是 `\r\n` 则输出时回填 `\r\n`。当前所有 OpenAI 兼容实现均为 LF，本地 mock 用 LF。

#### 4.3.2 工具名归一化语义（对每个 `data:` JSON 行）

对 `choices[].delta.tool_calls[]` 逐个处理。每个 tool call 以 `index` 为键，维护**首次捕获的完整 name**（`seen_names[index]`）。name（非空）按以下规则处理：

| 输入 name（相对该 index 已捕获 name） | 含义 | 输出 |
|---|---|---|
| （该 index 首次出现 name） | 首个完整 name | 保留 `name`，记 `seen_names = name` |
| `name == seen` | 重发完整名（kso） | **删除 `function.name`** |
| `name.startswith(seen)` 且更长 | 增长式重发（前缀补齐） | 保留（更完整），`seen = name` |
| `seen.startswith(name)` 且更短 | 冗余前缀 | 删除 `function.name` |
| 其它（不相等也不互为前后缀） | 真片段续写 | 保留并拼入：`seen += name`，输出 `name = seen` |

> 语义上等价于"**name 取首值但容忍重复/增长/真片段**"。对 kso（重发完整名）退化为"保留首帧、删重复"，即 DSH 的等价行为；对未来可能"把 name 拆成片段"的上游同样健壮（不会像纯覆盖那样只留末段）。

对每个 chunk，**只改 `function.name` 的取舍**，其余（`id`、`type`、`function.arguments`、`index`）原样。若删掉 name 后 `function` 仍含 `arguments`，则 `function` 保留；若删掉 name 后 `function` 变空对象且无 `arguments`，则把 `function` 也一并删除（最小化失真）。

#### 4.3.3 输出字节

- 工具行：重新 `serde_json::to_string` 序列化改后对象，输出 `data: {json}\n`。
- 非工具行：`data: [DONE]`、`data: {…无 tool_calls}`、`: keepalive`、空行、纯文本行——**原样输出**。
- 解析失败（非 JSON 或 JSON 缺字段）的 `data:` 行：**原样输出**，仅 `tracing::debug` 记录，绝不中断流、绝不报错。

### 4.4 生命周期与内存

- 不整包缓冲：`pending` 仅保留"当前未完成行"的字节，上限受单行长度约束（SSE 单行 JSON 通常 < 几十 KB）。可设一个 `MAX_LINE` 防御上限，超过则原样透传该行并清空 `seen`（降级，不抛错）。
- `seen_names` 按 index 数增长；工具调用数量有限，内存可忽略。
- 流结束后 `flush` 残余 `pending`（若上游没发尾随 `\n`）。
- 错误传播：`inner` 的 `Err` 原样上抛，不吞。

### 4.5 与既有语义的一致性

- P3 请求体流式、P4 响应逐 chunk 流式：不变。
- P5 凭证零接触：不触碰请求头、不注入 Key，仅改响应 body 内容。
- 响应头：`content-length` 本就被 `RESPONSE_HEADER_BLACKLIST` 剥离（`src/aiproxy.rs:66-74`），Body 由流式框架重建，改写不影响长度一致性。
- `content-type` 保持上游的 `text/event-stream`。

---

## 4.6 请求侧：stream 字段归一化（本次新增）

`normalize_stream_field`（`src/sse_normalize.rs`）在 `respond_forwarded` 中，当 provider 开启
`normalize_sse` 且请求携带 body 时，读取 body（受 `--max-body` 约束）并注入：

| 请求 body | 处理 | 输出 |
|---|---|---|
| JSON 对象且缺 `stream` | 注入 `"stream": false` | 让「缺 stream 即流式」的网关返回合规 JSON |
| 已带 `"stream": true` | 不改写 | 保持流式请求语义 |
| 已带 `"stream": false` | 不改写 | 幂等 |
| 非 JSON 对象 / 解析失败 | 原样 | 透传 |

实现要点：
- 仅当 `body` 首字符为 `{` 且能解析为 JSON 对象时才处理；否则原样透传。
- 注入后重新序列化并转发；请求 `content-length` 由 reqwest 按新的 body 重建（aiproxy 本就剥离
  请求 `content-length`，见 `REQUEST_HEADER_BLACKLIST`）。
- 仅在 `normalize_sse = true` 时执行；未开启仍走 `limited_body` 逐字节流式透传（保持 P3）。

---

## 5. 边界与兼容性清单

用一张表覆盖关键成败路径（开发与测试都以此为准）：

| 输入 | 归一化开启 | 期望输出 |
|---|---|---|
| 合规流（name 仅首帧） | 是 | 字节不变（幂等） |
| kso 重发流（name 每帧） | 是 | 仅首帧含 name |
| kso 重发流 | **否（默认）** | 逐字节透传（今天的行为） |
| `data: [DONE]` | 是/否 | 原样 |
| `: ping` / 空行 | 是/否 | 原样 |
| 非 SSE（`application/json` 单响应） | 是 | 透传，不解析 |
| 一行被拆到多个 TCP chunk | 是 | 合并后正确切行处理 |
| `data:` 行 JSON 解析失败 | 是 | 原样透传，不中断 |
| 并行多工具（index 0/1） | 是 | 各自独立：各自只保留首帧 name |
| 同一 index 跨事件复用 | 是 | 以"已完成 + 新 tool-call"判定；详见 6.1 风险 |
| 流中途上游断开 `Err` | 是 | 原样上抛 |

---

## 6. 风险与开放问题

### 6.1 同一 index 的复用

chat completion 中，一个 tool-call index 在 `finish_reason="tool_calls"` 后不会再增长；但极少数网关可能复用 index。方案：在收到 `finish_reason`（任意 choice）或 `data: [DONE]` 后，**清空 `seen_names`**，使下一轮的 index 重新计数。这样即便复用 index 也不会错误地删掉新一轮的首帧 name。

### 6.2 是否需要同时归一化 `id`

kso 也重复下发 `id`。`id` 是常量，累加/覆盖下游都不会错（消费端对 `id` 取首值）。为最小改动，本次不处理；若未来发现某消费端对 `id` 也有 `+=` 缺陷，可复用同一机制（规则类似，但 `id` 通常是"首值优先、重复删除"）。

### 6.3 只覆盖"重发"这一类

本方案面向**协议违规类**（重发完整 name / 片段续写）。若上游本身把工具名**生成成重复乱码**（模型退化型重复，如 `exeexe`），本方案不会把它还原成 `exec`——那不是网关该做的语义修复。此类仍属模型/消费端问题，超出本方案范围（文档中已声明为非目标）。

### 6.4 性能

每行一次 `serde_json` 解析仅在**含 `tool_calls` 的行**发生；普通文本/思考流仍是透传。SSE 吞吐下开销可忽略。可在开启归一化的 provider 上加一个可选观测，监控改写是否触发。

### 6.5 后端是否有其它兼容字段需要透传

`reasoning_content`、`usage`、`finish_reason` 等**一律透传**，不改写。仅 `delta.tool_calls[].function.name` 落入归一化逻辑。

---

## 7. 测试计划

### 7.1 单元测试（`src/sse_normalize.rs`，`#[cfg(test)]`）

- `resend_full_name_is_collapsed`：喂 kso 风格 6 帧（每帧都带 `name:"list_dir"`），断言输出仅首帧含 name，`arguments` 片段完整拼接。
- `compliant_stream_passes_through`：喂"name 仅首帧"的合规流，断言输出与输入**字节一致**。
- `growing_prefix_and_fragment_continuation`：分别喂"增长式"与"真片段"name 流，断言得到完整合并名。
- `parallel_tool_calls_independent`：index 0/1 并行，各自只保留首帧 name。
- `malformed_data_line_passthrough`：某 `data:` 行非 JSON/缺字段，断言原样透传、不中断。
- `line_split_across_chunks`：把一行拆成多个 `Bytes` 喂入，断言正确复原。
- `done_and_finish_reset_seen_names`：`[DONE]`/`finish_reason` 后，index 复用不被误删。
- `keepalive_and_empty_lines_passthrough`。

### 7.2 集成测试（`tests/aiproxy.rs`）

现有 harness 已提供真实 TCP 回环 mock 上游与 `Behavior::Sse { chunks, delay_ms }`（`tests/aiproxy.rs:60-70`）。扩展：

- 新增 mock 行为 `SseRepeatedName`：发出 kso 风格流（每帧带完整 name）。
- `normalize_on_repeated_name_upstream`：配置 `providers.<id>.normalize: true`，经 `router()` 打完整 axum 栈打一次，断言客户端收到的流**首帧含 name、后续帧无 name**。
- `normalize_off_is_byte_identical`：同上游、`normalize: false`，断言客户端收到的字节与 mock 上游**完全一致**。
- `normalize_compliant_upstream_unchanged`：合规上游 + `normalize: true`，断言输出不变。

### 7.3 端到端验证（可选）

用 `kso` 真实上游 + `ali/qwen3.8-flash`，开 `normalize: true`，把 Octop 的 `base_url` 指向 duct `/aiproxy/kso`，确认聊天不再出现 `list_dirlist_dir…`、工具能正常执行。属于发布前验证，不在 CI。

---

## 8. 变更清单（预估）

| 文件 | 变更 | 影响 |
|---|---|---|
| `src/config.rs` | `ProviderConfig` 加 `normalize_sse`；`parse_str` 读该字段 | 默认 false，向后兼容 |
| `src/sse_normalize.rs` | 新增：`normalize_stream_field`（请求侧 stream 注入）+ `SseToolNormalizer`（响应侧工具名归一化）+ 单测 | 无 |
| `src/aiproxy.rs` | `respond_forwarded` 按 `provider.normalize_sse` 分流：请求侧读 body 并注入 `stream:false`；响应侧 SSE 包 `SseToolNormalizer` | 关闭时行为不变 |
| `src/lib.rs` | 导出 `sse_normalize` | 无 |
| `docs/deploy.md` | 补 `normalize_sse` 字段说明 | 文档 |
| `tests/aiproxy.rs` | 新增 `Behavior::SseRepeatedName` 与 4 个集成用例 | 无 |

---

## 9. 评审要点

1. 归一化语义是否同意采用"首值优先 + 容忍重复/增长/真片段"的合并规则（§4.3.2 表），而不是只做"删重复"。
2. 是否需要处理 `id` 重复（§6.2）。
3. 是否按 provider 粒度开启（推荐），还是全局开关。
4. `MAX_LINE` 防御上限的取值与超限降级策略是否可接受。
5. 该方案与消费端自身放大器（非目标 §2.2）之间的边界是否清晰。
