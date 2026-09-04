# aiproxy 请求轨迹（trace）

为「AI 大模型流量转发」提供逐请求的事件轨迹，替代改造前仅有一行
`aiproxy forwarded` 的日志形态。设计参考 deepseek-harness 的会话轨迹
（`SessionEvent` 事件溯源 + `SessionTelemetryRecord` 的 severity/attributes/body +
崩溃时合成 `turn/end{interrupted}` 的恢复语义），并按 duct 的转发场景重排事件词表。

## 落地形态

- **一行一事件的 JSONL**，append-only。信封：

  ```json
  {"v":1,"time":1788486741625,"trace":"1a06a245fcf-0","seq":0,"type":"request/start","severity":"info","data":{...}}
  ```

  | 字段 | 含义 |
  |---|---|
  | `v` | 轨迹格式版本（读侧遇到更高版本应拒读而非猜读） |
  | `time` | Unix epoch 毫秒 |
  | `trace` | 单请求关联 id（毫秒时间戳+计数，字典序即时间序） |
  | `seq` | 轨迹内从 0 连续递增 |
  | `type` | 事件类型（下表） |
  | `severity` | info / warn / error，发射时预映射，接收端零配置可告警 |
  | `data` | 事件负载（派生事实，绝不含正文内容） |

- 开关：`--trace-file <path>`，默认取 XDG 状态目录 `$XDG_STATE_HOME/duct/trace.jsonl`
  （未设则 `~/.local/state/duct/trace.jsonl`，再退当前目录）；
  传空串关闭文件 sink。文件不可写时进程**不致命**，WARN 后降级为 tracing 输出。
  选 state 而非 share：轨迹属操作状态/日志类（XDG 2021 的 state 分类），且 systemd
  `StateDirectory=` 恰好把 `~/.local/state/<name>` 作为 `ProtectHome` 下的写例外挂回，
  share 无对应机制。
- **缺失自动创建**：指定路径不存在时，启动即创建文件连同整条父目录链；
  运行期被外部 `rm`（含连目录一起删）或被 logrotate `create`/`move` 换走 inode 后，
  writer 线程在下一条记录前比对 dev/inode 自动重建重开（`copytruncate` 同 inode 不误触发，
  两种轮转模式均兼容）。自愈是按记录驱动的：文件消失但无新请求时不预建空文件。
- 内容采集开关：`--trace-body <BYTES>`（默认 0=关闭）。>0 时轨迹额外记录请求体与
  响应流的头部快照（`request/body.req_content_head`、`request/end.resp_content_head`，
  各截断至该字节预算）——**prompt 与补全内容将落盘**，轨迹文件随之成为敏感数据
  （建议 600 权限 + 更紧的 logrotate 保留期）。采集开启时 duct 向上游协商
  `Accept-Encoding: identity`（仅改请求侧压缩协商，响应字节仍原样透传给客户端），
  这同时修复了压缩网关上 `normalize_sse` 行级改写空转的问题。
- **压缩流观察解码**：上游回 `content-encoding: gzip/deflate/br` 时（kso 即如此，且无视 identity 协商），
  透传语义下不解帧，但 writer 侧对可识别编码做**观察式解码**（独立于透传路径，
  输入上限 4 MiB）：SSE 事实（usage/finish_reason/done）与内容快照从解码字节
  提取，记 `sse.encoded:true, sse.decoded:true`；未知算法（如 zstd）/超限/解码错
  时保留 `encoded` 标记并按采集开关记 `resp_content_skipped`。
- 双通道：文件 sink 开启时 canonical 行落文件，warn/error 级同时回落 tracing
  （便于 journald 告警）；关闭时全部事件以 JSONL 行走 tracing
  （target `duct::trace`）。
- **崩溃/断连收尾**：响应流被完整消费、流中出错、或一节未消费即被 Drop，
  分别补发 `request/end{completed|stream_error|interrupted}`；进程崩溃导致的
  残缺以「末行缺 `request/end`」识别。
- 旁路定位：轨迹经有界通道交给独立写线程，缓冲满即丢行并计数——观测永不反噬转发热路径。

## 事件词表

| type | 触发时机 | 关键字段 |
|---|---|---|
| `request/start` | 入口分流命中 aiproxy 后第一行 | `method` `path` `query_keys` `provider` `normalize_sse` `request_headers`；未注册 provider 时另有 `known:false` + `available` |
| `request/body` | 请求体事实。流式路径由前缀扫描旁路产生（发送期，故在 `upstream/request` **之后**）；normalize 路径为全量解析（在之前） | `model` `stream`（值为字符串 `"true"/"false"`）`bytes` `parse:"prefix"|"full"`；`full` 时另有 `n_messages` `n_tools` `last_role` `max_tokens` 等；`--trace-body` 开启时另有 `req_content_head` |
| `upstream/request` | 上游请求确定后 | `url`（剥离 userinfo 与 query 值，只留参数名） |
| `upstream/response` | 上游响应头到达 | `status` `ttfb_ms` `response_headers` |
| `upstream/error` | 上游连接失败/超时/超限中途截断/坏 URL | `class`（`connect_timeout\|connect_failed\|body_limit_exceeded\|bad_upstream_url`）`message` |
| `request/end` | 唯一收尾事件 | `outcome` `duration_ms` `status` `ttfb_ms` `resp_bytes` `resp_chunks`；SSE 响应带 `sse{events,done,first_data_ms,usage,finish_reasons,model,id,error,encoded}`（`encoded:true`=上游压缩流，仅字节统计可用）；非流式带 `body{...}`（同一套提取）与 4xx/5xx 时的 `resp_preview`；网关自产错误带 `gateway_error{message,type,status}`；流错误带 `error{message}`；采集开启时 `resp_content_head`（压缩流为 `resp_content_skipped`） |

`outcome` 取值与 severity 映射：

| outcome | 含义 | severity |
|---|---|---|
| `completed` | 响应流正常走完（含上游回 4xx/5xx 的透传——`status>=400` 时升为 warn） | info / warn |
| `rejected` | 网关在触达上游前拒绝（404 provider 不存在 / 413 超限） | error |
| `upstream_error` | 连接失败/超时（502/504） | error |
| `stream_error` | 上游响应中途流错误 | error |
| `interrupted` | 客户端提前断连 / 响应被取消（Drop 兜底） | warn |
| `client_disconnected` | （预留语义，当前由 `interrupted` 覆盖） | warn |

## 脱敏契约（凭证零接触在日志侧的强制落点）

- `authorization` `proxy-authorization` `x-api-key` `api-key` `x-goog-api-key`
  `x-amz-security-token` `cookie` `set-cookie` `x-forwarded-authorization`
  —— 只记录 `名字:***`，值永不出现。
- 其余头默认**只记名字**；语义/排查头白名单（`content-type` `user-agent`
  `x-request-id` `retry-after` 等）可带值（截断 96 字符）。
- URL 只记 `scheme://host[:port]/path?{参数名}`；query 值与 userinfo 不进轨迹。
- 默认形态：请求/响应正文**只记派生事实**（model、stream、计数、usage、finish_reason、
  错误体摘要），prompt 与补全内容不落盘。唯一例外：上游 4xx/5xx 非 SSE 响应附
  2 KiB 截断 `resp_preview`（网关/上游错误信息，不含凭证）。
- 显式 opt-in：`--trace-body` 采集头部快照时，内容（含 prompt）会进 `req_content_head` /
  `resp_content_head`——这是设计内的例外，凭证头依旧永不采集；开启即把轨迹文件按
  密钥级文件管理。

## 排查配方（jq）

```bash
TR=~/.local/state/duct/trace.jsonl

# 最近一次失败请求的完整事件链
tail -n 2000 $TR | jq -s 'group_by(.trace) | last | sort_by(.seq)'

# 按 trace 串起一次对话请求的全部上下文
jq -r 'select(.trace=="1a06a245fcf-0") | [.seq,.type,.severity,(.data|tostring)] | @tsv' $TR

# 上游慢：TTFB > 3s
jq -c 'select(.type=="request/end" and (.data.ttfb_ms // 0) > 3000)' $TR

# 被截断的流（客户端断连/上游掐流）
jq -c 'select(.type=="request/end" and (.data.outcome=="interrupted" or .data.outcome=="stream_error"))' $TR

# SSE 未见 [DONE] 的流（协议异常）
jq -c 'select(.data.sse.done==false)' $TR

# 按模型聚合 token 用量（排查配额/计费）
jq -r 'select(.type=="request/end" and .data.sse.usage) | [.data.sse.model, .data.sse.usage.total_tokens] | @tsv' $TR

# 限流现场：429 与其 retry-after
jq -c 'select(.data.status==429 or .data.gateway_error.status==429)' $TR
```

## 已知边界

- 仅覆盖 `/aiproxy/*` 分支；CONNECT 隧道与 HTTP 正向代理是字节盲转发（TLS 内容不可见），
  其连接级日志维持 tracing 原样。
- 轨迹不含客户端地址：aiproxy 走 duplex 桥接进 hyper，peer 信息未贯穿到 handler
  （需要时以 `ConnectInfo` 注入，属后续增量）。
- 轨迹文件本身不自动轮转/清理，但 writer 兼容外部轮转（`create`/`move`/`copytruncate`
  均可，见「缺失自动创建」）；留存策略交给 logrotate 或定时任务。
- 缓冲满丢行只影响轨迹完整性（丢行计数经 WARN 暴露），不影响转发。
