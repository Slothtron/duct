# duct

轻量级网络中转服务：TCP 级 HTTP/HTTPS 代理（CONNECT 隧道 + 正向代理）与 AI 转发反向代理（`/aiproxy`）共用一个端口。

## 功能特性

- **HTTP CONNECT 隧道代理**：支持 HTTPS 流量的透明转发
- **HTTP 正向代理**：支持浏览器插件模式（如 SwitchyOmega）的 HTTP 请求转发
- **AI 转发反向代理（aiproxy）**：`/aiproxy/{provider}/*` 按路径前缀转发至预配置上游，YAML 声明式 provider 清单，凭证零接触（不存 Key、不注入 Key），SSE 流式透传
- **aiproxy 请求轨迹（trace）**：参考 DSH 会话轨迹的事件溯源设计，每次转发落一条 JSONL 事件链（`request/start → request/body → upstream/response → request/end`），记录 model/stream、TTFB、token usage、finish_reason、流完整性与失败归因；凭证与 prompt 正文强制不进轨迹。详见 [docs/aiproxy-trace.md](docs/aiproxy-trace.md)
- **探活端点**：`GET /healthz` 进程级判活，独立于任何配置状态
- **单端口五路分流**：同一监听端口按请求行形状区分 CONNECT / 正向代理 / aiproxy / healthz
- **HTTP Basic 认证**：保护 CONNECT 与正向代理分支（通过 `--user` / `--passwd` 或 `DUCT_USER` / `DUCT_PASSWD`）
- **进程名伪装**：通过 `--disguise` 指定进程名，绕过基于 argv[0] 的访问控制
- **高性能**：基于 Rust + tokio 异步运行时，单二进制部署
- **完整测试覆盖**：74 个单元测试 + 45 个集成测试（含桥接长头部专项与轨迹全链路断言）

## 安装

```bash
cargo build --release
cp target/release/duct /usr/local/bin/
```

## 使用方法

### 基础用法

```bash
# 默认监听 0.0.0.0:11088（⚠️ 默认端口自本版本起由 10999 变更为 11088）
duct

# 指定端口
duct -p 8080

# 调试模式（详细日志）
duct -v
```

### 进程名伪装

某些环境会基于发起连接的**进程名（argv[0]）**进行访问控制。通过 `--disguise` 参数，duct 可以 re-exec 自身并伪装为指定进程名。

```bash
# 启用伪装，指定进程名
duct --disguise curl

# 指定其他名称
duct --disguise wget
```

### HTTP Basic 认证

```bash
# 方式 1：CLI 参数（本地测试用）
duct --user alice --passwd p@ss123

# 方式 2：环境变量（推荐 systemd 部署，ps 不可见）
DUCT_USER=alice DUCT_PASSWD=p@ss123 duct

# 配合认证使用 curl
curl -x http://alice:p@ss123@127.0.0.1:11088 https://httpbin.org/get

# 或通过 --proxy-user 参数
curl -x http://127.0.0.1:11088 --proxy-user alice:p@ss123 https://httpbin.org/get
```

> **注意**: `--user` 和 `--passwd` 必须同时使用。未提供时认证默认关闭。

### 配置浏览器代理

1. 安装 SwitchyOmega 等代理插件
2. 配置代理服务器：
   - 协议：HTTP
   - 地址：127.0.0.1
   - 端口：11088（默认）
3. 启用代理，访问内网资源

### 命令行测试

```bash
# CONNECT 隧道（HTTPS）
curl -x http://127.0.0.1:11088 https://httpbin.org/get

# HTTP 正向代理
curl -x http://127.0.0.1:11088 http://httpbin.org/get
```

### AI 转发反向代理（aiproxy）

在同一端口上，以路径前缀将请求转发至预配置的上游 AI 服务：

```
{base_url}/aiproxy/{provider}/{剩余路径}  →  {provider.url}/{剩余路径}
```

**配置**：`~/.config/duct/config.yaml`（或 `--config-file` 指定）。只有 id 与 base url 两项字段，
**不含任何密钥**——上游凭证由各客户端工具自行配置（ duct 不做上游鉴权）：

```yaml
providers:
  openai:
    url: https://api.openai.com/v1
  ollama:
    url: http://ollama:11434
  anthropic:
    url: https://api.anthropic.com
```

**使用**（aider / cline / OpenAI SDK 等，把 base_url 指向前缀即可）：

```bash
curl http://127.0.0.1:11088/aiproxy/ollama/api/tags

curl http://127.0.0.1:11088/aiproxy/openai/v1/chat/completions \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -d '{"model":"gpt-4o","messages":[...]}'
```

```python
OpenAI(base_url="http://127.0.0.1:11088/aiproxy/openai/v1", api_key=真实key)
```

语义要点：

- 方法 / 查询串 / 请求体透明透传；响应（含 SSE 流）逐 chunk 回传
- 头处理为黑名单制：仅剥离逐跳头与 `Proxy-*`，`Authorization` 等一切凭据头原样透传
- 未注册 provider → 404 OpenAI 兼容 JSON 错误；请求体超过 `--max-body`（默认 16 MiB）→ 413
- ⚠️ **无鉴权设计**：信任边界 = 内网/防火墙边界，请勿暴露公网
- 探活：`curl http://127.0.0.1:11088/healthz`（进程级判活，任何配置状态下均可用）
- 每一次转发都可落一条 JSONL 请求轨迹（见下节），SSE 流同样有始有终

### AI 转发请求轨迹（排查入口）

参考 deepseek-harness 会话轨迹的事件溯源设计：一次转发 = 一条 append-only 事件链，
信封 `{v, time, trace, seq, type, severity, data}`，`request/end` 唯一收尾
（`completed / rejected / upstream_error / stream_error / interrupted`）。

```bash
# 默认即启用，落 $XDG_STATE_HOME/duct/trace.jsonl（未设则 ~/.local/state/duct/trace.jsonl）；显式指定：
duct --trace-file /var/log/duct/trace.jsonl
# 关闭文件 sink（事件回落 tracing，target=duct::trace）：
duct --trace-file ""
```

> 轨迹文件缺失会自动创建（含父目录链）；运行期被外部删除或被 logrotate
> `create`/`move`/`copytruncate` 换走 inode 时，writer 会在下一条记录前自动重建续写。

每条请求至少回答：谁转给了哪个 provider、什么模型、是否流式、上游多久应答（TTFB）、
回了什么状态、流是否走完（`[DONE]`）、token 用量与停止原因、失败卡在哪一段：

```bash
TR=~/.local/state/duct/trace.jsonl
tail -n 200 $TR | jq -s 'group_by(.trace) | last | sort_by(.seq)'          # 最近一次请求全貌
jq -c 'select(.type=="request/end" and .severity!="info")' $TR               # 只看异常收尾
jq -c 'select(.data.sse.done==false)' $TR                                    # 被截断的流
```

**凭证零接触在日志侧同步强制**：`authorization` / `x-api-key` 等只记 `名字:***`，
prompt 与补全正文默认不落盘（只留 model/usage/finish_reason 等派生事实）。
需要看内容本身时显式开采集：`--trace-body 2048` 会把请求/响应头部快照记入轨迹
（轨迹文件随之含 prompt，按敏感文件管理；同时对上游协商明文）。即便网关（如 kso）
无视协商仍回压缩流：轨迹侧做**观察式解码**恢复 usage/finish_reason/内容快照
（`sse.encoded+decoded`）；开了 `normalize_sse` 的 provider 则走
**解码→改写→明文重发**，工具名归一化对压缩流同样生效（brotli 除外，退回透传并 WARN）。
事件词表、severity 映射与排查配方见 [docs/aiproxy-trace.md](docs/aiproxy-trace.md)。

## 架构

```
src/
├── main.rs      # CLI 入口 + 配置装载 + 进程名伪装 + tracing + trace sink 接线
├── server.rs    # TCP 接收循环 + 五分支分流（healthz / aiproxy / CONNECT / 正向代理）+ 认证检查
├── connect.rs   # CONNECT 隧道逻辑 + 请求解析 + HTTP 转发
├── aiproxy.rs   # /aiproxy 反向代理：路由/头处理/流式转发 + 轨迹埋点 + 入口桥接
├── trace.rs     # aiproxy 请求轨迹：JSONL 事件溯源 + sink（文件/tracing/内存）+ 脱敏 + 流观测 tap
├── sse_normalize.rs # SSE 流兼容归一化（stream 字段注入 + 工具名去重）
├── config.rs    # config.yaml 装载（三层语义：缺省禁用/损坏致命/条目跳过）
├── error.rs     # OpenAI 兼容错误响应
├── auth.rs      # HTTP Basic 认证检查 + base64 解码
└── lib.rs       # 模块导出
```

### 核心工作流

```
客户端                duct                   上游服务器
  |                    |                        |
  |── CONNECT -------->|                        |
  |                    |── TCP connect -------->|
  |                    |<------- 200 OK --------|
  |<-- 200 OK ---------|                        |
  |                    |                        |
  |====== 双向数据转发（copy_bidirectional）======|
  |                    |                        |
```

```
客户端                duct                   上游服务器
  |                    |                        |
  |── GET http://host  |                        |
  |       /path ------->                        |
  |                    |── TCP connect -------->|
  |                    |── GET /path ---------->|
  |                    |<------- 响应 ----------|
  |<------ 响应 --------|                        |
```

### 配置选项

```
duct [OPTIONS]

Options:
  -p, --port <PORT>           监听端口 [default: 11088]（⚠️ 原 10999，升级时注意存量部署）
  -b, --bind <ADDR>           监听地址 [default: 0.0.0.0]
  -v, --verbose               启用 debug 级别日志
      --config-file <PATH>    aiproxy provider 配置（YAML）路径
                              [默认 ~/.config/duct/config.yaml；缺省文件存在则启用，不存在则禁用 aiproxy]
      --max-body <BYTES>      aiproxy 请求体上限 [default: 16777216]
      --trace-file <PATH>     aiproxy 请求轨迹 JSONL 路径（append-only）
                              [默认 $XDG_STATE_HOME/duct/trace.jsonl，即 ~/.local/state/...；
                               空串关闭文件 sink，事件回落 tracing]
      --trace-body <BYTES>    轨迹内容采集预算（默认 0=不采集正文）；>0 时记录
                              请求/响应头部快照，并对上游协商 Accept-Encoding: identity
  -V, --version               版本信息
  -h, --help                  帮助信息
      --disguise <NAME>       进程伪装名称（可选，默认不启用）
      --user <USER>         HTTP Basic 认证用户名（也支持 DUCT_USER 环境变量）
      --passwd <PASS>       HTTP Basic 认证密码（也支持 DUCT_PASSWD 环境变量）
```

## 开发

### 运行测试

```bash
cargo test
```

### 代码质量检查

```bash
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

### 调试模式

```bash
RUST_LOG=debug duct -v
```

## 故障排查

### 问题：AI 转发结果不符合预期（慢、断流、报错、计费异常）

**入口：** 查 `~/.local/state/duct/trace.jsonl`（或 `--trace-file` 指定路径），按
`trace` 字段串起一次请求的完整事件链，再按 `request/end` 的 `outcome` 定位断点：

| 现象 | 轨迹特征 |
|---|---|
| 上游不可达 / 连接超时 | `upstream/error{class:connect_timeout\|connect_failed}` + `request/end{upstream_error}` |
| 客户端断连 / 流被取消 | `request/end{outcome:"interrupted"}`（Drop 兜底合成） |
| SSE 中途断 | `request/end{outcome:"stream_error"}` 或 `sse.done:false` |
| 上游 4xx/5xx | `request/end{completed, status>=400, resp_preview}`（错误体截断预览） |
| 限流 | `status:429` + `response_headers` 中的 `retry-after:*` |
| token 用量核对 | `request/end.sse.usage`（SSE 尾帧）或 `body.usage`（非流式） |

更多 jq 配方见 [docs/aiproxy-trace.md](docs/aiproxy-trace.md#排查配方-jq)。

### 问题：上游连接超时

**症状：** 日志显示 `upstream connection timed out after 10s`

**原因：** 上游服务器不可达或网络问题

**解决：**
1. 检查网络连接状态
2. 确认上游地址和端口正确
3. 尝试直接 curl 上游地址（不通过代理）

### 问题：浏览器插件报 `expected CONNECT method`

**症状：** SwitchyOmega 等插件无法正常工作

**原因：** 插件发送 HTTP 正向代理请求（`GET http://host/path`），而非 CONNECT 隧道

**解决：** duct 已支持 HTTP 正向代理，请确认使用最新版本

### 问题：认证失败

**症状：** 收到 `407 Proxy Authentication Required` 错误

**解决：**
```bash
# 提供正确的凭据
curl -x http://alice:p@ss123@127.0.0.1:11088 https://httpbin.org/get
```

### 问题：连接被拒绝或立即关闭

**症状：** 连接建立后立即被关闭

**原因：** 某些环境会基于进程名对 TCP 连接做访问控制

**解决：**
```bash
# 启用进程名伪装
duct --disguise curl

# 或检查当前进程名
ps aux | grep duct
```

## 技术细节

### 为什么需要进程名伪装？

某些安全软件或 VPN 客户端会在内核层面拦截 TCP 连接，检查发起连接的进程名。非允许名单中的进程的连接可能被关闭。 duct 通过 `--disguise <name>` 使用 `CommandExt::arg0()` 重新执行自身来绕过这一限制。

### CONNECT 隧道

- 解析 `CONNECT host:port HTTP/1.1` 请求
- 建立上游连接（10s 超时）
- 发送 `200 Connection Established`
- 使用 `copy_bidirectional` 双向转发数据

### HTTP 正向代理

- 解析绝对 URL 形式的 HTTP 请求（如 `GET http://host/path`）
- 重写请求行为相对路径（`GET /path`）
- 转发到上游服务器并返回响应

## 许可证

MIT

## 相关资源

- [HTTP CONNECT 方法 (RFC 7231)](https://tools.ietf.org/html/rfc7231#section-4.3.6)
- [tokio 异步运行时](https://tokio.rs/)
