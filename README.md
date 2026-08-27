# duct

轻量级网络中转服务：TCP 级 HTTP/HTTPS 代理（CONNECT 隧道 + 正向代理）与 AI 转发反向代理（`/aiproxy`）共用一个端口。

## 功能特性

- **HTTP CONNECT 隧道代理**：支持 HTTPS 流量的透明转发
- **HTTP 正向代理**：支持浏览器插件模式（如 SwitchyOmega）的 HTTP 请求转发
- **AI 转发反向代理（aiproxy）**：`/aiproxy/{provider}/*` 按路径前缀转发至预配置上游，YAML 声明式 provider 清单，凭证零接触（不存 Key、不注入 Key），SSE 流式透传
- **探活端点**：`GET /healthz` 进程级判活，独立于任何配置状态
- **单端口五路分流**：同一监听端口按请求行形状区分 CONNECT / 正向代理 / aiproxy / healthz
- **HTTP Basic 认证**：保护 CONNECT 与正向代理分支（通过 `--user` / `--passwd` 或 `DUCT_USER` / `DUCT_PASSWD`）
- **进程名伪装**：通过 `--disguise` 指定进程名，绕过基于 argv[0] 的访问控制
- **高性能**：基于 Rust + tokio 异步运行时，单二进制部署
- **完整测试覆盖**：44 个单元测试 + 33 个集成测试（含桥接长头部专项）

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

## 架构

```
src/
├── main.rs      # CLI 入口 + 配置装载 + 进程名伪装 + tracing
├── server.rs    # TCP 接收循环 + 五分支分流（healthz / aiproxy / CONNECT / 正向代理）+ 认证检查
├── connect.rs   # CONNECT 隧道逻辑 + 请求解析 + HTTP 转发
├── aiproxy.rs   # /aiproxy 反向代理：路由/头处理/流式转发 + 入口桥接
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
