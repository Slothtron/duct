# duct

轻量级 HTTP/HTTPS 代理服务，支持 CONNECT 隧道和 HTTP 正向代理。

## 功能特性

- **HTTP CONNECT 隧道代理**：支持 HTTPS 流量的透明转发
- **HTTP 正向代理**：支持浏览器插件模式（如 SwitchyOmega）的 HTTP 请求转发
- **HTTP Basic 认证**：支持通过 `--username` / `--password` 保护代理访问
- **进程名伪装**：通过 `--disguise` 指定进程名，绕过基于 argv[0] 的访问控制
- **高性能**：基于 Rust + tokio 异步运行时，单二进制部署
- **完整测试覆盖**：16 个单元测试 + 10 个集成测试

## 安装

```bash
cargo build --release
cp target/release/duct /usr/local/bin/
```

## 使用方法

### 基础用法

```bash
# 默认监听 127.0.0.1:10999
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
# 启用认证（每个请求需提供用户名/密码）
duct --username alice --password p@ss123

# 配合认证使用 curl
curl -x http://alice:p@ss123@127.0.0.1:10999 https://httpbin.org/get

# 或通过 --proxy-user 参数
curl -x http://127.0.0.1:10999 --proxy-user alice:p@ss123 https://httpbin.org/get
```

> **注意**: `--username` 和 `--password` 必须同时使用。未提供时认证默认关闭。

### 配置浏览器代理

1. 安装 SwitchyOmega 等代理插件
2. 配置代理服务器：
   - 协议：HTTP
   - 地址：127.0.0.1
   - 端口：10999（默认）
3. 启用代理，访问内网资源

### 命令行测试

```bash
# CONNECT 隧道（HTTPS）
curl -x http://127.0.0.1:10999 https://httpbin.org/get

# HTTP 正向代理
curl -x http://127.0.0.1:10999 http://httpbin.org/get
```

## 架构

```
src/
├── main.rs      # CLI 入口 + 进程名伪装 + tracing 配置
├── server.rs    # TCP 接收循环 + CONNECT/HTTP 请求分发 + 认证检查
├── connect.rs   # CONNECT 隧道逻辑 + 请求解析 + HTTP 转发
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
  -p, --port <PORT>         监听端口 [default: 10999]
  -b, --bind <ADDR>         监听地址 [default: 0.0.0.0]
  -v, --verbose             启用 debug 级别日志
  -V, --version             版本信息
  -h, --help                帮助信息
      --disguise <NAME>     进程伪装名称（可选，默认不启用）
      --username <USER>     HTTP Basic 认证用户名
      --password <PASS>     HTTP Basic 认证密码
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
curl -x http://alice:p@ss123@127.0.0.1:10999 https://httpbin.org/get
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
