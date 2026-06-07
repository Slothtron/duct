# duct

轻量级 HTTP 代理，专为 WSL2 环境下的 VPN 隧道访问设计。

## 功能特性

- **HTTP CONNECT 隧道代理**：支持 HTTPS 流量的透明转发
- **HTTP 正向代理**：支持浏览器插件模式（如 SwitchyOmega）的 HTTP 请求转发
- **VPN 进程名伪装**：自动规避 yunshu VPN 的进程名过滤机制
- **高性能**：基于 Rust + tokio 异步运行时，单二进制部署
- **完整测试覆盖**：16 个单元测试 + 4 个集成测试

## 背景

yunshu VPN 守护进程基于**进程名（argv[0]）**过滤 TCP 连接，只有白名单中的进程才能访问内网地址。duct 通过 re-exec 自身并伪装进程名，绕过这一限制。

**白名单验证结果：**

| ✅ 允许 | ❌ 阻止 |
|---------|--------|
| curl, wget, python3, python, node, java, firefox, chrome | duct, ssh, socat, nc, bash, sh |

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

### VPN 进程名伪装

默认自动伪装为 `curl`（白名单中的进程名）：

```bash
# 自动伪装为 curl（默认）
duct

# 指定伪装名称
duct --disguise wget

# 禁用伪装（手动 symlink 时使用）
duct --no-disguise
```

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

# 访问内网域名
curl -x http://127.0.0.1:10999 https://dmc.kso.net/
```

## 架构

```
src/
├── main.rs      # CLI 入口 + 进程名伪装 + tracing 配置
├── server.rs    # TCP 接收循环 + HTTP 转发代理
├── connect.rs   # CONNECT 隧道逻辑 + 请求解析
└── lib.rs       # 模块导出
```

### 核心组件

**1. CONNECT 隧道 (`connect.rs`)**
- 解析 `CONNECT host:port HTTP/1.1` 请求
- 建立上游连接（10s 超时）
- 发送 `200 Connection Established`
- 使用 `copy_bidirectional` 双向转发数据

**2. HTTP 转发代理 (`server.rs`)**
- 解析绝对 URL 形式的 HTTP 请求（如 `GET http://host/path`）
- 重写请求行为相对路径（`GET /path`）
- 转发到上游服务器并返回响应

**3. 进程名伪装 (`main.rs`)**
- 启动时检查 argv[0] 是否在白名单中
- 若不在，使用 `CommandExt::arg0()` re-exec 自身
- 伪装为白名单中的进程名（默认 `curl`）

## 技术细节

### 为什么需要进程名伪装？

yunshu VPN 守护进程在内核层面拦截 TCP 连接，检查发起连接的进程名。非白名单进程的连接会在 ~60ms 内被服务器关闭（TCP FIN）。

**验证过程：**
1. 直接运行 `duct` → 连接被立即关闭
2. 将 `duct` 重命名为 `curl` → 连接成功
3. 测试其他白名单名称（wget, python3 等）→ 均成功

### CONNECT 隧道工作原理

```
客户端                duct                  上游服务器
  |                    |                        |
  |-- CONNECT -------->|                        |
  |                    |-- TCP connect -------->|
  |                    |<------- 200 OK --------|
  |<-- 200 OK ---------|                        |
  |                    |                        |
  |====== 双向数据转发（copy_bidirectional）======|
  |                    |                        |
```

### HTTP 正向代理工作原理

```
客户端                duct                  上游服务器
  |                    |                        |
  |-- GET http://host->|                        |
  |                    |-- TCP connect -------->|
  |                    |-- GET /path ---------->|
  |                    |<------- 响应 ----------|
  |<------ 响应 --------|                        |
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

### 问题：内网域名无法访问

**症状：** 连接立即关闭，curl 返回 `Failed to connect`

**原因：** 进程名不在 VPN 白名单中

**解决：**
```bash
# 确认伪装已启用
duct --disguise curl

# 或检查当前进程名
ps aux | grep duct
```

### 问题：浏览器插件报 `expected CONNECT method`

**症状：** SwitchyOmega 等插件无法正常工作

**原因：** 插件发送 HTTP 正向代理请求（`GET http://host/path`），而非 CONNECT 隧道

**解决：** 已在 Task 6 中实现 HTTP 正向代理支持，升级到最新版本即可

### 问题：上游连接超时

**症状：** 日志显示 `upstream connection timed out after 10s`

**原因：** 上游服务器不可达或网络问题

**解决：**
1. 检查 VPN 连接状态
2. 确认上游地址和端口正确
3. 尝试直接 curl 上游地址（不通过代理）

## 许可证

MIT

## 相关资源

- [实现计划文档](docs/plans/2026-06-06-duct-implementation-plan.md)
- [HTTP CONNECT 方法 (RFC 7231)](https://tools.ietf.org/html/rfc7231#section-4.3.6)
- [tokio 异步运行时](https://tokio.rs/)
