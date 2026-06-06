# duct — 轻量正向代理桥设计

## Overview

**duct** 是一个极简的 HTTP CONNECT 代理工具，运行在 WSL 中，将宿主机（Windows）或其他局域网机器的流量桥接到 WSL 内的 VPN 通道（yunshu）上，从而访问内网域名（如 `*.kso.net`）。

**核心理念**：只做 TCP 层隧道转发，不触及 TLS 解密、不处理 MITM、不进行证书管理。SSL 证书问题在这一层根本不会出现。

### 为什么需要 duct

- 环境：WSL2 安装 yunshu（云枢）VPN，公司内网 `*.kso.net` 需经 VPN 访问
- 需求：宿主机 Windows 没有 yunshu，希望通过设置系统代理访问内网资源
- 尝试过一些开源代理工具（mitmproxy、squid 等），常遇到 SSL 证书问题
- 已有项目 reqcraft 是 MITM 调试工具，方向不同，独立为新项目

### 与 reqcraft 的关系

- **reqcraft**：HTTP/HTTPS MITM 调试代理工具（Rust + Tauri 桌面端），负责拦截、解密、修改流量
- **duct**：纯 TCP 层正向代理桥，不做解密，只做字节透传
- 两个项目同级存放于 `~/workspace/homelab/` 下，互不依赖

## Architecture

```
宿主机 (Windows)                    WSL
┌──────────────────┐  CONNECT    ┌──────────────────────────────────┐
│ 浏览器 / 应用     │ ──:1080──→ │ duct (HTTP CONNECT Proxy)       │
│                  │             │                                  │
│ (不解析 DNS)      │             │ DNS 解析 → yunshu 路由          │
│                  │             │      ↓                           │
│ 系统代理:         │             │ TcpStream::connect(ip:443)      │
│ 127.0.0.1:1080   │             │      ↓                           │
└──────────────────┘             │ yunshu 虚拟网卡 → 内网 *.kso.net │
                                 └──────────────────────────────────┘
```

WSL2（Windows 11 / 较新 Win10）自动将 `localhost:1080` 转发到 WSL2 对应端口，宿主机直接设 `127.0.0.1:1080` 为系统代理即可。

**流量路径：**
1. 宿主机应用将 `dmc.kso.net:443` 通过 CONNECT 发给 duct
2. duct 在 WSL 内解析 DNS（yunshu 已修改 `/etc/resolv.conf`，得到正确内网 IP）
3. duct 建立到目标的 TCP 连接（经 yunshu 路由）
4. 返回 `200 Connection Established`
5. 后续双向字节透传直到断开

## Project Structure

```
duct/
├── Cargo.toml
├── src/
│   ├── main.rs        # CLI 入口 + tracing 初始化
│   ├── server.rs      # TCP 监听 + accept 循环
│   └── connect.rs     # CONNECT 隧道处理 + copy_bidirectional
```

**依赖（v0.1.0）：**

```toml
[dependencies]
tokio = { version = "1", features = ["net", "io-util", "macros", "rt-multi-thread"] }
clap = { version = "4", features = ["derive"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
anyhow = "1"
```

| 依赖 | 用途 |
|------|------|
| `tokio` | `net` 提供 TcpListener/TcpStream；`io-util` 提供 copy_bidirectional；`macros` + `rt-multi-thread` 提供 async runtime 支持多并发 |
| `clap` | CLI 参数解析（derive 宏） |
| `tracing` + `tracing-subscriber` | 结构化日志，支持 env-filter 按级别控制 |
| `anyhow` | 错误处理，简化 main 的 Result 类型 |

预计核心逻辑 200-300 行。

## CLI Design

```
duct [OPTIONS]

Options:
  -p, --port <PORT>        监听端口 [default: 1080]
  -b, --bind <ADDR>        监听地址 [default: 0.0.0.0]
  -v, --verbose            启用 debug 级别日志（默认 info 及以上）
  -V, --version            版本信息
  -h, --help               帮助信息
```

`--verbose` 行为：默认 tracing level 设为 `info`，加 `-v` 降为 `debug`。使用 `tracing-subscriber` 的 `EnvFilter` 实现。

使用示例：
```bash
# WSL 中启动
duct -p 1080

# 宿主机验证
curl -x 127.0.0.1:1080 https://dmc.kso.net
```

## Core Logic

### server.rs — 主循环

```
TcpListener::bind(addr) 开始监听
loop {
  接受新连接 → tokio::spawn 一个 task
  task 内:
    1. 读取第一行（读到 \r\n 或 \n）
    2. 手动解析 CONNECT 请求（不依赖 HTTP 解析库）
       - 按空格切分，第一段必须是 "CONNECT"
       - 第二段按 ':' 切分得到 host:port
       - 第三段可忽略
    3. 提取 host:port
    4. 调用 connect::handle_connect(stream, host, port)
}
```

手动解析 CONNECT 示例：
```
输入: "CONNECT dmc.kso.net:443 HTTP/1.1\r\n"
拆分: ["CONNECT", "dmc.kso.net:443", "HTTP/1.1"]
host: "dmc.kso.net", port: 443u16
后续 headers 全部丢弃（第一版不验证）
```

### connect.rs — 隧道握手与转发

```
handle_connect(client_stream, host, port):
  1. TcpStream::connect(host:port)  —— 系统 DNS 解析已受 yunshu 影响
  2. 若连接成功，回复 "HTTP/1.1 200 Connection Established\r\n\r\n"
  3. tokio::io::copy_bidirectional(client, upstream)
     → 双向透传字节，直到 EOF 或出错
```

**为什么不会有 SSL 问题：** duct 只做 CONNECT 隧道透传，TLS 握手在客户端和目标服务器之间直接在 TCP 隧道内完成，duct 完全不干预。

## Error Handling

| 场景 | 行为 |
|------|------|
| 非 CONNECT 请求 / 格式错误 | 回复 `HTTP/1.1 400 Bad Request\r\n\r\n`，关闭连接 |
| 目标地址 host 或 port 解析失败 | 回复 `HTTP/1.1 400 Bad Request\r\n\r\n` |
| DNS 解析失败 / 上游连接被拒 | 回复 `HTTP/1.1 502 Bad Gateway\r\n\r\n` |
| 隧道传输中一边断开 | copy_bidirectional 返回 → 关闭另一边，task 退出 |

所有错误回复为纯裸 status line，无 body。发完后直接关闭连接。

## Testing

### 第一优先级 — 纯本地集成测试（不依赖 yunshu）

在测试内启动 duct 监听随机端口，通过 TCP 连接发送 CONNECT 请求。

| 测试 | 方法 |
|------|------|
| `test_connect_tunnel_success` | 本地启动 echo server → 通过 duct CONNECT → 验证双向透传 |
| `test_bad_request_not_connect` | 发 `GET / HTTP/1.1\r\n` → 期望 400 |
| `test_bad_request_malformed` | 发 `CONNECT nonsense\r\n`（缺 port）→ 期望 400 |
| `test_unreachable_target` | CONNECT 到 `127.0.0.1:1`（大概率被拒）→ 期望 502 |

### 第二优先级 — E2E（需要 yunshu 环境）

```rust
#[tokio::test]
#[ignore = "需要 yunshu 运行环境"]
async fn test_internal_domain_via_proxy() {
    // 1. 启动 duct（本地随机端口）
    // 2. 通过代理发送 CONNECT + HTTPS 请求到 dmc.kso.net
    // 3. 断言收到 200/302, body 符合预期
}
```

## Release Strategy (v0.1.0)

1. **安装方式**: `cargo install duct`
2. **CI**: GitHub Actions — `cargo test` + `cargo clippy` + 跨平台 build
3. **发布**: `cargo publish` 到 crates.io

## Future Considerations (v0.2.0+)

- SOCKS5 协议支持
- 上游 DNS 服务器选项
- 连接池 / 连接复用
- 基本认证支持 (Proxy-Authorization)
- 健康检查和指标暴露
