# duct

轻量级 HTTP/HTTPS 网络代理：提供 CONNECT 隧道与 HTTP 正向代理，共用一个监听端口。

## 功能特性

- **HTTP CONNECT 隧道代理**：转发 HTTPS 流量
- **HTTP 正向代理**：支持浏览器代理插件（如 SwitchyOmega）和 HTTP 请求转发
- **HTTP Basic 认证**：可选保护 CONNECT 与 HTTP 正向代理（`--user` / `--passwd` 或 `DUCT_USER` / `DUCT_PASSWD`）
- **进程名伪装**：通过 `--disguise` 指定进程名
- **探活**：`GET /healthz` 返回进程存活状态
- **轻量部署**：Rust + tokio，单二进制运行

## 安装

```bash
cargo build --release
sudo cp target/release/duct /usr/local/bin/
```

## 使用方法

默认监听 `127.0.0.1:11088`：

```bash
duct

# 指定端口、监听地址
 duct --port 8080 --bind 127.0.0.1

# 调试日志
 duct -v
```

### HTTP Basic 认证

```bash
# CLI 参数（适合本地测试）
duct --user alice --passwd 'change-me'

# systemd 等服务环境推荐通过环境变量传入
DUCT_USER=alice DUCT_PASSWD='change-me' duct

# curl 使用认证
curl -x http://127.0.0.1:11088 --proxy-user alice:change-me https://example.com/
```

`--user` 和 `--passwd` 必须同时提供；未提供时认证关闭。无认证时请仅绑定本机或可信网络，勿直接将代理暴露到公网。

### 配置浏览器代理

在 SwitchyOmega 等代理插件中新增 HTTP 代理：地址 `127.0.0.1`，端口 `11088`（或实际配置端口）。

### 命令行测试

```bash
# HTTPS：CONNECT 隧道
curl -x http://127.0.0.1:11088 https://httpbin.org/get

# HTTP 正向代理
curl -x http://127.0.0.1:11088 http://httpbin.org/get

# 探活
curl http://127.0.0.1:11088/healthz
```

### 进程名伪装

某些环境会按发起连接的进程名（argv[0]）进行访问控制时，可选择 re-exec 并设置进程名：

```bash
duct --disguise curl
```

## 开发与测试

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo build --release
```
