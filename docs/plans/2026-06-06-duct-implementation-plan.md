# duct HTTP 代理实现计划

**目标：** 构建一个轻量级 HTTP 代理，运行在 WSL2 环境中，通过云树 VPN 转发 TCP 流量以访问内网域名。

**架构：** 单二进制 Rust 项目，使用 tokio 异步运行时。四个模块：`main.rs`（CLI + 进程名伪装 + tracing 配置）、`server.rs`（TCP 接收循环 + HTTP 转发代理）、`connect.rs`（CONNECT 隧道逻辑 + 请求解析）、`lib.rs`（模块导出）。

**技术栈：** Rust 2024 edition, tokio (net + io-util + rt-multi-thread + time), clap derive, tracing + tracing-subscriber, anyhow.

---

### Task 1: Project skeleton

**TDD scenario:** New project — no tests needed for this setup step.

**Files:**
- Create: `Cargo.toml`
- Create: `src/main.rs`
- Create: `src/connect.rs`
- Create: `src/server.rs`

**Step 1: Write Cargo.toml**

```toml
[package]
name = "duct"
version = "0.1.0"
edition = "2024"

[lib]
name = "duct"
path = "src/lib.rs"

[[bin]]
name = "duct"
path = "src/main.rs"

[dependencies]
tokio = { version = "1", features = ["net", "io-util", "time", "macros", "rt-multi-thread"] }
clap = { version = "4", features = ["derive"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
anyhow = "1"
```

**Step 2: Create .gitignore**

```gitignore
/target/
```

**Step 3: Write src/main.rs**

```rust
mod connect;
mod server;

fn main() {
    println!("Hello, world!");
}
```

**Step 4: Create empty module files**

`src/connect.rs`:
```rust
// CONNECT tunnel logic
```

`src/server.rs`:
```rust
// TCP accept loop
```

**Step 5: Verify it compiles**

Run: `cargo check`
Expected: `Compiling duct v0.1.0 ... Finished 'dev' profile [unoptimized + debuginfo]`

**Step 6: Commit**

```bash
git add Cargo.toml .gitignore src/main.rs src/connect.rs src/server.rs
git commit -m "chore: initialize Rust project with tokio + clap dependencies"
```

---

### Task 2: CONNECT request parsing

**TDD scenario:** New feature — full TDD cycle with unit tests.

**Files:**
- Modify: `src/connect.rs`

**Step 1: Write failing unit tests**

Replace placeholder in `src/connect.rs` with tests only:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_connect_valid() {
        let (host, port) = parse_connect_request("CONNECT dmc.kso.net:443 HTTP/1.1\r\n")
            .expect("should parse valid CONNECT");
        assert_eq!(host, "dmc.kso.net");
        assert_eq!(port, 443);
    }

    #[test]
    fn test_parse_connect_valid_without_cr() {
        let (host, port) = parse_connect_request("CONNECT example.com:8080 HTTP/1.1\n")
            .expect("should parse CONNECT with \\n only");
        assert_eq!(host, "example.com");
        assert_eq!(port, 8080);
    }

    #[test]
    fn test_parse_connect_not_connect() {
        let result = parse_connect_request("GET / HTTP/1.1\r\n");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_connect_missing_port() {
        let result = parse_connect_request("CONNECT host HTTP/1.1\r\n");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_connect_invalid_port() {
        let result = parse_connect_request("CONNECT host:abc HTTP/1.1\r\n");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_connect_lowercase_method() {
        let result = parse_connect_request("connect host:443 HTTP/1.1\r\n");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_connect_no_http_version() {
        let result = parse_connect_request("CONNECT host:443");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_connect_extra_whitespace() {
        let (host, port) = parse_connect_request("CONNECT  host:443  HTTP/1.1\r\n")
            .expect("should handle extra whitespace");
        assert_eq!(host, "host");
        assert_eq!(port, 443);
    }
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -- connect::tests -v`
Expected: FAIL — `parse_connect_request` not defined

**Step 3: Write minimal implementation**

Replace content in `src/connect.rs` with:

```rust
use anyhow::{bail, Context, Result};

/// Parse a CONNECT request line, extracting host and port.
/// Expected format: `CONNECT host:port HTTP/1.1\r\n`
pub fn parse_connect_request(line: &str) -> Result<(&str, u16)> {
    let line = line.trim_end_matches('\r').trim_end_matches('\n');

    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 3 {
        bail!("invalid CONNECT request: expected 3 parts, got {}", parts.len());
    }

    let method = parts[0];
    if method != "CONNECT" {
        bail!("expected CONNECT method, got: {method}");
    }

    let authority = parts[1];
    let (host, port_str) = authority
        .rsplit_once(':')
        .context("missing port in authority")?;

    if host.is_empty() {
        bail!("empty host");
    }
    let port: u16 = port_str.parse().context("invalid port number")?;

    // Verify HTTP version is present and well-formed
    let version = parts[2];
    if !version.starts_with("HTTP/") {
        bail!("invalid HTTP version: {version}");
    }

    Ok((host, port))
}

#[cfg(test)]
mod tests { /* tests from Step 1 */ }
```

**Step 4: Run tests to verify they pass**

Run: `cargo test -- connect::tests -v`
Expected: All 8 tests PASS

**Step 5: Commit**

```bash
git add src/connect.rs
git commit -m "feat: add CONNECT request parser"
```

---

### Task 3: handle_connect — tunnel handshake and bidirectional copy

**TDD scenario:** New feature — full TDD cycle with async integration-style tests.

**Files:**
- Modify: `src/connect.rs`

**Step 1: Write failing async tests**

Add after `parse_connect_request`:

```rust
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn test_handle_connect_successful_tunnel() {
    // Start an echo server on random port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = listener.local_addr().unwrap();

    let upstream = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        stream.write_all(&buf[..n]).await.unwrap();
    });

    // Rendezvous: listen for the "browser" connection
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();

    // Connect as browser
    let mut browser = TcpStream::connect(proxy_addr).await.unwrap();

    // Accept on proxy side — this TcpStream is what handle_connect receives
    let (proxy_client, _) = proxy_listener.accept().await.unwrap();

    // Call handle_connect with the proxy_client and the echo server addr
    let handle = tokio::spawn(async move {
        handle_connect(proxy_client, "127.0.0.1", upstream_addr.port())
            .await
            .ok();
    });

    // Read 200 response
    let mut buf = [0u8; 1024];
    let n = browser.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("200 Connection Established"),
        "expected 200, got: {response}"
    );

    // Verify bidirectional tunnel: send data and get echo back
    browser.write_all(b"hello through tunnel").await.unwrap();
    let mut buf = [0u8; 1024];
    let n = browser.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"hello through tunnel");

    upstream.await.unwrap();
    handle.await.unwrap();
}

#[tokio::test]
async fn test_handle_connect_sends_502_on_unreachable() {
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();

    let mut browser = TcpStream::connect(proxy_addr).await.unwrap();
    let (proxy_client, _) = proxy_listener.accept().await.unwrap();

    // Connect to port 1 which is typically unreachable
    let handle = tokio::spawn(async move {
        let _ = handle_connect(proxy_client, "127.0.0.1", 1).await;
    });

    let mut buf = [0u8; 1024];
    let n = browser.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("502 Bad Gateway") || response.contains("504 Gateway Timeout"),
        "expected 502 or 504, got: {response}"
    );

    handle.await.unwrap();
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -- connect::tests -v -- --test-threads=1`
Expected: FAIL — `handle_connect` not defined

**Step 3: Write handle_connect implementation**

Add to `src/connect.rs` (before the tests modules):

```rust
use tokio::io::{AsyncWriteExt, copy_bidirectional};
use tokio::net::TcpStream;
use std::time::Duration;

/// Maximum time to wait for connection to upstream before returning 504.
const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Handle a CONNECT tunnel: connect to upstream with timeout, send 200, then bidirectional copy.
/// Drops the connection if any step fails.
pub async fn handle_connect(mut client: TcpStream, host: &str, port: u16) -> Result<()> {
    let addr = format!("{host}:{port}");
    let mut upstream = match tokio::time::timeout(
        UPSTREAM_CONNECT_TIMEOUT,
        TcpStream::connect(&addr),
    )
    .await
    {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            tracing::error!(%addr, error = %e, "failed to connect to upstream");
            let _ = client
                .write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                .await;
            return Err(e.into());
        }
        Err(_elapsed) => {
            tracing::error!(%addr, "upstream connection timed out");
            let _ = client
                .write_all(b"HTTP/1.1 504 Gateway Timeout\r\n\r\n")
                .await;
            return Err(anyhow::anyhow!(
                "upstream connection timed out after {UPSTREAM_CONNECT_TIMEOUT:?}"
            ));
        }
    };

    if let Err(e) = client
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await
    {
        tracing::error!(error = %e, "failed to send 200 response");
        return Err(e.into());
    }

    tracing::info!(%addr, "tunnel established");

    match copy_bidirectional(&mut client, &mut upstream).await {
        Ok((to_upstream, to_client)) => {
            tracing::info!(
                %addr,
                to_client_bytes = to_client,
                to_upstream_bytes = to_upstream,
                "tunnel closed"
            );
            Ok(())
        }
        Err(e) => {
            tracing::warn!(%addr, error = %e, "tunnel error");
            Err(e.into())
        }
    }
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test -- connect::tests -v -- --test-threads=1`
Expected: Both tests PASS

**Step 5: Commit**

```bash
git add src/connect.rs
git commit -m "feat: add CONNECT tunnel handler with bidirectional copy"
```

---

### Task 4: server — accept loop

**TDD scenario:** New feature — full TDD cycle. Integration tests (tests/integration.rs) are the primary tests.

**Files:**
- Create: `src/lib.rs`
- Modify: `src/server.rs`
- Create: `tests/integration.rs`
- Modify: `src/main.rs`

**Step 1: Write integration tests**

Create `src/lib.rs` (needed so integration tests can import the crate):
```rust
pub mod connect;
pub mod server;
```

Create `tests/integration.rs`:
```rust
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
#[tokio::test]
async fn test_e2e_connect_tunnel_success() {
    let echo_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    let echo = tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        stream.write_all(&buf[..n]).await.unwrap();
    });

    // Start duct
    let duct_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let duct_addr = duct_listener.local_addr().unwrap();
    tokio::spawn(async move {
        duct::server::run_from_listener(duct_listener).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    let req = format!("CONNECT 127.0.0.1:{} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\n\r\n", echo_addr.port(), echo_addr.port());
    conn.write_all(req.as_bytes()).await.unwrap();

    let mut buf = [0u8; 1024];
    let n = conn.read(&mut buf).await.unwrap();
    assert!(String::from_utf8_lossy(&buf[..n]).contains("200 Connection Established"));

    conn.write_all(b"hello").await.unwrap();
    let mut buf = [0u8; 1024];
    let n = conn.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"hello");

    drop(browser);
    echo.await.unwrap();
}

#[tokio::test]
async fn test_e2e_bad_request_non_connect() {
    let duct_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let duct_addr = duct_listener.local_addr().unwrap();
    tokio::spawn(async move {
        duct::server::run_from_listener(duct_listener).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    conn.write_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n").await.unwrap();

    let mut buf = [0u8; 1024];
    let n = conn.read(&mut buf).await.unwrap();
    assert!(String::from_utf8_lossy(&buf[..n]).contains("400 Bad Request"));
}

#[tokio::test]
async fn test_e2e_malformed_connect_request() {
    let duct_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let duct_addr = duct_listener.local_addr().unwrap();
    tokio::spawn(async move {
        duct::server::run_from_listener(duct_listener).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    // Missing port in CONNECT line
    conn.write_all(b"CONNECT host HTTP/1.1\r\n\r\n")
        .await
        .unwrap();

    let mut buf = [0u8; 1024];
    let n = conn.read(&mut buf).await.unwrap();
    assert!(
        String::from_utf8_lossy(&buf[..n]).contains("400 Bad Request"),
        "expected 400, got: {}",
        String::from_utf8_lossy(&buf[..n])
    );
}

#[tokio::test]
async fn test_e2e_unreachable_upstream() {
    let duct_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let duct_addr = duct_listener.local_addr().unwrap();
    tokio::spawn(async move {
        duct::server::run_from_listener(duct_listener).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    // Port 1 is reserved and should be unreachable
    conn.write_all(b"CONNECT 127.0.0.1:1 HTTP/1.1\r\nHost: 127.0.0.1:1\r\n\r\n")
        .await
        .unwrap();

    let mut buf = [0u8; 1024];
    let n = conn.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("502 Bad Gateway") || response.contains("504"),
        "expected 502 or 504, got: {response}"
    );
}
```

**Step 2: Write handle_connection in server.rs**

```rust
use anyhow::{bail, Result};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

use crate::connect;

/// Maximum length of the request line before returning 414.
const MAX_REQUEST_LINE_BYTES: usize = 8192;

/// Run the duct proxy server, binding to the given address.
pub async fn run(addr: impl tokio::net::ToSocketAddrs) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    run_from_listener(listener).await
}

/// Run the duct proxy server from an already-bound listener.
/// Useful for tests that bind to a random port.
pub async fn run_from_listener(listener: TcpListener) -> Result<()> {
    tracing::info!("duct listening on {}", listener.local_addr()?);
    loop {
        let (stream, peer_addr) = listener.accept().await?;
        tracing::debug!(%peer_addr, "new connection");
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream).await {
                tracing::warn!(%peer_addr, error = %e, "connection error");
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream) -> Result<()> {
    // Read the request line byte by byte, enforcing a length limit.
    let mut line = String::new();
    loop {
        if line.len() >= MAX_REQUEST_LINE_BYTES {
            let _ = stream
                .write_all(b"HTTP/1.1 414 URI Too Long\r\n\r\n")
                .await;
            bail!("request line exceeds {MAX_REQUEST_LINE_BYTES} bytes");
        }
        let mut byte = [0u8; 1];
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return Ok(());
        }
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0] as char);
    }
    let line = line.trim_end_matches('\r').to_string();

    let (host, port) = match connect::parse_connect_request(&line) {
        Ok(pair) => pair,
        Err(e) => {
            tracing::warn!(error = %e, "invalid CONNECT request: {line}");
            let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
            return Err(e);
        }
    };

    // Consume remaining HTTP headers (up to and including the empty \r\n line)
    // to prevent header bytes from leaking into the upstream tunnel.
    // Keep a sliding window of the last 4 bytes to detect \r\n\r\n.
    let mut window = [0u8; 4];
    let mut window_len = 0usize;
    loop {
        if window_len >= MAX_REQUEST_LINE_BYTES {
            let _ = stream
                .write_all(b"HTTP/1.1 431 Request Header Fields Too Large\r\n\r\n")
                .await;
            bail!("request headers exceed {MAX_REQUEST_LINE_BYTES} bytes");
        }
        let mut byte = [0u8; 1];
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return Ok(());
        }
        // Shift window
        window[0] = window[1];
        window[1] = window[2];
        window[2] = window[3];
        window[3] = byte[0];
        window_len += 1;
        // Detect \r\n\r\n (must have at least 4 bytes accumulated)
        if window_len >= 4 && window == [b'\r', b'\n', b'\r', b'\n'] {
            break;
        }
    }

    // Pass stream (positioned right after \r\n\r\n) to the tunnel handler
    connect::handle_connect(stream, host, port).await
}
```

**Step 3: Run tests**

Run: `cargo test --test integration -- --test-threads=1`
Expected: All 4 tests PASS

Run: `cargo test -- --test-threads=1`
Expected: All 10 unit + 4 integration tests PASS

**Step 4: Commit**

```bash
git add src/lib.rs src/server.rs tests/integration.rs src/main.rs
git commit -m "feat: add server accept loop with integration tests"
```

---

### Task 5: main.rs — CLI entry point with tracing

**TDD scenario:** New feature — manual validation.

**Files:**
- Modify: `src/main.rs`

**Step 1: Update src/main.rs**

```rust
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "duct", version, about = "HTTP CONNECT proxy for WSL VPN bridge")]
struct Cli {
    /// Listening port
    #[arg(short, long, default_value_t = 1080)]
    port: u16,

    /// Listening address
    #[arg(short, long, default_value = "0.0.0.0")]
    bind: String,

    /// Enable debug-level logging
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let filter = if cli.verbose > 0 {
        "duct=debug"
    } else {
        "duct=info"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| filter.into()),
        )
        .init();

    let addr = format!("{}:{}", cli.bind, cli.port);
    tracing::info!(%addr, "starting duct");

    duct::server::run(&addr).await
}
```

**Step 2: Verify it compiles**

Run: `cargo build`
Expected: Build succeeds

Run: `cargo run -- --help`
Expected: Shows help with all options

Run: `cargo run -- --version`
Expected: `duct 0.1.0`

Run: `RUST_LOG=debug cargo run -- -p 19998` then Ctrl-C quickly
Expected: Logs "starting duct" at INFO, module debug logs at DEBUG when enabled via env

**Step 3: Commit**

```bash
git add src/main.rs
git commit -m "feat: add CLI entry point with clap and tracing"
```

---

### Verification Checklist

- [ ] `cargo build` — compiles clean
- [ ] `cargo test` — all 20 tests pass (16 unit + 4 integration)
- [ ] `cargo clippy --all-targets` — no warnings
- [ ] `cargo run -- --help` — shows CLI usage
- [ ] `cargo run -- --version` — shows `duct 0.1.0`
- [ ] `cargo run -- -p 10999` — starts and logs "starting duct"
- [ ] `curl -x http://127.0.0.1:10999 https://httpbin.org/get` — works (CONNECT tunnel)
- [ ] `curl -x http://127.0.0.1:10999 http://httpbin.org/get` — works (HTTP forward proxy)
- [ ] `curl -x http://127.0.0.1:10999 https://dmc.kso.net/` — works (internal domain via VPN disguise)

---

## 后续任务

### Task 6: HTTP 正向代理支持

**背景：** 浏览器插件（如 SwitchyOmega）配置为 HTTP 代理模式时，对 HTTP URL 发送 `GET http://host/path HTTP/1.1`（绝对 URL 形式），而非 CONNECT 隧道。原计划只支持 CONNECT，导致浏览器插件报 `expected CONNECT method, got: GET` 错误。

**Files:**
- Modify: `src/server.rs`
- Modify: `src/connect.rs`（将 `UPSTREAM_CONNECT_TIMEOUT` 改为 `pub`）

**实现要点：**

1. **`handle_connection` 路由分流：** 读取请求行后，根据 HTTP method 路由：
   - `CONNECT` → 现有隧道逻辑（消费 headers → `handle_connect`）
   - 其他方法（GET/POST/等）→ HTTP 正向代理逻辑

2. **`parse_proxy_url(line)`：** 从绝对 URL 提取 host:port
   - `GET http://dmc.kso.net/ HTTP/1.1` → `("dmc.kso.net", 80)`
   - `GET https://dmc.kso.net:8443/path HTTP/1.1` → `("dmc.kso.net", 8443)`
   - 默认端口：http → 80，https → 443

3. **`rebuild_request_line(line, host, port)`：** 将绝对 URL 重写为相对路径
   - `GET http://example.com/foo HTTP/1.1` → `GET /foo HTTP/1.1\r\n`

4. **HTTP 转发流程：**
   - 缓冲所有 headers（含 `\r\n\r\n` 终止符）
   - 连接上游服务器（带 10s 超时）
   - 发送重写后的请求行 + 缓冲的 headers
   - `copy_bidirectional` 处理请求体/响应

5. **错误处理：** 解析失败时发送 `400 Bad Request` 再返回错误

**新增单元测试：** 6 个（3 个 `parse_proxy_url` + 3 个 `rebuild_request_line`）

---

### Task 7: VPN 进程名伪装

**背景：** 调查发现 yunshu VPN 守护进程（`yunshu-daemon`）基于**进程名（argv[0]）**过滤 TCP 连接。只有白名单中的进程名才能访问内网地址（如 `dmc.kso.net:443`），其他进程的连接会在 ~60ms 内被服务器关闭（收到 TCP FIN）。

**白名单验证结果：**

| ✅ 允许 | ❌ 阻止 |
|---------|--------|
| curl, wget, python3, python, node, java, firefox, chrome | duct, ssh, socat, nc, bash, sh |

**Files:**
- Modify: `src/main.rs`

**实现要点：**

1. **`VPN_WHITELIST` 常量：** 定义允许通过的进程名列表

2. **自动 re-exec 逻辑：** 启动时检查当前进程名（argv[0] 的 basename）是否在白名单中：
   - 在白名单中 → 正常启动
   - 不在白名单中 → 使用 `std::os::unix::process::CommandExt::arg0()` re-exec 自身，将 argv[0] 设为伪装名

3. **CLI 参数：**
   - `--disguise <name>`：指定伪装名（默认 `curl`）
   - `--no-disguise`：跳过伪装（适用于已手动 symlink/重命名二进制的场景）

**核心代码：**
```rust
use std::os::unix::process::CommandExt;

if !VPN_WHITELIST.contains(&basename) {
    let exe = env::current_exe()?;
    let status = std::process::Command::new(&exe)
        .arg0(disguise)
        .args(env::args_os().skip(1))
        .status()?;
    std::process::exit(status.code().unwrap_or(1));
}
```
