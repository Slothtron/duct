# duct HTTP CONNECT Proxy Implementation Plan

> **REQUIRED SUB-SKILL:** Use the executing-plans skill to implement this plan task-by-task.

**Goal:** Build a minimal HTTP CONNECT proxy that runs in WSL2, forwarding TCP traffic through yunshu VPN for internal domain access.

**Architecture:** Single-binary Rust project using tokio async runtime. Three modules: `main.rs` (CLI + tracing setup), `server.rs` (TCP accept loop), `connect.rs` (CONNECT tunnel logic + request parsing). No HTTP parsing library — manual CONNECT line parsing.

**Tech Stack:** Rust 2024 edition, tokio (net + io-util + rt-multi-thread), clap derive, tracing + tracing-subscriber, anyhow.

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

[dependencies]
tokio = { version = "1", features = ["net", "io-util", "macros", "rt-multi-thread"] }
clap = { version = "4", features = ["derive"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
anyhow = "1"
```

**Step 2: Write src/main.rs**

```rust
mod connect;
mod server;

fn main() {
    println!("Hello, world!");
}
```

**Step 3: Create empty module files**

`src/connect.rs`:
```rust
// CONNECT tunnel logic
```

`src/server.rs`:
```rust
// TCP accept loop
```

**Step 4: Verify it compiles**

Run: `cargo check`
Expected: `Compiling duct v0.1.0 ... Finished 'dev' profile [unoptimized + debuginfo]`

**Step 5: Commit**

```bash
git add Cargo.toml src/main.rs src/connect.rs src/server.rs
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

    let mut parts = line.splitn(3, char::is_whitespace).filter(|s| !s.is_empty());
    let method = parts.next().context("missing method")?;
    if method != "CONNECT" {
        bail!("expected CONNECT method, got: {method}");
    }

    let authority = parts.next().context("missing authority")?;
    let (host, port_str) = authority
        .rsplit_once(':')
        .context("missing port in authority")?;

    if host.is_empty() {
        bail!("empty host");
    }
    let port: u16 = port_str.parse().context("invalid port number")?;

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
        response.contains("502 Bad Gateway"),
        "expected 502, got: {response}"
    );

    handle.await.unwrap();
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -- connect::tests -v -- --test-threads=1`
Expected: FAIL — `handle_connect` not defined

**Step 3: Write handle_connect implementation**

Add to `src/connect.rs` (before the tests module):

```rust
use tokio::io::{AsyncWriteExt, copy_bidirectional};
use tokio::net::TcpStream;

/// Handle a CONNECT tunnel: connect to upstream, send 200, then bidirectional copy.
/// Drops the connection if any step fails.
pub async fn handle_connect(mut client: TcpStream, host: &str, port: u16) -> Result<()> {
    let addr = format!("{host}:{port}");
    let upstream = match TcpStream::connect(&addr).await {
        Ok(stream) => stream,
        Err(e) => {
            tracing::error!(%addr, error = %e, "failed to connect to upstream");
            let _ = client
                .write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                .await;
            return Err(e.into());
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
        Ok((to_client, to_upstream)) => {
            tracing::info!(%addr, to_client_bytes = to_client, to_upstream_bytes = to_upstream, "tunnel closed");
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
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn start_duct() -> SocketAddr {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr_clone = addr;
    tokio::spawn(async move {
        duct::server::run(addr_clone).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Connect to find out what port was assigned
    // Since duct binds to :0, we need a different approach:
    // Use a known port or find the assigned port
    unimplemented!("need to figure out how to get the bound port")
}
```

Wait — the integration test starts duct with `:0` but can't know which port was assigned. Need a different approach. Options:
1. Make `run()` return the listener so we can extract local_addr()
2. Pass an already-bound listener
3. Use a helper function that binds first, returns addr, then spawns run

Option 3 is cleanest:

```rust
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn start_duct() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        duct::server::run_from_listener(listener).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}
```

This requires adding a `run_from_listener` function to `server.rs`. Keeps it simple.

Update `server.rs`:

```rust
use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::connect;

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
```

Then integration test uses `run_from_listener`:

```rust
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
    let req = format!("CONNECT 127.0.0.1:{} HTTP/1.1\r\n\r\n", echo_addr.port());
    conn.write_all(req.as_bytes()).await.unwrap();

    let mut buf = [0u8; 1024];
    let n = conn.read(&mut buf).await.unwrap();
    assert!(String::from_utf8_lossy(&buf[..n]).contains("200 Connection Established"));

    conn.write_all(b"hello").await.unwrap();
    let mut buf = [0u8; 1024];
    let n = conn.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"hello");

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

// ... similar for malformed and unreachable tests
```

**Step 2: Write handle_connection in server.rs**

```rust
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::connect;

async fn handle_connection(mut stream: TcpStream) -> Result<()> {
    // Read byte by byte until \n
    let mut line_bytes = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return Ok(());
        }
        if byte[0] == b'\n' {
            break;
        }
        line_bytes.push(byte[0]);
    }

    let line = String::from_utf8_lossy(&line_bytes);
    let line = line.trim_end_matches('\r');

    let (host, port) = match connect::parse_connect_request(line) {
        Ok(pair) => pair,
        Err(e) => {
            tracing::warn!(error = %e, "invalid CONNECT request: {line}");
            let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
            return Err(e);
        }
    };

    connect::handle_connect(stream, host, port).await
}
```

**Step 3: Run tests**

Run: `cargo test --test integration -v -- --test-threads=1`
Expected: All 4 tests PASS

Run: `cargo test -v -- --test-threads=1`
Expected: All unit + integration tests PASS

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
        .with_env_filter(filter)
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

**Step 3: Commit**

```bash
git add src/main.rs
git commit -m "feat: add CLI entry point with clap and tracing"
```

---

### Verification Checklist

- [ ] `cargo build` — compiles clean
- [ ] `cargo test` — all tests pass
- [ ] `cargo clippy` — no warnings
- [ ] `cargo run -- --help` — shows CLI usage
- [ ] `cargo run -- --version` — shows `duct 0.1.0`
- [ ] `cargo run -- -p 10999` — starts and logs "starting duct"
- [ ] `curl -x http://127.0.0.1:10999 https://httpbin.org/get` — works in WSL
