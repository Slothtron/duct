use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use duct::auth::AuthConfig;

/// Start an HTTP echo server that reads a request and sends back a canned response.
/// Returns the port it's listening on.
async fn start_http_echo_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };
            tokio::spawn(async move {
                // Read the full HTTP request (headers + optional body)
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    return;
                }
                let request = String::from_utf8_lossy(&buf[..n]);

                // Extract method and path
                let first_line = request.lines().next().unwrap_or("");
                let parts: Vec<&str> = first_line.split_whitespace().collect();
                let method = *parts.first().unwrap_or(&"GET");
                let path = *parts.get(1).unwrap_or(&"/");

                // Send canned HTTP response
                let body = format!(
                    "{{ \"method\": \"{method}\", \"path\": \"{path}\" }}"
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
                    body.len(),
                    body
                );
                // write_all returns a Result, we ignore errors
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });
    port
}

/// Start a duct server on a random port and return its address.
async fn start_duct() -> std::net::SocketAddr {
    start_duct_with_auth(None).await
}

/// Start a duct server with optional authentication.
async fn start_duct_with_auth(auth: Option<AuthConfig>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        duct::server::run_from_listener(listener, auth).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}

// ── Existing CONNECT tests ──

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

    let duct_addr = start_duct().await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    let req = format!(
        "CONNECT 127.0.0.1:{} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\n\r\n",
        echo_addr.port(),
        echo_addr.port()
    );
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
    let duct_addr = start_duct().await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    conn.write_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
        .await
        .unwrap();

    let mut buf = [0u8; 1024];
    let n = conn.read(&mut buf).await.unwrap();
    assert!(String::from_utf8_lossy(&buf[..n]).contains("400 Bad Request"));
}

#[tokio::test]
async fn test_e2e_malformed_connect_request() {
    let duct_addr = start_duct().await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
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
    let duct_addr = start_duct().await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
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

// ── HTTP forward proxy tests ──

#[tokio::test]
async fn test_e2e_http_proxy_get() {
    let upstream_port = start_http_echo_server().await;
    let duct_addr = start_duct().await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    let req = format!(
        "GET http://127.0.0.1:{}/hello HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nUser-Agent: test\r\n\r\n",
        upstream_port, upstream_port
    );
    conn.write_all(req.as_bytes()).await.unwrap();

    // Read the full HTTP response
    let mut buf = vec![0u8; 4096];
    let n = conn.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    eprintln!("HTTP proxy GET response:\n{response}");

    assert!(
        response.contains("200 OK"),
        "expected 200 OK, got: {response}"
    );
    assert!(
        response.contains(r#""path": "/hello""#),
        "expected path /hello, got: {response}"
    );
    assert!(
        response.contains(r#""method": "GET""#),
        "expected method GET, got: {response}"
    );
}

#[tokio::test]
async fn test_e2e_http_proxy_post() {
    let upstream_port = start_http_echo_server().await;
    let duct_addr = start_duct().await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    let body = "key=value&name=test";
    let req = format!(
        "POST http://127.0.0.1:{}/api/data HTTP/1.1\r\n\
         Host: 127.0.0.1:{}\r\n\
         Content-Type: application/x-www-form-urlencoded\r\n\
         Content-Length: {}\r\n\
         \r\n\
         {}",
        upstream_port,
        upstream_port,
        body.len(),
        body
    );
    conn.write_all(req.as_bytes()).await.unwrap();

    // Read the full HTTP response
    let mut buf = vec![0u8; 4096];
    let n = conn.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    eprintln!("HTTP proxy POST response:\n{response}");

    assert!(
        response.contains("200 OK"),
        "expected 200 OK, got: {response}"
    );
    assert!(
        response.contains(r#""path": "/api/data""#),
        "expected path /api/data, got: {response}"
    );
}

#[tokio::test]
async fn test_e2e_http_proxy_malformed_url() {
    let duct_addr = start_duct().await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    // No absolute URL — should get 400
    conn.write_all(b"GET /just/a/path HTTP/1.1\r\nHost: example.com\r\n\r\n")
        .await
        .unwrap();

    let mut buf = [0u8; 1024];
    let n = conn.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("400 Bad Request"),
        "expected 400, got: {response}"
    );
}

#[tokio::test]
async fn test_e2e_http_proxy_unreachable_upstream() {
    let duct_addr = start_duct().await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    // Port 1 is unreachable
    let req = "GET http://127.0.0.1:1/test HTTP/1.1\r\nHost: 127.0.0.1:1\r\n\r\n";
    conn.write_all(req.as_bytes()).await.unwrap();

    let mut buf = [0u8; 1024];
    let n = conn.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("502") || response.contains("504"),
        "expected 502 or 504, got: {response}"
    );
}

#[tokio::test]
async fn test_e2e_http_proxy_with_query_string() {
    let upstream_port = start_http_echo_server().await;
    let duct_addr = start_duct().await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    let req = format!(
        "GET http://127.0.0.1:{}/search?q=rust&page=1 HTTP/1.1\r\nHost: 127.0.0.1:{}\r\n\r\n",
        upstream_port, upstream_port
    );
    conn.write_all(req.as_bytes()).await.unwrap();

    let mut buf = vec![0u8; 4096];
    let n = conn.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    eprintln!("HTTP proxy query string response:\n{response}");

    assert!(
        response.contains("200 OK"),
        "expected 200 OK, got: {response}"
    );
    // The rebuild should preserve the query string: /search?q=rust&page=1
    assert!(
        response.contains(r#""path": "/search"#),
        "expected path with /search, got: {response}"
    );
}

// ── Authentication tests ──

fn auth_header(username: &str, password: &str) -> String {
    let credentials = format!("{}:{}", username, password);
    let encoded = base64_simple(&credentials);
    format!("Proxy-Authorization: Basic {}", encoded)
}

fn base64_simple(input: &str) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut result = String::new();

    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;

        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);

        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

#[tokio::test]
async fn test_e2e_connect_valid_auth() {
    let auth = AuthConfig {
        username: "alice".to_string(),
        password: "p@ss123".to_string(),
    };
    let duct_addr = start_duct_with_auth(Some(auth)).await;

    let echo_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    let echo = tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        stream.write_all(&buf[..n]).await.unwrap();
    });

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    let req = format!(
        "CONNECT 127.0.0.1:{} HTTP/1.1\r\n\
         Host: 127.0.0.1:{}\r\n\
         {}\r\n\
         \r\n",
        echo_addr.port(),
        echo_addr.port(),
        auth_header("alice", "p@ss123")
    );
    conn.write_all(req.as_bytes()).await.unwrap();

    let mut buf = [0u8; 1024];
    let n = conn.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("200 Connection Established"),
        "expected 200, got: {response}"
    );

    conn.write_all(b"hello through auth tunnel").await.unwrap();
    let mut buf = [0u8; 1024];
    let n = conn.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"hello through auth tunnel");

    echo.await.unwrap();
}

#[tokio::test]
async fn test_e2e_connect_without_auth_returns_407() {
    let auth = AuthConfig {
        username: "alice".to_string(),
        password: "p@ss123".to_string(),
    };
    let duct_addr = start_duct_with_auth(Some(auth)).await;

    let echo_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    let req = format!(
        "CONNECT 127.0.0.1:{} HTTP/1.1\r\n\
         Host: 127.0.0.1:{}\r\n\
         \r\n",
        echo_addr.port(),
        echo_addr.port()
    );
    conn.write_all(req.as_bytes()).await.unwrap();

    let mut buf = [0u8; 1024];
    let n = conn.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("407"),
        "expected 407, got: {response}"
    );
    assert!(
        response.contains("Proxy Authentication Required"),
        "expected Proxy Authentication Required, got: {response}"
    );
}

#[tokio::test]
async fn test_e2e_connect_wrong_password_returns_407() {
    let auth = AuthConfig {
        username: "alice".to_string(),
        password: "p@ss123".to_string(),
    };
    let duct_addr = start_duct_with_auth(Some(auth)).await;

    let echo_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    let req = format!(
        "CONNECT 127.0.0.1:{} HTTP/1.1\r\n\
         Host: 127.0.0.1:{}\r\n\
         {}\r\n\
         \r\n",
        echo_addr.port(),
        echo_addr.port(),
        auth_header("alice", "wrongpass")
    );
    conn.write_all(req.as_bytes()).await.unwrap();

    let mut buf = [0u8; 1024];
    let n = conn.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("407"),
        "expected 407, got: {response}"
    );
}

#[tokio::test]
async fn test_e2e_http_proxy_valid_auth() {
    let auth = AuthConfig {
        username: "alice".to_string(),
        password: "p@ss123".to_string(),
    };
    let duct_addr = start_duct_with_auth(Some(auth)).await;
    let upstream_port = start_http_echo_server().await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    let req = format!(
        "GET http://127.0.0.1:{}/auth-test HTTP/1.1\r\n\
         Host: 127.0.0.1:{}\r\n\
         {}\r\n\
         \r\n",
        upstream_port,
        upstream_port,
        auth_header("alice", "p@ss123")
    );
    conn.write_all(req.as_bytes()).await.unwrap();

    let mut buf = vec![0u8; 4096];
    let n = conn.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("200 OK"),
        "expected 200 OK, got: {response}"
    );
}

#[tokio::test]
async fn test_e2e_http_proxy_without_auth_returns_407() {
    let auth = AuthConfig {
        username: "alice".to_string(),
        password: "p@ss123".to_string(),
    };
    let duct_addr = start_duct_with_auth(Some(auth)).await;
    let upstream_port = start_http_echo_server().await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    let req = format!(
        "GET http://127.0.0.1:{}/test HTTP/1.1\r\n\
         Host: 127.0.0.1:{}\r\n\
         \r\n",
        upstream_port, upstream_port
    );
    conn.write_all(req.as_bytes()).await.unwrap();

    let mut buf = [0u8; 1024];
    let n = conn.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("407"),
        "expected 407, got: {response}"
    );
}

#[tokio::test]
async fn test_e2e_auth_disabled_backward_compat() {
    // Without auth, existing behavior should still work
    let duct_addr = start_duct().await;  // no auth
    let upstream_port = start_http_echo_server().await;

    let mut conn = tokio::net::TcpStream::connect(duct_addr).await.unwrap();
    let req = format!(
        "GET http://127.0.0.1:{}/compat HTTP/1.1\r\n\
         Host: 127.0.0.1:{}\r\n\
         \r\n",
        upstream_port, upstream_port
    );
    conn.write_all(req.as_bytes()).await.unwrap();

    let mut buf = vec![0u8; 4096];
    let n = conn.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("200 OK"),
        "expected 200 OK (auth disabled), got: {response}"
    );
}
