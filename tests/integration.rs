use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;

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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        duct::server::run_from_listener(listener).await.unwrap();
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
