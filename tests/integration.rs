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
    conn.write_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
        .await
        .unwrap();

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
    // Port 1 is reserved and should be unreachable or time out
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
