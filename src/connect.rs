use anyhow::{bail, Context, Result};
use std::time::Duration;
use tokio::io::{AsyncWriteExt, copy_bidirectional};
use tokio::net::TcpStream;/// Parse a CONNECT request line, extracting host and port.
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

/// Maximum time to wait for connection to upstream before returning 504.
pub const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

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

/// ── Tests for parse_connect_request ──────────────────────────
#[cfg(test)]
mod handle_connect_tests {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;

    #[tokio::test]
    async fn test_handle_connect_successful_tunnel() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();

        let upstream = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).await.unwrap();
            stream.write_all(&buf[..n]).await.unwrap();
        });

        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();

        let mut browser = TcpStream::connect(proxy_addr).await.unwrap();
        let (proxy_client, _) = proxy_listener.accept().await.unwrap();

        let handle = tokio::spawn(async move {
            handle_connect(proxy_client, "127.0.0.1", upstream_addr.port())
                .await
                .ok();
        });

        let mut buf = [0u8; 1024];
        let n = browser.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(
            response.contains("200 Connection Established"),
            "expected 200, got: {response}"
        );

        browser.write_all(b"hello through tunnel").await.unwrap();
        let mut buf = [0u8; 1024];
        let n = browser.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello through tunnel");

        drop(browser);

        upstream.await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_handle_connect_sends_502_on_unreachable() {
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();

        let mut browser = TcpStream::connect(proxy_addr).await.unwrap();
        let (proxy_client, _) = proxy_listener.accept().await.unwrap();

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
}
