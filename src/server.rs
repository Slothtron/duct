use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

use crate::auth::{self, AuthConfig};
use crate::connect;

/// Maximum length of the request line before returning 414.
const MAX_REQUEST_LINE_BYTES: usize = 8192;

/// Maximum total request size (request line + headers) we'll buffer for HTTP proxy.
const MAX_HTTP_REQUEST_BYTES: usize = 65536;

/// Run the duct proxy server, binding to the given address.
pub async fn run(addr: impl tokio::net::ToSocketAddrs, auth: Option<AuthConfig>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    run_from_listener(listener, auth).await
}

/// Run the duct proxy server from an already-bound listener.
/// Useful for tests that bind to a random port.
pub async fn run_from_listener(listener: TcpListener, auth: Option<AuthConfig>) -> Result<()> {
    tracing::info!("duct listening on {}", listener.local_addr()?);
    loop {
        let (stream, peer_addr) = listener.accept().await?;
        tracing::debug!(%peer_addr, "new connection");
        let auth = auth.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, auth.as_ref()).await {
                tracing::warn!(%peer_addr, error = %e, "connection error");
            }
        });
    }
}

/// Parse the absolute URL from an HTTP forward proxy request line.
/// E.g. `GET http://example.com/path HTTP/1.1` → host: example.com, port: 80
fn parse_proxy_url(line: &str) -> Result<(&str, u16)> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 2 {
        bail!("invalid request line");
    }
    let url = parts[1];
    let without_scheme = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .context("unsupported URL scheme in proxy request")?;
    let (host_port, _path) = without_scheme
        .split_once('/')
        .unwrap_or((without_scheme, ""));

    if let Some((host, port_str)) = host_port.rsplit_once(':') {
        let port: u16 = port_str.parse().context("invalid port number")?;
        Ok((host, port))
    } else {
        let port = if url.starts_with("https://") { 443 } else { 80 };
        Ok((host_port, port))
    }
}

/// Read a single line (ending with \n) from stream, enforcing a length limit.
async fn read_line(stream: &mut TcpStream, max_bytes: usize) -> Result<String> {
    let mut line = String::new();
    loop {
        if line.len() >= max_bytes {
            bail!("line exceeds {max_bytes} bytes");
        }
        let mut byte = [0u8; 1];
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return Ok(line);
        }
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0] as char);
    }
    Ok(line.trim_end_matches('\r').to_string())
}

/// Read all HTTP headers into a buffer (up to and including the empty line).
/// Returns the buffered header bytes (including trailing \r\n\r\n).
async fn read_headers(stream: &mut TcpStream, max_bytes: usize) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut window = [0u8; 4];
    let mut window_len = 0usize;

    loop {
        if buf.len() >= max_bytes {
            bail!("headers exceed {max_bytes} bytes");
        }
        let mut byte = [0u8; 1];
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return Ok(buf);
        }
        buf.push(byte[0]);
        window[0] = window[1];
        window[1] = window[2];
        window[2] = window[3];
        window[3] = byte[0];
        window_len += 1;
        if window_len >= 4 && window == *b"\r\n\r\n" {
            break;
        }
    }
    Ok(buf)
}

async fn handle_connection(mut stream: TcpStream, auth: Option<&AuthConfig>) -> Result<()> {
    let line = read_line(&mut stream, MAX_REQUEST_LINE_BYTES)
        .await
        .context("failed to read request line")?;
    if line.is_empty() {
        return Ok(());
    }

    let method = line.split_whitespace().next().unwrap_or("");
    if method != "CONNECT" {
        let uri = line.split_whitespace().nth(1).unwrap_or("");
        if method == "GET" && uri == "/healthz" {
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok")
                .await;
            return Ok(());
        }
        if uri.starts_with('/') && !uri.contains("://") {
            let _ = stream
                .write_all(
                    b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                )
                .await;
            bail!("relative path requests are not supported by the forward proxy");
        }
    }

    match method {
        "CONNECT" => {
            let (host, port) = match connect::parse_connect_request(&line) {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::warn!(error = %e, "invalid CONNECT request: {line}");
                    let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
                    return Err(e);
                }
            };
            let headers = read_headers(&mut stream, MAX_HTTP_REQUEST_BYTES).await?;
            if auth.is_some_and(|auth_config| !check_auth(auth_config, &headers)) {
                let _ = stream
                    .write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"duct\"\r\n\r\n")
                    .await;
                bail!("authentication failed");
            }
            connect::handle_connect(stream, host, port).await
        }
        _ => {
            let (host, port) = match parse_proxy_url(&line) {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::warn!(error = %e, "invalid proxy request: {line}");
                    let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
                    return Err(e);
                }
            };
            let headers = read_headers(&mut stream, MAX_HTTP_REQUEST_BYTES).await?;
            if auth.is_some_and(|auth_config| !check_auth(auth_config, &headers)) {
                let _ = stream
                    .write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"duct\"\r\n\r\n")
                    .await;
                bail!("authentication failed");
            }
            let request_line = rebuild_request_line(&line)?;
            let addr = format!("{host}:{port}");
            let mut upstream = match tokio::time::timeout(
                connect::UPSTREAM_CONNECT_TIMEOUT,
                TcpStream::connect(&addr),
            )
            .await
            {
                Ok(Ok(stream)) => stream,
                Ok(Err(e)) => {
                    tracing::error!(%addr, error = %e, "failed to connect to upstream");
                    let _ = stream.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
                    return Err(e.into());
                }
                Err(_elapsed) => {
                    tracing::error!(%addr, "upstream connection timed out");
                    let _ = stream
                        .write_all(b"HTTP/1.1 504 Gateway Timeout\r\n\r\n")
                        .await;
                    return Err(anyhow::anyhow!(
                        "upstream connection timed out after {:?}",
                        connect::UPSTREAM_CONNECT_TIMEOUT
                    ));
                }
            };

            tracing::info!(%addr, method, "forwarding HTTP request");
            upstream.write_all(request_line.as_bytes()).await?;
            upstream.write_all(&headers).await?;
            let _ = stream.set_nodelay(true);
            let _ = upstream.set_nodelay(true);
            match tokio::io::copy_bidirectional(&mut stream, &mut upstream).await {
                Ok((to_upstream, to_client)) => {
                    tracing::info!(%addr, to_client_bytes = to_client, to_upstream_bytes = to_upstream, "HTTP proxy request complete");
                    Ok(())
                }
                Err(e) => {
                    tracing::warn!(%addr, error = %e, "HTTP proxy copy error");
                    Err(e.into())
                }
            }
        }
    }
}

/// Check authentication against raw HTTP headers.
/// Returns true if auth passes or if not required.
fn check_auth(config: &AuthConfig, headers: &[u8]) -> bool {
    let header_str = String::from_utf8_lossy(headers);
    for line in header_str.lines() {
        if !line.to_lowercase().starts_with("proxy-authorization:") {
            continue;
        }
        let parsed = line
            .split_once(':')
            .and_then(|(_, value)| auth::parse_proxy_authorization(value.trim()));
        if let Some((user, pass)) = parsed {
            return auth::check(config, &user, &pass);
        }
    }
    false
}

/// Rebuild the request line from an absolute URL to a relative one.
/// `GET http://example.com/path HTTP/1.1` → `GET /path HTTP/1.1`
fn rebuild_request_line(line: &str) -> Result<String> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 3 {
        bail!("invalid request line");
    }
    let method = parts[0];
    let url = parts[1];
    let version = parts[2];
    let path = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .map(|rest| rest.find('/').map(|i| &rest[i..]).unwrap_or("/"))
        .unwrap_or("/");
    let full_path = if let Some(qi) = url.find('?') {
        let query = &url[qi..];
        if !path.contains('?') {
            format!("{path}{query}")
        } else {
            path.to_string()
        }
    } else {
        path.to_string()
    };
    Ok(format!("{method} {full_path} {version}\r\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_proxy_url_http() {
        let (host, port) = parse_proxy_url("GET http://example.com/ HTTP/1.1").unwrap();
        assert_eq!(host, "example.com");
        assert_eq!(port, 80);
    }

    #[test]
    fn test_parse_proxy_url_https() {
        let (host, port) = parse_proxy_url("GET https://example.com:8443/path HTTP/1.1").unwrap();
        assert_eq!(host, "example.com");
        assert_eq!(port, 8443);
    }

    #[test]
    fn test_parse_proxy_url_default_port_https() {
        let (host, port) = parse_proxy_url("GET https://example.com/path HTTP/1.1").unwrap();
        assert_eq!(host, "example.com");
        assert_eq!(port, 443);
    }

    #[test]
    fn test_rebuild_request_line_basic() {
        assert_eq!(
            rebuild_request_line("GET http://example.com/foo HTTP/1.1").unwrap(),
            "GET /foo HTTP/1.1\r\n"
        );
    }

    #[test]
    fn test_rebuild_request_line_root() {
        assert_eq!(
            rebuild_request_line("GET http://example.com/ HTTP/1.1").unwrap(),
            "GET / HTTP/1.1\r\n"
        );
    }

    #[test]
    fn test_rebuild_request_line_no_slash() {
        assert_eq!(
            rebuild_request_line("GET http://example.com HTTP/1.1").unwrap(),
            "GET / HTTP/1.1\r\n"
        );
    }
}
