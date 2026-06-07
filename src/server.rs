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
