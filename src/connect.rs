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
