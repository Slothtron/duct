/// HTTP Basic proxy authentication support.
///
/// Provides credential checking and `Proxy-Authorization` header parsing
/// for HTTP CONNECT and HTTP forward proxy requests.
/// Configuration for proxy authentication.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub username: String,
    pub password: String,
}

/// Check if the given credentials match the configured auth.
pub fn check(config: &AuthConfig, username: &str, password: &str) -> bool {
    config.username == username && config.password == password
}

/// Parse a `Proxy-Authorization` header value.
/// Expected format: `Basic base64(username:password)`
pub fn parse_proxy_authorization(header_value: &str) -> Option<(String, String)> {
    let header_value = header_value.trim();

    // Must start with "Basic " (case-insensitive)
    let encoded = match header_value.strip_prefix("Basic ") {
        Some(rest) => rest,
        None => header_value.strip_prefix("basic ")?,
    };

    let decoded = decode_base64(encoded.trim())?;

    // Split on the first ':'
    let colon_pos = decoded.find(':')?;
    let username = decoded[..colon_pos].to_string();
    let password = decoded[colon_pos + 1..].to_string();

    Some((username, password))
}

/// Decode a base64-encoded string (RFC 4648).
/// Returns `None` on invalid input.
fn decode_base64(input: &str) -> Option<String> {
    const DECODE_TABLE: [i8; 128] = {
        let mut table = [-1i8; 128];
        let mut i = 0;
        // A-Z
        while i < 26 {
            table[b'A' as usize + i] = i as i8;
            i += 1;
        }
        // a-z
        i = 0;
        while i < 26 {
            table[b'a' as usize + i] = (i + 26) as i8;
            i += 1;
        }
        // 0-9
        i = 0;
        while i < 10 {
            table[b'0' as usize + i] = (i + 52) as i8;
            i += 1;
        }
        table[b'+' as usize] = 62;
        table[b'/' as usize] = 63;
        table
    };

    // Strip padding and whitespace
    let cleaned: Vec<u8> = input
        .bytes()
        .filter(|&b| b != b'=' && b != b'\r' && b != b'\n' && b != b' ' && b != b'\t')
        .collect();

    if cleaned.is_empty() {
        return None;
    }

    // All characters must be valid base64
    for &b in &cleaned {
        if b >= 128 || DECODE_TABLE[b as usize] == -1 {
            return None;
        }
    }

    let mut result = Vec::with_capacity(cleaned.len() / 4 * 3);

    for chunk in cleaned.chunks(4) {
        let mut buf: u32 = 0;
        let mut valid_bits = 0;

        for (i, &byte) in chunk.iter().enumerate() {
            let val = DECODE_TABLE[byte as usize] as u32;
            buf |= val << (18 - i * 6);
            valid_bits += 6;
        }

        // Extract 1-3 bytes depending on valid_bits
        for i in 0..valid_bits / 8 {
            result.push((buf >> (16 - i * 8)) as u8);
        }
    }

    String::from_utf8(result).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_base64_simple() {
        // "hello" in base64
        assert_eq!(decode_base64("aGVsbG8=").as_deref(), Some("hello"));
    }

    #[test]
    fn test_decode_base64_no_padding() {
        assert_eq!(decode_base64("aGVsbG8").as_deref(), Some("hello"));
    }

    #[test]
    fn test_decode_base64_empty() {
        assert!(decode_base64("").is_none());
    }

    #[test]
    fn test_decode_base64_invalid_char() {
        assert!(decode_base64("aGVs!!!!bG8=").is_none());
    }

    #[test]
    fn test_parse_proxy_authorization_valid() {
        // "alice:p@ss123" in base64
        let header = "Basic YWxpY2U6cEBzczEyMw==";
        let (user, pass) = parse_proxy_authorization(header).unwrap();
        assert_eq!(user, "alice");
        assert_eq!(pass, "p@ss123");
    }

    #[test]
    fn test_parse_proxy_authorization_colon_in_password() {
        // "bob:pass:word" in base64
        let header = "Basic Ym9iOnBhc3M6d29yZA==";
        let (user, pass) = parse_proxy_authorization(header).unwrap();
        assert_eq!(user, "bob");
        assert_eq!(pass, "pass:word");
    }

    #[test]
    fn test_parse_proxy_authorization_no_basic() {
        let result = parse_proxy_authorization("Bearer token123");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_proxy_authorization_empty() {
        let result = parse_proxy_authorization("");
        assert!(result.is_none());
    }

    #[test]
    fn test_check_valid() {
        let config = AuthConfig {
            username: "alice".to_string(),
            password: "p@ss123".to_string(),
        };
        assert!(check(&config, "alice", "p@ss123"));
    }

    #[test]
    fn test_check_invalid_password() {
        let config = AuthConfig {
            username: "alice".to_string(),
            password: "p@ss123".to_string(),
        };
        assert!(!check(&config, "alice", "wrongpass"));
    }

    #[test]
    fn test_check_invalid_username() {
        let config = AuthConfig {
            username: "alice".to_string(),
            password: "p@ss123".to_string(),
        };
        assert!(!check(&config, "bob", "p@ss123"));
    }
}
