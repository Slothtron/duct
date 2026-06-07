## MODIFIED Requirements

### Requirement: Proxy authentication via CLI credentials

**FROM:** The proxy SHALL support HTTP Basic authentication using credentials provided via CLI arguments `--username` and `--password`.

**TO:** The proxy SHALL support HTTP Basic authentication using credentials provided via CLI arguments `--user` and `--passwd`.

#### Scenario: Authenticated CONNECT request succeeds
- **WHEN** a client sends a CONNECT request with a valid `Proxy-Authorization: Basic <base64>` header
- **THEN** the proxy SHALL establish the tunnel and return `200 Connection Established`

#### Scenario: Unauthenticated CONNECT request receives 407
- **WHEN** a client sends a CONNECT request without `Proxy-Authorization` header (and auth is enabled)
- **THEN** the proxy SHALL return `HTTP/1.1 407 Proxy Authentication Required` with a `Proxy-Authenticate: Basic realm="duct"` header

#### Scenario: Invalid credentials on CONNECT returns 407
- **WHEN** a client sends a CONNECT request with an invalid `Proxy-Authorization` header (wrong username or password)
- **THEN** the proxy SHALL return `HTTP/1.1 407 Proxy Authentication Required`

#### Scenario: Authenticated HTTP forward proxy request succeeds
- **WHEN** a client sends an HTTP GET/POST request with a valid `Proxy-Authorization` header
- **THEN** the proxy SHALL forward the request to the upstream server

#### Scenario: Unauthenticated HTTP forward proxy request returns 407
- **WHEN** a client sends an HTTP GET/POST request without `Proxy-Authorization` header (and auth is enabled)
- **THEN** the proxy SHALL return `HTTP/1.1 407 Proxy Authentication Required`

#### Scenario: Auth disabled works as before
- **WHEN** a client sends a CONNECT or HTTP proxy request without `Proxy-Authorization` (and auth is disabled)
- **THEN** the proxy SHALL process the request normally without checking credentials

### Requirement: Credentials via CLI or environment

**FROM:** The `--password` argument SHALL accept the password as a plaintext string directly on the command line.

**TO:** Credentials SHALL be provided via either CLI arguments (`--user`/`--passwd`) or environment variables (`DUCT_USER`/`DUCT_PASSWD`). CLI arguments take precedence over environment variables.

#### Scenario: Password provided via CLI
- **WHEN** a user runs `duct --user alice --passwd p@ss123`
- **THEN** the proxy SHALL accept requests with `Proxy-Authorization: Basic base64("alice:p@ss123")`

#### Scenario: Password provided via env vars
- **WHEN** a user sets `DUCT_USER=alice` `DUCT_PASSWD=p@ss123` and runs `duct` without `--user`/`--passwd`
- **THEN** the proxy SHALL accept requests with `Proxy-Authorization: Basic base64("alice:p@ss123")`

#### Scenario: Only one credential provided
- **WHEN** a user provides `--user` without `--passwd`, or `--passwd` without `--user`
- **THEN** clap SHALL report an error and exit