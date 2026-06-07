## ADDED Requirements

### Requirement: Proxy authentication via CLI credentials

The proxy SHALL support HTTP Basic authentication using credentials provided via CLI arguments `--username` and `--password`. When either argument is provided, the proxy MUST require authentication on all incoming proxy requests (both CONNECT and HTTP forward proxy). If neither argument is provided, authentication MUST be disabled (existing behavior).

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

#### Scenario: Auth disabled (no --username/--password) works as before
- **WHEN** a client sends a CONNECT or HTTP proxy request without `Proxy-Authorization` (and auth is disabled)
- **THEN** the proxy SHALL process the request normally without checking credentials

### Requirement: Password passed via CLI as plaintext string

The `--password` argument SHALL accept the password as a plaintext string directly on the command line.

#### Scenario: Password provided via CLI
- **WHEN** a user runs `duct --username alice --password p@ss123`
- **THEN** the proxy SHALL accept requests with `Proxy-Authorization: Basic base64("alice:p@ss123")`

#### Scenario: Only one credential provided
- **WHEN** a user provides `--username` without `--password`, or `--password` without `--username`
- **THEN** the proxy SHALL print an error and exit

### Requirement: Disguise mode is opt-in

The `--disguise` argument SHALL be optional. When not provided, the proxy SHALL run without any process name disguise. When provided, the proxy SHALL re-exec itself with argv[0] set to the specified name. The `--no-disguise` flag SHALL be removed.

#### Scenario: No disguise argument runs normally
- **WHEN** a user runs `duct` without `--disguise`
- **THEN** the proxy SHALL start without re-execing or modifying argv[0]

#### Scenario: Disguise with a specified name
- **WHEN** a user runs `duct --disguise curl`
- **THEN** the proxy SHALL re-exec itself with argv[0] set to `curl`

#### Scenario: Disguise with an invalid name
- **WHEN** a user runs `duct --disguise ""`
- **THEN** the proxy SHALL treat it the same as not providing `--disguise` (or print an error)
