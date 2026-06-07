## 1. Refactor disguise — make it opt-in, remove old logic

- [x] 1.1 Change `--disguise` from `String` to `Option<String>` in CLI struct
- [x] 1.2 Remove `--no-disguise` flag
- [x] 1.3 Remove `ALLOWED_NAMES` constant and the automatic argv[0] checking logic
- [x] 1.4 Replace disguise block with simple `if let Some(name) = cli.disguise { re-exec }` pattern
- [x] 1.5 Update `cargo test` to confirm all tests still pass (existing behavior unchanged for non-disguise code paths)
- [x] 1.6 Update README.md disguise section to reflect opt-in behavior

## 2. Create auth module

- [x] 2.1 Create `src/auth.rs` with `AuthConfig` struct and `check()` function
- [x] 2.2 Implement base64 decoding (manual, ~40 lines, RFC 4648)
- [x] 2.3 Implement `parse_proxy_authorization()` to extract username/password from HTTP header
- [x] 2.4 Write unit tests for base64 decode, header parsing, and auth check
- [x] 2.5 Export `pub mod auth;` from `src/lib.rs`

## 3. Wire authentication into server

- [x] 3.1 Pass `Option<AuthConfig>` from main through `server::run()` → `run_from_listener()` → `handle_connection()`
- [x] 3.2 In `handle_connection()`, add auth check block: if auth enabled, read headers, extract `Proxy-Authorization`, validate or return 407
- [x] 3.3 Wire auth into CONNECT path (read headers before discarding them)
- [x] 3.4 Wire auth into HTTP forward proxy path (auth check before forwarding)
- [x] 3.5 Ensure `handle_connect()` **unchanged** — stream position after auth check is the same as before

## 4. Add CLI auth arguments

- [x] 4.1 Add `--username <USER>` and `--password <PASS>` to CLI struct
- [x] 4.2 Add validation: both or neither must be provided; error and exit otherwise
- [x] 4.3 Construct `AuthConfig` from CLI args and pass to `server::run()`

## 5. Write integration tests for auth

- [x] 5.1 Test: CONNECT with valid auth → 200 + tunnel works
- [x] 5.2 Test: CONNECT without auth → 407
- [x] 5.3 Test: CONNECT with wrong password → 407
- [x] 5.4 Test: HTTP forward proxy with valid auth → 200 + response
- [x] 5.5 Test: HTTP forward proxy without auth → 407
- [x] 5.6 Test: auth disabled → existing behavior (backward compat)
