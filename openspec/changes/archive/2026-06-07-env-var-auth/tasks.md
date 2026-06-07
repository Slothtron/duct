## 1. Update CLI struct — rename args + add env support

- [x] 1.1 Rename `--username` → `--user` with `#[arg(long, env = "DUCT_USER", requires = "passwd")]`
- [x] 1.2 Rename `--password` → `--passwd` with `#[arg(long, env = "DUCT_PASSWD", requires = "user")]`

## 2. Simplify auth construction

- [x] 2.1 Replace hand-written match/validation with `cli.user.zip(cli.passwd).map(|(u, p)| AuthConfig { ... })`
- [x] 2.2 Verify `cargo check` compiles clean

## 3. Update docs/deploy.md

- [x] 3.1 Update environment variable names to `DUCT_USER` / `DUCT_PASSWD`
- [x] 3.2 Update CLI examples from `--username`/`--password` to `--user`/`--passwd`
- [x] 3.3 Verify service example no longer passes credentials via CLI args

## 4. Run tests

- [x] 4.1 Run `cargo test` to confirm all 42 tests still pass