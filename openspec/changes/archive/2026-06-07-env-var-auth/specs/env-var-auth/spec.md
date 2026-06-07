## ADDED Requirements

### Requirement: Credentials via environment variables

The proxy SHALL support providing HTTP Basic authentication credentials via environment variables `DUCT_USER` and `DUCT_PASSWD`. When both environment variables are set, the proxy MUST enable authentication. When neither is set and no CLI arguments are provided, authentication MUST remain disabled.

#### Scenario: Auth enabled via env vars
- **WHEN** a user sets `DUCT_USER` and `DUCT_PASSWD` environment variables without CLI `--user`/`--passwd`
- **THEN** the proxy SHALL require authentication with the provided credentials

#### Scenario: Env vars absent defaults to no auth
- **WHEN** a user runs `duct` without `DUCT_USER`/`DUCT_PASSWD` and without `--user`/`--passwd`
- **THEN** the proxy SHALL start without authentication

#### Scenario: CLI overrides env vars
- **WHEN** a user sets `DUCT_USER=wrong` `DUCT_PASSWD=wrong` but also passes `--user alice --passwd p@ss123`
- **THEN** the proxy SHALL use the CLI-provided credentials (`alice`/`p@ss123`), not the environment values

#### Scenario: Only one env var is set
- **WHEN** a user sets `DUCT_USER=alice` but not `DUCT_PASSWD` (or vice versa)
- **THEN** clap SHALL report an error and exit, since `--user` requires `--passwd` (and env vars map to these CLI args)

### Requirement: Credentials never appear in process argv

When using environment variables, the credentials MUST NOT appear in the process command line (`ps -ef` / `/proc/PID/cmdline`).

#### Scenario: Env var credentials not visible in ps
- **WHEN** `DUCT_USER=alice` `DUCT_PASSWD=p@ss123` are set and duct is started
- **THEN** the process argv (`ps -ef`) SHALL contain only the binary path and any other CLI flags, but NOT the credentials

### Requirement: Renamed CLI arguments

The CLI arguments for authentication SHALL be renamed from `--username`/`--password` to `--user`/`--passwd`. The new names MUST also be accepted as environment variables.

#### Scenario: New CLI argument names
- **WHEN** a user runs `duct --user alice --passwd p@ss123`
- **THEN** the proxy SHALL enable authentication with the provided credentials

#### Scenario: Old argument names are not accepted
- **WHEN** a user runs `duct --username alice --password p@ss123`
- **THEN** clap SHALL report an error (unknown argument) and exit