use anyhow::Context as _;
use clap::Parser;
use std::env;
use std::os::unix::process::CommandExt;

use duct::auth::AuthConfig;

#[derive(Parser, Debug)]
#[command(name = "duct", version, about = "Lightweight HTTP/HTTPS proxy with process name disguise")]
struct Cli {
    /// Listening port
    #[arg(short, long, default_value_t = 11088)]
    port: u16,

    /// Listening address
    #[arg(short, long, default_value = "0.0.0.0")]
    bind: String,

    /// Enable debug-level logging
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Process name to use when re-execing for argv[0] filtering compatibility.
    /// When not provided, no disguise is performed.
    #[arg(long)]
    disguise: Option<String>,

    /// Username for HTTP Basic proxy authentication.
    /// Can also be set via DUCT_USER environment variable.
    /// Must be used together with --passwd.
    #[arg(long, env = "DUCT_USER", requires = "passwd")]
    user: Option<String>,

    /// Password for HTTP Basic proxy authentication.
    /// Can also be set via DUCT_PASSWD environment variable.
    /// Must be used together with --user.
    #[arg(long, env = "DUCT_PASSWD", requires = "user")]
    passwd: Option<String>,

    /// Path to the aiproxy provider config (YAML).
    /// When omitted, defaults to ~/.config/duct/config.yaml;
    /// a missing default file disables the aiproxy feature.
    #[arg(long)]
    config_file: Option<String>,

    /// Maximum request body size in bytes forwarded by aiproxy.
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    max_body: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Construct auth config from CLI args or environment variables.
    // clap's `requires` ensures both --user and --passwd (or their env counterparts)
    // are provided together, or neither is provided.
    let auth = cli
        .user
        .zip(cli.passwd)
        .map(|(user, passwd)| AuthConfig {
            username: user,
            password: passwd,
        });

    // ── Process name disguise (opt-in) ──
    // Some environments filter TCP connections by the originating process name
    // (argv[0]). When --disguise is provided, re-exec with argv[0] set to the
    // specified name.
    if let Some(ref disguise) = cli.disguise {
        let exe = env::current_exe()?;

        // Filter out --disguise (and its value) from args to prevent infinite re-exec
        let filtered_args: Vec<_> = {
            let mut skip_next = false;
            env::args_os().skip(1).filter(|a| {
                if skip_next {
                    skip_next = false;
                    return false;
                }
                if a == "--disguise" {
                    skip_next = true;
                    return false;
                }
                true
            }).collect()
        };

        tracing::info!(%disguise, "re-execing with disguised process name");
        let status = std::process::Command::new(&exe)
            .arg0(disguise)
            .args(&filtered_args)
            .status()?;
        std::process::exit(status.code().unwrap_or(1));
    }

    let filter = if cli.verbose > 0 {
        "duct=debug"
    } else {
        "duct=info"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| filter.into()),
        )
        .init();

    let addr = format!("{}:{}", cli.bind, cli.port);

    // ── aiproxy 配置装载（三层语义，§6.2）──
    let config = match &cli.config_file {
        Some(path) => {
            let cfg = duct::config::Config::load_explicit(std::path::Path::new(path))
                .with_context(|| format!("加载 aiproxy 配置失败: {path}"))?;
            tracing::info!(
                providers = cfg.len(),
                ids = %cfg.provider_ids().join(","),
                "aiproxy enabled"
            );
            cfg
        }
        None => match duct::config::Config::load_default()? {
            Some(cfg) => {
                tracing::info!(
                    providers = cfg.len(),
                    ids = %cfg.provider_ids().join(","),
                    "aiproxy enabled"
                );
                cfg
            }
            None => {
                tracing::info!("aiproxy disabled (no provider config found)");
                duct::config::Config::default()
            }
        },
    };

    let state = duct::aiproxy::AppState::new(std::sync::Arc::new(config), cli.max_body)
        .context("构建 aiproxy 状态失败")?;

    tracing::info!(%addr, auth = auth.is_some(), "starting duct");

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    duct::server::run_with_aiproxy_from_listener(listener, auth, state).await
}
