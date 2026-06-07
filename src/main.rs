use clap::Parser;
use std::env;
use std::os::unix::process::CommandExt;

use duct::auth::AuthConfig;

#[derive(Parser, Debug)]
#[command(name = "duct", version, about = "Lightweight HTTP/HTTPS proxy with process name disguise")]
struct Cli {
    /// Listening port
    #[arg(short, long, default_value_t = 1080)]
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
    /// Must be used together with --password.
    #[arg(long)]
    username: Option<String>,

    /// Password for HTTP Basic proxy authentication.
    /// Must be used together with --username.
    #[arg(long)]
    password: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Validate credential pairing
    let auth = match (&cli.username, &cli.password) {
        (Some(u), Some(p)) => Some(AuthConfig {
            username: u.clone(),
            password: p.clone(),
        }),
        (None, None) => None,
        _ => {
            eprintln!("error: --username and --password must be used together");
            std::process::exit(1);
        }
    };

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
    tracing::info!(%addr, auth = auth.is_some(), "starting duct");

    duct::server::run(&addr, auth).await
}
