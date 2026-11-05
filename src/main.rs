use clap::Parser;
use std::env;
use std::os::unix::process::CommandExt;

use duct::auth::AuthConfig;

#[derive(Parser, Debug)]
#[command(
    name = "duct",
    version,
    about = "Lightweight HTTP/HTTPS proxy with process name disguise"
)]
struct Cli {
    /// Listening port
    #[arg(short, long, default_value_t = 11088)]
    port: u16,

    /// Listening address
    #[arg(short, long, default_value = "127.0.0.1")]
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let auth = cli.user.zip(cli.passwd).map(|(user, passwd)| AuthConfig {
        username: user,
        password: passwd,
    });

    if let Some(ref disguise) = cli.disguise {
        let exe = env::current_exe()?;
        let filtered_args: Vec<_> = {
            let mut skip_next = false;
            env::args_os()
                .skip(1)
                .filter(|a| {
                    if skip_next {
                        skip_next = false;
                        return false;
                    }
                    if a == "--disguise" {
                        skip_next = true;
                        return false;
                    }
                    true
                })
                .collect()
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
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| filter.into()),
        )
        .init();

    let addr = format!("{}:{}", cli.bind, cli.port);
    if matches!(cli.bind.as_str(), "0.0.0.0" | "[::]" | "::") && auth.is_none() {
        tracing::warn!(
            bind = %cli.bind,
            "listening on all interfaces without HTTP Basic auth; network proxy access is unrestricted"
        );
    }

    tracing::info!(%addr, auth = auth.is_some(), "starting duct");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    duct::server::run_from_listener(listener, auth).await
}
