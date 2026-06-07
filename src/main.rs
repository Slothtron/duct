use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "duct", version, about = "HTTP CONNECT proxy for WSL VPN bridge")]
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

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
    tracing::info!(%addr, "starting duct");

    duct::server::run(&addr).await
}
