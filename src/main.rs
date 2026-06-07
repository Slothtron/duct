use clap::Parser;
use std::env;
use std::os::unix::process::CommandExt;

/// Process names that yunshu VPN whitelists for internal network access.
const VPN_WHITELIST: &[&str] = &["curl", "wget", "python3", "python", "node", "java", "firefox", "chrome"];

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

    /// Process name to use for VPN compatibility (yunshu whitelists by argv[0]).
    /// Defaults to "curl".
    #[arg(long, default_value = "curl")]
    disguise: String,

    /// Skip the VPN process-name disguise re-exec. Use this if you've already
    /// symlinked/renamed the binary to a whitelisted name.
    #[arg(long)]
    no_disguise: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // ── VPN process-name disguise ──
    // yunshu VPN daemon filters TCP connections by the originating process name
    // (argv[0]). If our process name isn't on the whitelist, connections to
    // internal hosts (e.g. dmc.kso.net) get RST'd within ~60ms.
    // Solution: re-exec ourselves with argv[0] set to a whitelisted name.
    if !cli.no_disguise {
        let current_name = env::args().next().unwrap_or_default();
        let basename = current_name.rsplit('/').next().unwrap_or(&current_name);

        if !VPN_WHITELIST.contains(&basename) {
            let exe = env::current_exe()?;
            let disguise = &cli.disguise;

            // Re-exec with argv[0] set to the disguise name
            let status = std::process::Command::new(&exe)
                .arg0(disguise)
                .args(env::args_os().skip(1))
                .status()?;

            std::process::exit(status.code().unwrap_or(1));
        }
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
    tracing::info!(%addr, "starting duct");

    duct::server::run(&addr).await
}
