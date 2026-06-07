use clap::Parser;
use std::env;
use std::os::unix::process::CommandExt;

/// Process names that are typically exempt from argv[0]-based filtering.
/// When disguise is enabled, duct re-execs itself with one of these names.
const ALLOWED_NAMES: &[&str] = &["curl", "wget", "python3", "python", "node", "java", "firefox", "chrome"];

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
    /// Defaults to "curl".
    #[arg(long, default_value = "curl")]
    disguise: String,

    /// Skip the process-name disguise re-exec. Use this if your environment
    /// doesn't filter by argv[0] or you've manually renamed the binary.
    #[arg(long)]
    no_disguise: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // ── Process name disguise ──
    // Some environments filter TCP connections by the originating process name
    // (argv[0]). If our process name isn't recognized, connections may be
    // rejected. Solution: re-exec ourselves with argv[0] set to an allowed name.
    if !cli.no_disguise {
        let current_name = env::args().next().unwrap_or_default();
        let basename = current_name.rsplit('/').next().unwrap_or(&current_name);

        if !ALLOWED_NAMES.contains(&basename) {
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
