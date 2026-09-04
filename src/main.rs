use anyhow::Context as _;
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

    /// JSONL request-trace file for aiproxy (append-only, one event per line,
    /// modeled on the DSH session-trace design). Defaults to the XDG state dir
    /// $XDG_STATE_HOME/duct/trace.jsonl (usually ~/.local/state/duct/trace.jsonl).
    /// "~" is expanded; an empty value disables the file sink and trace events
    /// fall back to tracing (target=duct::trace). Credentials are always
    /// redacted to `***`. Missing files and parent dirs are created; runtime
    /// deletion/rotation is self-healed. If unopenable, duct warns and continues.
    #[arg(long, value_name = "PATH")]
    trace_file: Option<String>,

    /// Opt-in content capture. When > 0, the trace also records a head snapshot
    /// of the request body and response stream (this byte budget) as
    /// `req_content_head` / `resp_content_head` — prompts and completions then
    /// DO land on disk, so treat the trace file as sensitive (chmod 600).
    /// Also sends `Accept-Encoding: identity` upstream (response bytes are still
    /// relayed verbatim), which makes provider `normalize_sse` effective again
    /// against compressing gateways. 0 (default) = never capture content.
    #[arg(long, default_value_t = 0, value_name = "BYTES")]
    trace_body: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Construct auth config from CLI args or environment variables.
    // clap's `requires` ensures both --user and --passwd (or their env counterparts)
    // are provided together, or neither is provided.
    let auth = cli.user.zip(cli.passwd).map(|(user, passwd)| AuthConfig {
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

    // ── 请求轨迹 sink（JSONL，参考 DSH 会话轨迹设计）；打开失败降级为 tracing-only ──
    let trace_sink = match cli.trace_file.as_deref() {
        Some(s) if s.trim().is_empty() => {
            tracing::info!(
                "aiproxy trace file disabled (trace events -> tracing target duct::trace)"
            );
            std::sync::Arc::new(duct::trace::TraceSink::none())
        }
        specified => {
            // 未指定时用 XDG 状态目录默认值；指定值做 ~ 展开。
            let path = match specified {
                Some(s) => expand_tilde(s),
                None => duct::trace::default_trace_path(),
            };
            match duct::trace::TraceSink::to_file(&path) {
                Ok(sink) => std::sync::Arc::new(sink),
                Err(e) => {
                    tracing::warn!(error = %e, file = %path.display(), "aiproxy 轨迹文件不可用，降级为 tracing 输出");
                    std::sync::Arc::new(duct::trace::TraceSink::none())
                }
            }
        }
    };

    let state = duct::aiproxy::AppState::with_trace_body(
        std::sync::Arc::new(config),
        cli.max_body,
        trace_sink,
        cli.trace_body,
    )
    .context("构建 aiproxy 状态失败")?;

    if cli.trace_body > 0 {
        tracing::warn!(
            bytes = cli.trace_body,
            "trace content capture ON: prompts/completions land in trace file; protect it like a key"
        );
    }

    tracing::info!(%addr, auth = auth.is_some(), "starting duct");

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    duct::server::run_with_aiproxy_from_listener(listener, auth, state).await
}

/// 展开开头的 `~` 为 $HOME（无 HOME 时保持原样，由上层报错/降级）。
fn expand_tilde(s: &str) -> std::path::PathBuf {
    let home = || env::var_os("HOME").map(std::path::PathBuf::from);
    match s {
        "~" => home(),
        _ if s.len() > 1 && s.as_bytes()[1] == b'/' => home().map(|h| h.join(&s[2..])),
        _ => None,
    }
    .unwrap_or_else(|| std::path::PathBuf::from(s))
}
