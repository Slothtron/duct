//! 入口分流与桥接的 socket 级测试（T6/T7 验收，§9 端口约定：绑 :0）。
//!
//! 走真实 TcpListener 全链路：手工请求行判定 → 桥接注入 → hyper 解析 → mock 上游。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use duct::aiproxy::AppState;
use duct::config::Config;

static TMP_SEQ: AtomicUsize = AtomicUsize::new(0);

fn config_from_yaml(yaml_body: &str) -> Arc<Config> {
    let seq = TMP_SEQ.fetch_add(1, Ordering::SeqCst);
    let path: PathBuf = std::env::temp_dir().join(format!(
        "duct-dispatch-{}-{seq}.yaml",
        std::process::id()
    ));
    std::fs::write(&path, format!("providers:\n{yaml_body}")).unwrap();
    let cfg = Config::load_explicit(&path).unwrap();
    std::fs::remove_file(&path).ok();
    Arc::new(cfg)
}

/// 复用 tests/aiproxy.rs 的行为 —— 这里以最小内联 echo 上游代替，
/// 避免跨测试文件共享（cargo 集成测试各文件独立编译单元）。
async fn spawn_echo_upstream() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else { break };
            tokio::spawn(async move {
                // 读到头部结束
                let mut buf: Vec<u8> = Vec::new();
                let mut byte = [0u8; 1];
                loop {
                    match sock.read(&mut byte).await {
                        Ok(0) | Err(_) => return,
                        Ok(_) => buf.push(byte[0]),
                    }
                    if buf.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                let head = String::from_utf8_lossy(&buf);
                let chunked = head
                    .to_ascii_lowercase()
                    .contains("transfer-encoding: chunked");
                if chunked {
                    // 追加读到终止块
                    let mut byte = [0u8; 1];
                    while !buf.ends_with(b"0\r\n\r\n") {
                        match sock.read(&mut byte).await {
                            Ok(0) | Err(_) => break,
                            Ok(_) => buf.push(byte[0]),
                        }
                    }
                } else if let Some(cl) = head
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse::<usize>().ok())?
                    })
                {
                    let consumed = buf.len();
                    let head_end = head.find("\r\n\r\n").map(|i| i + 4).unwrap_or(consumed);
                    let already = consumed.saturating_sub(head_end);
                    if cl > already {
                        let mut rest = vec![0u8; cl - already];
                        sock.read_exact(&mut rest).await.ok();
                    }
                }
                // 全量消费后回固定 JSON 并关闭
                let summary = br#"{"echo":true}"#;
                let resp_head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    summary.len()
                );
                sock.write_all(resp_head.as_bytes()).await.ok();
                sock.write_all(summary).await.ok();
            });
        }
    });
    addr
}

/// 起一个真实 duct 监听 :0，返回地址。
async fn start_duct(config: Arc<Config>) -> SocketAddr {
    let state = AppState::new(config, 16 << 20).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        duct::server::run_with_aiproxy_from_listener(listener, None, state)
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}

async fn send_recv(addr: SocketAddr, raw: &[u8]) -> String {
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(raw).await.unwrap();
    sock.flush().await.unwrap();
    let mut buf = Vec::new();
    sock.read_to_end(&mut buf).await.unwrap_or(0);
    String::from_utf8_lossy(&buf).to_string()
}

#[tokio::test]
async fn healthz_answers_200_at_dispatch_layer() {
    // 空配置也能探活 —— 这是「进程存活」语义的关键断言
    let addr = start_duct(Arc::new(Config::default())).await;
    let text = send_recv(
        addr,
        b"GET /healthz HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n",
    )
    .await;
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(text.ends_with("ok"), "{text}");
}

#[tokio::test]
async fn non_get_healthz_falls_to_400() {
    let addr = start_duct(Arc::new(Config::default())).await;
    let text = send_recv(
        addr,
        b"POST /healthz HTTP/1.1\r\nhost: t\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
    )
    .await;
    assert!(text.starts_with("HTTP/1.1 400"), "{text}");
}

#[tokio::test]
async fn bare_aiproxy_prefix_without_provider_is_400() {
    for uri in ["/aiproxy", "/aiproxy/"] {
        let addr = start_duct(Arc::new(Config::default())).await;
        let req = format!("GET {uri} HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n");
        let text = send_recv(addr, req.as_bytes()).await;
        assert!(text.starts_with("HTTP/1.1 400"), "uri={uri}: {text}");
    }
}

#[tokio::test]
async fn arbitrary_relative_path_is_400() {
    let addr = start_duct(Arc::new(Config::default())).await;
    let text = send_recv(
        addr,
        b"GET /index.html HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n",
    )
    .await;
    assert!(text.starts_with("HTTP/1.1 400"), "{text}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_port_aiproxy_end_to_end_through_bridge() {
    let upstream = spawn_echo_upstream().await;
    let config = config_from_yaml(&format!("  up:\n    url: http://{upstream}/v1"));
    let addr = start_duct(config).await;

    let body = br#"{"q":1}"#;
    let req = format!(
        "POST /aiproxy/up/chat/completions?k=v HTTP/1.1\r\nhost: t\r\nauthorization: Bearer sk-real-key\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    let mut raw = req.into_bytes();
    raw.extend_from_slice(body);
    let text = send_recv(addr, &raw).await;
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(text.contains("application/json"), "{text}");
    assert!(text.contains("echo"), "{text}");
}

/// R1 专项：长请求头跨越桥接内部缓冲（>64KB），hyper 流式消化不丢字节。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_headers_cross_the_bridge() {
    let upstream = spawn_echo_upstream().await;
    let config = config_from_yaml(&format!("  up:\n    url: http://{upstream}"));
    let addr = start_duct(config).await;

    let padding = vec![b'x'; 96 * 1024];
    let req = format!(
        "GET /aiproxy/up/ping HTTP/1.1\r\nhost: t\r\nx-pad: {}\r\nconnection: close\r\n\r\n",
        std::str::from_utf8(&padding).unwrap()
    );
    let text = send_recv(addr, req.as_bytes()).await;
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
}
