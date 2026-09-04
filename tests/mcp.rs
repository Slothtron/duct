//! mcp 转发集成测试（设计 §8 T 项：I1/M 集成套件）。
//!
//! 上游为真实 TCP 回环 mock（:0 随机端口）；duct 侧路由经 tower oneshot 打完整 axum 栈，
//! 部分全链路用例走 `run_with_states_from_listener`（桥接 + 六分支）经真实 socket。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::Body;
use bytes::Bytes;
use futures::StreamExt;
use tower::util::ServiceExt;

use duct::aiproxy::AppState;
use duct::config::Config;
use duct::mcp::{McpState, router};
use duct::trace::TraceSink;

static TMP_SEQ: AtomicUsize = AtomicUsize::new(0);

/// 经真实装载路径生成配置（写入 `mcp.servers` 段）。`servers_yaml` 各级行缩进会统一再深一层，
/// 使其嵌套在 `servers:` 之下。
fn config_mcp(servers_yaml: &str) -> Arc<Config> {
    let seq = TMP_SEQ.fetch_add(1, Ordering::SeqCst);
    let path: PathBuf =
        std::env::temp_dir().join(format!("duct-mcp-{}-{seq}.yaml", std::process::id()));
    let indented = servers_yaml
        .lines()
        .map(|l| format!("  {l}"))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&path, format!("mcp:\n  servers:\n{indented}")).unwrap();
    let cfg = Config::load_explicit(&path).expect("load test config");
    std::fs::remove_file(&path).ok();
    Arc::new(cfg)
}

// ── Mock MCP 上游（原始 TCP，自解析 HTTP/1.1）────────────────────────

#[derive(Clone)]
enum Behavior {
    /// MCP 合约上游：按方法/body 分发（initialize→session 头+JSON，tools/list→JSON，
    /// notify→202，DELETE→204，GET→SSE 滴流）。`check_origin` 设定后校验 Origin。
    Mcp { check_origin: Option<&'static str> },
    /// SSE 滴流：`chunks` 帧，间隔 `delay_ms`。
    Sse { chunks: usize, delay_ms: u64 },
    /// 固定状态码 + 附加头（供超时/异常路径断言）。
    Status {
        code: u16,
        body: &'static str,
        extra_headers: Vec<(&'static str, &'static str)>,
    },
}

#[derive(Clone, Debug)]
struct Record {
    method: String,
    target: String,
    headers: std::collections::HashMap<String, String>,
    body: Vec<u8>,
}

impl Record {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(|s| s.as_str())
    }
}

struct MockUpstream {
    addr: SocketAddr,
    records: Arc<Mutex<Vec<Record>>>,
}

fn spawn_mock(behavior: Behavior) -> MockUpstream {
    use tokio::io::AsyncWriteExt as _;
    use tokio::net::TcpListener;

    let (tx, rx) = std::sync::mpsc::channel::<SocketAddr>();
    let records = Arc::new(Mutex::new(Vec::new()));
    let rt_records = records.clone();

    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tx.send(addr).unwrap();
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let behavior = behavior.clone();
                let records = rt_records.clone();
                tokio::spawn(async move {
                    let (method, target, header_map, body) =
                        read_request(&mut sock).await.unwrap_or_default();
                    // 记录用克隆值，保留 method/body 供 Mcp 分派处置使用
                    records.lock().unwrap().push(Record {
                        method: method.clone(),
                        target,
                        headers: header_map.clone(),
                        body: body.clone(),
                    });

                    match behavior {
                        Behavior::Mcp { check_origin } => {
                            let origin = records
                                .lock()
                                .unwrap()
                                .last()
                                .and_then(|r| r.header("origin").map(str::to_string));
                            if let Some(want) = check_origin
                                && origin.as_deref() != Some(want)
                            {
                                respond(
                                    &mut sock,
                                    "403 Forbidden",
                                    &[("content-length", "0")],
                                    b"",
                                )
                                .await;
                                return;
                            }
                            // 分派需要 body 的方法
                            let text = String::from_utf8_lossy(&body).into_owned();
                            match method.as_str() {
                                "DELETE" => {
                                    respond(&mut sock, "204 No Content", &[], b"").await;
                                }
                                "GET" => {
                                    let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n";
                                    sock.write_all(head.as_bytes()).await.ok();
                                    for i in 0..3 {
                                        let payload = format!("data: frame-{i}\n\n");
                                        let framed = format!("{:x}\r\n{}\r\n", payload.len(), payload);
                                        sock.write_all(framed.as_bytes()).await.ok();
                                        sock.flush().await.ok();
                                        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
                                    }
                                    sock.write_all(b"0\r\n\r\n").await.ok();
                                }
                                _ => {
                                    if text.contains("initialize") {
                                        let js = r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-03-26","capabilities":{}}}"#;
                                        respond(
                                            &mut sock,
                                            "200 OK",
                                            &[
                                                ("content-type", "application/json"),
                                                ("content-length", &js.len().to_string()),
                                                ("mcp-session-id", "test-session"),
                                            ],
                                            js.as_bytes(),
                                        )
                                        .await;
                                    } else if text.contains("tools/list") {
                                        let js = r#"{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}"#;
                                        respond(
                                            &mut sock,
                                            "200 OK",
                                            &[
                                                ("content-type", "application/json"),
                                                ("content-length", &js.len().to_string()),
                                            ],
                                            js.as_bytes(),
                                        )
                                        .await;
                                    } else {
                                        respond(&mut sock, "202 Accepted", &[("content-length", "0")], b"")
                                            .await;
                                    }
                                }
                            }
                        }
                        Behavior::Sse { chunks, delay_ms } => {
                            let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n";
                            sock.write_all(head.as_bytes()).await.ok();
                            for i in 0..chunks {
                                let payload = format!("data: chunk-{i}\n\n");
                                let framed = format!("{:x}\r\n{}\r\n", payload.len(), payload);
                                sock.write_all(framed.as_bytes()).await.ok();
                                sock.flush().await.ok();
                                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                            }
                            sock.write_all(b"0\r\n\r\n").await.ok();
                        }
                        Behavior::Status {
                            code,
                            body,
                            extra_headers,
                        } => {
                            let reason = status_reason(code);
                            let mut h = format!("HTTP/1.1 {code} {reason}\r\n");
                            for (k, v) in &extra_headers {
                                h.push_str(&format!("{k}: {v}\r\n"));
                            }
                            h.push_str(&format!("content-length: {}\r\n", body.len()));
                            h.push_str("\r\n");
                            sock.write_all(h.as_bytes()).await.ok();
                            sock.write_all(body.as_bytes()).await.ok();
                        }
                    }
                    let _ = sock.shutdown().await;
                });
            }
        });
    });

    let addr = rx.recv().unwrap();
    MockUpstream { addr, records }
}

impl MockUpstream {
    fn records(&self) -> Vec<Record> {
        self.records.lock().unwrap().clone()
    }
}

/// 从 socket 读取一个完整 HTTP 请求（到头部结束 + body 排空）。
async fn read_request(
    sock: &mut tokio::net::TcpStream,
) -> std::io::Result<(
    String,
    String,
    std::collections::HashMap<String, String>,
    Vec<u8>,
)> {
    use tokio::io::AsyncReadExt as _;

    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        if sock.read(&mut byte).await? == 0 {
            break;
        }
        buf.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&buf);
    let mut lines = head.lines();
    let req_line = lines.next().unwrap_or("");
    let mut parts = req_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();
    let mut header_map = std::collections::HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            header_map.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }
    let chunked = header_map
        .get("transfer-encoding")
        .map(|v| v.to_ascii_lowercase().contains("chunked"))
        .unwrap_or(false);
    let mut body = Vec::new();
    if chunked {
        while !buf.ends_with(b"0\r\n\r\n") {
            if sock.read(&mut byte).await? == 0 {
                break;
            }
            buf.push(byte[0]);
        }
        let tail = String::from_utf8_lossy(&buf);
        let after_head = tail.split("\r\n\r\n").nth(1).unwrap_or("");
        for frame in after_head.split("\r\n").collect::<Vec<_>>().chunks(2) {
            if frame.len() == 2 && !frame[0].is_empty() && frame[0] != "0" {
                body.extend_from_slice(frame[1].as_bytes());
            }
        }
    } else if let Some(cl) = header_map
        .get("content-length")
        .and_then(|v| v.parse().ok())
    {
        body = vec![0u8; cl];
        if cl > 0 && sock.read_exact(&mut body).await.is_err() {
            // 无 body 也放行
        }
    }
    Ok((method, target, header_map, body))
}

async fn respond(
    sock: &mut tokio::net::TcpStream,
    status: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) {
    use tokio::io::AsyncWriteExt as _;
    let mut h = format!("HTTP/1.1 {status}\r\n");
    for (k, v) in headers {
        h.push_str(&format!("{k}: {v}\r\n"));
    }
    h.push_str("\r\n");
    sock.write_all(h.as_bytes()).await.ok();
    sock.write_all(body).await.ok();
}

fn status_reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        202 => "Accepted",
        204 => "No Content",
        403 => "Forbidden",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    }
}

// ── 工具 ──────────────────────────────────────────────────────────────

fn mcp_state(config: Arc<Config>, max_body: usize) -> McpState {
    McpState::new(config, max_body).unwrap()
}

fn mcp_config(addr: SocketAddr) -> Arc<Config> {
    config_mcp(&format!("  github:\n    url: http://{addr}/mcp"))
}

async fn oneshot(
    state: McpState,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: Option<Vec<u8>>,
) -> axum::http::Response<Body> {
    let app = router(state.clone());
    let mut builder = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .header("host", "duct.internal");
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let req = builder.body(Body::from(body.unwrap_or_default())).unwrap();
    app.oneshot(req).await.unwrap()
}

async fn drain(resp: axum::http::Response<Body>) -> Bytes {
    let mut out = Vec::new();
    let mut stream = resp.into_body().into_data_stream();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    Bytes::from(out)
}

/// 起一个真实 duct 监听（aiproxy 空 + mcp state），返回地址。
async fn start_duct(mcp: McpState) -> SocketAddr {
    let aiproxy = AppState::new(Arc::new(Config::default()), 16 << 20).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        duct::server::run_with_states_from_listener(listener, None, aiproxy, mcp)
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}

async fn send_raw(addr: SocketAddr, raw: &[u8]) -> String {
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    sock.write_all(raw).await.unwrap();
    sock.flush().await.unwrap();
    let mut buf = Vec::new();
    sock.read_to_end(&mut buf).await.unwrap_or(0);
    String::from_utf8_lossy(&buf).to_string()
}

// ── 用例 ──────────────────────────────────────────────────────────────

/// I1：经 duct 全链路（六分支桥接）握手成功，`mcp-session-id` 响应头透传回客户端。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i1_session_header_passthrough_over_dispatch() {
    let mock = spawn_mock(Behavior::Mcp { check_origin: None });
    let config = mcp_config(mock.addr);
    let addr = start_duct(mcp_state(config, 16 << 20)).await;

    let body = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}"#;
    let req = format!(
        "POST /mcp/github HTTP/1.1\r\nhost: t\r\ncontent-type: application/json\r\naccept: application/json, text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let text = send_raw(addr, req.as_bytes()).await;
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(text.to_lowercase().contains("mcp-session-id"), "{text}");
    assert!(text.contains("protocolVersion"), "{text}");
}

/// I1b：POST/DELETE/GET 三方法一套端点（oneshot），且 `mcp-session-id` 随请求透传上游。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i1b_multi_methods_and_session_forwarded() {
    let mock = spawn_mock(Behavior::Mcp { check_origin: None });
    let state = mcp_state(mcp_config(mock.addr), 16 << 20);

    // POST initialize → 200 + session 头
    let init = br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
    let resp = oneshot(
        state.clone(),
        "POST",
        "/mcp/github",
        &[("content-type", "application/json")],
        Some(init.to_vec()),
    )
    .await;
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("mcp-session-id").unwrap(),
        "test-session"
    );

    // POST tools/list（带 session 头回显）→ 200，上游应收到 session 头
    let tl = br#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#;
    let _ = oneshot(
        state.clone(),
        "POST",
        "/mcp/github",
        &[
            ("content-type", "application/json"),
            ("mcp-session-id", "test-session"),
        ],
        Some(tl.to_vec()),
    )
    .await;
    let recs = mock.records();
    assert!(
        recs.iter()
            .any(|r| r.method == "POST" && r.target == "/mcp")
    );
    assert!(recs.iter().any(|r| {
        r.body
            .windows(b"tools/list".len())
            .any(|w| w == b"tools/list")
    }));
    assert!(
        recs.iter()
            .any(|r| r.header("mcp-session-id") == Some("test-session"))
    );

    // DELETE → 204
    let resp = oneshot(state.clone(), "DELETE", "/mcp/github", &[], None).await;
    assert_eq!(resp.status(), 204);
    assert!(
        mock.records()
            .iter()
            .any(|r| r.method == "DELETE" && r.target == "/mcp")
    );

    // GET → text/event-stream 滴流
    let resp = oneshot(
        state.clone(),
        "GET",
        "/mcp/github",
        &[("accept", "text/event-stream")],
        None,
    )
    .await;
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/event-stream"),
        "{:?}",
        resp.headers().get("content-type")
    );
    let body = drain(resp).await;
    assert!(String::from_utf8_lossy(&body).contains("frame-0"));
}

/// I2：SSE 分帧增量透传 —— 上游每帧 sleep，客户端非聚合先后收到（以耗时反证未整包缓冲）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i2_sse_incremental_passthrough() {
    let chunks = 3usize;
    let delay = 120u64;
    let mock = spawn_mock(Behavior::Sse {
        chunks,
        delay_ms: delay,
    });
    let state = mcp_state(mcp_config(mock.addr), 16 << 20);

    let t0 = std::time::Instant::now();
    let resp = oneshot(
        state.clone(),
        "GET",
        "/mcp/github",
        &[("accept", "text/event-stream")],
        None,
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body = drain(resp).await;
    let elapsed = t0.elapsed();

    let text = String::from_utf8_lossy(&body);
    // 帧序完整且原样（无改写）
    for i in 0..chunks {
        assert!(
            text.contains(&format!("data: chunk-{i}")),
            "missing chunk-{i}: {text}"
        );
    }
    // 若被整包缓冲，会近乎即时返回；至少隔了 (chunks-1)*delay 才说明逐帧抵达
    assert!(
        elapsed >= std::time::Duration::from_millis((chunks.saturating_sub(1)) as u64 * delay),
        "stream aggregated too fast: elapsed={elapsed:?}"
    );
}

/// I3：长挂流不被总超时掐断 —— 无整体超时，流式读完全部帧且轨迹收尾 completed。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i3_long_stream_not_cut_by_total_timeout() {
    let mock = spawn_mock(Behavior::Sse {
        chunks: 3,
        delay_ms: 300,
    });
    let state = mcp_state(mcp_config(mock.addr), 16 << 20);
    let resp = oneshot(
        state.clone(),
        "GET",
        "/mcp/github",
        &[("accept", "text/event-stream")],
        None,
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body = drain(resp).await;
    let text = String::from_utf8_lossy(&body);
    for i in 0..3 {
        assert!(text.contains(&format!("data: chunk-{i}")));
    }
}

/// I4：未注册 server id → 404 含可用列表；裸 /mcp → 404。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i4_unknown_and_bare_404() {
    let mock = spawn_mock(Behavior::Mcp { check_origin: None });
    let config = config_mcp(&format!("  github:\n    url: http://{}/mcp", mock.addr));
    let state = mcp_state(config, 16 << 20);

    // 未注册 id
    let resp = oneshot(
        state.clone(),
        "POST",
        "/mcp/nope",
        &[("content-type", "application/json")],
        Some(b"{}".to_vec()),
    )
    .await;
    assert_eq!(resp.status(), 404);
    let body = drain(resp).await;
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("github"),
        "available list should include github: {text}"
    );
    assert!(text.contains("not found"), "{text}");

    // 裸 /mcp（经桥接、六分支）
    let addr = start_duct(state).await;
    let text = send_raw(
        addr,
        b"GET /mcp HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n",
    )
    .await;
    assert!(text.starts_with("HTTP/1.1 404"), "{text}");
    assert!(text.contains("github"), "{text}");
}

/// I5：origin_policy=upstream 时上游收到改写后的 Origin。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i5_origin_policy_upstream_rewrites_origin() {
    let mock = spawn_mock(Behavior::Mcp { check_origin: None });
    let cfg = config_mcp(&format!(
        "  internal:\n    url: http://{}/mcp\n    origin_policy: upstream",
        mock.addr
    ));
    let state = mcp_state(cfg, 16 << 20);
    let _ = oneshot(
        state.clone(),
        "POST",
        "/mcp/internal",
        &[
            ("content-type", "application/json"),
            ("origin", "https://client.example"),
        ],
        Some(br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#.to_vec()),
    )
    .await;
    let recs = mock.records();
    // upstream 应收到 server.url 的 origin（非客户端 origin）
    let expected = format!("http://{}", mock.addr);
    assert!(
        recs.iter()
            .any(|r| r.header("origin") == Some(expected.as_str())),
        "expected origin={expected}, got {:?}",
        recs.iter().map(|r| r.header("origin")).collect::<Vec<_>>()
    );
}

/// I8：轨迹链 + 脱敏 + sse:false（mcp 事件不应带 sse 字段）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i8_trace_chain_redaction_and_sse_false() {
    use serde_json::Value;

    let mock = spawn_mock(Behavior::Mcp { check_origin: None });
    let (sink, cap) = TraceSink::capture();
    let config = mcp_config(mock.addr);
    let state = McpState::with_trace(config, 16 << 20, Arc::new(sink)).unwrap();

    let init = br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
    let resp = oneshot(
        state.clone(),
        "POST",
        "/mcp/github",
        &[
            ("content-type", "application/json"),
            ("authorization", "Bearer sk-secret-123"),
            ("mcp-session-id", "session-replay"),
        ],
        Some(init.to_vec()),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let _ = drain(resp).await;

    // 等背景 writer 刷完（capture 是实时 push，但保险起见稍等）
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let lines: Vec<String> = cap.lock().unwrap().clone();
    let events: Vec<Value> = lines
        .iter()
        .map(|l| serde_json::from_str(l).expect("每条轨迹都是合法 JSONL"))
        .collect();

    let types: Vec<&str> = events.iter().map(|e| e["type"].as_str().unwrap()).collect();
    assert!(
        types.contains(&"request/start"),
        "want request/start, got {types:?}"
    );
    assert!(types.contains(&"upstream/request"), "{types:?}");
    assert!(types.contains(&"upstream/response"), "{types:?}");
    assert!(types.contains(&"request/end"), "{types:?}");

    // branch/server 标识 + sse:false（不应有 sse 字段）
    let start = events
        .iter()
        .find(|e| e["type"] == "request/start")
        .unwrap();
    assert_eq!(start["data"]["branch"], "mcp");
    assert_eq!(start["data"]["server"], "github");
    assert!(
        start["data"].get("sse").is_none(),
        "mcp 事件不应带 sse 字段"
    );
    let end = events.iter().find(|e| e["type"] == "request/end").unwrap();
    assert_eq!(end["data"]["outcome"], "completed");
    assert!(
        end["data"].get("body").is_some(),
        "JSON 回包应产出 body 事实"
    );

    // 脱敏全文扫描：任何行都不出现授权值 / 会话 id 值
    for line in &lines {
        assert!(
            !line.contains("sk-secret-123"),
            "authorization 值泄漏: {line}"
        );
        assert!(
            !line.contains("session-replay"),
            "mcp-session-id 值泄漏: {line}"
        );
    }

    // provider-miss 成对 rejected
    let resp = oneshot(
        state.clone(),
        "POST",
        "/mcp/nope",
        &[],
        Some(b"{}".to_vec()),
    )
    .await;
    assert_eq!(resp.status(), 404);
    let _ = drain(resp).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let events: Vec<Value> = cap
        .lock()
        .unwrap()
        .clone()
        .iter()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let miss = events
        .iter()
        .find(|e| e["type"] == "request/end" && e["data"]["outcome"] == "rejected")
        .unwrap();
    assert_eq!(miss["data"]["gateway_error"]["status"], 404);
}

/// I8b：客户端中断 → 轨迹合成 interrupted（Drop 兜底）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i8b_client_abort_yields_interrupted() {
    use serde_json::Value;

    let mock = spawn_mock(Behavior::Sse {
        chunks: 8,
        delay_ms: 200,
    });
    let (sink, cap) = TraceSink::capture();
    let state = McpState::with_trace(mcp_config(mock.addr), 16 << 20, Arc::new(sink)).unwrap();

    let resp = oneshot(
        state.clone(),
        "GET",
        "/mcp/github",
        &[("accept", "text/event-stream")],
        None,
    )
    .await;
    // 只读一帧即弃流 —— 触发 TracedBody Drop 兜底 interrupted
    let mut stream = resp.into_body().into_data_stream();
    let _ = stream.next().await;
    drop(stream);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let events: Vec<Value> = cap
        .lock()
        .unwrap()
        .clone()
        .iter()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let end = events.iter().find(|e| e["type"] == "request/end").unwrap();
    assert_eq!(end["data"]["outcome"], "interrupted");
}

/// 上游 4xx/5xx 状态体原样透传（AGENTS.md 不变量 #8：上游错误透传，不重造）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i_upstream_error_passthrough_verbatim() {
    let mock = spawn_mock(Behavior::Status {
        code: 500,
        body: "{\"err\":\"upstream\"}",
        extra_headers: vec![("x-upstream", "yes")],
    });
    let state = mcp_state(mcp_config(mock.addr), 16 << 20);
    let resp = oneshot(
        state.clone(),
        "POST",
        "/mcp/github",
        &[("content-type", "application/json")],
        Some(b"{}".to_vec()),
    )
    .await;
    assert_eq!(resp.status(), 500);
    // 上游错误体逐字节透传，不重造
    let upstream_hdr = resp
        .headers()
        .get("x-upstream")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let body = drain(resp).await;
    assert_eq!(String::from_utf8_lossy(&body), "{\"err\":\"upstream\"}");
    assert_eq!(upstream_hdr, "yes");
}
