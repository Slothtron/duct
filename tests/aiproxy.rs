//! aiproxy 集成测试（T5，设计文档 §9）。
//!
//! 配置经真实 YAML 装载路径构造；上游为真实 TCP 回环 mock（:0 随机端口，§9 端口约定）；
//! duct 侧路由经 tower oneshot 打完整 axum 栈。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::Body;
use bytes::Bytes;
use futures::StreamExt;
use tower::util::ServiceExt;

use duct::aiproxy::{AppState, router, serve_standalone};
use duct::config::Config;

static TMP_SEQ: AtomicUsize = AtomicUsize::new(0);

/// 经真实装载路径生成配置。
fn config_from_yaml(yaml_body: &str) -> Arc<Config> {
    let seq = TMP_SEQ.fetch_add(1, Ordering::SeqCst);
    let path: PathBuf =
        std::env::temp_dir().join(format!("duct-test-{}-{seq}.yaml", std::process::id()));
    std::fs::write(&path, format!("providers:\n{yaml_body}")).unwrap();
    let cfg = Config::load_explicit(&path).expect("load test config");
    std::fs::remove_file(&path).ok();
    Arc::new(cfg)
}

// ── Mock 上游 ─────────────────────────────────────────────────────────

#[derive(Clone)]
enum Behavior {
    /// 记录请求后回 200 + JSON 摘要。
    Echo,
    /// SSE：N 个 data chunk，间隔 delay_ms。
    Sse { chunks: usize, delay_ms: u64 },
    /// kso 风格：SSE 流中每个 chunk 重复下发完整 function.name（协议违规）。
    SseRepeatedName,
    /// 固定状态码与附加头。
    Status {
        code: u16,
        body: &'static str,
        extra_headers: Vec<(&'static str, &'static str)>,
    },
}

/// kso 风格重复工具名 SSE 负载(不含 chunked 帧，仅为 data 行)。
const SSE_REPEATED_NAME: &str = r##"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"list_dir","arguments":""}}]}}]}

data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"list_dir","arguments":"{\"path\": "}}]}}]}

data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"list_dir","arguments":"\"/\""}}]}}]}

data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}

data: [DONE]
"##;

#[derive(Clone, Debug)]
struct Record {
    method: String,
    target: String,
    headers: HashMap<String, String>,
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
    connections_seen: Arc<Mutex<usize>>,
}

fn spawn_mock(behavior: Behavior) -> MockUpstream {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    let (tx, rx) = std::sync::mpsc::channel::<SocketAddr>();
    let records = Arc::new(Mutex::new(Vec::new()));
    let conns = Arc::new(Mutex::new(0));
    let rt_records = records.clone();
    let rt_conns = conns.clone();

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
                *rt_conns.lock().unwrap() += 1;
                let behavior = behavior.clone();
                let records = rt_records.clone();
                // current_thread runtime：block_on 存续期间由同一运行时驱动
                tokio::spawn(async move {
                    // 读到头部结束
                    let mut buf = Vec::new();
                    let mut byte = [0u8; 1];
                    while !buf.ends_with(b"\r\n\r\n") {
                        match sock.read(&mut byte).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => buf.push(byte[0]),
                        }
                    }
                    let head = String::from_utf8_lossy(&buf);
                    let mut lines = head.lines();
                    let request_line = lines.next().unwrap_or("");
                    let mut parts = request_line.split_whitespace();
                    let method = parts.next().unwrap_or("").to_string();
                    let target = parts.next().unwrap_or("").to_string();
                    let mut header_map = HashMap::new();
                    for line in lines {
                        if let Some((k, v)) = line.split_once(':') {
                            header_map.insert(k.trim().to_lowercase(), v.trim().to_string());
                        }
                    }
                    // 收 body：chunked（reqwest 流式默认）或 Content-Length
                    let chunked = header_map
                        .get("transfer-encoding")
                        .map(|v| v.to_ascii_lowercase().contains("chunked"))
                        .unwrap_or(false);
                    let mut body: Vec<u8> = Vec::new();
                    if chunked {
                        // 读到终止块 0\r\n\r\n，再做去帧解码
                        let mut byte = [0u8; 1];
                        while !buf.ends_with(b"0\r\n\r\n") {
                            match sock.read(&mut byte).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => buf.push(byte[0]),
                            }
                        }
                        let tail = String::from_utf8_lossy(&buf);
                        let after_head = tail.split("\r\n\r\n").nth(1).unwrap_or("");
                        for frame in after_head.split("\r\n").collect::<Vec<_>>().chunks(2) {
                            if frame.len() == 2 && !frame[0].is_empty() && frame[0] != "0" {
                                body.extend_from_slice(frame[1].as_bytes());
                            }
                        }
                    } else {
                        let cl: usize = header_map
                            .get("content-length")
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(0);
                        body = vec![0u8; cl];
                        if cl > 0 && sock.read_exact(&mut body).await.is_err() {
                            return;
                        }
                    }
                    records.lock().unwrap().push(Record {
                        method,
                        target,
                        headers: header_map,
                        body,
                    });

                    let resp_head = |status: &str, extra: &[(String, String)]| {
                        let mut h = format!("HTTP/1.1 {status}\r\ncontent-length: {}\r\n", 0usize);
                        for (k, v) in extra {
                            h.push_str(&format!("{k}: {v}\r\n"));
                        }
                        h.push_str("\r\n");
                        h
                    };

                    match behavior {
                        Behavior::Echo => {
                            const SUMMARY: &str = r#"{"ok":true}"#;
                            let mut head =
                                String::from("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n");
                            head.push_str(&format!("content-length: {}\r\n", SUMMARY.len()));
                            head.push_str("\r\n");
                            sock.write_all(head.as_bytes()).await.ok();
                            sock.write_all(SUMMARY.as_bytes()).await.ok();
                        }
                        Behavior::Sse { chunks, delay_ms } => {
                            let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n";
                            sock.write_all(head.as_bytes()).await.ok();
                            for i in 0..chunks {
                                let payload = format!("data: chunk-{i}\n\n");
                                let framed = format!("{:x}\r\n{}\r\n", payload.len(), payload);
                                sock.write_all(framed.as_bytes()).await.ok();
                                sock.flush().await.ok();
                                tokio::time::sleep(std::time::Duration::from_millis(delay_ms))
                                    .await;
                            }
                            sock.write_all(b"0\r\n\r\n").await.ok();
                        }
                        Behavior::SseRepeatedName => {
                            let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n";
                            sock.write_all(head.as_bytes()).await.ok();
                            let framed = format!("{:x}\r\n{}\r\n", SSE_REPEATED_NAME.len(), SSE_REPEATED_NAME);
                            sock.write_all(framed.as_bytes()).await.ok();
                            sock.write_all(b"0\r\n\r\n").await.ok();
                        }
                        Behavior::Status {
                            code,
                            body,
                            extra_headers,
                        } => {
                            let reason = if code == 500 { "Internal Server Error" } else { "OK" };
                            let mut head = format!("HTTP/1.1 {code} {reason}\r\n");
                            for (k, v) in &extra_headers {
                                head.push_str(&format!("{k}: {v}\r\n"));
                            }
                            head.push_str(&format!("content-length: {}\r\n", body.len()));
                            head.push_str("\r\n");
                            sock.write_all(head.as_bytes()).await.ok();
                            sock.write_all(body.as_bytes()).await.ok();
                        }
                    }
                    let _ = resp_head("", &[]);
                    let _ = sock.shutdown().await;
                });
            }
        });
    });

    let addr = rx.recv().unwrap();
    MockUpstream {
        addr,
        records,
        connections_seen: conns,
    }
}

impl MockUpstream {
    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }
    fn records(&self) -> Vec<Record> {
        self.records.lock().unwrap().clone()
    }
    fn connections(&self) -> usize {
        *self.connections_seen.lock().unwrap()
    }
}

// ── 工具 ──────────────────────────────────────────────────────────────

fn app_state(config: Arc<Config>, max_body: usize) -> AppState {
    AppState::new(config, max_body).unwrap()
}

async fn oneshot(
    state: AppState,
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

async fn body_bytes(resp: axum::http::Response<Body>) -> Bytes {
    axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap()
}

const UNIQUE_HEADER_VALUE: &str = "Bearer sk-secret~!@#$%^&*()_+-={}[]|\\:<>?,./;'";

// ── 用例 ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn post_forwards_path_method_and_body() {
    let mock = spawn_mock(Behavior::Echo);
    let config = config_from_yaml(&format!("  mock:\n    url: {}", mock.base_url()));
    let state = app_state(config, 16 << 20);

    let payload = br#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
    let resp = oneshot(
        state,
        "POST",
        "/aiproxy/mock/v1/chat/completions",
        &[
            ("authorization", UNIQUE_HEADER_VALUE),
            ("content-type", "application/json"),
        ],
        Some(payload.to_vec()),
    )
    .await;

    assert_eq!(resp.status(), 200);
    let recs = mock.records();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].method, "POST");
    assert_eq!(recs[0].target, "/v1/chat/completions");
    assert_eq!(recs[0].body, payload);
    assert_eq!(recs[0].header("authorization"), Some(UNIQUE_HEADER_VALUE));
}

#[tokio::test]
async fn query_string_is_preserved() {
    let mock = spawn_mock(Behavior::Echo);
    let config = config_from_yaml(&format!("  mock:\n    url: {}", mock.base_url()));
    let resp = oneshot(
        app_state(config, 16 << 20),
        "GET",
        "/aiproxy/mock/v1/models?id=x%201&y=1",
        &[],
        None,
    )
    .await;
    assert_eq!(resp.status(), 200);
    assert_eq!(mock.records()[0].target, "/v1/models?id=x%201&y=1");
}

#[tokio::test]
async fn provider_root_forwards_to_base_itself() {
    let mock = spawn_mock(Behavior::Echo);
    let config = config_from_yaml(&format!("  mock:\n    url: {}", mock.base_url()));
    for uri in ["/aiproxy/mock", "/aiproxy/mock/"] {
        let resp = oneshot(app_state(config.clone(), 16 << 20), "GET", uri, &[], None).await;
        assert_eq!(resp.status(), 200, "uri={uri}");
    }
    let targets: Vec<String> = mock.records().iter().map(|r| r.target.clone()).collect();
    assert_eq!(targets, vec!["/", "/"]);
}

#[tokio::test]
async fn credentials_pass_through_byte_exact() {
    let mock = spawn_mock(Behavior::Echo);
    let config = config_from_yaml(&format!("  mock:\n    url: {}", mock.base_url()));
    oneshot(
        app_state(config, 16 << 20),
        "GET",
        "/aiproxy/mock/api/tags",
        &[
            ("authorization", UNIQUE_HEADER_VALUE),
            ("x-api-key", "&%$#@! special=value"),
        ],
        None,
    )
    .await;
    let rec = &mock.records()[0];
    assert_eq!(rec.header("authorization"), Some(UNIQUE_HEADER_VALUE));
    assert_eq!(rec.header("x-api-key"), Some("&%$#@! special=value"));
}

#[tokio::test]
async fn unknown_provider_is_404_openai_json() {
    let mock = spawn_mock(Behavior::Echo);
    let config = config_from_yaml(&format!("  mock:\n    url: {}", mock.base_url()));
    let resp = oneshot(
        app_state(config, 16 << 20),
        "GET",
        "/aiproxy/nope/v1/x",
        &[],
        None,
    )
    .await;
    assert_eq!(resp.status(), 404);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["error"]["type"], "invalid_request_error");
    assert!(json["error"]["message"].as_str().unwrap().contains("mock"));
}

#[tokio::test]
async fn empty_config_lists_nothing() {
    let config = config_from_yaml("  {}: {}"); // providers: {} 空
    let resp = oneshot(
        app_state(config, 16 << 20),
        "GET",
        "/aiproxy/x/y",
        &[],
        None,
    )
    .await;
    assert_eq!(resp.status(), 404);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("(none configured)")
    );
}

#[tokio::test]
async fn oversized_content_length_rejected_before_upstream_contact() {
    let mock = spawn_mock(Behavior::Echo);
    let config = config_from_yaml(&format!("  mock:\n    url: {}", mock.base_url()));
    let before = mock.connections();
    let resp = oneshot(
        app_state(config, 1024),
        "POST",
        "/aiproxy/mock/v1/x",
        &[("content-length", "65536")],
        Some(vec![b'a'; 64]), // 声明超限；实际字节不足也会被前置拦截
    )
    .await;
    assert_eq!(resp.status(), 413);
    assert_eq!(mock.connections(), before, "上游不应被接触");
}

#[tokio::test]
async fn sse_streams_all_chunks_in_order() {
    let mock = spawn_mock(Behavior::Sse {
        chunks: 5,
        delay_ms: 20,
    });
    let config = config_from_yaml(&format!("  mock:\n    url: {}", mock.base_url()));
    let resp = oneshot(
        app_state(config, 16 << 20),
        "POST",
        "/aiproxy/mock/v1/chat/completions",
        &[("content-type", "application/json")],
        Some(b"{}".to_vec()),
    )
    .await;
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );

    let mut stream = resp.into_body().into_data_stream();
    let mut collected = Vec::new();
    while let Some(chunk) = stream.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    let text = String::from_utf8(collected).unwrap();
    for i in 0..5 {
        assert!(
            text.contains(&format!("data: chunk-{i}\n")),
            "missing {i}: {text}"
        );
    }
}

#[tokio::test]
async fn upstream_error_status_passed_through_verbatim() {
    let mock = spawn_mock(Behavior::Status {
        code: 500,
        body: r#"{"error":{"message":"upstream boom","type":"upstream"}}""#,
        extra_headers: vec![("x-upstream-cause", "unit-test")],
    });
    let config = config_from_yaml(&format!("  mock:\n    url: {}", mock.base_url()));
    let resp = oneshot(
        app_state(config, 16 << 20),
        "GET",
        "/aiproxy/mock/v1/x",
        &[],
        None,
    )
    .await;
    assert_eq!(resp.status(), 500);
    assert_eq!(resp.headers().get("x-upstream-cause").unwrap(), "unit-test");
    let body = body_bytes(resp).await;
    assert!(String::from_utf8_lossy(&body).contains("upstream boom"));
}

#[tokio::test]
async fn unreachable_provider_maps_to_502() {
    // 关闭的本地端口 → 连接拒绝 → 502
    let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = dead.local_addr().unwrap().port();
    drop(dead); // 释放以复现 connection refused
    let config = config_from_yaml(&format!("  dead:\n    url: http://127.0.0.1:{port}"));
    let resp = oneshot(
        app_state(config, 16 << 20),
        "GET",
        "/aiproxy/dead/v1/x",
        &[],
        None,
    )
    .await;
    assert_eq!(resp.status(), 502);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["error"]["type"], "upstream_error");
}

#[tokio::test]
async fn dns_failure_maps_to_502() {
    let config = config_from_yaml("  ghost:\n    url: http://duct-test-nonexistent.invalid");
    let resp = oneshot(
        app_state(config, 16 << 20),
        "GET",
        "/aiproxy/ghost/v1/x",
        &[],
        None,
    )
    .await;
    assert_eq!(resp.status(), 502);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_serving_smoke_over_real_socket() {
    let mock = spawn_mock(Behavior::Echo);
    let config = config_from_yaml(&format!("  mock:\n    url: {}", mock.base_url()));
    let state = app_state(config, 16 << 20);

    // 绑 :0 随机端口（§9），通过 mpsc 拿实际地址
    let bind = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = bind.local_addr().unwrap();
    drop(bind);
    let server = tokio::spawn(async move { serve_standalone(&addr.to_string(), state).await });

    // 等 server 就绪
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();

    // /healthz 探活
    sock.write_all(b"GET /healthz HTTP/1.1\r\nhost: t\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    sock.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(text.contains("ok"));

    // aiproxy 转发
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = b"{}";
    let req = format!(
        "POST /aiproxy/mock/v1/embeddings HTTP/1.1\r\nhost: t\r\nauthorization: Bearer zzz\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    sock.write_all(req.as_bytes()).await.unwrap();
    sock.write_all(body).await.unwrap();
    let mut buf2 = Vec::new();
    sock.read_to_end(&mut buf2).await.unwrap();
    let text2 = String::from_utf8_lossy(&buf2);
    assert!(text2.starts_with("HTTP/1.1 200"), "{text2}");

    drop(sock);
    server.abort();
}

#[tokio::test]
async fn normalize_sse_injects_stream_false_when_missing() {
    let mock = spawn_mock(Behavior::Echo);
    let config = config_from_yaml(&format!(
        "  mock:\n    url: {}\n    normalize_sse: true",
        mock.base_url()
    ));

    // 非流式请求：body 不含 stream 字段。
    let payload = br#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
    let resp = oneshot(
        app_state(config, 16 << 20),
        "POST",
        "/aiproxy/mock/v1/chat/completions",
        &[("content-type", "application/json")],
        Some(payload.to_vec()),
    )
    .await;
    assert_eq!(resp.status(), 200);

    let rec = &mock.records()[0];
    let sent = String::from_utf8_lossy(&rec.body);
    assert!(
        sent.contains(r#""stream":false"#),
        "normalize_sse 应注入 stream:false，实际 body: {sent}"
    );
}

#[tokio::test]
async fn normalize_sse_keeps_explicit_stream_untouched() {
    let mock = spawn_mock(Behavior::Echo);
    let config = config_from_yaml(&format!(
        "  mock:\n    url: {}\n    normalize_sse: true",
        mock.base_url()
    ));

    // 流式请求：body 已带 stream:true，不应被改写。
    let payload = br#"{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let resp = oneshot(
        app_state(config, 16 << 20),
        "POST",
        "/aiproxy/mock/v1/chat/completions",
        &[("content-type", "application/json")],
        Some(payload.to_vec()),
    )
    .await;
    assert_eq!(resp.status(), 200);

    let rec = &mock.records()[0];
    let v: serde_json::Value = serde_json::from_slice(&rec.body).unwrap();
    assert_eq!(v["stream"], true, "显式 stream:true 不得被改写: {:?}", v);
}

#[tokio::test]
async fn normalize_sse_collapses_repeated_name_in_stream() {
    let mock = spawn_mock(Behavior::SseRepeatedName);
    let config = config_from_yaml(&format!(
        "  mock:\n    url: {}\n    normalize_sse: true",
        mock.base_url()
    ));

    let resp = oneshot(
        app_state(config, 16 << 20),
        "POST",
        "/aiproxy/mock/v1/chat/completions",
        &[("content-type", "application/json")],
        Some(b"{}".to_vec()),
    )
    .await;
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );

    let mut stream = resp.into_body().into_data_stream();
    let mut collected = Vec::new();
    while let Some(chunk) = stream.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    let text = String::from_utf8(collected).unwrap();
    // 每个 tool-call index 只保留首帧 name
    assert_eq!(text.matches(r#""name":"list_dir""#).count(), 1, "{text}");
    assert!(text.contains(r#""arguments":"{\"path\": ""#), "{text}");
    assert!(text.contains(r#""arguments":"\"/\""#), "{text}");
}

#[tokio::test]
async fn normalize_sse_off_is_byte_identical() {
    let mock = spawn_mock(Behavior::SseRepeatedName);
    let config = config_from_yaml(&format!(
        "  mock:\n    url: {}\n    normalize_sse: false",
        mock.base_url()
    ));

    let resp = oneshot(
        app_state(config, 16 << 20),
        "POST",
        "/aiproxy/mock/v1/chat/completions",
        &[("content-type", "application/json")],
        Some(b"{}".to_vec()),
    )
    .await;
    assert_eq!(resp.status(), 200);

    let mut stream = resp.into_body().into_data_stream();
    let mut collected = Vec::new();
    while let Some(chunk) = stream.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }
    // normalize_sse=false：逐字节透传，与 mock 上游负载完全一致。
    assert_eq!(&collected[..], SSE_REPEATED_NAME.as_bytes());
}
