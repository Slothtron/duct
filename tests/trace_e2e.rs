//! aiproxy 请求轨迹端到端断言（参考 DSH 会话轨迹的事件契约）。
//!
//! 上游为真实 TCP 回环 mock；轨迹经 TraceSink::capture / to_file 走真实代码路径。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::Body;
use bytes::Bytes;
use futures::StreamExt;
use serde_json::Value;
use tower::util::ServiceExt;

use duct::aiproxy::{AppState, router};
use duct::config::Config;
use duct::trace::TraceSink;

static TMP_SEQ: AtomicUsize = AtomicUsize::new(0);

fn config_from_yaml(yaml_body: &str) -> Arc<Config> {
    let seq = TMP_SEQ.fetch_add(1, Ordering::SeqCst);
    let path: PathBuf =
        std::env::temp_dir().join(format!("duct-trace-{}-{seq}.yaml", std::process::id()));
    std::fs::write(&path, format!("providers:\n{yaml_body}")).unwrap();
    let cfg = Config::load_explicit(&path).expect("load test config");
    std::fs::remove_file(&path).ok();
    Arc::new(cfg)
}

// ── Mock 上游 ─────────────────────────────────────────────────────────

/// OpenAI 风格 SSE：内容两帧 + finish_reason + usage 尾帧 + [DONE]。
const SSE_COMPLIANT: &str = concat!(
    "data: {\"id\":\"chatcmpl-1\",\"model\":\"gpt-trace-test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
    "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\" there\"}}]}\n\n",
    "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":18,\"completion_tokens\":5,\"total_tokens\":23}}\n\n",
    "data: [DONE]\n\n",
);

const JSON_NON_STREAM: &str = r#"{"id":"chatcmpl-2","model":"gpt-trace-test","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9}}"#;

#[derive(Clone, Copy)]
enum Behavior {
    Sse,
    Json,
}

fn spawn_mock(behavior: Behavior) -> SocketAddr {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    let (tx, rx) = std::sync::mpsc::channel::<SocketAddr>();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            tx.send(listener.local_addr().unwrap()).unwrap();
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { break };
                tokio::spawn(async move {
                    // 读到头部结束；排空请求体（chunked 或 content-length），
                    // 避免客户端写入半路被 shutdown 干扰。
                    let mut buf = Vec::new();
                    let mut byte = [0u8; 1];
                    while !buf.ends_with(b"\r\n\r\n") {
                        match sock.read(&mut byte).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => buf.push(byte[0]),
                        }
                    }
                    let head = String::from_utf8_lossy(&buf).to_lowercase();
                    if head.contains("transfer-encoding: chunked") {
                        while !buf.ends_with(b"0\r\n\r\n") {
                            match sock.read(&mut byte).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => buf.push(byte[0]),
                            }
                        }
                    } else if let Some(cl) = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .and_then(|v| v.trim().parse::<usize>().ok())
                    {
                        let mut body = vec![0u8; cl];
                        if cl > 0 {
                            let _ = sock.read_exact(&mut body).await;
                        }
                    }
                    match behavior {
                        Behavior::Sse => {
                            // 回显收到的 accept-encoding，供轨迹 identity 协商断言。
                            let ae = head
                                .lines()
                                .find_map(|l| l.trim().strip_prefix("accept-encoding:"))
                                .unwrap_or("<absent>")
                                .replace(' ', "");
                            let h = format!(
                                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nx-request-id: ae={ae}\r\nconnection: close\r\n\r\n"
                            );
                            let framed =
                                format!("{:x}\r\n{}\r\n", SSE_COMPLIANT.len(), SSE_COMPLIANT);
                            let _ = sock.write_all(h.as_bytes()).await;
                            let _ = sock.write_all(framed.as_bytes()).await;
                            let _ = sock.write_all(b"0\r\n\r\n").await;
                        }
                        Behavior::Json => {
                            let h = format!(
                                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nx-request-id: req-42\r\n\r\n",
                                JSON_NON_STREAM.len()
                            );
                            let _ = sock.write_all(h.as_bytes()).await;
                            let _ = sock.write_all(JSON_NON_STREAM.as_bytes()).await;
                        }
                    }
                    let _ = sock.shutdown().await;
                });
            }
        });
    });
    rx.recv().unwrap()
}

// ── 工具 ──────────────────────────────────────────────────────────────

fn state_with_capture(addr: SocketAddr) -> (AppState, Arc<Mutex<Vec<String>>>) {
    let (sink, cap) = TraceSink::capture();
    let config = config_from_yaml(&format!("  mock:\n    url: http://{addr}"));
    let st = AppState::with_trace(config, 16 << 20, Arc::new(sink)).unwrap();
    (st, cap)
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

async fn drain(resp: axum::http::Response<Body>) -> Bytes {
    let mut out = Vec::new();
    let mut stream = resp.into_body().into_data_stream();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    Bytes::from(out)
}

fn parse_events(cap: &Arc<Mutex<Vec<String>>>) -> Vec<Value> {
    let lines = cap.lock().unwrap().clone();
    lines
        .iter()
        .map(|l| serde_json::from_str(l).expect("每行必须是合法 JSONL"))
        .collect()
}

fn types_of(events: &[Value]) -> Vec<&str> {
    events.iter().map(|e| e["type"].as_str().unwrap()).collect()
}

/// 按类型取事件（流式路径下 `request/body` 在 `upstream/request` 之后、
/// 发送期由 `ScannedBody` 触发，故不做位置断言）。
fn event_of<'a>(events: &'a [Value], typ: &str) -> &'a Value {
    events
        .iter()
        .find(|e| e["type"].as_str() == Some(typ))
        .unwrap_or_else(|| panic!("缺事件 {typ}: {:?}", types_of(events)))
}

const REQ_BODY: &[u8] =
    br#"{"model":"gpt-trace-test","stream":true,"messages":[{"role":"user","content":"SECRET PROMPT CONTENT"}]}"#;

// ── 用例 ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn streaming_chat_completes_with_full_event_chain() {
    let addr = spawn_mock(Behavior::Sse);
    let (state, cap) = state_with_capture(addr);

    let resp = oneshot(
        state,
        "POST",
        "/aiproxy/mock/v1/chat/completions",
        &[
            ("content-type", "application/json"),
            ("authorization", "Bearer sk-super-secret-key"),
        ],
        Some(REQ_BODY.to_vec()),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let drained = drain(resp).await;
    assert!(
        String::from_utf8_lossy(&drained).contains("[DONE]"),
        "SSE 必须字节级透传"
    );

    let events = parse_events(&cap);
    // 流式路径的真实事件序：body 事实由发送期旁路产生，落在 upstream/request 之后。
    assert_eq!(
        types_of(&events),
        vec![
            "request/start",
            "upstream/request",
            "request/body",
            "upstream/response",
            "request/end"
        ],
        "事件序列必须完整有序"
    );
    for (i, e) in events.iter().enumerate() {
        assert_eq!(e["seq"], i as u64, "seq 必须从 0 连续");
        assert_eq!(e["v"], 1);
        assert!(e["time"].is_number());
    }
    // 同一请求共享 trace id。
    let trace = events[0]["trace"].as_str().unwrap();
    assert!(events.iter().all(|e| e["trace"].as_str() == Some(trace)));

    let start = event_of(&events, "request/start");
    assert_eq!(start["data"]["method"], "POST");
    assert_eq!(start["data"]["provider"], "mock");
    assert_eq!(start["data"]["path"], "/aiproxy/mock/v1/chat/completions");
    let hdrs = start["data"]["request_headers"].as_array().unwrap();
    assert!(
        hdrs.iter().any(|h| h == "authorization:***"),
        "凭证头必须脱敏: {hdrs:?}"
    );

    // 前缀扫描在不缓冲透传下拿到 model/stream。
    let body_ev = event_of(&events, "request/body");
    assert_eq!(body_ev["data"]["model"], "gpt-trace-test");
    assert_eq!(body_ev["data"]["stream"], "true");
    assert_eq!(body_ev["data"]["bytes"], REQ_BODY.len() as u64);

    assert_eq!(
        event_of(&events, "upstream/request")["data"]["url"],
        format!("http://{addr}/v1/chat/completions")
    );
    let resp_ev = event_of(&events, "upstream/response");
    assert_eq!(resp_ev["data"]["status"], 200);
    assert!(resp_ev["data"]["ttfb_ms"].is_number());

    let end = event_of(&events, "request/end");
    assert_eq!(end["severity"], "info");
    assert_eq!(end["data"]["outcome"], "completed");
    assert_eq!(end["data"]["status"], 200);
    assert_eq!(end["data"]["sse"]["done"], true);
    assert_eq!(end["data"]["sse"]["finish_reasons"][0], "stop");
    assert_eq!(end["data"]["sse"]["usage"]["total_tokens"], 23);
    assert_eq!(end["data"]["sse"]["model"], "gpt-trace-test");
    assert_eq!(end["data"]["sse"]["id"], "chatcmpl-1");
    assert!(end["data"]["resp_bytes"].as_u64().unwrap() > 0);
    assert!(end["data"]["duration_ms"].is_number());

    // 凭证与 prompt 正文绝不入轨迹（全文扫描）。
    let dumped = cap.lock().unwrap().join("\n");
    assert!(
        !dumped.contains("sk-super-secret-key"),
        "API key 泄漏进了轨迹"
    );
    assert!(!dumped.contains("SECRET PROMPT"), "prompt 正文泄漏进了轨迹");
}

#[tokio::test]
async fn non_stream_json_yields_body_facts() {
    let addr = spawn_mock(Behavior::Json);
    let (state, cap) = state_with_capture(addr);

    let resp = oneshot(
        state,
        "POST",
        "/aiproxy/mock/v1/chat/completions",
        &[("content-type", "application/json")],
        Some(br#"{"model":"gpt-trace-test","stream":false,"messages":[]}"#.to_vec()),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body = drain(resp).await;
    assert_eq!(
        body.len(),
        JSON_NON_STREAM.len(),
        "非流式响应体必须原样透传"
    );

    let events = parse_events(&cap);
    let end = events.last().unwrap();
    assert_eq!(end["type"], "request/end");
    assert_eq!(end["data"]["outcome"], "completed");
    assert_eq!(end["data"]["body"]["usage"]["total_tokens"], 9);
    assert_eq!(end["data"]["body"]["finish_reasons"][0], "stop");
    assert_eq!(end["data"]["body"]["model"], "gpt-trace-test");
}

#[tokio::test]
async fn provider_miss_forms_paired_trace() {
    let addr = spawn_mock(Behavior::Json);
    let (state, cap) = state_with_capture(addr);

    let resp = oneshot(state, "GET", "/aiproxy/nope/anything", &[], None).await;
    assert_eq!(resp.status(), 404);

    let events = parse_events(&cap);
    assert_eq!(types_of(&events), vec!["request/start", "request/end"]);
    assert_eq!(events[0]["data"]["known"], false);
    assert_eq!(events[0]["data"]["provider"], "nope");
    assert_eq!(events[1]["severity"], "error", "rejected 收尾映射 error");
    assert_eq!(events[1]["data"]["outcome"], "rejected");
    assert_eq!(events[1]["data"]["gateway_error"]["status"], 404);
}

#[tokio::test]
async fn oversized_body_ends_rejected_413() {
    let addr = spawn_mock(Behavior::Json);
    // max_body=128，转发 4 KiB body → Content-Length 前置快路径直接 413。
    let (sink, cap) = TraceSink::capture();
    let config = config_from_yaml(&format!("  mock:\n    url: http://{addr}"));
    let state = AppState::with_trace(config, 128, Arc::new(sink)).unwrap();

    let resp = oneshot(
        state,
        "POST",
        "/aiproxy/mock/v1/x",
        &[("content-type", "application/json")],
        Some(vec![b'x'; 4000]),
    )
    .await;
    assert_eq!(resp.status(), 413);
    drop(resp);

    let events = parse_events(&cap);
    let kinds = types_of(&events);
    assert_eq!(kinds.first().unwrap(), &"request/start");
    assert_eq!(kinds.last().unwrap(), &"request/end");
    // 413 由网关自产（mock 只会回 JSON 体；此处无 200 即证明未触达上游语义）。
    let end = events.last().unwrap();
    assert_eq!(end["data"]["outcome"], "rejected");
    assert_eq!(end["data"]["gateway_error"]["status"], 413);
    assert_eq!(end["severity"], "error");
}

#[tokio::test]
async fn client_abort_yields_interrupted_end() {
    let addr = spawn_mock(Behavior::Sse);
    let (state, cap) = state_with_capture(addr);

    let resp = oneshot(
        state,
        "POST",
        "/aiproxy/mock/v1/chat/completions",
        &[("content-type", "application/json")],
        Some(REQ_BODY.to_vec()),
    )
    .await;
    // 头部已达，但响应体一节都不消费就 Drop —— tap 的 Drop 兜底。
    drop(resp);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let events = parse_events(&cap);
    let end = events.last().expect("必须有收尾事件");
    assert_eq!(end["type"], "request/end");
    assert_eq!(end["data"]["outcome"], "interrupted");
    assert_eq!(end["severity"], "warn");
}

#[tokio::test]
async fn upstream_unreachable_ends_upstream_error() {
    // 绑一个端口随即释放，得到必拒地址。
    let tmp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = tmp.local_addr().unwrap();
    drop(tmp);

    let (sink, cap) = TraceSink::capture();
    let config = config_from_yaml(&format!("  mock:\n    url: http://{dead_addr}"));
    let state = AppState::with_trace(config, 16 << 20, Arc::new(sink)).unwrap();

    let resp = oneshot(
        state,
        "POST",
        "/aiproxy/mock/v1/chat/completions",
        &[("content-type", "application/json")],
        Some(br#"{"model":"m","stream":true}"#.to_vec()),
    )
    .await;
    assert_eq!(resp.status(), 502);

    let events = parse_events(&cap);
    let kinds = types_of(&events);
    assert!(kinds.contains(&"upstream/error"), "{kinds:?}");
    let end = events.last().unwrap();
    assert_eq!(end["data"]["outcome"], "upstream_error");
    assert_eq!(end["severity"], "error");
    assert_eq!(end["data"]["gateway_error"]["status"], 502);
}

#[tokio::test]
async fn file_sink_writes_appended_readable_jsonl() {
    let addr = spawn_mock(Behavior::Json);
    let seq = TMP_SEQ.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!("duct-trace-{}-{seq}.jsonl", std::process::id()));
    let sink = Arc::new(TraceSink::to_file(&path).unwrap());
    let config = config_from_yaml(&format!("  mock:\n    url: http://{addr}"));
    let state = AppState::with_trace(config, 16 << 20, sink).unwrap();

    let resp = oneshot(
        state,
        "POST",
        "/aiproxy/mock/v1/chat/completions",
        &[("content-type", "application/json")],
        Some(br#"{"model":"file-sink-check","stream":false,"messages":[]}"#.to_vec()),
    )
    .await;
    let _ = drain(resp).await;

    // writer 线程异步落盘：轮询直到收尾行出现（上限 ~2s）。
    let mut text = String::new();
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        text = std::fs::read_to_string(&path).unwrap_or_default();
        if text.matches("\"request/end\"").count() == 1 {
            break;
        }
    }
    std::fs::remove_file(&path).ok();
    let lines: Vec<Value> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert!(lines.len() >= 4, "文件 sink 应落完整链: {lines:?}");
    assert_eq!(lines[0]["type"], "request/start");
    assert_eq!(lines.last().unwrap()["type"], "request/end");
    assert_eq!(lines.last().unwrap()["data"]["outcome"], "completed");
}

#[test]
fn redaction_lists_are_prefix_safe() {
    // 防回归：确保敏感清单覆盖主流供应商凭证头。
    let mut h = axum::http::HeaderMap::new();
    h.insert(
        "x-api-key",
        axum::http::HeaderValue::from_static("sk-ant-secret"),
    );
    h.insert(
        "api-key",
        axum::http::HeaderValue::from_static("azure-secret"),
    );
    h.insert(
        "anthropic-version",
        axum::http::HeaderValue::from_static("2023-06-01"),
    );
    let summary = duct::trace::header_summary(&h);
    let joined = summary.join("|");
    assert!(joined.contains("x-api-key:***"));
    assert!(joined.contains("api-key:***"));
    assert!(
        joined.contains("anthropic-version:2023-06-01"),
        "语义头应带值"
    );
    assert!(!joined.contains("secret"));
}

#[tokio::test]
async fn trace_body_capture_records_heads_and_identity() {
    let addr = spawn_mock(Behavior::Sse);
    let (sink, cap) = TraceSink::capture();
    let config = config_from_yaml(&format!("  mock:\n    url: http://{addr}"));
    // 开启内容采集：请求/响应头部快照应出现，且上游收到 identity 协商。
    let state = AppState::with_trace_body(config, 16 << 20, Arc::new(sink), 256).unwrap();

    let resp = oneshot(
        state,
        "POST",
        "/aiproxy/mock/v1/chat/completions",
        &[
            ("content-type", "application/json"),
            ("accept-encoding", "gzip, deflate"),
        ],
        Some(REQ_BODY.to_vec()),
    )
    .await;
    assert_eq!(resp.status(), 200);
    drain(resp).await;

    let events = parse_events(&cap);
    let body_ev = event_of(&events, "request/body");
    let head = body_ev["data"]["req_content_head"]
        .as_str()
        .expect("req head");
    assert!(head.starts_with("{\"model\":\"gpt-trace-test\""), "{head}");
    assert!(head.chars().count() <= 256);

    let end = event_of(&events, "request/end");
    let rhead = end["data"]["resp_content_head"]
        .as_str()
        .expect("resp head");
    assert!(rhead.starts_with("data: {\"id\":\"chatcmpl-1\""), "{rhead}");
    assert!(
        end["data"].get("resp_content_skipped").is_none(),
        "明文流不应跳过"
    );

    // 响应可解析出内容事实 ⇒ 流确为明文。
    assert_eq!(end["data"]["sse"]["done"], true);
    assert_eq!(end["data"]["sse"]["usage"]["total_tokens"], 23);
    // 直接证据：mock 回显它收到的 accept-encoding —— 客户端原文是 gzip,deflate，
    // 上游实际收到的是 duct 协商改写后的 identity。
    let resp_ev = event_of(&events, "upstream/response");
    let rh = resp_ev["data"]["response_headers"].as_array().unwrap();
    assert!(
        rh.iter().any(|h| h == "x-request-id:ae=identity"),
        "上游应收到 identity 协商: {rh:?}"
    );
}

// ── 压缩上游 + normalize_sse 行为复现（kso 定性实验）────────────────────

/// kso 风格流：每个 chunk 都重发完整 function.name，且**整条流 gzip 压缩、
/// 无视 identity 协商** —— 复现真实网关形态。
fn gz(bytes: &[u8]) -> Vec<u8> {
    let mut enc = Vec::new();
    {
        let mut g = flate2::write::GzEncoder::new(&mut enc, flate2::Compression::default());
        std::io::Write::write_all(&mut g, bytes).unwrap();
        g.finish().unwrap();
    }
    enc
}

fn kso_repeated_stream() -> &'static str {
    r#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"read_file","arguments":""}}]}}]}

data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"read_file","arguments":"{\"path\":"}}]}}]}

data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"read_file","arguments":"\"a.rs\"}"}}]}}]}

data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}

data: [DONE]

"#
}

#[tokio::test]
async fn compressed_kso_style_stream_is_decoded_normalized_and_reemitted() {
    // kso 形态定性复现：每帧重发完整 function.name + 强制 gzip + 无视 identity。
    // 期望（修复后）：客户端收到明文 SSE，name 只在首帧出现一次；
    // 轨迹侧事实完整（decoded/done/finish_reason=tool_calls）。
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut byte = [0u8; 1];
                while !buf.ends_with(b"\r\n\r\n") {
                    if sock.read(&mut byte).await.unwrap_or(0) == 0 {
                        return;
                    }
                    buf.push(byte[0]);
                }
                let body = gz(kso_repeated_stream().as_bytes());
                let h = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-encoding: gzip\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n";
                let mut framed = Vec::new();
                framed.extend_from_slice(format!("{:x}\r\n", body.len()).as_bytes());
                framed.extend_from_slice(&body);
                framed.extend_from_slice(b"\r\n");
                let _ = sock.write_all(h.as_bytes()).await;
                let _ = sock.write_all(&framed).await;
                let _ = sock.write_all(b"0\r\n\r\n").await;
                let _ = sock.shutdown().await;
            });
        }
    });

    let (sink, cap) = TraceSink::capture();
    let config = config_from_yaml(&format!(
        "  kso:\n    url: http://{addr}\n    normalize_sse: true"
    ));
    let state = AppState::with_trace(config, 16 << 20, Arc::new(sink)).unwrap();

    let resp = oneshot(
        state,
        "POST",
        "/aiproxy/kso/chat/completions",
        &[
            ("content-type", "application/json"),
            ("accept-encoding", "gzip, deflate, br"),
        ],
        Some(
            br#"{"model":"kso-repro","stream":true,"messages":[{"role":"user","content":"hi"}]}"#
                .to_vec(),
        ),
    )
    .await;
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("content-encoding").is_none(),
        "明文重发必须剥掉 content-encoding"
    );
    let plain = String::from_utf8_lossy(&drain(resp).await).into_owned();
    assert!(
        plain.starts_with("data: "),
        "客户端应直接收到明文 SSE: {plain}"
    );
    let dup = plain.matches(r#""name":"read_file""#).count();
    assert_eq!(dup, 1, "重复 name 必须被归一化为首帧一次，实得 {dup}");
    assert!(plain.contains(r"[DONE]"));

    let events = parse_events(&cap);
    let end = event_of(&events, "request/end");
    assert_eq!(end["data"]["sse"]["decoded"], true);
    assert_eq!(end["data"]["sse"]["done"], true);
    assert!(
        end["data"]["sse"]["finish_reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "tool_calls")
    );
}
