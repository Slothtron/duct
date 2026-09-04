//! MCP 转发核心（设计 §5，本期 = 方案 A：Streamable HTTP 透明转发）。
//!
//! `{method} /mcp/{server}/{剩余路径}?{query}` → `{method} {server.url}/{剩余路径}?{query}`
//!
//! 与 aiproxy 同构（§1.2 / §4）——传输层整套复用其底座：
//! - 路径前缀一次切分，剩余路径不解释；`server` 仅来自预配置清单（`mcp.servers`）
//! - 请求体逐字节流式透传、响应逐 chunk 回传（含 `text/event-stream` 长流），仅 connect 超时、无整体超时
//! - 凭证零接触：头按黑名单制透传（`Authorization` 等原样透传），不存 Key、不注入 Key
//! - 轨迹接入同一 `TraceSink`（`branch:"mcp"`），`TracedBody` 固定 `sse:false`
//!   （SSE 词汇属 OpenAI 语义，见 §5.6；JSON-RPC 回包仍走 JSON 事实提取）
//!
//! 差异（§5.3）：MCP `DELETE` 默认无体；`origin_policy` 控制 Origin 头三态；
//! 上游流不做任何行级改写（二期 B 才做 endpoint 改写）。

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Uri, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::Stream;

use crate::config::{Config, McpServerConfig, OriginPolicy};
use crate::error::AppError;
use crate::trace::{
    RequestTrace, RespFacts, TraceSink, TracedBody, header_summary, query_keys, url_display,
};
use serde_json::{Map, Value, json};

/// 上游连接超时；不设整体请求超时（GET 长挂通知流合法）。
pub const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// mcp 转发共享状态（同构 `AppState`；§5.7）。
#[derive(Clone)]
pub struct McpState {
    pub config: Arc<Config>,
    pub client: reqwest::Client,
    /// 请求体上限（字节），来自 `--max-body`。
    pub max_body: usize,
    /// 请求轨迹接收端（与 aiproxy 共用同一 `TraceSink`，单一排查入口）。
    pub trace: Arc<TraceSink>,
    /// `--trace-body`：>0 时记录请求/响应头部快照并协商 `accept-encoding: identity`。
    pub trace_body: usize,
}

impl McpState {
    pub fn new(config: Arc<Config>, max_body: usize) -> anyhow::Result<Self> {
        Self::with_trace(config, max_body, Arc::new(TraceSink::none()))
    }

    pub fn with_trace(
        config: Arc<Config>,
        max_body: usize,
        trace: Arc<TraceSink>,
    ) -> anyhow::Result<Self> {
        Self::with_trace_body(config, max_body, trace, 0)
    }

    pub fn with_trace_body(
        config: Arc<Config>,
        max_body: usize,
        trace: Arc<TraceSink>,
        trace_body: usize,
    ) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(UPSTREAM_CONNECT_TIMEOUT)
            .build()?;
        Ok(Self {
            config,
            client,
            max_body,
            trace,
            trace_body,
        })
    }
}

// ── Header 黑名单（§6.4 黑名单制；mcp 侧各自持有，避免过早抽象）──────────

/// 请求方向需剥离的头。
const REQUEST_HEADER_BLACKLIST: &[&str] = &[
    "connection",
    "keep-alive",
    "te",
    "trailer",
    "upgrade",
    "transfer-encoding",
    "content-length",
    "host",
    "expect",
];

/// 响应方向需剥离的头。
const RESPONSE_HEADER_BLACKLIST: &[&str] = &[
    "connection",
    "keep-alive",
    "te",
    "trailer",
    "upgrade",
    "transfer-encoding",
    "content-length",
];

fn is_blacklisted(name: &HeaderName, list: &[&str]) -> bool {
    let lower = name.as_str();
    if lower.starts_with("proxy-") {
        return true;
    }
    list.contains(&lower)
}

fn forward_allowed(headers: &HeaderMap, blacklist: &[&str]) -> HeaderMap {
    headers
        .iter()
        .filter(|(name, _)| !is_blacklisted(name, blacklist))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

// ── 请求体限长流式适配器（§6.3：--max-body 强制）──────────────────────

#[derive(Debug)]
struct BodyLimitExceeded;

impl std::fmt::Display for BodyLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "request body exceeds limit")
    }
}

impl std::error::Error for BodyLimitExceeded {}

fn map_stream_err<E>(_e: E) -> BodyLimitExceeded {
    BodyLimitExceeded
}

/// 包裹入站 body 流，累计字节数超限即报错，中断对上游的发送。
struct LimitedBody<S> {
    inner: S,
    remaining: usize,
}

impl<S> Stream for LimitedBody<S>
where
    S: Stream<Item = Result<Bytes, axum::Error>> + Unpin,
{
    type Item = Result<Bytes, BodyLimitExceeded>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                if bytes.len() > self.remaining {
                    return Poll::Ready(Some(Err(BodyLimitExceeded)));
                }
                self.remaining -= bytes.len();
                Poll::Ready(Some(Ok(bytes)))
            }
            other => other.map(|opt| opt.map(|res| res.map_err(map_stream_err))),
        }
    }
}

fn limited_body<S>(body: S, max_body: usize) -> reqwest::Body
where
    S: Stream<Item = Result<Bytes, axum::Error>> + Unpin + Send + 'static,
{
    reqwest::Body::wrap_stream(LimitedBody {
        inner: body,
        remaining: max_body,
    })
}

/// 判断请求是否携带 body。MCP `DELETE` 规范上无体，按「默认无体」处理
/// （避免给上游挂空 chunked 体）；POST/PUT/PATCH 及带 CL 请求照旧。
fn mcp_request_has_body(method: &Method, headers: &HeaderMap) -> bool {
    if headers.contains_key(axum::http::header::CONTENT_LENGTH) {
        return true;
    }
    matches!(*method, Method::POST | Method::PUT | Method::PATCH)
}

/// 提取 `server.url` 的 origin（`scheme://authority`），供 `origin_policy: upstream` 使用。
fn origin_of(url: &str) -> Option<String> {
    let u = reqwest::Url::parse(url).ok()?;
    let port = u.port().map(|p| format!(":{p}")).unwrap_or_default();
    Some(format!("{}://{}{}", u.scheme(), u.host_str()?, port))
}

/// 根据 origin_policy 应用 Origin 头（§5.5 三态）。
fn apply_origin_policy(headers: &mut HeaderMap, policy: OriginPolicy, server_url: &str) {
    match policy {
        OriginPolicy::Keep => {}
        OriginPolicy::Strip => {
            headers.remove(header::ORIGIN);
        }
        OriginPolicy::Upstream => {
            if let Some(v) = origin_of(server_url).and_then(|o| HeaderValue::from_str(&o).ok()) {
                headers.insert(header::ORIGIN, v);
            }
        }
    }
}

// ── 路由与处理器 ──────────────────────────────────────────────────────

/// 构建 mcp 路由（仅 `/mcp/*` 子树；`/healthz` 归入口分流层）。
pub fn router(state: McpState) -> axum::Router {
    axum::Router::new()
        .route("/mcp", axum::routing::any(mcp_bare))
        .route("/mcp/", axum::routing::any(mcp_bare))
        .route("/mcp/:server", axum::routing::any(forward_root))
        .route("/mcp/:server/", axum::routing::any(forward_root))
        .route("/mcp/:server/*rest", axum::routing::any(forward))
        .with_state(state)
}

fn server_not_found(id: &str, config: &Config) -> AppError {
    AppError::ServerNotFound {
        requested: id.to_string(),
        available: if config.mcp_is_empty() {
            "(none configured)".to_string()
        } else {
            config.mcp_server_ids().join(", ")
        },
    }
}

/// 解析目标 URL 并拼接 query（原样保留编码）。
fn target_url(server_url: &str, rest: &str, query: Option<&str>) -> Result<reqwest::Url, AppError> {
    let mut url = if rest.is_empty() {
        server_url.to_string()
    } else {
        format!("{server_url}/{rest}")
    };
    if let Some(q) = query {
        url.push('?');
        url.push_str(q);
    }
    reqwest::Url::parse(&url)
        .map_err(|e| AppError::UpstreamError(format!("invalid upstream url: {e}")))
}

/// 入站请求部件束（axum 提取器 + 一次切分的剩余路径）。
struct Incoming {
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
    rest: String,
}

/// 网关自身产生的错误映射为 `request/end` 的 `gateway_error` 对象。
fn gateway_error(err: &AppError, status: u16, etype: &'static str) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert(
        "gateway_error".into(),
        json!({"message": err.to_string(), "type": etype, "status": status}),
    );
    m
}

/// reqwest 在发送途中因流错误中止时，错误源链里能找到我们的限长标记。
fn is_body_limit_exceeded(err: &reqwest::Error) -> bool {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(c) = cause {
        if c.downcast_ref::<BodyLimitExceeded>().is_some() {
            return true;
        }
        cause = c.source();
    }
    false
}

async fn respond_forwarded(
    state: &McpState,
    tr: &Arc<RequestTrace>,
    p: &McpServerConfig,
    req: Incoming,
) -> Response {
    tr.emit(
        "request/start",
        json!({
            "method": req.method.as_str(),
            "path": req.uri.path(),
            "query_keys": query_keys(req.uri.query()),
            "branch": "mcp",
            "server": p.id,
            "origin_policy": p.origin_policy.as_str(),
            "request_headers": header_summary(&req.headers),
        }),
    );

    let url = match target_url(&p.url, &req.rest, req.uri.query()) {
        Ok(u) => u,
        Err(e) => {
            tr.emit_with(
                "upstream/error",
                "error",
                json!({"class": "bad_upstream_url", "message": e.to_string()}),
            );
            let (status, etype) = e.trace_identity();
            tr.end("rejected", gateway_error(&e, status, etype));
            return e.into_response();
        }
    };

    // Content-Length 前置快路径校验（§6.5）
    let oversized = req
        .headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<usize>().ok())
        .is_some_and(|len| len > state.max_body);
    if oversized {
        let err = AppError::BodyTooLarge;
        let (status, etype) = err.trace_identity();
        tr.end("rejected", gateway_error(&err, status, etype));
        return err.into_response();
    }

    tr.emit("upstream/request", json!({"url": url_display(&url)}));

    let mut outgoing_headers = forward_allowed(&req.headers, REQUEST_HEADER_BLACKLIST);
    apply_origin_policy(&mut outgoing_headers, p.origin_policy, &p.url);
    if state.trace_body > 0 {
        // 内容采集开启：协商明文响应，使头部快照可读。
        outgoing_headers.insert(
            axum::http::header::ACCEPT_ENCODING,
            axum::http::HeaderValue::from_static("identity"),
        );
    }
    let attach_body = mcp_request_has_body(&req.method, &req.headers);
    let Incoming {
        method, uri, body, ..
    } = req;

    let mut send = state
        .client
        .request(method.clone(), url)
        .headers(outgoing_headers);
    if attach_body {
        // 流式透传语义不变；MCP 请求体不做扫描（ScannedBody 提取的 model/stream 无意义）。
        send = send.body(limited_body(body.into_data_stream(), state.max_body));
    }

    let upstream = match send.send().await {
        Ok(resp) => resp,
        Err(e) => {
            let (class, err) = if is_body_limit_exceeded(&e) {
                tracing::warn!(trace = %tr.trace_id, server = %p.id, "request body exceeded --max-body mid-stream");
                ("body_limit_exceeded", AppError::BodyTooLarge)
            } else if e.is_timeout() {
                tracing::error!(trace = %tr.trace_id, server = %p.id, error = %e, "upstream connect timeout");
                ("connect_timeout", AppError::UpstreamTimeout)
            } else {
                tracing::error!(trace = %tr.trace_id, server = %p.id, error = %e, "upstream connection failed");
                ("connect_failed", AppError::UpstreamError(e.to_string()))
            };
            tr.emit_with(
                "upstream/error",
                "error",
                json!({"class": class, "message": e.to_string()}),
            );
            let (status, etype) = err.trace_identity();
            let outcome = if matches!(err, AppError::BodyTooLarge) {
                "rejected"
            } else {
                "upstream_error"
            };
            tr.end(outcome, gateway_error(&err, status, etype));
            return err.into_response();
        }
    };

    let status = upstream.status();
    let response_headers = forward_allowed(upstream.headers(), RESPONSE_HEADER_BLACKLIST);
    let ttfb_ms = tr.elapsed_ms();
    tr.set_resp(RespFacts {
        status: status.as_u16(),
        ttfb_ms,
    });
    tr.emit(
        "upstream/response",
        json!({
            "status": status.as_u16(),
            "ttfb_ms": ttfb_ms,
            "response_headers": header_summary(upstream.headers()),
        }),
    );

    tracing::info!(
        trace = %tr.trace_id,
        server = %p.id,
        method = %method,
        path = %uri.path(),
        status = %status,
        elapsed_ms = ttfb_ms,
        "mcp forwarded"
    );

    // 观测在最内层：TracedBody 看到的是上游原始字节；固定 sse:false（§5.6）。
    let upstream_encoding = upstream
        .headers()
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.eq_ignore_ascii_case("identity") && !v.is_empty())
        .map(str::to_string);
    let traced = TracedBody::new(
        upstream.bytes_stream(),
        tr.clone(),
        false,
        upstream_encoding.as_deref(),
        state.trace_body,
    );
    let mut builder = Response::builder().status(status.as_u16());
    for (name, value) in &response_headers {
        builder = builder.header(name, value);
    }
    builder
        .body(Body::from_stream(traced))
        // tap 的 Drop 兜底会为这条被丢弃的响应写出 request/end{interrupted}。
        .unwrap_or_else(|e| AppError::UpstreamError(e.to_string()).into_response())
}

/// 裸 `/mcp` 或 `/mcp/`：404 JSON，提示用法 + 已配置 server 列表（§5.2）。
async fn mcp_bare(
    State(state): State<McpState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let tr = RequestTrace::new(state.trace.clone());
    tr.emit(
        "request/start",
        json!({
            "method": method.as_str(),
            "path": uri.path(),
            "query_keys": query_keys(uri.query()),
            "branch": "mcp",
            "known": false,
            "available": state.config.mcp_server_ids(),
            "request_headers": header_summary(&headers),
        }),
    );
    let available = if state.config.mcp_is_empty() {
        "(none configured)".to_string()
    } else {
        state.config.mcp_server_ids().join(", ")
    };
    let message = format!(
        "MCP forwarding expects /mcp/<server>/<path>?query (Streamable HTTP). Available servers: {available}"
    );
    let err = AppError::ServerNotFound {
        requested: "(bare /mcp)".into(),
        available: available.clone(),
    };
    let (status, _etype) = err.trace_identity();
    tr.end("rejected", gateway_error(&err, status, _etype));
    (
        axum::http::StatusCode::from_u16(status).unwrap_or(axum::http::StatusCode::NOT_FOUND),
        axum::Json(json!({ "error": { "message": message, "type": "invalid_request_error" } })),
    )
        .into_response()
}

async fn dispatch_forward(
    state: McpState,
    server: String,
    rest: String,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let tr = RequestTrace::new(state.trace.clone());
    let Some(p) = state.config.get_mcp(&server) else {
        tracing::debug!(trace = %tr.trace_id, server = %server, path = %uri.path(), "mcp server miss");
        // 未注册 server 也成轨迹：start + end 成对，404 根因可回放。
        tr.emit(
            "request/start",
            json!({
                "method": method.as_str(),
                "path": uri.path(),
                "query_keys": query_keys(uri.query()),
                "branch": "mcp",
                "server": server,
                "known": false,
                "available": state.config.mcp_server_ids(),
                "request_headers": header_summary(&headers),
            }),
        );
        let err = server_not_found(&server, &state.config);
        let (status, etype) = err.trace_identity();
        tr.end("rejected", gateway_error(&err, status, etype));
        return err.into_response();
    };
    respond_forwarded(
        &state,
        &tr,
        p,
        Incoming {
            method,
            uri,
            headers,
            body,
            rest,
        },
    )
    .await
}

/// `/mcp/{server}` —— 无剩余路径，转发到 server.url 本身（Streamable HTTP 主用法）。
async fn forward_root(
    State(state): State<McpState>,
    Path(server): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    dispatch_forward(state, server, String::new(), method, uri, headers, body).await
}

/// `/mcp/{server}/{*rest}` —— 一次切分后整段透传。
async fn forward(
    State(state): State<McpState>,
    Path((server, rest)): Path<(String, String)>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    dispatch_forward(state, server, rest, method, uri, headers, body).await
}

/// 将「已被手工读走请求行」的连接嫁接给进程内 axum/hyper 栈（经 `bridge.rs` 公共桥）。
pub async fn serve_conn_from_prelude(
    state: McpState,
    prelude: &[u8],
    client: tokio::net::TcpStream,
) -> anyhow::Result<()> {
    crate::bridge::serve_conn_from_prelude(router(state), prelude, client).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn m1_target_url_joins_root_rest_and_query() {
        // 根路径：转发到 server.url 本身
        assert_eq!(
            target_url("https://api.githubcopilot.com/mcp", "", None)
                .unwrap()
                .as_str(),
            "https://api.githubcopilot.com/mcp"
        );
        // rest + query 原样保留（含编码）
        assert_eq!(
            target_url(
                "https://api.githubcopilot.com/mcp",
                "tools/call",
                Some("k=1%20a&b=2"),
            )
            .unwrap()
            .as_str(),
            "https://api.githubcopilot.com/mcp/tools/call?k=1%20a&b=2"
        );
    }

    #[test]
    fn m2_origin_policy_three_states() {
        // keep：存在则原样保留
        let mut h = HeaderMap::new();
        h.insert("origin", HeaderValue::from_static("https://client.example"));
        apply_origin_policy(&mut h, OriginPolicy::Keep, "https://up.example/mcp");
        assert_eq!(h.get("origin").unwrap(), "https://client.example");

        // keep：无 Origin 时不造
        let mut h = HeaderMap::new();
        apply_origin_policy(&mut h, OriginPolicy::Keep, "https://up.example/mcp");
        assert!(h.get("origin").is_none());

        // strip：剥掉原有 Origin
        let mut h = HeaderMap::new();
        h.insert("origin", HeaderValue::from_static("https://client.example"));
        apply_origin_policy(&mut h, OriginPolicy::Strip, "https://up.example/mcp");
        assert!(h.get("origin").is_none());

        // upstream：改写为 server.url 的 origin（含端口）
        let mut h = HeaderMap::new();
        apply_origin_policy(&mut h, OriginPolicy::Upstream, "http://127.0.0.1:9100/mcp");
        assert_eq!(h.get("origin").unwrap(), "http://127.0.0.1:9100");
    }

    #[test]
    fn m3_delete_defaults_to_no_body_but_post_and_cl_body() {
        let mut headers = HeaderMap::new();
        // 无 CL：DELETE 视为无体（MCP DELETE 规范无体）；POST 视为有体
        assert!(!mcp_request_has_body(&Method::DELETE, &headers));
        assert!(mcp_request_has_body(&Method::POST, &headers));
        assert!(mcp_request_has_body(&Method::PUT, &headers));
        assert!(mcp_request_has_body(&Method::PATCH, &headers));
        // 有 CL 一律有体（含显式 0）
        headers.insert("content-length", HeaderValue::from_static("0"));
        assert!(mcp_request_has_body(&Method::DELETE, &headers));
        assert!(mcp_request_has_body(&Method::GET, &headers));
    }

    #[test]
    fn origin_of_extracts_authority() {
        assert_eq!(
            origin_of("https://api.githubcopilot.com/mcp").unwrap(),
            "https://api.githubcopilot.com"
        );
        assert_eq!(
            origin_of("http://127.0.0.1:9100/mcp").unwrap(),
            "http://127.0.0.1:9100"
        );
    }
}
