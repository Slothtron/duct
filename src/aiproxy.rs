//! aiproxy 反向代理核心（设计文档 v3.2 §6.1–§6.4）。
//!
//! `{method} /aiproxy/{provider}/{剩余路径}?{query}` → `{method} {base_url}/{剩余路径}?{query}`
//!
//! 关键语义（P3/P4/P5）：
//! - 路径前缀一次切分，剩余路径不再解释；provider 仅来自预配置清单
//! - 请求体逐字节流式透传（不整体缓冲），响应逐 chunk 流式回传
//! - 凭证零接触：`Authorization` / `x-api-key` 等一切头逐字节透传，不存 Key、不注入 Key
//! - 头处理为黑名单制：仅剥离逐跳头与 `Proxy-*` 系列，其余全透传

use std::time::{Duration, Instant};
use std::{pin::Pin, sync::Arc, task::{Context as TaskContext, Poll}};

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, Method, Uri};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::Stream;

use crate::config::Config;
use crate::error::AppError;
use crate::sse_normalize::{SseToolNormalizer, normalize_stream_field};

/// 上游连接超时；不设整体请求超时（SSE 长流不能被总时长掐断，§6.3）。
pub const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// aiproxy 共享状态。
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub client: reqwest::Client,
    /// 请求体上限（字节），来自 `--max-body`。
    pub max_body: usize,
}

impl AppState {
    pub fn new(config: Arc<Config>, max_body: usize) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(UPSTREAM_CONNECT_TIMEOUT)
            // 依赖集未启用任何自动解压特性，保证字节级透传语义（含 Content-Length 一致性）
            .build()?;
        Ok(Self {
            config,
            client,
            max_body,
        })
    }
}

// ── Header 黑名单（§6.4：黑名单制，其余一律原样透传）─────────────────

/// 请求方向需剥离的头：逐跳头 + 代理语义头 + 由转发层重建的头。
const REQUEST_HEADER_BLACKLIST: &[&str] = &[
    "connection",
    "keep-alive",
    "te",
    "trailer",
    "upgrade",
    "transfer-encoding",
    "content-length", // 由 hyper/reqwest 按实际 body 重建
    "host",           // 重写为上游 host（reqwest 依 Url 自动处理）
    "expect",         // 100-continue 语义由转发栈自行协商
];

/// 响应方向需剥离的头。
const RESPONSE_HEADER_BLACKLIST: &[&str] = &[
    "connection",
    "keep-alive",
    "te",
    "trailer",
    "upgrade",
    "transfer-encoding",
    "content-length", // 回传 body 以流式框架重建，避免长度不一致的协议错误
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

/// 请求体超过上限时终止转发。
#[derive(Debug)]
struct BodyLimitExceeded;

impl std::fmt::Display for BodyLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "request body exceeds limit")
    }
}

impl std::error::Error for BodyLimitExceeded {}

/// 将任一流错误统一映射为限长错误（配合前置 CL 快路径，§6.3）。
fn map_stream_err<E>(_e: E) -> BodyLimitExceeded {
    BodyLimitExceeded
}

/// 包裹入站 body 流，累计字节数超限即报错，从而中断对上游的发送。
///
/// 前置快路径：带 `Content-Length` 且超限的请求在发起上游连接前就被拒绝；
/// 本适配器兜底无 `Content-Length`（chunked）的超限流。
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

fn limited_body(body: Body, max_body: usize) -> reqwest::Body {
    reqwest::Body::wrap_stream(LimitedBody {
        inner: body.into_data_stream(),
        remaining: max_body,
    })
}

/// 判断请求是否携带 body（GET/HEAD 等通常无体；有 CL 或典型携带体的方法才挂载），
/// 避免给幂等方法附加空 chunked 体干扰上游。
fn request_has_body(method: &Method, headers: &HeaderMap) -> bool {
    if headers.contains_key(axum::http::header::CONTENT_LENGTH) {
        return true;
    }
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

// ── 路由与处理器 ──────────────────────────────────────────────────────

/// 构建 aiproxy 路由（仅 `/aiproxy/*` 子树；不包含 /healthz——其归入口分流层）。
pub fn router(state: AppState) -> axum::Router {
    axum::Router::new()
        .route("/aiproxy/:provider", axum::routing::any(forward_root))
        .route("/aiproxy/:provider/", axum::routing::any(forward_root))
        .route("/aiproxy/:provider/*rest", axum::routing::any(forward))
        .with_state(state)
}

fn provider_not_found(id: &str, config: &Config) -> AppError {
    AppError::ProviderNotFound {
        requested: id.to_string(),
        available: if config.is_empty() {
            "(none configured)".to_string()
        } else {
            config.provider_ids().join(", ")
        },
    }
}

/// 解析目标 URL 并拼接 query（原样保留编码）。
/// 剩余路径为空时转发到 base url 本身（§6.1：探活与根路径型端点）。
fn target_url(base_url: &str, rest: &str, query: Option<&str>) -> Result<reqwest::Url, AppError> {
    let mut url = if rest.is_empty() {
        base_url.to_string()
    } else {
        format!("{base_url}/{rest}")
    };
    if let Some(q) = query {
        url.push('?');
        url.push_str(q);
    }
    reqwest::Url::parse(&url)
        .map_err(|e| AppError::UpstreamError(format!("invalid upstream url: {e}")))
}

async fn respond_forwarded(
    state: &AppState,
    method: &Method,
    provider_base_url: &str,
    rest: &str,
    uri: &Uri,
    headers: HeaderMap,
    body: Body,
    normalize_sse: bool,
) -> Response {
    let provider_id_log = uri.path().split('/').nth(2).unwrap_or("?").to_string();

    let url = match target_url(provider_base_url, rest, uri.query()) {
        Ok(u) => u,
        Err(e) => return e.into_response(),
    };

    // Content-Length 前置快路径校验（§6.5）
    let oversized = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<usize>().ok())
        .is_some_and(|len| len > state.max_body);
    if oversized {
        return AppError::BodyTooLarge.into_response();
    }

    let outgoing_headers = forward_allowed(&headers, REQUEST_HEADER_BLACKLIST);
    let attach_body = request_has_body(method, &headers);

    let started = Instant::now();
    let mut send = state
        .client
        .request(method.clone(), url)
        .headers(outgoing_headers);
    if attach_body {
        if normalize_sse {
            // 规范化请求：读取 body（受 --max-body 约束）并补上缺失的 stream 字段，
            // 让「缺 stream 即流式」的网关对非流式请求返回合规 JSON。
            match read_and_normalize_body(body, state.max_body).await {
                Ok(bytes) => {
                    send = send.body(reqwest::Body::from(bytes));
                }
                Err(_) => return AppError::BodyTooLarge.into_response(),
            }
        } else {
            send = send.body(limited_body(body, state.max_body));
        }
    }

    let upstream = match send.send().await {
        Ok(resp) => resp,
        Err(e) => {
            if is_body_limit_exceeded(&e) {
                tracing::warn!(provider = %provider_id_log, "request body exceeded --max-body mid-stream");
                return AppError::BodyTooLarge.into_response();
            }
            if e.is_timeout() {
                tracing::error!(provider = %provider_id_log, error = %e, "upstream connect timeout");
                return AppError::UpstreamTimeout.into_response();
            }
            tracing::error!(provider = %provider_id_log, error = %e, "upstream connection failed");
            return AppError::UpstreamError(e.to_string()).into_response();
        }
    };

    let status = upstream.status();
    let response_headers = forward_allowed(upstream.headers(), RESPONSE_HEADER_BLACKLIST);

    tracing::info!(
        provider = %provider_id_log,
        method = %method,
        path = %uri.path(),
        status = %status,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "aiproxy forwarded"
    );

    let upstream_ct = upstream
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let stream = upstream.bytes_stream();
    let mut builder = Response::builder().status(status.as_u16());
    for (name, value) in &response_headers {
        builder = builder.header(name, value);
    }
    // 工具名归一化仅在「开启 normalize_sse 且上游返回 SSE」时生效；
    // 其余情况维持现有逐字节透传。
    let body = if normalize_sse && upstream_ct.starts_with("text/event-stream") {
        tracing::debug!(provider = %provider_id_log, "sse-normalize: wrapping upstream event-stream with SseToolNormalizer");
        Body::from_stream(SseToolNormalizer::new(stream))
    } else {
        Body::from_stream(stream)
    };
    builder
        .body(body)
        .unwrap_or_else(|e| AppError::UpstreamError(e.to_string()).into_response())
}

/// 读取请求体(受 `--max-body` 约束)并对缺失的 `stream` 字段做归一化。
async fn read_and_normalize_body(body: Body, max_body: usize) -> Result<Bytes, axum::Error> {
    let bytes = axum::body::to_bytes(body, max_body).await?;
    Ok(normalize_stream_field(bytes))
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

/// `/aiproxy/{provider}` —— 无剩余路径，转发到 base url 本身。
async fn forward_root(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    dispatch_forward(state, provider, "", method, uri, headers, body).await
}

/// `/aiproxy/{provider}/{*rest}` —— 一次切分后整段透传。
async fn forward(
    State(state): State<AppState>,
    Path((provider, rest)): Path<(String, String)>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    dispatch_forward(state, provider, &rest, method, uri, headers, body).await
}

async fn dispatch_forward(
    state: AppState,
    provider: String,
    rest: &str,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let Some(p) = state.config.get(&provider) else {
        tracing::debug!(provider = %provider, path = %uri.path(), "aiproxy provider miss");
        return provider_not_found(&provider, &state.config).into_response();
    };
    let base_url = p.base_url.clone();
    let normalize_sse = p.normalize_sse;
    respond_forwarded(
        &state,
        &method,
        &base_url,
        rest,
        &uri,
        headers,
        body,
        normalize_sse,
    )
    .await
}

// ── 入口桥接（T7，设计文档 §11-R1）────────────────────────────────────

/// 将「已被手工读走请求行」的连接嫁接给进程内 axum/hyper 栈。
///
/// 预读字节先注入内部缓冲管道的一端，随后 socket 与该端双向拷贝；
/// hyper 从另一端看到完整报文（请求行 + 余下头部/body），解析与流式语义全部由其接管。
/// 覆盖「长请求头跨越内部缓冲」的场景：duplex 容量仅决定内核态拷贝节奏，不限制报文长度。
pub async fn serve_conn_from_prelude(
    state: AppState,
    prelude: &[u8],
    client: tokio::net::TcpStream,
) -> anyhow::Result<()> {
    use hyper_util::{rt::TokioIo, service::TowerToHyperService};
    use tokio::io::{AsyncWriteExt as _, copy_bidirectional, duplex};

    const BRIDGE_BUFFER: usize = 64 * 1024;

    let service = TowerToHyperService::new(router(state));
    let (mut client_half, server_half) = duplex(BRIDGE_BUFFER);
    client_half.write_all(prelude).await?;

    let conn = tokio::spawn(async move {
        hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(server_half), service)
            .await
    });

    let mut client_half_ref = client_half;
    let mut client = client;
    let _ = copy_bidirectional(&mut client, &mut client_half_ref).await;
    // 连接任一侧结束即收尾；hyper 连接任务随后自行终止
    let _ = conn.await?;
    Ok(())
}

// ── 独立启动辅助（阶段 B 里程碑用；生产形态经 T6/T7 并入单端口）────────

/// 阶段 B 手工联调用的独立 axum 服务。
///
/// 注意：生产单端口形态下 `/healthz` 由入口分流层直接应答、不进此路由；
/// 此处的 healthz 仅供独立联调进程探活。
pub async fn serve_standalone(addr: &str, state: AppState) -> anyhow::Result<()> {
    use axum::routing::get;

    // router() 已应用 with_state，返回具体 Router；healthz 以独立路由并入
    let app = router(state).route("/healthz", get(|| async { "ok" }));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("aiproxy standalone listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn target_url_joins_rest_and_query() {
        let url = target_url("https://api.openai.com/v1", "chat/completions", Some("k=1&a=b"))
            .unwrap();
        assert_eq!(url.as_str(), "https://api.openai.com/v1/chat/completions?k=1&a=b");
    }

    #[test]
    fn target_url_root_goes_to_base_itself() {
        let url = target_url("https://api.openai.com/v1", "", None).unwrap();
        assert_eq!(url.as_str(), "https://api.openai.com/v1");
    }

    #[test]
    fn request_header_blacklist_strips_hop_by_hop_only() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Bearer sk-test"));
        headers.insert("x-api-key", HeaderValue::from_static("&%20 special"));
        headers.insert("connection", HeaderValue::from_static("close"));
        headers.insert("proxy-authorization", HeaderValue::from_static("x"));
        headers.insert("content-length", HeaderValue::from_static("5"));
        let out = forward_allowed(&headers, REQUEST_HEADER_BLACKLIST);
        assert_eq!(out.get("authorization").unwrap(), "Bearer sk-test");
        assert_eq!(out.get("x-api-key").unwrap(), "&%20 special");
        assert!(out.get("connection").is_none());
        assert!(out.get("proxy-authorization").is_none());
        assert!(out.get("content-length").is_none());
    }

    #[test]
    fn request_has_body_heuristics() {
        let mut headers = HeaderMap::new();
        // 无 CL 时：幂等方法视为无体；POST/PUT/PATCH/DELETE 按方法判携带体
        assert!(!request_has_body(&Method::GET, &headers));
        assert!(request_has_body(&Method::POST, &headers));
        // 有 CL 一律有体（含显式 0）
        headers.insert("content-length", HeaderValue::from_static("0"));
        assert!(request_has_body(&Method::GET, &headers));
        assert!(request_has_body(&Method::POST, &headers));
        assert!(request_has_body(&Method::DELETE, &headers));
    }
}
