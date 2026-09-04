//! OpenAI 兼容错误响应（自 vent 平移，按设计文档 v3.2 §6.5 调整）。
//!
//! 所有 aiproxy 网关自身产生的错误统一输出：
//! `{"error":{"message":"...","type":"..."}}`
//! 上游返回的错误状态**原样透传**，不在网关伪造 OpenAI 语义。

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug, Clone, thiserror::Error)]
pub enum AppError {
    /// provider id 未注册或不合法。
    #[error("Provider '{requested}' not found. Available providers: {available}")]
    ProviderNotFound {
        requested: String,
        available: String,
    },

    /// mcp server id 未注册或不合法。
    #[error("MCP server '{requested}' not found. Available servers: {available}")]
    ServerNotFound {
        requested: String,
        available: String,
    },

    /// 配置未装载（aiproxy 功能未启用），对外表现与无可用 provider 一致。
    #[error("aiproxy is not configured on this instance. Available providers: none")]
    AiproxyDisabled,

    /// 请求体超过 --max-body。
    #[error("Request body exceeds the configured size limit")]
    BodyTooLarge,

    /// 上游连接失败。
    #[error("Upstream connection failed: {0}")]
    UpstreamError(String),

    /// 上游连接超时。
    #[error("Upstream connection timed out")]
    UpstreamTimeout,
}

#[derive(Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    message: String,
    #[serde(rename = "type")]
    error_type: &'static str,
}

impl AppError {
    fn status_and_type(&self) -> (StatusCode, &'static str) {
        match self {
            AppError::ProviderNotFound { .. } | AppError::AiproxyDisabled => {
                (StatusCode::NOT_FOUND, "invalid_request_error")
            }
            AppError::ServerNotFound { .. } => (StatusCode::NOT_FOUND, "invalid_request_error"),
            AppError::BodyTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "invalid_request_error"),
            AppError::UpstreamError(_) => (StatusCode::BAD_GATEWAY, "upstream_error"),
            AppError::UpstreamTimeout => (StatusCode::GATEWAY_TIMEOUT, "upstream_error"),
        }
    }

    /// 供集成测试断言状态码。
    pub fn status(&self) -> StatusCode {
        self.status_and_type().0
    }

    /// 供请求轨迹记录：(HTTP 状态码, OpenAI 兼容错误类型)。
    pub fn trace_identity(&self) -> (u16, &'static str) {
        let (status, error_type) = self.status_and_type();
        (status.as_u16(), error_type)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_type) = self.status_and_type();
        let body = ErrorBody {
            error: ErrorDetail {
                message: self.to_string(),
                error_type,
            },
        };
        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use tower::util::ServiceExt;

    async fn render(err: AppError) -> (StatusCode, serde_json::Value) {
        // 经完整 axum 栈渲染，验证 IntoResponse 行为而非手拼 JSON
        let app = axum::Router::new().fallback(move || async move { err });
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[tokio::test]
    async fn provider_not_found_lists_available() {
        let (status, json) = render(AppError::ProviderNotFound {
            requested: "nope".into(),
            available: "openai, ollama".into(),
        })
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(json["error"]["type"], "invalid_request_error");
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap()
                .contains("openai, ollama")
        );
    }

    #[test]
    fn disabled_maps_to_404() {
        assert_eq!(AppError::AiproxyDisabled.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn body_too_large_maps_to_413() {
        assert_eq!(
            AppError::BodyTooLarge.status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[tokio::test]
    async fn upstream_timeout_maps_to_504() {
        let (status, json) = render(AppError::UpstreamTimeout).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(json["error"]["type"], "upstream_error");
    }

    #[tokio::test]
    async fn upstream_error_maps_to_502() {
        let (status, _) = render(AppError::UpstreamError("refused".into())).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
    }
}
