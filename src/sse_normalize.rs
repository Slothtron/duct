//! SSE 流兼容归一化(providers.<id>.normalize_sse = true 时启用)。
//!
//! 两类逻辑,均对外部消费者透明、按 provider 显式开启:
//! - 请求侧 [`normalize_stream_field`]:检测请求 JSON 是否携带 `stream`;缺失则显式
//!   注入 `"stream": false`。规避「把缺 stream 当作默认流式」的网关(如 kso),使
//!   非流式请求拿到合规 JSON 而非 `text/event-stream`。
//! - 响应侧 [`SseToolNormalizer`]:把上游 SSE 流中「每个 chunk 重复下发完整
//!   `function.name`」改写为合规流——每个 tool-call index 只在首帧保留 name,
//!   后续帧去掉 name(工具名归一化,即原方案)。

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use bytes::Bytes;
use futures::Stream;
use serde_json::Value;

// ── 请求侧:stream 字段归一化 ────────────────────────────────────────────

/// 若 `body` 是 JSON 对象且缺少顶层 `stream` 字段,则显式注入 `"stream": false`。
/// 非对象 / 解析失败 / 已带 `stream`(true 或 false)的 body 原样返回,保证:
/// - 对携带 `stream: true` 的流式请求不做改动(不破坏流式);
/// - 对已显式 `stream: false` 的请求不做改动;
/// - 仅对「未声明 stream」的非流式请求补齐,让上游明确其为非流式。
pub fn normalize_stream_field(body: Bytes) -> Bytes {
    if body.first() != Some(&b'{') {
        return body;
    }
    let Ok(mut value) = serde_json::from_slice::<Value>(&body) else {
        return body;
    };
    let Some(obj) = value.as_object_mut() else {
        return body;
    };
    if obj.contains_key("stream") {
        return body;
    }
    obj.insert("stream".to_string(), Value::Bool(false));
    match serde_json::to_vec(&value) {
        Ok(out) => Bytes::from(out),
        Err(_) => body,
    }
}

// ── 响应侧:流式工具调用 name 归一化 ─────────────────────────────────────

/// 把上游 SSE 流改写为工具名合规的 SSE 流。
///
/// 归一化语义(等价于「name 取首值,但容忍重复/增长/真片段」):
/// | 输入 name (对某 index 已捕获 seen) | 含义 | 输出 |
/// |---|---|---|
/// | 首次出现 name | 首个完整 name | 保留 name,记 seen=name |
/// | name == seen | 重发完整名(kso) | 删除 function.name |
/// | name 以 seen 开头且更长 | 增长式重发 | 保留,seen=name |
/// | seen 以 name 开头且更短 | 冗余前缀 | 删除 function.name |
/// | 其它(不相等也不互为前后缀) | 真片段续写 | 保留并拼入:seen+=name,输出 name=seen |
///
/// 收到任意 choice 的 `finish_reason` 或 `data: [DONE]` 后清空 `seen_names`,
/// 使下一轮复用同一 index 时重新计数。
pub struct SseToolNormalizer<S> {
    inner: S,
    /// 尚未切出完整行的字节。
    pending: Vec<u8>,
    /// 已切出、待下发的字节(一次 poll 最多出一条)。
    outgoing: VecDeque<Bytes>,
    /// tool-call index -> 已捕获的完整 name。
    seen_names: HashMap<usize, String>,
}

impl<S> SseToolNormalizer<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            pending: Vec::new(),
            outgoing: VecDeque::new(),
            seen_names: HashMap::new(),
        }
    }

    /// 追加 chunk 字节,切出完整行并处理入队。
    fn ingest(&mut self, chunk: Bytes) {
        self.pending.extend_from_slice(&chunk);
        loop {
            let Some(nl) = self.pending.iter().position(|&b| b == b'\n') else {
                break;
            };
            // 行 = pending[..=nl] 含换行符;换行符前可能带 `\r`。
            let line: Vec<u8> = self.pending.drain(..=nl).collect();
            let out = self.process_line(&line);
            self.outgoing.push_back(out);
        }
    }

    /// 处理一行(含末尾 `\n`,可能末尾为 `\r\n`),返回要下发的字节。
    fn process_line(&mut self, line: &[u8]) -> Bytes {
        // 分离内容与行终止符。
        let (content, terminator) = split_term(line);
        let content_str = std::str::from_utf8(content).unwrap_or("");
        let content_trimmed = content_str.trim_start_matches('\r');

        if let Some(data) = content_trimmed.strip_prefix("data:") {
            let data = data.trim();
            if data.is_empty() || data == "[DONE]" {
                self.seen_names.clear();
                return Bytes::copy_from_slice(line);
            }
            let Ok(mut value) = serde_json::from_str::<Value>(data) else {
                tracing::debug!(line = %content_trimmed, "sse-normalize: non-JSON data line, pass through");
                return Bytes::copy_from_slice(line);
            };
            let Some(changed) = self.normalize_tool_calls(&mut value) else {
                return Bytes::copy_from_slice(line);
            };
            if changed {
                match serde_json::to_vec(&value) {
                    Ok(json) => {
                        let mut out = Vec::with_capacity(json.len() + terminator.len() + 6);
                        out.extend_from_slice(b"data: ");
                        out.extend_from_slice(&json);
                        out.extend_from_slice(terminator);
                        Bytes::from(out)
                    }
                    Err(_) => Bytes::copy_from_slice(line),
                }
            } else {
                Bytes::copy_from_slice(line)
            }
        } else {
            // : keepalive / 空行 / 纯文本行 —— 原样透传。
            Bytes::copy_from_slice(line)
        }
    }

    /// 对 `choices[].delta.tool_calls[].function.name` 做归一化。
    /// 返回 `Some(changed)`;若该行不含 tool_calls(无需改动)返回 `None`。
    fn normalize_tool_calls(&mut self, value: &mut Value) -> Option<bool> {
        let choices = value.get_mut("choices")?.as_array_mut()?;

        let saw_finish = choices.iter().any(|c| {
            c.get("finish_reason")
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.is_empty())
        });

        let mut changed = false;
        for choice in choices.iter_mut() {
            let delta = choice.get_mut("delta")?;
            let tool_calls = delta.get_mut("tool_calls")?.as_array_mut()?;
            for tc in tool_calls.iter_mut() {
                let index = tc
                    .get("index")
                    .and_then(|v| v.as_u64())
                    .map(|i| i as usize)
                    .unwrap_or(0);
                let Some(name) = tc
                    .get("function")
                    .and_then(|f| f.get("name"))
                    .and_then(|n| n.as_str())
                else {
                    continue;
                };
                if name.is_empty() {
                    continue;
                }

                let seen = self.seen_names.get(&index).cloned();
                let action = match seen {
                    None => {
                        // 该 index 首次出现：保留完整 name，并记为 seen。
                        self.seen_names.insert(index, name.to_string());
                        ToolNameAction::Keep
                    }
                    Some(seen) if name == seen => ToolNameAction::Delete,
                    Some(seen) if name.len() > seen.len() && name.starts_with(&seen) => {
                        self.seen_names.insert(index, name.to_string());
                        ToolNameAction::Keep
                    }
                    Some(seen) if seen.len() > name.len() && seen.starts_with(name) => {
                        ToolNameAction::Delete
                    }
                    Some(seen) => {
                        let merged = seen + name;
                        self.seen_names.insert(index, merged.clone());
                        ToolNameAction::Set(merged)
                    }
                };

                match action {
                    ToolNameAction::Keep => {}
                    ToolNameAction::Delete => {
                        if let Some(fobj) = tc.get_mut("function").and_then(|f| f.as_object_mut()) {
                            fobj.remove("name");
                            changed = true;
                        }
                    }
                    ToolNameAction::Set(name) => {
                        if let Some(fobj) = tc.get_mut("function").and_then(|f| f.as_object_mut()) {
                            fobj.insert("name".to_string(), Value::String(name));
                            changed = true;
                        }
                    }
                }
            }
        }

        // 删除 name 后若 function 既无 name 也无 arguments,则删除整个 function。
        for choice in choices.iter_mut() {
            let Some(delta) = choice.get_mut("delta") else {
                continue;
            };
            let Some(tool_calls) = delta.get_mut("tool_calls").and_then(|v| v.as_array_mut())
            else {
                continue;
            };
            for tc in tool_calls.iter_mut() {
                let Some(fobj) = tc.get("function").and_then(|f| f.as_object()) else {
                    continue;
                };
                if !fobj.contains_key("name") && !fobj.contains_key("arguments") {
                    if let Some(tcobj) = tc.as_object_mut() {
                        tcobj.remove("function");
                        changed = true;
                    }
                }
            }
        }

        if saw_finish {
            self.seen_names.clear();
        }

        Some(changed)
    }
}

#[derive(Debug)]
enum ToolNameAction {
    /// 保留原有 name(首帧 / 增长式)。
    Keep,
    /// 删除 function.name。
    Delete,
    /// 用合并后的完整 name 覆盖(真片段续写)。
    Set(String),
}

impl<S, E> Stream for SseToolNormalizer<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(out) = self.outgoing.pop_front() {
                return Poll::Ready(Some(Ok(out)));
            }
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => self.ingest(chunk),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    // 流结束:冲刷残余 pending(上游可能没发尾随换行)。
                    if !self.pending.is_empty() {
                        let tail: Vec<u8> = std::mem::take(&mut self.pending);
                        let out = self.process_line(&tail);
                        self.outgoing.push_back(out);
                        continue;
                    }
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// 把一行拆成(内容, 行终止符)。行以 `\n` 结尾,终止符可能是 `\n` 或 `\r\n`。
fn split_term(line: &[u8]) -> (&[u8], &[u8]) {
    if line.len() >= 2 && line[line.len() - 2] == b'\r' && line[line.len() - 1] == b'\n' {
        (&line[..line.len() - 2], b"\r\n")
    } else {
        (&line[..line.len().saturating_sub(1)], b"\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    /// 用同步字节切片列表喂给归一化流并收集输出。
    async fn run_normalizer(chunks: Vec<&[u8]>) -> Vec<u8> {
        let src = futures::stream::iter(
            chunks
                .into_iter()
                .map(|c| Ok::<Bytes, std::convert::Infallible>(Bytes::copy_from_slice(c))),
        );
        let out: Vec<Result<Bytes, _>> = SseToolNormalizer::new(src).collect().await;
        out.into_iter()
            .map(|r| r.unwrap().to_vec())
            .flatten()
            .collect()
    }

    /// 构造带 tool_calls 的 SSE data 行。name 为 Some 时携带 function.name。
    fn tool_chunk(name: Option<&str>, args: &str, index: usize) -> String {
        let mut function = serde_json::Map::new();
        if let Some(n) = name {
            function.insert("name".into(), Value::String(n.into()));
        }
        function.insert("arguments".into(), Value::String(args.into()));
        let mut tc = serde_json::Map::new();
        tc.insert("index".into(), Value::Number(index.into()));
        tc.insert("function".into(), Value::Object(function));
        let event = serde_json::json!({
            "choices": [{
                "index": 0,
                "delta": { "tool_calls": [Value::Object(tc)] }
            }]
        });
        format!("data: {}\n", serde_json::to_string(&event).unwrap())
    }

    /// 只带 reasoning_content 的 chunk(验证纯文本行透传)。
    fn reasoning_chunk() -> String {
        format!(
            "data: {}\n",
            serde_json::to_string(&serde_json::json!({
                "choices": [{"index": 0, "delta": {"reasoning_content": "think"}}]
            }))
            .unwrap()
        )
    }

    // ── 请求侧 ───────────────────────────────────────────────────────────

    #[test]
    fn inject_stream_false_when_missing() {
        let body = Bytes::from_static(
            b"{\"model\":\"m\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}",
        );
        let out = normalize_stream_field(body);
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["stream"], false);
        assert_eq!(v["model"], "m");
    }

    #[test]
    fn leave_stream_field_untouched() {
        for body in [
            br#"{"model":"m","stream":true}"#.as_slice(),
            br#"{"model":"m","stream":false}"#.as_slice(),
        ] {
            let out = normalize_stream_field(Bytes::from(body));
            assert_eq!(out.as_ref(), body, "stream 已显式设置时不得改动");
        }
    }

    #[test]
    fn non_json_object_passthrough() {
        let body = Bytes::from_static(b"not json");
        assert_eq!(normalize_stream_field(body).as_ref(), b"not json");
    }

    // ── 响应侧(工具名归一化,原方案)────────────────────────────────────

    #[tokio::test]
    async fn repeated_full_name_is_collapsed() {
        let frames = vec![
            tool_chunk(Some("list_dir"), "", 0),
            tool_chunk(Some("list_dir"), r#"{"path": "#, 0),
            tool_chunk(Some("list_dir"), r#""/""#, 0),
            "data: [DONE]\n".to_string(),
        ];
        let out =
            String::from_utf8(run_normalizer(frames.iter().map(|s| s.as_bytes()).collect()).await)
                .unwrap();
        // 每个 index 只保留首帧 name:"list_dir"
        assert_eq!(out.matches(r#""name":"list_dir""#).count(), 1, "{out}");
        // arguments 片段完整拼接
        assert!(out.contains(r#""arguments":"{\"path\": ""#), "{out}");
        assert!(out.contains(r#""arguments":"\"/\""#), "{out}");
    }

    #[tokio::test]
    async fn compliant_stream_passes_through() {
        let frames = vec![
            tool_chunk(Some("list_dir"), "", 0),
            tool_chunk(None, r#"{"p":1}"#, 0),
            "data: [DONE]\n".to_string(),
        ];
        let input: Vec<u8> = frames.concat().into_bytes();
        let out = run_normalizer(frames.iter().map(|s| s.as_bytes()).collect()).await;
        assert_eq!(out, input, "合规流开启归一化后输出应字节不变");
    }

    #[tokio::test]
    async fn fragment_continuation_merges_name() {
        let frames = vec![
            tool_chunk(Some("fu"), "", 0),
            tool_chunk(Some("nc"), "", 0),
            "data: [DONE]\n".to_string(),
        ];
        let out =
            String::from_utf8(run_normalizer(frames.iter().map(|s| s.as_bytes()).collect()).await)
                .unwrap();
        assert!(out.contains(r#""name":"func""#), "{out}");
    }

    #[tokio::test]
    async fn done_resets_seen_names() {
        let frames = vec![
            tool_chunk(Some("a"), "", 0),
            "data: [DONE]\n".to_string(),
            tool_chunk(Some("b"), "", 0),
            "data: [DONE]\n".to_string(),
        ];
        let out =
            String::from_utf8(run_normalizer(frames.iter().map(|s| s.as_bytes()).collect()).await)
                .unwrap();
        assert!(out.contains(r#""name":"a""#), "{out}");
        assert!(out.contains(r#""name":"b""#), "{out}");
    }

    #[tokio::test]
    async fn line_split_across_chunks() {
        let line = tool_chunk(Some("list_dir"), "", 0);
        let (a, b) = line.split_at(10);
        let out =
            String::from_utf8(run_normalizer(vec![a.as_bytes(), b.as_bytes()]).await).unwrap();
        assert!(out.contains(r#""name":"list_dir""#), "{out}");
    }

    #[tokio::test]
    async fn non_data_lines_pass_through() {
        let frames = vec![
            ": keepalive\n".to_string(),
            "\n".to_string(),
            "data: [DONE]\n".to_string(),
        ];
        let input: Vec<u8> = frames.concat().into_bytes();
        let out = run_normalizer(frames.iter().map(|s| s.as_bytes()).collect()).await;
        assert_eq!(out, input);
    }

    #[tokio::test]
    async fn reasoning_line_passes_through() {
        let frames = vec![
            reasoning_chunk(),
            tool_chunk(Some("f"), "", 0),
            "data: [DONE]\n".to_string(),
        ];
        let input: Vec<u8> = frames.concat().into_bytes();
        let out = run_normalizer(frames.iter().map(|s| s.as_bytes()).collect()).await;
        assert_eq!(out, input, "无 tool_calls 的行应原样透传");
    }
}
