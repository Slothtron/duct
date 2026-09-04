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
use std::io;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use bytes::Bytes;
use flate2::{Decompress, FlushDecompress, Status};
use futures::Stream;
use serde_json::Value;

use crate::trace::EncKind;

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
/// 工具名状态机改写器：行进字节出，与数据来源（明文流 / 解码缓冲）无关。
/// 从 `SseToolNormalizer` 抽出，使压缩流可在「解码后」复用同一套改写逻辑。
///
/// 归一化语义（等价于「name 取首值，但容忍重复/增长/真片段」）：
/// | 输入 name (对某 index 已捕获 seen) | 含义 | 输出 |
/// |---|---|---|
/// | 首次出现 name | 首个完整 name | 保留 name，记 seen=name |
/// | name == seen | 重发完整名（kso） | 删除 function.name |
/// | name 以 seen 开头且更长 | 增长式重发 | 保留，seen=name |
/// | seen 以 name 开头且更短 | 冗余前缀 | 删除 function.name |
/// | 其它（不相等也不互为前后缀） | 真片段续写 | 保留并拼入：seen+=name，输出 name=seen |
///
/// 收到任意 choice 的 `finish_reason` 或 `data: [DONE]` 后清空 `seen_names`，
/// 使下一轮复用同一 index 时重新计数。
pub struct ToolNameRewriter {
    /// tool-call index -> 已捕获的完整 name。
    seen_names: HashMap<usize, String>,
    /// 尚未切出完整行的字节。
    pending: Vec<u8>,
}

impl Default for ToolNameRewriter {
    fn default() -> Self {
        Self::new()
    }
}

impl ToolNameRewriter {
    pub fn new() -> Self {
        Self {
            seen_names: HashMap::new(),
            pending: Vec::new(),
        }
    }

    /// 喂入字节，切完整行逐条改写；返回可直接下发的行（含终止符）。
    pub fn ingest(&mut self, chunk: &[u8]) -> Vec<Bytes> {
        let mut out = Vec::new();
        self.pending.extend_from_slice(chunk);
        while let Some(nl) = self.pending.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.pending.drain(..=nl).collect();
            out.push(self.process_line(&line));
        }
        out
    }

    /// 流结束：冲刷无尾随换行的残行。
    pub fn finish(&mut self) -> Vec<Bytes> {
        if self.pending.is_empty() {
            return Vec::new();
        }
        let tail = std::mem::take(&mut self.pending);
        vec![self.process_line(&tail)]
    }

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
                if !fobj.contains_key("name")
                    && !fobj.contains_key("arguments")
                    && let Some(tcobj) = tc.as_object_mut()
                {
                    tcobj.remove("function");
                    changed = true;
                }
            }
        }

        if saw_finish {
            self.seen_names.clear();
        }

        Some(changed)
    }
}

/// 把上游明文 SSE 流改写为工具名合规的 SSE 流（行切分与改写状态机在
/// `ToolNameRewriter`；本类型只做 Stream 适配）。
pub struct SseToolNormalizer<S> {
    inner: S,
    /// 已切出、待下发的字节（一次 poll 最多出一条）。
    outgoing: VecDeque<Bytes>,
    rw: ToolNameRewriter,
}

impl<S> SseToolNormalizer<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            outgoing: VecDeque::new(),
            rw: ToolNameRewriter::new(),
        }
    }

    /// 追加 chunk 字节，切出完整行并入队。
    fn ingest(&mut self, chunk: &[u8]) {
        self.outgoing.extend(self.rw.ingest(chunk));
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
                Poll::Ready(Some(Ok(chunk))) => self.ingest(&chunk),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    // 流结束:冲刷残余 pending(上游可能没发尾随换行)。
                    if !self.rw.pending.is_empty() {
                        let tail_lines = self.rw.finish();
                        self.outgoing.extend(tail_lines);
                        continue;
                    }
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ── 压缩流 + normalize：解码 → 行改写 → 明文重发 ─────────────────────────

/// 流式 inflate（gzip 或 zlib/raw deflate）。rust_backend 的 `flate2::Decompress`
/// 无 gzip 构造器，故 gzip 头由本结构手工剥离后按 raw deflate 解；member 尾
/// （CRC32+ISIZE 8 字节）在 `StreamEnd` 后自然忽略，多 member 流只解首个
/// （LLM 响应网关均为单 member）。`EncKind::Br` 无 poll 内增量 API，不走本路径
/// （由调用方回退为压缩透传 + WARN）。
struct Inflate {
    kind: EncKind,
    dz: Option<Decompress>,
    /// 头解析前的字节缓冲（gzip 头可变长；上限见 feed 中的护栏）。
    pending: Vec<u8>,
    ended: bool,
}

/// 解析 gzip 定长+可选字段头。返回已用字节数；输入不足时 None（待续）。
fn parse_gzip_head(buf: &[u8]) -> Option<usize> {
    if buf.len() < 10 {
        return None;
    }
    if buf[0] != 0x1f || buf[1] != 0x8b || buf[2] != 8 {
        return Some(usize::MAX); // 哨兵：非法头
    }
    let flg = buf[3];
    let mut i = 10;
    if flg & 0x04 != 0 {
        // FEXTRA: XLEN 两字节 + XLEN 任意字节
        if buf.len() < i + 2 {
            return None;
        }
        let xlen = u16::from_le_bytes([buf[i], buf[i + 1]]) as usize;
        i += 2 + xlen;
    }
    for mask in [0x08u8, 0x10] {
        // FNAME / FCOMMENT: NUL 终止
        if flg & mask != 0 {
            while i < buf.len() && buf[i] != 0 {
                i += 1;
            }
            if i >= buf.len() {
                return None;
            }
            i += 1;
        }
    }
    if flg & 0x02 != 0 {
        // FHCRC
        if buf.len() < i + 2 {
            return None;
        }
        i += 2;
    }
    Some(i)
}

/// zlib 头合法性（CM/窗口位/FCHECK），合法则 `Decompress::new(true)` 可自吞头。
fn looks_zlib(buf: &[u8]) -> bool {
    buf.len() >= 2
        && (buf[0] & 0x0f) == 8
        && ((buf[0] as u16) << 8 | buf[1] as u16).is_multiple_of(31)
}

impl Inflate {
    fn new(kind: EncKind) -> Self {
        Self {
            kind,
            dz: None,
            pending: Vec::new(),
            ended: false,
        }
    }

    /// 头解析完成后返回 raw inflate 引擎；失败/待续返回 None。
    fn engine(&mut self) -> Option<Decompress> {
        let buf = &self.pending;
        match self.kind {
            EncKind::Gzip => match parse_gzip_head(buf) {
                None => None,
                Some(usize::MAX) => Some(Decompress::new(false)), // 非法头：交由 inflate 报错
                Some(used) => {
                    self.pending.drain(..used);
                    Some(Decompress::new(false))
                }
            },
            // deflate 二义性：zlib 头合法走 zlib（引擎自吞头），否则直接 raw。
            EncKind::Deflate => {
                if buf.len() < 2 {
                    return None;
                }
                Some(Decompress::new(looks_zlib(buf)))
            }
            EncKind::Br => Some(Decompress::new(true)),
        }
    }

    /// 喂压缩字节，解码输出追加到 `out`。解码错误向上抛（触发流终止）。
    fn feed(&mut self, input: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
        if self.ended {
            return Ok(());
        }
        if self.dz.is_none() {
            self.pending.extend_from_slice(input);
            if self.pending.len() > HEAD_GUARD {
                return Err(io::Error::other("compression header too large"));
            }
            match self.engine() {
                None => return Ok(()),
                Some(dz) => self.dz = Some(dz),
            }
            let buffered = std::mem::take(&mut self.pending);
            return self.pump(&buffered, out);
        }
        self.pump(input, out)
    }

    /// 头就绪后的解压主循环。
    fn pump(&mut self, input: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
        let dz = self.dz.as_mut().expect("engine established");
        let mut pos = 0usize;
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            let tin0 = dz.total_in();
            let tout0 = dz.total_out();
            match dz.decompress(&input[pos..], &mut buf, FlushDecompress::None) {
                Ok(status) => {
                    let consumed = (dz.total_in() - tin0) as usize;
                    let produced = (dz.total_out() - tout0) as usize;
                    out.extend_from_slice(&buf[..produced]);
                    pos += consumed;
                    if matches!(status, Status::StreamEnd) {
                        self.ended = true;
                        return Ok(());
                    }
                    if consumed == 0 && produced == 0 {
                        return Ok(()); // 等待更多输入
                    }
                    if pos >= input.len() {
                        return Ok(());
                    }
                }
                Err(e) => return Err(io::Error::other(format!("inflate error: {e}"))),
            }
        }
    }
}

const HEAD_GUARD: usize = 64 * 1024;

/// 压缩 SSE 的「解码 → 工具名改写 → 明文重发」流适配器。
///
/// 存在理由（kso 定性结论）：`normalize_sse` 的行改写要求明文，而 kso 类网关
/// 无视 `Accept-Encoding: identity` 协商、恒发 gzip —— 明文改写器在压缩字节上
/// 空转，重复的 function.name 原样漏给客户端。本适配器在 duct 内完成
/// 解码→改写，客户端收到的是合规明文 SSE；**响应头须由调用方去掉
/// `content-encoding`**。透传语义的偏离由 provider 显式声明的 `normalize_sse`
/// 选项授权。
///
/// 解码失败（坏帧/字典/多 member 尾）→ 以 `Err` 终止流；已改写的行照常先出。
pub struct SseRewindStream<S> {
    inner: S,
    inflate: Inflate,
    rw: ToolNameRewriter,
    out: VecDeque<Bytes>,
    done: bool,
}

impl<S> SseRewindStream<S> {
    pub fn new(inner: S, kind: EncKind) -> Self {
        Self {
            inner,
            inflate: Inflate::new(kind),
            rw: ToolNameRewriter::new(),
            out: VecDeque::new(),
            done: false,
        }
    }
}

impl<S, E> Stream for SseRewindStream<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    type Item = Result<Bytes, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(b) = self.out.pop_front() {
                return Poll::Ready(Some(Ok(b)));
            }
            if self.done {
                return Poll::Ready(None);
            }
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    let mut decoded = Vec::new();
                    if let Err(e) = self.inflate.feed(&chunk, &mut decoded) {
                        self.done = true;
                        // 已产出的行先排空，再报解码错——错误在数据之后到达。
                        let mut tail = self.rw.finish();
                        tail.reverse();
                        for b in tail {
                            self.out.push_back(b);
                        }
                        return Poll::Ready(Some(Err(e)));
                    }
                    let lines = self.rw.ingest(&decoded);
                    self.out.extend(lines);
                }
                Poll::Ready(Some(Err(e))) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(io::Error::other(e.to_string()))));
                }
                Poll::Ready(None) => {
                    // 上游收尾：inflate 内部缓冲若有残余，用空输入再挤一次。
                    let mut residual = Vec::new();
                    let _ = self.inflate.feed(&[], &mut residual);
                    let mut lines = self.rw.ingest(&residual);
                    lines.extend(self.rw.finish());
                    self.out.extend(lines);
                    self.done = true;
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
    use crate::trace::EncKind;
    use futures::StreamExt;

    /// 用同步字节切片列表喂给归一化流并收集输出。
    async fn run_normalizer(chunks: Vec<&[u8]>) -> Vec<u8> {
        let src = futures::stream::iter(
            chunks
                .into_iter()
                .map(|c| Ok::<Bytes, std::convert::Infallible>(Bytes::copy_from_slice(c))),
        );
        let out: Vec<Result<Bytes, _>> = SseToolNormalizer::new(src).collect().await;
        out.into_iter().flat_map(|r| r.unwrap().to_vec()).collect()
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

    // ── SseRewindStream：gzip/deflate 解码 + 改写 ────────────────────────

    fn gz(bytes: &[u8]) -> Vec<u8> {
        let mut enc = Vec::new();
        {
            let mut g = flate2::write::GzEncoder::new(&mut enc, flate2::Compression::default());
            std::io::Write::write_all(&mut g, bytes).unwrap();
            g.finish().unwrap();
        }
        enc
    }

    fn zlib(bytes: &[u8]) -> Vec<u8> {
        let mut enc = Vec::new();
        {
            let mut z = flate2::write::ZlibEncoder::new(&mut enc, flate2::Compression::default());
            std::io::Write::write_all(&mut z, bytes).unwrap();
            z.finish().unwrap();
        }
        enc
    }

    /// 三帧重发 name 的明文流，喂法按 chunk 边界切碎。
    async fn run_rewind(encoded: Vec<u8>, split: usize, kind: EncKind) -> String {
        let mut parts: Vec<Bytes> = Vec::new();
        for i in (0..encoded.len()).step_by(split.max(1)) {
            let end = (i + split).min(encoded.len());
            parts.push(Bytes::from(encoded[i..end].to_vec()));
        }
        let src =
            futures::stream::iter(parts.into_iter().map(Ok::<Bytes, std::convert::Infallible>));
        let out: Vec<Result<Bytes, _>> = SseRewindStream::new(src, kind).collect().await;
        out.into_iter()
            .flat_map(|r| r.unwrap().to_vec())
            .collect::<Vec<u8>>()
            .pipe_utf8()
    }

    trait PipeUtf8 {
        fn pipe_utf8(self) -> String;
    }
    impl PipeUtf8 for Vec<u8> {
        fn pipe_utf8(self) -> String {
            String::from_utf8(self).expect("rewind 输出必须是合法 UTF-8 明文 SSE")
        }
    }

    fn kso_frames() -> Vec<u8> {
        [
            r#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"read_file","arguments":""}}]}}]}"#,
            r#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"read_file","arguments":"{\"p\":1}"}}]}}]}"#,
            r#"data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#,
            "data: [DONE]",
        ]
        .join("\n\n")
        .into_bytes()
        .into_iter()
        .chain("\n\n".bytes())
        .collect()
    }

    #[tokio::test]
    async fn rewind_gzip_normalizes_repeated_names() {
        let out = run_rewind(gz(&kso_frames()), 7, EncKind::Gzip).await;
        assert_eq!(out.matches(r#""name":"read_file""#).count(), 1, "{out}");
        assert!(out.contains("[DONE]"));
        assert!(out.contains(r#""arguments":"{\"p\":1}""#), "参数增量不丢");
    }

    #[tokio::test]
    async fn rewind_zlib_deflate_and_raw_both_work() {
        let out = run_rewind(zlib(&kso_frames()), 13, EncKind::Deflate).await;
        assert_eq!(out.matches(r#""name":"read_file""#).count(), 1);
        // raw deflate（无 zlib 头）
        let mut raw = Vec::new();
        {
            let mut e =
                flate2::write::DeflateEncoder::new(&mut raw, flate2::Compression::default());
            std::io::Write::write_all(&mut e, &kso_frames()).unwrap();
            e.finish().unwrap();
        }
        let out = run_rewind(raw, 5, EncKind::Deflate).await;
        assert_eq!(out.matches(r#""name":"read_file""#).count(), 1);
    }

    #[tokio::test]
    async fn rewind_corrupt_input_errors_stream() {
        let junk = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let src = futures::stream::iter(vec![Ok::<Bytes, std::convert::Infallible>(Bytes::from(
            junk,
        ))]);
        let items: Vec<_> = SseRewindStream::new(src, EncKind::Gzip).collect().await;
        assert!(
            items.iter().any(|r| r.is_err()),
            "坏 gzip 流必须以 Err 终止而非静默吞掉"
        );
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
        let frames = [
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
        let frames = [
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
        let frames = [
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
        let frames = [
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
        let frames = [
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
        let frames = [
            reasoning_chunk(),
            tool_chunk(Some("f"), "", 0),
            "data: [DONE]\n".to_string(),
        ];
        let input: Vec<u8> = frames.concat().into_bytes();
        let out = run_normalizer(frames.iter().map(|s| s.as_bytes()).collect()).await;
        assert_eq!(out, input, "无 tool_calls 的行应原样透传");
    }
}
