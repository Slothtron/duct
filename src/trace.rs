//! aiproxy 请求轨迹（参考 deepseek-harness 会话轨迹的事件溯源设计）。
//!
//! 每一次 AI 转发都是一条 append-only 的 JSONL 轨迹：一行一个事件，
//! 信封为 `{v, time, trace, seq, type, severity, data}`——对应 DSH
//! `SessionEvent` 的 `{type, seq, time, data}` 与 `SessionTelemetryRecord`
//! 的 `{time, severity, attributes, body}`。`seq` 在单条轨迹内从 0 连续递增，
//! 读者据此判序、并可用「末行缺 `request/end`」识别被崩溃切断的轨迹
//! （运行期则由 [`TracedBody`] 的 Drop 兜底写出 `interrupted` 收尾，
//! 对应 DSH 崩溃恢复中合成 `turn/end{interrupted}` 的语义）。
//!
//! 凭证零接触在本模块的强制落点：任何进入轨迹的头一律先过
//! [`header_summary`] 脱敏（`authorization` / `x-api-key` 等只留名字与 `***`），
//! 上游 URL 中的 userinfo 与 query 参数值不进轨迹（只留参数名）。
//! 正文只记录排查所需的派生事实（model / stream / usage / finish_reason /
//! 错误体摘要），不记录 prompt 内容。

use std::io::{Read as _, Write as _};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex, Once};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::http::HeaderMap;
use bytes::Bytes;
use futures::Stream;
use serde_json::{Map, Value, json};

/// 轨迹文件格式版本（信封 `v` 字段）。读侧遇到更高版本应拒读而非猜读。
pub const TRACE_FORMAT_VERSION: u64 = 1;

// ── 信封与 sink ────────────────────────────────────────────────────────

/// 轨迹行接收端。三种形态：
/// - [`TraceSink::none`]：无文件，事件以 JSONL 行回落 tracing（`duct::trace` target）；
/// - [`TraceSink::to_file`]：canonical 行经有界通道交给专用写线程 append 落盘，
///   warn/error 级同时回落 tracing，方便 journald 侧告警；
/// - [`TraceSink::capture`]：测试用，行收进内存向量。
pub struct TraceSink {
    inner: SinkInner,
    dropped: AtomicU64,
    warned_full: Once,
}

enum SinkInner {
    None,
    // SyncSender 非 Sync，包一层 Mutex 使 Arc<TraceSink> 可跨任务共享。
    File(Mutex<SyncSender<String>>),
    Capture(Arc<Mutex<Vec<String>>>),
}

impl TraceSink {
    pub fn none() -> Self {
        Self {
            inner: SinkInner::None,
            dropped: AtomicU64::new(0),
            warned_full: Once::new(),
        }
    }

    /// 打开（或创建）JSONL 轨迹文件并启动 writer 线程；进程退出即随线程终止收尾。
    ///
    /// 缺失自愈：文件连同父目录链在启动时自动创建；运行期文件被外部 `rm`、
    /// 或被 logrotate `create`/`move` 模式换走 inode 时，writer 在下一条记录前
    /// 比对 dev/inode 并重建重开（`copytruncate` 同 inode 不误触发）。
    pub fn to_file(path: &Path) -> anyhow::Result<Self> {
        let file = open_trace_file(path)?;
        tracing::info!(file = %path.display(), "aiproxy trace file enabled");
        let (tx, rx) = sync_channel::<String>(TRACE_CHANNEL_CAPACITY);
        let path = path.to_path_buf();
        std::thread::spawn(move || {
            let mut writer = std::io::BufWriter::new(file);
            let mut recreate_warned = false;
            for line in rx {
                if file_replaced(&path, writer.get_ref()) {
                    match open_trace_file(&path) {
                        Ok(f) => {
                            writer = std::io::BufWriter::new(f);
                            recreate_warned = false;
                            tracing::info!(
                                file = %path.display(),
                                "aiproxy trace file recreated (deleted or rotated away)"
                            );
                        }
                        Err(e) => {
                            if !recreate_warned {
                                tracing::warn!(
                                    error = %e,
                                    "aiproxy trace file recreate failed; continuing on current handle"
                                );
                                recreate_warned = true;
                            }
                            // 保留旧句柄继续写：写失败仅丢该行，不中断轨迹通道。
                        }
                    }
                }
                let _ = writer
                    .write_all(line.as_bytes())
                    .and_then(|_| writer.write_all(b"\n"));
                // 逐条 flush：轨迹单请求仅数行，换取 tail -f 实时可观察。
                let _ = writer.flush();
            }
        });
        Ok(Self {
            inner: SinkInner::File(Mutex::new(tx)),
            dropped: AtomicU64::new(0),
            warned_full: Once::new(),
        })
    }

    /// 测试用：把行收集到共享内存向量。
    pub fn capture() -> (Self, Arc<Mutex<Vec<String>>>) {
        let cap = Arc::new(Mutex::new(Vec::new()));
        let sink = Self {
            inner: SinkInner::Capture(cap.clone()),
            dropped: AtomicU64::new(0),
            warned_full: Once::new(),
        };
        (sink, cap)
    }

    /// 已因通道满而丢弃的轨迹行数。
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// 递交一行。永不阻塞：通道满则丢行并计数（轨迹是旁路，不能反噬转发热路径）。
    fn emit_line(&self, line: String, severity: &str) {
        match severity {
            "error" => tracing::error!(target: "duct::trace", "{line}"),
            "warn" => tracing::warn!(target: "duct::trace", "{line}"),
            // 有文件 sink 时 info 级只进文件，避免双写刷屏 journald。
            _ => {
                if !matches!(self.inner, SinkInner::File(_)) {
                    tracing::info!(target: "duct::trace", "{line}");
                }
            }
        }
        match &self.inner {
            SinkInner::None => {}
            SinkInner::File(tx) => {
                let tx = tx.lock().unwrap_or_else(|e| e.into_inner());
                match tx.try_send(line) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                        self.warned_full.call_once(|| {
                            tracing::warn!(
                                "aiproxy trace buffer full; trace lines are being dropped"
                            )
                        });
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                        self.warned_full.call_once(|| {
                            tracing::warn!("aiproxy trace file writer stopped; trace disabled")
                        });
                    }
                }
            }
            SinkInner::Capture(v) => v.lock().unwrap_or_else(|e| e.into_inner()).push(line),
        }
    }
}

const TRACE_CHANNEL_CAPACITY: usize = 4096;

/// 默认轨迹文件路径（XDG 状态目录语义，与 config.rs 查找 XDG_CONFIG_HOME 同构）：
/// `$XDG_STATE_HOME/duct/trace.jsonl`；未设则 `$HOME/.local/state/duct/trace.jsonl`；
/// 两者都缺时退为当前目录 `duct-trace.jsonl`。
///
/// 选 state 而非 share：运行轨迹属操作状态/日志类；且 systemd 的 `StateDirectory=`
/// 在 `ProtectHome=read-only` 下授予该子树写例外，share 没有对应机制。
pub fn default_trace_path() -> std::path::PathBuf {
    if let Some(dir) = std::env::var_os("XDG_STATE_HOME").filter(|d| !d.as_os_str().is_empty()) {
        return std::path::PathBuf::from(dir)
            .join("duct")
            .join("trace.jsonl");
    }
    if let Some(home) = std::env::var_os("HOME").filter(|h| !h.as_os_str().is_empty()) {
        return std::path::PathBuf::from(home)
            .join(".local")
            .join("state")
            .join("duct")
            .join("trace.jsonl");
    }
    std::path::PathBuf::from("duct-trace.jsonl")
}

/// 缺失即建：父目录链 + 文件本体，append 打开。启动首开与运行期自愈共用。
fn open_trace_file(path: &Path) -> anyhow::Result<std::fs::File> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("创建轨迹目录 {} 失败: {e}", parent.display()))?;
    }
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| anyhow::anyhow!("打开轨迹文件 {} 失败: {e}", path.display()))
}

/// 路径上的文件已消失或换了 inode（外部 rm、logrotate create/move 挪走重建），
/// 即当前句柄写的不再是这个路径。copytruncate 保留同 inode，返回 false 不误触发。
fn file_replaced(path: &Path, file: &std::fs::File) -> bool {
    use std::os::unix::fs::MetadataExt;
    let on_disk = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(_) => return true,
    };
    match file.metadata() {
        Ok(h) => (on_disk.dev(), on_disk.ino()) != (h.dev(), h.ino()),
        Err(_) => true,
    }
}

// ── 单请求轨迹记录器 ───────────────────────────────────────────────────

/// 上游响应到达时暂存的事实，供 `request/end` 归并。
#[derive(Clone, Copy)]
pub struct RespFacts {
    pub status: u16,
    pub ttfb_ms: u64,
}

/// 一次转发请求的轨迹记录器。跨 handler 与响应流共享，`seq` 原子递增。
pub struct RequestTrace {
    sink: Arc<TraceSink>,
    pub trace_id: String,
    t0: Instant,
    epoch_ms: u64,
    seq: AtomicU64,
    resp: Mutex<Option<RespFacts>>,
}

static TRACE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn next_trace_id() -> String {
    // 毫秒时间戳 + 进程内单调计数，字典序即时间序，短且无新依赖。
    format!(
        "{:x}-{:x}",
        epoch_ms(),
        TRACE_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

impl RequestTrace {
    pub fn new(sink: Arc<TraceSink>) -> Arc<Self> {
        Arc::new(Self {
            sink,
            trace_id: next_trace_id(),
            t0: Instant::now(),
            epoch_ms: epoch_ms(),
            seq: AtomicU64::new(0),
            resp: Mutex::new(None),
        })
    }

    pub fn t0(&self) -> Instant {
        self.t0
    }

    pub fn elapsed_ms(&self) -> u64 {
        self.t0.elapsed().as_millis() as u64
    }

    pub fn set_resp(&self, facts: RespFacts) {
        *self.resp.lock().unwrap_or_else(|e| e.into_inner()) = Some(facts);
    }

    fn resp_facts(&self) -> Option<RespFacts> {
        *self.resp.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// 以 info 级发射一个事件。
    pub fn emit(&self, typ: &str, data: Value) {
        self.emit_with(typ, "info", data);
    }

    pub fn emit_with(&self, typ: &str, severity: &str, data: Value) {
        let record = json!({
            "v": TRACE_FORMAT_VERSION,
            "time": self.epoch_ms + self.elapsed_ms(),
            "trace": self.trace_id,
            "seq": self.seq.fetch_add(1, Ordering::Relaxed),
            "type": typ,
            "severity": severity,
            "data": data,
        });
        match serde_json::to_string(&record) {
            Ok(line) => self.sink.emit_line(line, severity),
            Err(e) => tracing::warn!(error = %e, "trace record serialize failed"),
        }
    }

    /// 收尾事件：归并 status/ttfb/duration，并按 outcome + 状态码预映射 severity。
    pub fn end(&self, outcome: &str, extra: Map<String, Value>) {
        let mut data = Map::new();
        data.insert("outcome".into(), json!(outcome));
        data.insert("duration_ms".into(), json!(self.elapsed_ms()));
        let mut severity = match outcome {
            "client_disconnected" | "interrupted" => "warn",
            "completed" => "info",
            _ => "error",
        };
        if let Some(r) = self.resp_facts() {
            data.insert("status".into(), json!(r.status));
            data.insert("ttfb_ms".into(), json!(r.ttfb_ms));
            if outcome == "completed" && r.status >= 400 {
                severity = "warn";
            }
        }
        for (k, v) in extra {
            data.insert(k, v);
        }
        self.emit_with("request/end", severity, Value::Object(data));
    }
}

// ── 头脱敏 ─────────────────────────────────────────────────────────────

/// 永不记录值的头（凭证零接触在轨迹侧的强制名单）。
pub const SENSITIVE_HEADERS: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "x-api-key",
    "api-key",
    "x-goog-api-key",
    "x-amz-security-token",
    "cookie",
    "set-cookie",
    "x-forwarded-authorization",
];

/// 允许记录值的头（排查需要的语义/限流信息）。
pub const SAFE_HEADER_VALUES: &[&str] = &[
    "content-type",
    "accept",
    "accept-encoding",
    "user-agent",
    "anthropic-version",
    "openai-beta",
    "openai-organization",
    "openai-project",
    "x-request-id",
    "x-session-id",
    "x-app",
    "retry-after",
    "should-retry",
];

const MAX_HEADER_VALUE_LEN: usize = 96;

fn truncate_chars(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut cut = max;
    while !s.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}…", &s[..cut])
}

/// 头清单摘要：敏感头 `name:***`，语义头 `name:value(截断)`，其余仅留名字。
pub fn header_summary(headers: &HeaderMap) -> Vec<String> {
    let mut out: Vec<String> = headers
        .iter()
        .map(|(name, value)| {
            let lower = name.as_str().to_ascii_lowercase();
            if SENSITIVE_HEADERS.contains(&lower.as_str()) {
                format!("{lower}:***")
            } else if SAFE_HEADER_VALUES.contains(&lower.as_str()) {
                let v = value.to_str().unwrap_or("<binary>");
                format!("{lower}:{}", truncate_chars(v, MAX_HEADER_VALUE_LEN))
            } else {
                lower
            }
        })
        .collect();
    out.sort();
    out
}

/// URL 的轨迹安全形式：scheme://host[:port]/path?{参数名…}，
/// 剥离 userinfo 与全部 query 值（部分供应商在 query 携带 key）。
pub fn url_display(url: &reqwest::Url) -> String {
    let mut s = format!("{}://{}", url.scheme(), url.host_str().unwrap_or("?"));
    if let Some(p) = url.port() {
        s.push_str(&format!(":{p}"));
    }
    s.push_str(url.path());
    if let Some(q) = url.query() {
        let keys: Vec<&str> = q
            .split('&')
            .filter(|kv| !kv.is_empty())
            .map(|kv| kv.split('=').next().unwrap_or(kv))
            .collect();
        if !keys.is_empty() {
            s.push('?');
            s.push('{');
            s.push_str(&keys.join(","));
            s.push('}');
        }
    }
    s
}

/// query 参数名清单（请求行的安全摘要）。
pub fn query_keys(query: Option<&str>) -> Vec<String> {
    query
        .map(|q| {
            q.split('&')
                .filter(|kv| !kv.is_empty())
                .map(|kv| kv.split('=').next().unwrap_or(kv).to_string())
                .collect()
        })
        .unwrap_or_default()
}

// ── 请求体前缀扫描（流式透传不缓冲前提下的 model/stream 提取）──────────

/// 前缀扫描上限：LLM 请求的 model/stream 都在正文头部，128 KiB 足够。
pub const BODY_SCAN_LIMIT: usize = 128 * 1024;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct BodyPrefixFacts {
    pub model: Option<String>,
    pub stream: Option<String>,
}

fn find_seq(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// 从 `from` 起跳过空白找到 `:` 后的值起点。
fn value_start(hay: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i < hay.len() && matches!(hay[i], b' ' | b'\t' | b'\r' | b'\n') {
        i += 1;
    }
    if i < hay.len() && hay[i] == b':' {
        i += 1;
        while i < hay.len() && matches!(hay[i], b' ' | b'\t' | b'\r' | b'\n') {
            i += 1;
        }
        return Some(i);
    }
    None
}

/// 从字符串值起点读取（含转义处理）；值未闭合返回 None（等更多前缀）。
fn read_json_string(buf: &[u8], start: usize) -> Option<String> {
    if buf.get(start) != Some(&b'"') {
        return None;
    }
    let mut out = Vec::new();
    let mut i = start + 1;
    while i < buf.len() {
        match buf[i] {
            b'\\' if i + 1 < buf.len() => {
                out.push(buf[i + 1]);
                i += 2;
            }
            b'"' => return Some(String::from_utf8_lossy(&out).into_owned()),
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    None
}

fn read_json_bool(buf: &[u8], start: usize) -> Option<String> {
    if buf[start..].starts_with(b"true") {
        Some("true".into())
    } else if buf[start..].starts_with(b"false") {
        Some("false".into())
    } else {
        None
    }
}

/// 在 JSON 对象前缀中扫描顶层 `"model"` 与 `"stream"`。
/// 顶层与嵌套对象的键会混淆朴素扫描；这里取首个命中键——对 OpenAI/Anthropic
/// 请求体（顶层即含 model）足够，且只做只读观测、不影响透传。
pub fn scan_body_prefix(buf: &[u8]) -> BodyPrefixFacts {
    let mut facts = BodyPrefixFacts::default();
    if let Some(k) = find_seq(buf, br#""model""#)
        && let Some(v) = value_start(buf, k + 7).and_then(|s| read_json_string(buf, s))
    {
        facts.model = Some(v);
    }
    if let Some(k) = find_seq(buf, br#""stream""#)
        && let Some(v) = value_start(buf, k + 8).and_then(|s| read_json_bool(buf, s))
    {
        facts.stream = Some(v);
    }
    facts
}

/// 完整 JSON body 的派生事实（仅在 body 已被读取的路径使用，如 normalize_sse）。
/// 解析失败或非对象返回空 Map；绝不包含正文内容。
pub fn body_facts(buf: &[u8]) -> Map<String, Value> {
    let mut out = Map::new();
    let Ok(value) = serde_json::from_slice::<Value>(buf) else {
        return out;
    };
    let Some(obj) = value.as_object() else {
        return out;
    };
    for key in [
        "model",
        "stream",
        "max_tokens",
        "max_completion_tokens",
        "temperature",
        "top_p",
        "stop",
        "reasoning_effort",
    ] {
        if let Some(v) = obj.get(key) {
            out.insert(key.into(), truncate_fact(v));
        }
    }
    if let Some(m) = obj.get("messages").and_then(Value::as_array) {
        out.insert("n_messages".into(), json!(m.len()));
        if let Some(last_role) = m.last().and_then(|x| x.get("role")).and_then(Value::as_str) {
            out.insert("last_role".into(), json!(last_role));
        }
    }
    if let Some(t) = obj.get("tools").and_then(Value::as_array) {
        out.insert("n_tools".into(), json!(t.len()));
    }
    if let Some(p) = obj.get("prompt").and_then(Value::as_str) {
        out.insert("prompt_chars".into(), json!(p.chars().count()));
    }
    out
}

fn truncate_fact(v: &Value) -> Value {
    match v {
        Value::String(s) if s.len() > MAX_HEADER_VALUE_LEN => {
            Value::String(truncate_chars(s, MAX_HEADER_VALUE_LEN))
        }
        other => other.clone(),
    }
}

// ── 响应流事实提取 ─────────────────────────────────────────────────────

/// 从响应流提取的派生事实（token 用量、停止原因、模型回显、错误）。
#[derive(Debug, Default)]
pub struct StreamFacts {
    pub events: u64,
    pub done: bool,
    pub first_data_ms: Option<u64>,
    pub usage: Option<Value>,
    pub finish_reasons: Vec<String>,
    pub model: Option<String>,
    pub id: Option<String>,
    pub error: Option<Value>,
    pub non_json_data: bool,
    /// 上游响应带 content-encoding（压缩流）：原始字节按透传语义不解帧，
    /// 事实提取改走观察式解码；`decoded:true` 表示解码成功、事实来自解码字节。
    encoded: bool,
    decoded: bool,
    line_overflow: bool,
}

impl StreamFacts {
    /// 观测一条 SSE `data:` 载荷。
    pub fn observe_data(&mut self, payload: &[u8], now_ms: u64) {
        let text = String::from_utf8_lossy(payload).into_owned();
        let s = text.trim();
        if s == "[DONE]" {
            self.done = true;
            return;
        }
        if !s.starts_with('{') {
            self.non_json_data = true;
            return;
        }
        match serde_json::from_str::<Value>(s) {
            Ok(v) => self.observe_json(v, now_ms),
            Err(_) => self.non_json_data = true,
        }
    }

    fn observe_json(&mut self, v: Value, now_ms: u64) {
        self.events += 1;
        if self.first_data_ms.is_none() && now_ms != NO_TS {
            self.first_data_ms = Some(now_ms);
        }
        if let Some(obj) = v.as_object() {
            // usage 可能在顶层（OpenAI / Anthropic message_delta），也可能嵌在
            // message 下（Anthropic message_start）。取任一存在者。
            if let Some(u) = obj
                .get("usage")
                .or_else(|| obj.get("message").and_then(|m| m.get("usage")))
            {
                self.usage = Some(merge_usage(self.usage.take(), u));
            }
            if self.model.is_none() {
                self.model = obj
                    .get("model")
                    .or_else(|| obj.get("message").and_then(|m| m.get("model")))
                    .and_then(Value::as_str)
                    .map(str::to_string);
            }
            if self.id.is_none() {
                self.id = obj
                    .get("id")
                    .or_else(|| obj.get("message_id"))
                    .or_else(|| obj.get("message").and_then(|m| m.get("id")))
                    .and_then(Value::as_str)
                    .map(str::to_string);
            }
            if obj.contains_key("error") {
                self.error = Some(obj["error"].clone());
            }
            if let Some(choices) = obj.get("choices").and_then(Value::as_array) {
                for c in choices {
                    if let Some(f) = c.get("finish_reason").and_then(Value::as_str)
                        && !f.is_empty()
                    {
                        push_unique(&mut self.finish_reasons, f);
                    }
                }
            }
            // Anthropic：stop_reason 在 delta 内（message_delta 事件）。
            if let Some(sr) = obj
                .get("delta")
                .and_then(|d| d.get("stop_reason"))
                .and_then(Value::as_str)
                && !sr.is_empty()
            {
                push_unique(&mut self.finish_reasons, sr);
            }
        }
    }

    pub fn to_json(&self) -> Value {
        let mut o = Map::new();
        o.insert("events".into(), json!(self.events));
        o.insert("done".into(), json!(self.done));
        if let Some(ms) = self.first_data_ms {
            o.insert("first_data_ms".into(), json!(ms));
        }
        if let Some(u) = &self.usage {
            o.insert("usage".into(), u.clone());
        }
        if !self.finish_reasons.is_empty() {
            o.insert("finish_reasons".into(), json!(self.finish_reasons));
        }
        if let Some(m) = &self.model {
            o.insert("model".into(), json!(m));
        }
        if let Some(id) = &self.id {
            o.insert("id".into(), json!(id));
        }
        if let Some(e) = &self.error {
            o.insert("error".into(), e.clone());
        }
        if self.non_json_data {
            o.insert("non_json_data".into(), json!(true));
        }
        if self.encoded {
            o.insert("encoded".into(), json!(true));
        }
        if self.decoded {
            o.insert("decoded".into(), json!(true));
        }
        if self.line_overflow {
            o.insert("line_overflow".into(), json!(true));
        }
        Value::Object(o)
    }
}

fn push_unique(list: &mut Vec<String>, s: &str) {
    if !list.iter().any(|x| x == s) {
        list.push(s.to_string());
    }
}

/// usage 合并：后到的数值覆盖同名键（OpenAI 尾帧携带完整 usage；
/// Anthropic message_start/message_delta 分别携带输入/输出侧，按键合并取并集）。
fn merge_usage(prev: Option<Value>, next: &Value) -> Value {
    let mut base = match prev {
        Some(Value::Object(m)) => m,
        _ => Map::new(),
    };
    if let Value::Object(next_map) = next {
        for (k, v) in next_map {
            match base.get(k) {
                Some(Value::Object(inner)) if v.is_object() => {
                    let merged = merge_usage(Some(Value::Object(inner.clone())), v);
                    base.insert(k.clone(), merged);
                }
                _ => {
                    // 后到的数值覆盖同名键；新键直接收录。
                    base.insert(k.clone(), v.clone());
                }
            }
        }
    }
    Value::Object(base)
}

/// 非流式 JSON 响应事实（在预览缓冲完整时尝试解析；失败即放弃，只留预览）。
pub fn json_body_facts(buf: &[u8]) -> Option<StreamFacts> {
    let v = serde_json::from_slice::<Value>(buf).ok()?;
    let mut facts = StreamFacts::default();
    facts.observe_json(v, 0);
    Some(facts)
}

// ── 响应体观测流（透传字节不动，旁路记事实）───────────────────────────

/// 非 SSE 响应体预览上限（解析 usage/error 用）。
pub const BODY_PREVIEW_LIMIT: usize = 64 * 1024;
/// 错误响应体进入轨迹的截断上限。
pub const ERROR_PREVIEW_LIMIT: usize = 2048;
/// SSE 单行缓冲上限，超限放弃继续解析行（只影响观测，不影响透传）。
const SSE_LINE_LIMIT: usize = 1024 * 1024;

/// 观测专用解码的压缩输入上限；超限只存头部并标记不可完整解码。
const OBS_ENCODED_CAP: usize = 4 * 1024 * 1024;

/// `observe_data` 的时间戳哨兵：解码观测无逐帧到达时间，`first_data_ms` 不记。
const NO_TS: u64 = u64::MAX;

/// 可识别的响应压缩编码。`Gzip`/`Deflate` 支持流式解码（观察解码与
/// normalize 重发共用）；`Br` 仅观察解码（无 poll 内增量 API 可用）。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EncKind {
    Gzip,
    Deflate,
    Br,
}

/// 从 `content-encoding` 头值解析首个可识别编码。
pub fn enc_kind(value: &str) -> Option<EncKind> {
    let v = value
        .split(',')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    match v.as_str() {
        "gzip" | "x-gzip" => Some(EncKind::Gzip),
        "deflate" => Some(EncKind::Deflate),
        "br" => Some(EncKind::Br),
        _ => None,
    }
}

struct EncState {
    kind: EncKind,
    bytes: Vec<u8>,
    overflow: bool,
}

/// 观察式流解码：容忍中途错误（返回已恢复字节 + complete=false）。
/// 绝不参与透传路径——客户端拿到的仍是原始压缩字节。
fn decode_observed(kind: EncKind, input: &[u8]) -> (Vec<u8>, bool) {
    let mut dec: Box<dyn std::io::Read> = match kind {
        EncKind::Gzip => Box::new(flate2::bufread::MultiGzDecoder::new(std::io::Cursor::new(
            input,
        ))),
        EncKind::Deflate => Box::new(flate2::bufread::ZlibDecoder::new(std::io::Cursor::new(
            input,
        ))),
        EncKind::Br => Box::new(brotli::Decompressor::new(std::io::Cursor::new(input), 8192)),
    };
    let mut out = Vec::new();
    let mut buf = [0u8; 16384];
    loop {
        match dec.read(&mut buf) {
            Ok(0) => return (out, true),
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(_) => return (out, false),
        }
    }
}

/// 对完整（解码）字节串跑与明文流相同的 SSE 行事实提取。
fn scan_sse_bytes(bytes: &[u8], facts: &mut StreamFacts) {
    for line in bytes.split(|&b| b == b'\n') {
        let text = String::from_utf8_lossy(line).into_owned();
        let content = text.trim_end_matches('\r');
        if let Some(payload) = content.strip_prefix("data:") {
            facts.observe_data(payload.trim_start().as_bytes(), NO_TS);
        }
    }
}

/// 包裹上游响应流：字节级原样透传，同时计数并按 SSE 行法提取事实；
/// 流终了（完成/出错）或被提前 Drop（客户端断连、连接重置）时补发
/// `request/end`。Drop 兜底即 DSH「合成 interrupted 收尾」的对应物。
/// `encoding = Some(..)`（上游回 content-encoding）时压缩字节另走观察式解码
/// （gzip/deflate/br），SSE/JSON 事实与内容快照取自解码字节并标 `decoded:true`；
/// 不可解码（未知算法/超限/解码错）时保留 `encoded` 标记并记 `resp_content_skipped`。
/// `capture_limit > 0`（`--trace-body`）时另存内容头部快照。
pub struct TracedBody<S> {
    inner: S,
    tr: Arc<RequestTrace>,
    sse: bool,
    bytes: u64,
    chunks: u64,
    pending: Vec<u8>,
    facts: StreamFacts,
    preview: Vec<u8>,
    capture_limit: usize,
    content: Vec<u8>,
    enc: Option<EncState>,
    ended: bool,
}

impl<S> TracedBody<S> {
    pub fn new(
        inner: S,
        tr: Arc<RequestTrace>,
        sse: bool,
        encoding: Option<&str>,
        capture_limit: usize,
    ) -> Self {
        let enc = encoding.and_then(enc_kind).map(|kind| EncState {
            kind,
            bytes: Vec::new(),
            overflow: false,
        });
        Self {
            inner,
            tr,
            sse,
            bytes: 0,
            chunks: 0,
            pending: Vec::new(),
            facts: StreamFacts {
                encoded: encoding.is_some(),
                ..Default::default()
            },
            preview: Vec::new(),
            capture_limit,
            content: Vec::new(),
            enc,
            ended: false,
        }
    }

    fn ingest(&mut self, chunk: &[u8]) {
        self.bytes += chunk.len() as u64;
        self.chunks += 1;
        if self.facts.encoded {
            // 压缩流：只喂观察缓冲（可解算法才存），透传字节不受影响。
            if let Some(st) = self.enc.as_mut() {
                let room = OBS_ENCODED_CAP.saturating_sub(st.bytes.len());
                if chunk.len() > room {
                    st.overflow = true;
                }
                st.bytes.extend_from_slice(&chunk[..chunk.len().min(room)]);
            }
            return;
        }
        if self.capture_limit > 0 && self.content.len() < self.capture_limit {
            let take = chunk.len().min(self.capture_limit - self.content.len());
            self.content.extend_from_slice(&chunk[..take]);
        }
        if self.sse {
            self.pending.extend_from_slice(chunk);
            while let Some(nl) = self.pending.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.pending.drain(..=nl).collect();
                self.observe_line(&line);
            }
            if self.pending.len() > SSE_LINE_LIMIT {
                self.facts.line_overflow = true;
                self.pending.clear();
            }
        } else if self.preview.len() < BODY_PREVIEW_LIMIT {
            let take = chunk.len().min(BODY_PREVIEW_LIMIT - self.preview.len());
            self.preview.extend_from_slice(&chunk[..take]);
        }
    }

    fn observe_line(&mut self, line: &[u8]) {
        let text = String::from_utf8_lossy(line).into_owned();
        let content = text.trim_end_matches(['\n', '\r']);
        if let Some(payload) = content.strip_prefix("data:") {
            self.facts
                .observe_data(payload.trim_start().as_bytes(), self.tr.elapsed_ms());
        }
    }

    fn flush_tail(&mut self) {
        if self.sse && !self.pending.is_empty() {
            let tail = std::mem::take(&mut self.pending);
            self.observe_line(&tail);
        }
    }

    fn finish(&mut self, outcome: &str, error: Option<Value>) {
        if self.ended {
            return;
        }
        self.ended = true;
        // 压缩流收尾：观察式解码，把解码字节喂回与明文一致的事实提取路径。
        let mut decoded: Option<Vec<u8>> = None;
        if self.facts.encoded
            && let Some(st) = self.enc.take()
        {
            let (bytes, complete) = decode_observed(st.kind, &st.bytes);
            if complete {
                self.facts.decoded = true;
                if self.sse {
                    scan_sse_bytes(&bytes, &mut self.facts);
                }
                decoded = Some(bytes);
            }
        }
        let mut extra = Map::new();
        extra.insert("resp_bytes".into(), json!(self.bytes));
        extra.insert("resp_chunks".into(), json!(self.chunks));
        if self.sse {
            extra.insert("sse".into(), self.facts.to_json());
        } else {
            let source: &[u8] = match &decoded {
                Some(d) => d,
                None => &self.preview,
            };
            if let Some(f) = json_body_facts(source) {
                extra.insert("body".into(), f.to_json());
            }
            // 上游 4xx/5xx：无论能否解析，都附截断预览以定位根因。
            let status = self.tr.resp_facts().map(|r| r.status).unwrap_or(0);
            if status >= 400 && !source.is_empty() {
                let preview = String::from_utf8_lossy(source).into_owned();
                extra.insert(
                    "resp_preview".into(),
                    json!(truncate_chars(&preview, ERROR_PREVIEW_LIMIT)),
                );
            }
        }
        if self.capture_limit > 0 {
            let head: Option<&[u8]> = if !self.content.is_empty() {
                Some(&self.content)
            } else {
                decoded.as_deref().map(|d| {
                    let take = d.len().min(self.capture_limit);
                    &d[..take]
                })
            };
            match head {
                Some(h) if !h.is_empty() => {
                    let s = String::from_utf8_lossy(h).into_owned();
                    extra.insert("resp_content_head".into(), json!(s));
                }
                _ => {
                    extra.insert(
                        "resp_content_skipped".into(),
                        json!(if self.facts.encoded {
                            "content-encoded stream not observable (unknown codec, oversized, or decode error)"
                        } else {
                            "no response bytes seen"
                        }),
                    );
                }
            }
        }
        if let Some(e) = error {
            extra.insert("error".into(), e);
        }
        self.tr.end(outcome, extra);
    }
}

impl<S, E> Stream for TracedBody<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    type Item = Result<Bytes, E>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match std::pin::Pin::new(&mut self.inner).poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(chunk))) => {
                self.ingest(&chunk);
                std::task::Poll::Ready(Some(Ok(chunk)))
            }
            std::task::Poll::Ready(Some(Err(e))) => {
                self.finish("stream_error", Some(json!({"message": e.to_string()})));
                std::task::Poll::Ready(Some(Err(e)))
            }
            std::task::Poll::Ready(None) => {
                self.flush_tail();
                self.finish("completed", None);
                std::task::Poll::Ready(None)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl<S> Drop for TracedBody<S> {
    fn drop(&mut self) {
        // 未被消费完即 Drop（客户端断连/hyper 取消/响应构建失败）：合成 interrupted 收尾。
        self.finish("interrupted", None);
    }
}

// ── 请求体观测流（前缀扫描 model/stream）───────────────────────────────

/// 包裹请求体流：字节原样透传给上游，同时累积前缀并扫描顶层
/// `model` / `stream`；拿到即发 `request/body`（至多一次，流终了兜底）。
pub struct ScannedBody<S> {
    inner: S,
    tr: Arc<RequestTrace>,
    total: u64,
    prefix: Vec<u8>,
    emitted: bool,
    capture_limit: usize,
}

impl<S> ScannedBody<S> {
    pub fn new(inner: S, tr: Arc<RequestTrace>, capture_limit: usize) -> Self {
        Self {
            inner,
            tr,
            total: 0,
            prefix: Vec::new(),
            emitted: false,
            capture_limit,
        }
    }

    fn maybe_emit(&mut self, force: bool) {
        if self.emitted {
            return;
        }
        let facts = scan_body_prefix(&self.prefix);
        let known = facts.model.is_some() && facts.stream.is_some();
        if !known && !force {
            return;
        }
        self.emitted = true;
        let mut data = Map::new();
        data.insert("bytes".into(), json!(self.total));
        data.insert("parse".into(), json!("prefix"));
        if let Some(m) = facts.model {
            data.insert("model".into(), json!(m));
        }
        if let Some(s) = facts.stream {
            data.insert("stream".into(), json!(s));
        }
        if self.capture_limit > 0 && !self.prefix.is_empty() {
            let take = self.prefix.len().min(self.capture_limit);
            let head = String::from_utf8_lossy(&self.prefix[..take]).into_owned();
            data.insert("req_content_head".into(), json!(head));
        }
        self.tr.emit("request/body", Value::Object(data));
    }
}

impl<S, E> Stream for ScannedBody<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match std::pin::Pin::new(&mut self.inner).poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(chunk))) => {
                self.total += chunk.len() as u64;
                if self.prefix.len() < BODY_SCAN_LIMIT {
                    let take = chunk.len().min(BODY_SCAN_LIMIT - self.prefix.len());
                    self.prefix.extend_from_slice(&chunk[..take]);
                    self.maybe_emit(false);
                }
                std::task::Poll::Ready(Some(Ok(chunk)))
            }
            std::task::Poll::Ready(Some(Err(e))) => {
                self.maybe_emit(true);
                std::task::Poll::Ready(Some(Err(e)))
            }
            std::task::Poll::Ready(None) => {
                self.maybe_emit(true);
                std::task::Poll::Ready(None)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

// ── 单元测试 ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use futures::StreamExt;

    fn sink_lines() -> (Arc<TraceSink>, Arc<Mutex<Vec<String>>>) {
        let (sink, cap) = TraceSink::capture();
        (Arc::new(sink), cap)
    }

    #[test]
    fn envelope_fields_and_seq_contiguity() {
        let (sink, cap) = sink_lines();
        let tr = RequestTrace::new(sink);
        tr.emit("request/start", json!({"a": 1}));
        tr.emit("request/body", json!({"b": 2}));
        let lines = cap.lock().unwrap();
        assert_eq!(lines.len(), 2);
        for (i, line) in lines.iter().enumerate() {
            let v: Value = serde_json::from_str(line).unwrap();
            assert_eq!(v["v"], TRACE_FORMAT_VERSION);
            assert_eq!(v["trace"], tr.trace_id);
            assert_eq!(v["seq"], i as u64);
            assert!(v["time"].is_number());
            assert_eq!(v["severity"], "info");
        }
        assert_eq!(lines[0].lines().count(), 1, "单行 JSONL");
    }

    #[test]
    fn end_merges_resp_facts_and_severity() {
        let (sink, cap) = sink_lines();
        let tr = RequestTrace::new(sink);
        tr.set_resp(RespFacts {
            status: 429,
            ttfb_ms: 12,
        });
        tr.end("completed", Map::new());
        let line = cap.lock().unwrap().pop().unwrap();
        let v: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["type"], "request/end");
        assert_eq!(v["data"]["status"], 429);
        assert_eq!(v["data"]["ttfb_ms"], 12);
        assert_eq!(v["data"]["outcome"], "completed");
        assert_eq!(v["severity"], "warn", "透传 4xx 映射 warn");
    }

    #[test]
    fn interrupted_end_is_warn() {
        let (sink, cap) = sink_lines();
        let tr = RequestTrace::new(sink);
        tr.end("interrupted", Map::new());
        let v: Value = serde_json::from_str(cap.lock().unwrap().last().unwrap()).unwrap();
        assert_eq!(v["severity"], "warn");
        assert_eq!(v["data"]["outcome"], "interrupted");
    }

    #[test]
    fn header_summary_redacts_credentials() {
        let mut h = HeaderMap::new();
        h.insert(
            "authorization",
            HeaderValue::from_static("Bearer sk-super-secret"),
        );
        h.insert("x-api-key", HeaderValue::from_static("sk-another-secret"));
        h.insert("content-type", HeaderValue::from_static("application/json"));
        h.insert("x-client-trace", HeaderValue::from_static("abc"));
        let s = header_summary(&h);
        assert!(s.contains(&"authorization:***".to_string()));
        assert!(s.contains(&"x-api-key:***".to_string()));
        assert!(s.contains(&"content-type:application/json".to_string()));
        assert!(s.contains(&"x-client-trace".to_string()));
        let joined = s.join("|");
        assert!(!joined.contains("secret"), "脱敏不得泄漏值: {joined}");
        assert!(!joined.contains("abc"), "非白名单头不得带值: {joined}");
    }

    #[test]
    fn url_display_strips_userinfo_and_query_values() {
        let u = reqwest::Url::parse("https://key:pass@api.x.com/v1/chat?api-key=SEKRIT&model=m")
            .unwrap();
        let s = url_display(&u);
        assert_eq!(s, "https://api.x.com/v1/chat?{api-key,model}");
        assert!(!s.contains("SEKRIT") && !s.contains("pass"));
    }

    #[test]
    fn scan_prefix_finds_model_and_stream() {
        let buf =
            br#"{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
        let f = scan_body_prefix(buf);
        assert_eq!(f.model.as_deref(), Some("gpt-4o"));
        assert_eq!(f.stream.as_deref(), Some("true"));
    }

    #[test]
    fn scan_prefix_handles_split_across_chunks() {
        let full = br#"{"model":"deepseek-chat","stream":false}"#;
        // 截半时 model 未闭合 → None；补全后命中。
        let half = &full[..12];
        assert_eq!(scan_body_prefix(half).model, None);
        let f = scan_body_prefix(full);
        assert_eq!(f.model.as_deref(), Some("deepseek-chat"));
        assert_eq!(f.stream.as_deref(), Some("false"));
    }

    #[test]
    fn scan_prefix_anthropic_style() {
        let buf = br#"{"model":"claude-sonnet-4-5","max_tokens":1024,"stream":true,"messages":[]}"#;
        let f = scan_body_prefix(buf);
        assert_eq!(f.model.as_deref(), Some("claude-sonnet-4-5"));
        assert_eq!(f.stream.as_deref(), Some("true"));
    }

    #[test]
    fn body_facts_extracts_counts_never_content() {
        let buf = br#"{"model":"m","messages":[{"role":"user","content":"SECRET PROMPT"}],"tools":[{}],"max_tokens":16}"#;
        let f = body_facts(buf);
        assert_eq!(f["model"], "m");
        assert_eq!(f["n_messages"], 1);
        assert_eq!(f["last_role"], "user");
        assert_eq!(f["n_tools"], 1);
        assert_eq!(f["max_tokens"], 16);
        let dumped = serde_json::to_string(&f).unwrap();
        assert!(!dumped.contains("SECRET PROMPT"));
    }

    #[test]
    fn sse_facts_extract_usage_finish_done_error() {
        let mut f = StreamFacts::default();
        f.observe_data(
            br#"{"choices":[{"index":0,"delta":{"content":"hi"}}],"model":"gpt-4o"}"#,
            5,
        );
        f.observe_data(
            br#"{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
            9,
        );
        f.observe_data(
            br#"{"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}"#,
            10,
        );
        f.observe_data(b"[DONE]", 11);
        assert!(f.done);
        assert_eq!(f.events, 3);
        assert_eq!(f.first_data_ms, Some(5));
        assert_eq!(f.usage.as_ref().unwrap()["total_tokens"], 15);
        assert_eq!(f.finish_reasons, vec!["stop".to_string()]);
        assert_eq!(f.model.as_deref(), Some("gpt-4o"));
    }

    #[test]
    fn sse_facts_mid_stream_error_event() {
        let mut f = StreamFacts::default();
        f.observe_data(
            br#"{"error":{"message":"quota exceeded","type":"rate_limit"}}"#,
            3,
        );
        let e = f.error.as_ref().unwrap();
        assert_eq!(e["message"], "quota exceeded");
    }

    #[test]
    fn anthropic_style_usage_merges_across_events() {
        let mut f = StreamFacts::default();
        f.observe_data(
            br#"{"type":"message_start","message":{"model":"claude-x","usage":{"input_tokens":7}}}"#,
            1,
        );
        f.observe_data(
            br#"{"type":"message_delta","usage":{"output_tokens":3},"delta":{"stop_reason":"end_turn"}}"#,
            2,
        );
        let u = f.usage.unwrap();
        assert_eq!(u["input_tokens"], 7);
        assert_eq!(u["output_tokens"], 3);
        assert_eq!(f.finish_reasons, vec!["end_turn".to_string()]);
        assert_eq!(
            f.model.as_deref(),
            Some("claude-x"),
            "嵌套 message.model 也须提取"
        );
    }

    #[tokio::test]
    async fn traced_body_passthrough_and_end() {
        let (sink, cap) = sink_lines();
        let tr = RequestTrace::new(sink);
        tr.set_resp(RespFacts {
            status: 200,
            ttfb_ms: 1,
        });
        let src = futures::stream::iter(
            vec![
                "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: [DONE]\n\n",
            ]
            .into_iter()
            .map(|s| Ok::<Bytes, std::convert::Infallible>(Bytes::from(s))),
        );
        let out: Vec<u8> = TracedBody::new(src, tr, true, None, 0)
            .map(|r| r.unwrap().to_vec())
            .collect::<Vec<_>>()
            .await
            .concat();
        assert!(
            String::from_utf8_lossy(&out).contains("[DONE]"),
            "字节透传不变"
        );
        let v: Value = serde_json::from_str(cap.lock().unwrap().last().unwrap()).unwrap();
        assert_eq!(v["type"], "request/end");
        assert_eq!(v["data"]["outcome"], "completed");
        assert_eq!(v["data"]["sse"]["done"], true);
        assert_eq!(v["data"]["sse"]["finish_reasons"][0], "stop");
    }

    #[tokio::test]
    async fn encoded_stream_marks_fact_and_skips_parsing() {
        // 压缩流（gzip 字节对行解析是乱码）：字节/分块仍统计，sse.encoded:true
        // 解释 events:0——不被误读为流内容异常。
        let (sink, cap) = sink_lines();
        let tr = RequestTrace::new(sink);
        tr.set_resp(RespFacts {
            status: 200,
            ttfb_ms: 1,
        });
        let gz_like = vec![0x1f_u8, 0x8b, 0x08, 0x00, 0xb7, 0x2a, 0x0b, 0x6e];
        let src = futures::stream::iter(vec![Ok::<Bytes, std::convert::Infallible>(Bytes::from(
            gz_like.clone(),
        ))]);
        let out: Vec<u8> = TracedBody::new(src, tr, true, Some("gzip"), 0)
            .map(|r| r.unwrap().to_vec())
            .collect::<Vec<_>>()
            .await
            .concat();
        assert_eq!(out, gz_like, "压缩字节同样原样透传");
        let v: Value = serde_json::from_str(cap.lock().unwrap().last().unwrap()).unwrap();
        assert_eq!(v["data"]["outcome"], "completed");
        assert_eq!(v["data"]["sse"]["encoded"], true);
        assert_eq!(v["data"]["sse"]["events"], 0);
        assert_eq!(v["data"]["resp_bytes"], gz_like.len() as u64);
    }

    #[tokio::test]
    async fn content_capture_records_heads_when_on() {
        let (sink, cap) = sink_lines();
        let tr = RequestTrace::new(sink.clone());
        tr.set_resp(RespFacts {
            status: 200,
            ttfb_ms: 1,
        });
        // 请求侧：前缀扫描流捕获头部（按预算截断）。
        let src = futures::stream::iter(vec![Ok::<Bytes, std::convert::Infallible>(
            Bytes::from_static(b"{\"model\":\"m\",\"messages\":[{\"content\":\"hi\"}]}"),
        )]);
        let _: Vec<_> = ScannedBody::new(src, tr.clone(), 32)
            .collect::<Vec<_>>()
            .await;
        // 响应侧：tap 捕获头部。
        let rsrc = futures::stream::iter(vec![
            Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(b"data: {\"a\":")),
            Ok(Bytes::from_static(b"1}\n\n")),
        ]);
        let _: Vec<_> = TracedBody::new(rsrc, tr, true, None, 32)
            .collect::<Vec<_>>()
            .await;

        let lines = cap.lock().unwrap();
        let body_ev: Value =
            serde_json::from_str(lines.iter().find(|l| l.contains("request/body")).unwrap())
                .unwrap();
        assert_eq!(
            body_ev["data"]["req_content_head"], "{\"model\":\"m\",\"messages\":[{\"conte",
            "捕获应恰好截断到 32 字节",
        );
        assert_eq!(
            body_ev["data"]["req_content_head"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            32,
        );
        let end_ev: Value =
            serde_json::from_str(lines.iter().find(|l| l.contains("request/end")).unwrap())
                .unwrap();
        assert!(
            end_ev["data"]["resp_content_head"]
                .as_str()
                .unwrap()
                .starts_with("data: {\"a\":1}")
        );
    }

    #[tokio::test]
    async fn encoded_stream_skips_content_capture_with_note() {
        let (sink, cap) = sink_lines();
        let tr = RequestTrace::new(sink);
        tr.set_resp(RespFacts {
            status: 200,
            ttfb_ms: 1,
        });
        let src = futures::stream::iter(vec![Ok::<Bytes, std::convert::Infallible>(
            Bytes::from_static(&[0x1f_u8, 0x8b, 0x08, 0x00, 0x01, 0x02]),
        )]);
        let _: Vec<_> = TracedBody::new(src, tr, true, Some("gzip"), 64)
            .collect::<Vec<_>>()
            .await;
        let v: Value = serde_json::from_str(cap.lock().unwrap().last().unwrap()).unwrap();
        assert!(v["data"].get("resp_content_head").is_none());
        assert!(
            v["data"]["resp_content_skipped"]
                .as_str()
                .unwrap()
                .contains("content-encoded")
        );
    }

    #[tokio::test]
    async fn gzip_stream_decodes_facts_and_head() {
        // 真实 gzip 字节的观察解码：SSE 事实（done/usage/finish）与内容快照
        // 全部从解码字节恢复——压缩网关（如 kso）不再是盲区。
        fn gzip(bytes: &[u8]) -> Vec<u8> {
            let mut enc = Vec::new();
            {
                let mut g = flate2::write::GzEncoder::new(&mut enc, flate2::Compression::default());
                std::io::Write::write_all(&mut g, bytes).unwrap();
                g.finish().unwrap();
            }
            enc
        }
        let sse = b"data: {\"id\":\"c1\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"total_tokens\":42}}\n\ndata: [DONE]\n\n";
        let (sink, cap) = sink_lines();
        let tr = RequestTrace::new(sink);
        tr.set_resp(RespFacts {
            status: 200,
            ttfb_ms: 1,
        });
        let gz = gzip(sse);
        // 分两块喂，模拟 chunked 到达。
        let mid = gz.len() / 2;
        let src = futures::stream::iter(vec![
            Ok::<Bytes, std::convert::Infallible>(Bytes::from(gz[..mid].to_vec())),
            Ok(Bytes::from(gz[mid..].to_vec())),
        ]);
        let out: Vec<u8> = TracedBody::new(src, tr, true, Some("gzip"), 128)
            .map(|r| r.unwrap().to_vec())
            .collect::<Vec<_>>()
            .await
            .concat();
        assert_eq!(out, gz, "透传字节必须仍是原始压缩流");
        let v: Value = serde_json::from_str(cap.lock().unwrap().last().unwrap()).unwrap();
        let s = &v["data"]["sse"];
        assert_eq!(s["encoded"], true);
        assert_eq!(s["decoded"], true);
        assert_eq!(s["done"], true);
        assert_eq!(s["usage"]["total_tokens"], 42);
        assert_eq!(s["finish_reasons"][0], "stop");
        assert!(
            v["data"]["resp_content_head"]
                .as_str()
                .unwrap()
                .starts_with("data: {\"id\":\"c1\"")
        );
    }

    #[tokio::test]
    async fn brotli_stream_decodes_facts() {
        let sse = b"data: {\"choices\":[],\"usage\":{\"total_tokens\":7}}\n\ndata: [DONE]\n\n";
        let mut compressed = Vec::new();
        {
            let mut bw = brotli::CompressorWriter::new(&mut compressed, 4096, 4, 20);
            std::io::Write::write_all(&mut bw, sse).unwrap();
        }
        let (sink, cap) = sink_lines();
        let tr = RequestTrace::new(sink);
        let src = futures::stream::iter(vec![Ok::<Bytes, std::convert::Infallible>(Bytes::from(
            compressed,
        ))]);
        let _: Vec<_> = TracedBody::new(src, tr, true, Some("br"), 0)
            .collect::<Vec<_>>()
            .await;
        let v: Value = serde_json::from_str(cap.lock().unwrap().last().unwrap()).unwrap();
        assert_eq!(v["data"]["sse"]["decoded"], true);
        assert_eq!(v["data"]["sse"]["usage"]["total_tokens"], 7);
        assert!(
            v["data"].get("resp_content_head").is_none(),
            "未开采集不落快照"
        );
    }

    #[tokio::test]
    async fn traced_body_drop_emits_interrupted() {
        let (sink, cap) = sink_lines();
        let tr = RequestTrace::new(sink);
        let src = futures::stream::iter(vec![Ok::<Bytes, std::convert::Infallible>(
            Bytes::from_static(b"data: x\n"),
        )]);
        let mut stream = TracedBody::new(src, tr, true, None, 0);
        let _first = stream.next().await;
        drop(stream);
        let v: Value = serde_json::from_str(cap.lock().unwrap().last().unwrap()).unwrap();
        assert_eq!(v["type"], "request/end");
        assert_eq!(v["data"]["outcome"], "interrupted");
        assert_eq!(v["severity"], "warn");
    }

    #[tokio::test]
    async fn scanned_body_emits_once_with_prefix_facts() {
        let (sink, cap) = sink_lines();
        let tr = RequestTrace::new(sink);
        let src = futures::stream::iter(vec![
            Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(b"{\"model\":")),
            Ok(Bytes::from_static(b"\"deepseek\",\"strea")),
            Ok(Bytes::from_static(b"m\":true}")),
        ]);
        let out: Vec<u8> = ScannedBody::new(src, tr, 0)
            .map(|r| r.unwrap().to_vec())
            .collect::<Vec<_>>()
            .await
            .concat();
        assert_eq!(
            String::from_utf8_lossy(&out),
            "{\"model\":\"deepseek\",\"stream\":true}"
        );
        let lines = cap.lock().unwrap();
        let body_events: Vec<_> = lines
            .iter()
            .filter(|l| l.contains("\"request/body\""))
            .collect();
        assert_eq!(body_events.len(), 1);
        let v: Value = serde_json::from_str(body_events[0]).unwrap();
        assert_eq!(v["data"]["model"], "deepseek");
        assert_eq!(v["data"]["stream"], "true");
        assert_eq!(v["data"]["bytes"], 34);
    }

    #[test]
    fn dropped_counter_on_full_buffer() {
        // 无 writer 的 File sink（直接 drop rx）模拟缓冲满/断连不 panic。
        let (tx, rx) = sync_channel::<String>(1);
        drop(rx);
        let sink = TraceSink {
            inner: SinkInner::File(Mutex::new(tx)),
            dropped: AtomicU64::new(0),
            warned_full: Once::new(),
        };
        for i in 0..4 {
            sink.emit_line(format!("line{i}"), "info");
        }
        assert_eq!(sink.dropped(), 4);
    }

    // ── 文件 sink 的缺失自动创建 / 运行期自愈 ────────────────────────────

    static FILE_SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    fn temp_trace_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "duct-trace-file-{}-{tag}-{}",
            std::process::id(),
            FILE_SEQ.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn read_to_string_or_empty(path: &std::path::Path) -> String {
        std::fs::read_to_string(path).unwrap_or_default()
    }

    /// 轮询直到条件满足（writer 线程异步落盘），超 2s 判失败。
    fn wait_until(what: &str, cond: impl Fn() -> bool) {
        for _ in 0..100 {
            if cond() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        panic!("超时等待: {what}");
    }

    #[test]
    fn to_file_creates_missing_file_with_parent_dirs() {
        let root = temp_trace_path("create");
        let path = root.join("nested").join("deeper").join("t.jsonl");
        assert!(!path.exists());
        let sink = Arc::new(TraceSink::to_file(&path).unwrap());
        let tr = RequestTrace::new(sink);
        tr.emit("request/start", json!({"probe": "create-marker"}));
        wait_until("自动创建的文件可见且含记录", || {
            read_to_string_or_empty(&path).contains("create-marker")
        });
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn to_file_recreates_after_directory_wiped() {
        // 连目录一起删（极端运维/误删）：下一条记录前重建整条路径。
        let root = temp_trace_path("wipe");
        let path = root.join("t.jsonl");
        let sink = Arc::new(TraceSink::to_file(&path).unwrap());
        let tr = RequestTrace::new(sink.clone());
        tr.emit("request/start", json!({"probe": "before-wipe"}));
        wait_until("首条落盘", || {
            read_to_string_or_empty(&path).contains("before-wipe")
        });

        std::fs::remove_dir_all(&root).unwrap();
        tr.emit("request/end", json!({"probe": "after-recreate"}));
        wait_until("删除后自愈重建", || {
            read_to_string_or_empty(&path).contains("after-recreate")
        });
        // 新文件是全新的：旧内容不残留（append 语义针对存在文件）。
        let fresh = read_to_string_or_empty(&path);
        assert!(
            !fresh.contains("before-wipe"),
            "重建应为新文件而非拼接旧 inode 残流"
        );
        let _ = sink.dropped();
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn to_file_follows_inode_rotation_like_logrotate_create() {
        // logrotate `create` 形态：旧文件被改名挪走、原路径新建空文件。
        // writer 必须以 dev/inode 差异发现换把儿，续写新文件（不污染备份）。
        let root = temp_trace_path("rotate");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("t.jsonl");
        let backup = root.join("t.jsonl.1");
        let sink = Arc::new(TraceSink::to_file(&path).unwrap());
        let tr = RequestTrace::new(sink);
        tr.emit("request/start", json!({"probe": "pre-rotate"}));
        wait_until("轮窗前落盘", || {
            read_to_string_or_empty(&path).contains("pre-rotate")
        });

        std::fs::rename(&path, &backup).unwrap();
        std::fs::write(&path, b"").unwrap(); // 新 inode，空文件
        tr.emit("request/end", json!({"probe": "post-rotate"}));
        wait_until("新 inode 续写", || {
            read_to_string_or_empty(&path).contains("post-rotate")
        });
        assert!(
            read_to_string_or_empty(&backup).contains("pre-rotate"),
            "旧记录应留在被改名的备份里"
        );
        assert!(
            !read_to_string_or_empty(&path).contains("pre-rotate"),
            "writer 不得再写旧句柄（备份不应被追加）"
        );
        std::fs::remove_dir_all(&root).ok();
    }
}
