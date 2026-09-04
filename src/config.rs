//! aiproxy 配置装载（设计文档 v3.2 §6.2）。
//!
//! provider 清单仅有两项字段：id 与上游 base url。
//! 不含任何密钥：凭证由各调用方对最终上游出示（P5 凭证零接触）。
//!
//! 三层装载语义：
//! | 输入状态                                   | 行为                                       |
//! |--------------------------------------------|--------------------------------------------|
//! | 默认路径不存在                             | 返回 None，aiproxy 禁用，传统代理照常       |
//! | 显式 `--config-file` 但文件缺失/解析失败   | 致命错误（调用方以 anyhow 退出进程）        |
//! | 文件合法但个别条目无效                     | 跳过该条目并 WARN 指名 provider 与原因      |

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// 单个上游 provider 的运行期配置。
#[derive(Debug, Clone)]
pub struct ProviderConfig {
    pub id: String,
    pub base_url: String,
    /// 对上游做 SSE 流兼容归一化：请求侧检测/补齐 `stream` 字段，
    /// 响应侧对流式工具调用 `function.name` 做归一化。默认 false。
    /// 开启后：
    /// - 请求：若 body 为 JSON 对象且缺少 `stream`，显式注入 `"stream": false`
    ///   （规避把「缺 stream」当作默认流式的网关，如 kso）；
    /// - 响应：上游为 text/event-stream 时改写重复下发/片段续写的
    ///   `function.name` 为合规流。明文流经 `SseToolNormalizer` 原地改写；
    ///   gzip/deflate 压缩流（kso 无视 identity 协商恒发 gzip）经
    ///   `SseRewindStream` 解码→改写→以明文重发（剥 content-encoding 头）；
    ///   brotli 无法在 poll 内增量解码，退回压缩透传并 WARN。
    pub normalize_sse: bool,
}

/// MCP 上游请求头的 Origin 处理策略（设计 §5.5，防上游 DNS-rebinding 校验误杀）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OriginPolicy {
    /// 透传客户端 Origin（含无 Origin 时不造）。默认值。
    #[default]
    Keep,
    /// 剥掉 Origin。
    Strip,
    /// 改写为 server.url 的 origin。
    Upstream,
}

impl OriginPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            OriginPolicy::Keep => "keep",
            OriginPolicy::Strip => "strip",
            OriginPolicy::Upstream => "upstream",
        }
    }
}

/// 单个 MCP server 的运行期配置（设计 §5.1，本期仅 url + origin_policy 两键）。
#[derive(Debug, Clone)]
pub struct McpServerConfig {
    pub id: String,
    /// 上游完整端点（不含 query）；经 normalize_base_url 归一化。
    pub url: String,
    /// Origin 头策略；默认 keep。
    pub origin_policy: OriginPolicy,
}

/// provider + MCP server 清单；运行期只读共享。
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// 保持声明顺序，供错误信息与日志按配置顺序展示。
    providers: Vec<ProviderConfig>,
    index: HashMap<String, usize>,
    /// MCP server 清单（顺序敏感，供 404 列表与日志）。
    mcp_servers: Vec<McpServerConfig>,
    mcp_index: HashMap<String, usize>,
}

impl Config {
    /// provider 数量。
    pub fn len(&self) -> usize {
        self.providers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    /// 按 id 查找 provider。
    pub fn get(&self, id: &str) -> Option<&ProviderConfig> {
        self.index.get(id).map(|&i| &self.providers[i])
    }

    /// 全部 provider id（保持配置顺序），用于 404 错误信息。
    pub fn provider_ids(&self) -> Vec<&str> {
        self.providers.iter().map(|p| p.id.as_str()).collect()
    }

    /// 按 id 查找 MCP server。
    pub fn get_mcp(&self, id: &str) -> Option<&McpServerConfig> {
        self.mcp_index.get(id).map(|&i| &self.mcp_servers[i])
    }

    /// 全部 MCP server id（保持配置顺序），用于 404 错误信息与启动日志。
    pub fn mcp_server_ids(&self) -> Vec<&str> {
        self.mcp_servers.iter().map(|s| s.id.as_str()).collect()
    }

    /// MCP server 是否为空（无已配置 server）。
    pub fn mcp_is_empty(&self) -> bool {
        self.mcp_servers.is_empty()
    }

    /// 解析默认路径下的配置；不存在返回 None。
    pub fn load_default() -> Result<Option<Self>> {
        let path = default_config_path();
        if !path.exists() {
            return Ok(None);
        }
        Self::parse_file(&path).map(Some)
    }

    /// 显式加载：文件缺失/解析失败一律致命错误。
    pub fn load_explicit(path: &Path) -> Result<Self> {
        Self::parse_file(path)
    }
}

/// 默认配置路径：`$XDG_CONFIG_HOME/duct/config.yaml` 或 `~/.config/duct/config.yaml`。
pub fn default_config_path() -> PathBuf {
    if let Some(dir) = std::env::var("XDG_CONFIG_HOME")
        .ok()
        .filter(|d| !d.is_empty())
    {
        return PathBuf::from(dir).join("duct").join("config.yaml");
    }
    if let Some(home) = std::env::var("HOME").ok().filter(|h| !h.is_empty()) {
        return PathBuf::from(home)
            .join(".config")
            .join("duct")
            .join("config.yaml");
    }
    PathBuf::from("config.yaml")
}

/// provider id 合法性：`[a-z0-9][a-z0-9_-]*`（§6.1），大小写敏感。
fn is_valid_provider_id(id: &str) -> bool {
    let mut chars = id.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

/// 校验并归一化 base url：要求 http(s)，去除尾部 `/`，host 非空，
/// **禁止 query/fragment**（凭证不进配置文件，P5 一致性）。
fn normalize_base_url(raw: &str, id: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.contains('?') || trimmed.contains('#') {
        anyhow::bail!("{id}: url 不得包含 query 或 fragment（凭证不进配置文件）");
    }
    let (scheme, rest) = trimmed
        .split_once("://")
        .context(format!("provider '{id}': url 缺少 scheme"))?;
    if scheme != "http" && scheme != "https" {
        anyhow::bail!("provider '{id}': url 仅支持 http/https，实际为 '{scheme}'");
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.is_empty() {
        anyhow::bail!("provider '{id}': url 缺少 host");
    }
    Ok(trimmed.trim_end_matches('/').to_string())
}

/// 解析 YAML 文本为 provider + MCP server 清单；无效条目跳过并 WARN。
///
/// 以 `serde_yaml::Value` 中转而非直接反序列化到结构体，
/// 以获得重复 id 检测与逐条目错误定位能力。
/// 三层语义（§5.1）：`providers` 与 `mcp` **均变为可选段，但至少要有一个**；
/// 两者皆缺 → 文件级错误。
fn parse_str(content: &str, source: &Path) -> Result<Config> {
    let value: serde_yaml::Value = serde_yaml::from_str(content)
        .with_context(|| format!("解析配置失败: {}", source.display()))?;

    let has_providers = value.get("providers").is_some();
    let has_mcp = value.get("mcp").is_some();
    if !has_providers && !has_mcp {
        anyhow::bail!(
            "配置至少需要 'providers' 或 'mcp' 段之一（两者皆缺属文件级错误）: {}",
            source.display()
        );
    }

    let mut cfg = Config::default();
    if let Some(providers_value) = value.get("providers") {
        let (providers, index) = parse_providers(providers_value, source)?;
        cfg.providers = providers;
        cfg.index = index;
    }
    if let Some(mcp_value) = value.get("mcp") {
        let (mcp_servers, mcp_index) = parse_mcp_servers(mcp_value, source)?;
        cfg.mcp_servers = mcp_servers;
        cfg.mcp_index = mcp_index;
    }
    Ok(cfg)
}

/// 解析 `providers` 段。
fn parse_providers(
    providers_value: &serde_yaml::Value,
    source: &Path,
) -> Result<(Vec<ProviderConfig>, HashMap<String, usize>)> {
    let mapping = providers_value
        .as_mapping()
        .context("'providers' 段必须是映射（id → {url}）")?;

    let mut providers = Vec::new();
    let mut index = HashMap::new();
    let mut seen = HashSet::new();

    for (key, entry) in mapping {
        let Some(id) = key.as_str().map(|s| s.to_string()) else {
            tracing::warn!(file = %source.display(), "跳过非字符串 provider id: {key:?}");
            continue;
        };
        if !seen.insert(id.clone()) {
            // 防御分支：serde_yaml 已对字符串 key 静默去重（后者覆盖），
            // 此处理论不可达；若触发说明上游解析行为变化
            tracing::warn!(file = %source.display(), provider = %id, "检测到重复的 provider id 条目");
            continue;
        }
        if !is_valid_provider_id(&id) {
            tracing::warn!(file = %source.display(), provider = %id, "非法的 provider id（需匹配 [a-z0-9][a-z0-9_-]*），条目已跳过");
            continue;
        }
        let url = match entry.get("url").and_then(|v| v.as_str()) {
            Some(u) => u,
            None => {
                tracing::warn!(file = %source.display(), provider = %id, "缺少 url 字段，条目已跳过");
                continue;
            }
        };
        let base_url = match normalize_base_url(url, &id) {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(file = %source.display(), provider = %id, "{e:#}，条目已跳过");
                continue;
            }
        };
        let normalize_sse = entry
            .get("normalize_sse")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        index.insert(id.clone(), providers.len());
        providers.push(ProviderConfig {
            id,
            base_url,
            normalize_sse,
        });
    }

    Ok((providers, index))
}

/// 解析 `mcp` 段（`mcp.servers.<id>.{url, origin_policy}`）。
///
/// 未知键（如二期预留的 `transport`）静默忽略；个别条目非法跳过 + WARN，
/// 与 provider 条目互不影响。
fn parse_mcp_servers(
    mcp_value: &serde_yaml::Value,
    source: &Path,
) -> Result<(Vec<McpServerConfig>, HashMap<String, usize>)> {
    let mcp_mapping = mcp_value
        .as_mapping()
        .context("'mcp' 段必须是映射（servers: {id → {url}}）")?;

    let mut servers = Vec::new();
    let mut mcp_index = HashMap::new();
    let mut seen = HashSet::new();

    // `mcp:`（null）或 `mcp: {}` 均视为 0 个 server（合法）。
    if mcp_value.is_null() || mcp_mapping.is_empty() {
        return Ok((servers, mcp_index));
    }
    // mcp 段存在但无 servers —— 视为 0 个 server（合法）。
    let Some(servers_value) = mcp_value.get("servers") else {
        return Ok((servers, mcp_index));
    };
    let Some(servers_map) = servers_value.as_mapping() else {
        anyhow::bail!("'mcp.servers' 段必须是映射（id → {{url, origin_policy}}）");
    };

    for (key, entry) in servers_map {
        let Some(id) = key.as_str().map(|s| s.to_string()) else {
            tracing::warn!(file = %source.display(), "跳过非字符串 mcp server id: {key:?}");
            continue;
        };
        if !seen.insert(id.clone()) {
            tracing::warn!(file = %source.display(), server = %id, "检测到重复的 mcp server id 条目");
            continue;
        }
        if !is_valid_provider_id(&id) {
            tracing::warn!(file = %source.display(), server = %id, "非法的 mcp server id（需匹配 [a-z0-9][a-z0-9_-]*），条目已跳过");
            continue;
        }
        let url = match entry.get("url").and_then(|v| v.as_str()) {
            Some(u) => u,
            None => {
                tracing::warn!(file = %source.display(), server = %id, "缺少 url 字段，条目已跳过");
                continue;
            }
        };
        let base_url = match normalize_base_url(url, &id) {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(file = %source.display(), server = %id, "{e:#}，条目已跳过");
                continue;
            }
        };
        let origin_policy = entry
            .get("origin_policy")
            .and_then(|v| v.as_str())
            .map(parse_origin_policy)
            .unwrap_or_default();
        mcp_index.insert(id.clone(), servers.len());
        servers.push(McpServerConfig {
            id,
            url: base_url,
            origin_policy,
        });
    }

    Ok((servers, mcp_index))
}

/// 解析 origin_policy 字符串；未知值 WARN 并默认 keep。
fn parse_origin_policy(s: &str) -> OriginPolicy {
    match s {
        "keep" => OriginPolicy::Keep,
        "strip" => OriginPolicy::Strip,
        "upstream" => OriginPolicy::Upstream,
        other => {
            tracing::warn!(value = %other, "未知的 origin_policy 值（应为 keep|strip|upstream），默认 keep");
            OriginPolicy::Keep
        }
    }
}

impl Config {
    fn parse_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("读取配置失败: {}", path.display()))?;
        parse_str(&content, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(content: &str) -> Result<Config> {
        parse_str(content, Path::new("test.yaml"))
    }

    #[test]
    fn parse_basic_providers() {
        let cfg = parse(
            r#"
providers:
  openai:
    url: https://api.openai.com/v1/
  ollama:
    url: http://ollama:11434
"#,
        )
        .unwrap();
        assert_eq!(cfg.len(), 2);
        // 尾斜杠归一化
        assert_eq!(
            cfg.get("openai").unwrap().base_url,
            "https://api.openai.com/v1"
        );
        assert_eq!(cfg.get("ollama").unwrap().base_url, "http://ollama:11434");
        // 保持声明顺序
        assert_eq!(cfg.provider_ids(), vec!["openai", "ollama"]);
    }

    #[test]
    fn invalid_entries_skipped_with_remaining_intact() {
        let cfg = parse(
            r#"
providers:
  bad-scheme:
    url: ftp://example.com
  no-url:
    host: example.com
  BadCap:
    url: http://example.com
  good:
    url: https://ok.example.com
"#,
        )
        .unwrap();
        // 仅 good 存活；三类非法条目（scheme/id/缺字段）全部跳过
        assert_eq!(cfg.provider_ids(), vec!["good"]);
    }

    #[test]
    fn duplicate_id_is_file_level_parse_error() {
        // serde_yaml 对重复 key 报错：duplicate entry —— 属文件级损坏，
        // 归三层装载语义的第二层（致命），而非条目级跳过
        let err = parse(
            r#"
providers:
  dup:
    url: http://first:1
  dup:
    url: http://second:2
"#,
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("duplicate entry"), "{err:#}");
    }

    #[test]
    fn empty_providers_yields_empty_config() {
        let cfg = parse("providers: {}").unwrap();
        assert!(cfg.is_empty());
    }

    #[test]
    fn missing_providers_section_is_error() {
        assert!(parse("other: 1").is_err());
    }

    #[test]
    fn broken_yaml_is_error() {
        assert!(parse("providers: [unclosed").is_err());
    }

    #[test]
    fn provider_id_rules() {
        assert!(is_valid_provider_id("openai"));
        assert!(is_valid_provider_id("deep-seek_2"));
        assert!(!is_valid_provider_id(""));
        assert!(!is_valid_provider_id("-lead"));
        assert!(!is_valid_provider_id("_lead"));
        assert!(!is_valid_provider_id("HasCap"));
        assert!(!is_valid_provider_id("has space"));
    }

    #[test]
    fn normalize_rejects_bad_urls() {
        assert!(normalize_base_url("ftp://x.com", "p").is_err());
        assert!(normalize_base_url("http://", "p").is_err());
        assert!(normalize_base_url("not-a-url", "p").is_err());
        assert_eq!(
            normalize_base_url("  https://a.b/c/  ", "p").unwrap(),
            "https://a.b/c"
        );
    }

    #[test]
    fn normalize_sse_flag_default_false_and_parsed() {
        let cfg = parse(
            r#"
providers:
  plain:
    url: http://plain:1
  kso:
    url: http://kso:1
    normalize_sse: true
  off:
    url: http://off:1
    normalize_sse: false
"#,
        )
        .unwrap();
        // 未声明 → false；显式 true → true；显式 false → false
        assert!(!cfg.get("plain").unwrap().normalize_sse);
        assert!(cfg.get("kso").unwrap().normalize_sse);
        assert!(!cfg.get("off").unwrap().normalize_sse);
    }

    // ── MCP 段（设计 §5.1 / §8 C1–C6）─────────────────────────────────

    #[test]
    fn c1_pure_providers_old_file_still_loads() {
        // 纯 providers 旧文件（无 mcp 段）向后兼容
        let cfg = parse("providers:\n  p:\n    url: http://p:1\n").unwrap();
        assert_eq!(cfg.provider_ids(), vec!["p"]);
        assert!(cfg.mcp_is_empty());
        assert_eq!(cfg.mcp_server_ids(), Vec::<&str>::new());
    }

    #[test]
    fn c2_pure_mcp_file() {
        let cfg = parse(
            r#"
mcp:
  servers:
    github:
      url: https://api.githubcopilot.com/mcp
    filesystem:
      url: http://127.0.0.1:9100/mcp
      origin_policy: strip
"#,
        )
        .unwrap();
        assert_eq!(cfg.mcp_server_ids(), vec!["github", "filesystem"]);
        assert!(cfg.is_empty()); // providers 为空（is_empty 反映 providers）
        let g = cfg.get_mcp("github").unwrap();
        assert_eq!(g.url, "https://api.githubcopilot.com/mcp");
        assert_eq!(g.origin_policy, OriginPolicy::Keep);
        // 尾斜杠归一化
        let f = cfg.get_mcp("filesystem").unwrap();
        assert_eq!(f.url, "http://127.0.0.1:9100/mcp");
        assert_eq!(f.origin_policy, OriginPolicy::Strip);
    }

    #[test]
    fn c3_dual_sections_coexist() {
        let cfg = parse(
            r#"
providers:
  openai:
    url: https://api.openai.com/v1
mcp:
  servers:
    github:
      url: https://api.githubcopilot.com/mcp
"#,
        )
        .unwrap();
        assert_eq!(cfg.provider_ids(), vec!["openai"]);
        assert_eq!(cfg.mcp_server_ids(), vec!["github"]);
    }

    #[test]
    fn c4_both_missing_is_file_level_error() {
        // 两段皆缺 → 文件级错误（三层语义第二层，致命）
        assert!(parse("other: 1").is_err());
        // 空文件同样致命
        assert!(parse("").is_err());
    }

    #[test]
    fn c5_invalid_entries_skipped_others_survive() {
        let cfg = parse(
            r#"
mcp:
  servers:
    bad-scheme:
      url: ftp://example.com
    query-key:
      url: https://example.com/mcp?token=secret
    BadCap:
      url: http://example.com/mcp
    good:
      url: https://ok.example.com/mcp
"#,
        )
        .unwrap();
        // 仅 good 存活（非法 scheme / url 带 query / 非法 id 全部跳过）
        assert_eq!(cfg.mcp_server_ids(), vec!["good"]);
    }

    #[test]
    fn c6_mcp_defaults_and_unknown_keys_ignored() {
        let cfg = parse(
            r#"
mcp:
  servers:
    gh:
      url: https://api.githubcopilot.com/mcp
      transport: http_sse   # 二期预留键，本期未知键静默忽略
      origin_policy: bogus  # 非法值 → 默认 keep
"#,
        )
        .unwrap();
        let gh = cfg.get_mcp("gh").unwrap();
        assert_eq!(gh.origin_policy, OriginPolicy::Keep);
        assert_eq!(gh.url, "https://api.githubcopilot.com/mcp");
    }

    #[test]
    fn origin_policy_parsing() {
        assert_eq!(parse_origin_policy("keep"), OriginPolicy::Keep);
        assert_eq!(parse_origin_policy("strip"), OriginPolicy::Strip);
        assert_eq!(parse_origin_policy("upstream"), OriginPolicy::Upstream);
        assert_eq!(parse_origin_policy("bogus"), OriginPolicy::Keep);
    }
}
