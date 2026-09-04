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

/// provider 清单；运行期只读共享。
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// 保持声明顺序，供错误信息与日志按配置顺序展示。
    providers: Vec<ProviderConfig>,
    index: HashMap<String, usize>,
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

/// 校验并归一化 base url：要求 http(s)，去除尾部 `/`，host 非空。
fn normalize_base_url(raw: &str, id: &str) -> Result<String> {
    let trimmed = raw.trim();
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

/// 解析 YAML 文本为 provider 清单；无效条目跳过并 WARN。
///
/// 以 `serde_yaml::Value` 中转而非直接反序列化到结构体，
/// 以获得重复 id 检测与逐条目错误定位能力。
fn parse_str(content: &str, source: &Path) -> Result<Config> {
    let value: serde_yaml::Value = serde_yaml::from_str(content)
        .with_context(|| format!("解析配置失败: {}", source.display()))?;

    let providers_value = value.get("providers").context("配置缺少 'providers' 段")?;
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

    Ok(Config { providers, index })
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
}
