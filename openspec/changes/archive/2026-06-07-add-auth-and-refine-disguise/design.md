## Context

duct 是一个轻量级 HTTP/HTTPS 代理服务，单二进制部署。当前不支持任何认证，且进程名伪装机制默认启用。

本次设计覆盖两项正交改动：
1. **HTTP Basic Proxy 认证** — 通过 CLI 参数传入单组凭据
2. **伪装机制调整为 opt-in** — `--disguise` 默认关闭，移除 `--no-disguise`

## Goals / Non-Goals

**Goals:**
- 支持通过 `--username` / `--password` CLI 参数启用 HTTP Basic 认证
- CONNECT 隧道和 HTTP 正向代理两个路径均受认证保护
- 无凭据或凭据错误时返回 407，并携带 `Proxy-Authenticate` header
- `--disguise` 改为可选，默认不伪装
- 移除 `--no-disguise` 和 `ALLOWED_NAMES` 常量列表
- 保持单二进制、零外部依赖（不使用 base64 crate）

**Non-Goals:**
- 不支持多用户（仅单组用户名/密码）
- 不支持配置文件
- 不支持 SOCKS5 协议
- 不支持认证凭据热加载
- 不支持 token 或其他认证方式

## Decisions

### Decision 1: CLI 参数 vs 配置文件

| 方案 | 复杂度 | 依赖 | 选择 |
|------|--------|------|------|
| CLI 参数 `--username`/`--password` | 低 | 无 | ✅ |
| TOML/YAML 配置文件 | 中 | serde + toml | ❌ 破坏极简设计 |
| 环境变量 | 中 | 无 | ❌ 不符合 CLI 直觉 |

**理由**: 保持极简，单组凭据不值得引入配置系统。`ps aux` 泄露密码的风险通过 `--password-file` 可在未来迭代中引入。

### Decision 2: base64 解码方式

Rust 标准库不包含 base64 解码。方案对比：

| 方案 | 复杂度 | 选择 |
|------|--------|------|
| 手写解码（~40 行） | 低 | ✅ 首选，零依赖 |
| `base64` crate | 低 | ❌ 为 ~40 行逻辑引入依赖不值得 |
| `data-encoding` crate | 低 | ❌ 同上 |

**理由**: Base64 解码逻辑简单且稳定（RFC 4648），手写实现不超过 40 行，不随时间变化，不值得引入外部依赖。

### Decision 3: 认证检查点位置

认证检查在 `handle_connection()` 中，在解析请求行之后、建立上游连接之前：

```
读请求行
  → 检查 auth 是否启用
  → 是: 读 headers → 找 Proxy-Authorization → 验证 → 通过则继续，否则 407
  → 否: 直接继续（现有行为）
```

对于 CONNECT 路径，headers 之前被直接丢弃，现在改为先读 headers 检查认证、再丢弃（不做透传）。

### Decision 4: `--disguise` 类型

```rust
#[arg(long)]
disguise: Option<String>,  // 不传 = 不启用
```

之前是 `String` 默认值 `"curl"`，现在是 `Option<String>`，不传即为 `None`。

### Decision 5: 移除 `ALLOWED_NAMES` 和自动检查逻辑

之前:

```
检查 argv[0] → 若不在 ALLOWED_NAMES 中 → re-exec 为默认 disguise
```

之后:

```
if let Some(name) = disguise { re-exec 为 name }
else { 正常启动 }
```

不再需要知道"哪些名字被允许"——用户说伪装就伪装，说用什么名字就用什么名字。

## Risks / Trade-offs

- **[CLI 密码泄露]** `--password` 明文参数会出现在 `ps aux` 和 shell history 中。可接受（与 curl、mysql 等工具行为一致），未来可选加 `--password-file`。
- **[单用户限制]** 不支持多用户。如果未来需要多用户，需要在 CLI 参数模式外引入配置系统。
- **[无 TLS 加密]** 密码通过 base64 而非加密传输（HTTP Basic 本身特性）。如果流量被中间人捕获，密码可被还原。这是 HTTP Basic 协议层面的限制，不是 duct 独有的问题。
- **[伪装不再自动]** 以前用户可能依赖默认伪装行为。这是一个 breaking change，需要用户在升级后显式加 `--disguise curl`。
