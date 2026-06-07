## Context

duct 当前认证凭据仅通过 CLI 参数 `--username` / `--password` 传入。作为 systemd 服务部署时，`ExecStart` 中的密码以 argv 形式暴露，`ps -ef` 可见。

clap 内置的 `env` 属性和 `requires` 验证可以完美解决此问题——zero additional code，纯声明式。

## Goals / Non-Goals

**Goals:**
- 支持通过环境变量 `DUCT_USER` / `DUCT_PASSWD` 传入认证凭据
- CLI 参数名从 `--username` / `--password` 改为 `--user` / `--passwd`
- 利用 clap 的 `requires` 替代手写配对验证
- 利用 clap 的 `env` 替代手写 `env::var()` fallback
- AuthConfig 构造简化为 `cli.user.zip(cli.passwd).map(...)`
- 伪装 re-exec 后环境变量自动继承（Rust 默认行为）

**Non-Goals:**
- 不引入新的依赖
- 不修改 auth.rs 模块
- 不修改认证检查逻辑（只改凭据来源）

## Decisions

### Decision 1: 变量名 `DUCT_USER` / `DUCT_PASSWD`

| 方案 | 与 CLI 一致性 | 简洁度 |
|------|:-:|:-:|
| `DUCT_USER` / `DUCT_PASSWD` | ✅ | ✅ |
| `DUCT_USERNAME` / `DUCT_PASSWORD` | ✅（旧名） | ❌ 稍长 |

`--user` / `--passwd` 与 `DUCT_USER` / `DUCT_PASSWD` 完全对齐。

### Decision 2: clap `env` + `requires` vs 手动实现

| 方案 | 代码量 | 维护成本 |
|------|:-:|:-:|
| clap 声明式（`env` + `requires`） | 0 行逻辑代码 | 最低 |
| 手动 `env::var()` fallback + 配对检查 | ~20 行 | 更高 |

clap 已内置此功能，不必重复造轮子。

### Decision 3: 优先级规则

CLI 显式传入 > 环境变量。行为由 clap 原生保证：
1. 若 CLI 显式传了 `--user` 或 `--passwd` → 覆盖环境变量
2. 若仅通过环境变量提供 → 从环境读取
3. 若两者都未提供 → `None`（无认证）
4. 若只提供其一 → clap 的 `requires` 直接报错退出

### Decision 4: 伪装 re-exec 的环境继承

`Command::new()` 默认继承当前进程的所有环境变量。所以伪装后 `DUCT_USER` / `DUCT_PASSWD` 自动保留在子进程中，无需额外代码。

## Risks / Trade-offs

- **[BREAKING] CLI 参数名变更**: `--username` → `--user`，`--password` → `--passwd`。但 auth 功能刚在上一提交引入，没有稳定用户群体，可接受。
- **[CLI 参数与 env 同时存在时的意图混淆]**: 如果用户同时传了 CLI 参数和环境变量，clap 默认 CLI 优先。这是预期行为，与 `env` 的标准语义一致。
- **[Windows 兼容性]**: 环境变量 + clap 的方案在 Windows 上同样有效，但 `--disguise` 的 `CommandExt::arg0` 是 Unix-only。当前不改，保持现状。
