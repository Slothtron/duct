# duct 部署指南

将 duct 部署为 systemd 系统服务，实现后台常驻运行与开机自启。

## 前提

- Rust 工具链已安装
- systemd 可用（WSL2 需确认 systemd 已启用）
- `sudo` 权限

## 快速部署

### 1. 编译

```bash
cargo build --release
```

### 2. 安装二进制

```bash
sudo cp target/release/duct /usr/local/bin/
```

### 3. 创建环境变量文件（存放凭据）

```bash
sudo mkdir -p /etc/duct
sudo tee /etc/duct/env << 'EOF'
DUCT_USER=proxyuser
DUCT_PASSWD=change-me-to-a-strong-password
EOF

# 限制权限——仅 root 可读
sudo chmod 600 /etc/duct/env
sudo chown root:root /etc/duct/env
```

> **安全说明**：`/etc/duct/env` 是明文存储密码的。`chmod 600` 确保仅 root 可读，防止普通用户通过 `ps aux` 或 `env` 泄露凭据。

### 4. 创建 systemd 服务单元

```bash
sudo tee /etc/systemd/system/duct.service << 'EOF'
[Unit]
Description=duct HTTP/HTTPS Proxy
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/duct/env
ExecStart=/usr/local/bin/duct \
    --bind 0.0.0.0 \
    --port 10999

# 注：凭据通过 EnvironmentFile 中的 DUCT_USER / DUCT_PASSWD 传入
# 不在 argv 中，ps -ef 不可见

# 安全加固
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=full
ProtectHome=yes
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
MemoryDenyWriteExecute=yes

# 进程名伪装（按需启用）
# 如果环境基于 argv[0] 过滤进程，去掉下面这行的注释：
# ExecStart=/usr/local/bin/duct --bind 0.0.0.0 --port 10999 --disguise curl

[Install]
WantedBy=multi-user.target
EOF
```

### 5. 加载、启用、启动

```bash
sudo systemctl daemon-reload
sudo systemctl enable duct
sudo systemctl start duct
```

### 6. 验证服务状态

```bash
sudo systemctl status duct

# 输出示例（健康）：
# ● duct.service - duct HTTP/HTTPS Proxy
#      Loaded: loaded (/etc/systemd/system/duct.service; enabled; vendor preset: enabled)
#      Active: active (running) since ...
#    Main PID: 12345 (duct)
#      Tasks: 1 (limit: 9527)
#      Memory: 1.2M
```

### 7. 验证代理功能

```bash
curl -x http://proxyuser:change-me-to-a-strong-password@127.0.0.1:10999 https://httpbin.org/get
```

## 日志管理

```bash
# 实时查看日志
sudo journalctl -u duct -f

# 最近 50 行
sudo journalctl -u duct -n 50 --no-pager

# 按时间范围过滤
sudo journalctl -u duct --since "5 minutes ago"
```

## 常用管理命令

```bash
# 启动
sudo systemctl start duct

# 停止
sudo systemctl stop duct

# 重启（二进制更新后）
sudo systemctl restart duct

# 查看状态
sudo systemctl status duct

# 开机自启
sudo systemctl enable duct

# 关闭开机自启
sudo systemctl disable duct
```

## 端口配置

修改 `/etc/systemd/system/duct.service` 中的 `--port` 参数，然后重启：

```bash
sudo systemctl daemon-reload
sudo systemctl restart duct
```

如果端口小于 1024（如 `--port 80` 或 `--port 443`），需要授予 `CAP_NET_BIND_SERVICE` 能力或使用 root。推荐使用大于 1024 的端口（默认 10999）。

## 进程名伪装

如果部署环境会基于 **发起连接的进程名（argv[0]）** 进行访问控制，需要启用伪装：

1. 编辑 `/etc/systemd/system/duct.service`
2. 注释掉原有的 `ExecStart` 行
3. 取消注释带有 `--disguise curl` 的 `ExecStart` 行
4. 重启服务：

```bash
sudo systemctl daemon-reload
sudo systemctl restart duct
```

## 故障排查

### 服务启动失败

```bash
# 查看详细错误
sudo journalctl -u duct -n 20 --no-pager

# 确认二进制存在且可执行
ls -l /usr/local/bin/duct

# 确认环境变量文件格式正确（无多余引号/空格）
sudo cat /etc/duct/env
```

### 端口被占用

```bash
# 检查端口占用
sudo ss -tlnp | grep 10999

# 更换端口后在服务文件中修改 --port 参数
```

### 连接被拒绝

```bash
# 确认服务正在运行
sudo systemctl status duct

# 确认防火墙未拦截
sudo iptables -L -n | grep 10999
```

### 认证失败

```bash
# 确认环境变量文件内容正确
sudo cat /etc/duct/env

# 重启服务加载新凭据
sudo systemctl restart duct
```

## WSL2 systemd 支持

WSL2 默认未启用 systemd。需在 `/etc/wsl.conf` 中启用：

```ini
# /etc/wsl.conf
[boot]
systemd=true
```

然后在 Windows PowerShell 中重启 WSL：

```powershell
wsl --shutdown
```

重新进入 WSL 后检查：

```bash
systemctl list-units --type=service --state=running | grep duct
```