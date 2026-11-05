# duct 部署指南

将 duct 部署为 systemd 系统服务，实现 HTTP/HTTPS 网络代理后台常驻与开机自启。

## 前提

- Rust 工具链已安装
- systemd 可用（WSL2 需确认 systemd 已启用）
- `sudo` 权限

## 快速部署

### 1. 编译并安装

```bash
cargo build --release
sudo cp target/release/duct /usr/local/bin/
```

### 2. 配置代理认证（可选）

```bash
sudo mkdir -p /etc/duct
sudo tee /etc/duct/env << 'EOF'
DUCT_USER=proxyuser
DUCT_PASSWD=change-me-to-a-strong-password
EOF
sudo chmod 600 /etc/duct/env
sudo chown root:root /etc/duct/env
```

`/etc/duct/env` 明文保存密码，应限制为仅 root 可读。未设置认证时，只应绑定本机或可信网络。

### 3. 创建 systemd 服务单元

```bash
sudo tee /etc/systemd/system/duct.service << 'EOF'
[Unit]
Description=duct HTTP/HTTPS Proxy
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=-/etc/duct/env
ExecStart=/usr/local/bin/duct --bind 127.0.0.1 --port 11088
Restart=on-failure
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=full
ProtectHome=yes
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
MemoryDenyWriteExecute=yes

[Install]
WantedBy=multi-user.target
EOF
```

如确需向局域网提供代理，将 `--bind` 改为对应内网地址；监听所有接口（`0.0.0.0`）时务必启用 Basic 认证并配置防火墙。

### 4. 启动服务并验证

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now duct
sudo systemctl status duct

# HTTP CONNECT 隧道
curl -x http://127.0.0.1:11088 https://httpbin.org/get

# HTTP 正向代理
curl -x http://127.0.0.1:11088 http://httpbin.org/get

# 进程存活探测
curl http://127.0.0.1:11088/healthz
```

启用认证后，为 curl 增加 `--proxy-user proxyuser:password`。

## 日志与管理

```bash
sudo journalctl -u duct -f
sudo systemctl restart duct
sudo systemctl stop duct
sudo systemctl start duct
```

更改监听地址或端口后执行 `sudo systemctl daemon-reload && sudo systemctl restart duct`。

## WSL2 systemd 支持

在 `/etc/wsl.conf` 中启用：

```ini
[boot]
systemd=true
```

然后在 Windows PowerShell 中执行 `wsl --shutdown`，重新进入 WSL 后确认服务状态：

```bash
systemctl list-units --type=service --state=running | grep duct
```

## 故障排查

- 服务启动失败：`sudo journalctl -u duct -n 50 --no-pager`
- 端口冲突：`sudo ss -tlnp | grep 11088`
- 检查服务监听：`sudo systemctl status duct`
- 检查认证配置权限：`sudo ls -l /etc/duct/env`
