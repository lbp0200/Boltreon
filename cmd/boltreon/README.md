# Boltreon 服务器运行指南

## 快速开始

### 方式1：直接运行（开发模式）

```bash
# 使用默认配置（监听 :6379，数据库存储在临时目录）
go run cmd/boltreon/main.go

# 指定监听地址
go run cmd/boltreon/main.go -addr :6380

# 指定数据库目录
go run cmd/boltreon/main.go -dir /path/to/data

# 同时指定地址和目录
go run cmd/boltreon/main.go -addr :6380 -dir /path/to/data
```

### 方式2：构建后运行（生产模式）

```bash
# 构建可执行文件
go build -o boltreon cmd/boltreon/main.go

# 运行
./boltreon

# 指定参数运行
./boltreon -addr :6380 -dir /path/to/data
```

### 方式3：安装到系统路径

```bash
# 安装到 $GOPATH/bin 或 $GOBIN
go install github.com/lbp0200/Boltreon/cmd/boltreon

# 然后直接运行（如果 $GOPATH/bin 在 PATH 中）
boltreon -addr :6380 -dir /path/to/data
```

## 命令行参数

- `-addr`: 监听地址（默认: `:6379`）
  - 示例: `-addr :6379` 或 `-addr 0.0.0.0:6380`
  
- `-dir`: BadgerDB 数据存储目录（默认: 系统临时目录）
  - 示例: `-dir /var/lib/boltreon` 或 `-dir ./data`

## 使用示例

### 1. 启动服务器（默认配置）

```bash
go run cmd/boltreon/main.go
```

输出：
```
Boltreon listening on :6379
```

### 2. 使用自定义端口和目录

```bash
go run cmd/boltreon/main.go -addr :6380 -dir ./boltreon-data
```

### 3. 使用 redis-cli 连接测试

在另一个终端：

```bash
# 连接到默认端口
redis-cli -p 6379

# 或连接到自定义端口
redis-cli -p 6380

# 测试命令
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET key1 value1
OK
127.0.0.1:6379> GET key1
"value1"
127.0.0.1:6379> HSET user:1 name Alice age 30
(integer) 2
127.0.0.1:6379> HGETALL user:1
1) "name"
2) "Alice"
3) "age"
4) "30"
```

### 4. 后台运行（Linux/macOS）

```bash
# 使用 nohup
nohup ./boltreon -addr :6379 -dir /var/lib/boltreon > boltreon.log 2>&1 &

# 或使用 systemd（创建服务文件）
```

### 5. 使用 systemd 管理（Linux）

创建 `/etc/systemd/system/boltreon.service`:

```ini
[Unit]
Description=Boltreon Redis Server
After=network.target

[Service]
Type=simple
User=boltreon
ExecStart=/usr/local/bin/boltreon -addr :6379 -dir /var/lib/boltreon
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

然后：

```bash
sudo systemctl daemon-reload
sudo systemctl enable boltreon
sudo systemctl start boltreon
sudo systemctl status boltreon
```

## 环境变量（可选扩展）

如果需要支持环境变量，可以修改 `main.go` 添加：

```go
import "os"

func main() {
    addr := flag.String("addr", getEnv("BOLTREON_ADDR", ":6379"), "listen addr")
    dbPath := flag.String("dir", getEnv("BOLTREON_DIR", os.TempDir()), "badger dir")
    // ...
}

func getEnv(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}
```

## 故障排查

### 1. 端口被占用

```bash
# 检查端口占用
lsof -i :6379
# 或
netstat -an | grep 6379

# 使用其他端口
go run cmd/boltreon/main.go -addr :6380
```

### 2. 权限问题

```bash
# 确保有写入数据库目录的权限
mkdir -p /var/lib/boltreon
chmod 755 /var/lib/boltreon
```

### 3. 依赖问题

```bash
# 确保所有依赖已安装
go mod download
go mod tidy
```

## 性能优化

### 1. 使用持久化目录

避免使用临时目录，使用持久化存储：

```bash
./boltreon -dir /var/lib/boltreon
```

### 2. 生产环境建议

- 使用固定端口（如 6379）
- 使用持久化数据目录
- 配置日志记录
- 使用进程管理器（systemd, supervisor等）
- 配置防火墙规则

## 停止服务器

- 前台运行：按 `Ctrl+C`
- 后台运行：使用 `kill` 命令或 systemd 管理

```bash
# 查找进程
ps aux | grep boltreon

# 停止进程
kill <PID>

# 或使用 systemd
sudo systemctl stop boltreon
```

