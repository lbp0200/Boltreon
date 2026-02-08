# Redis Benchmark 压力测试指南

本文档介绍如何使用 `redis-benchmark` 工具对 BoltDB 服务器进行压力测试。

## 前置条件

1. **安装 Redis**（用于获取 `redis-benchmark` 工具）
   ```bash
   # macOS
   brew install redis
   
   # Linux (Ubuntu/Debian)
   sudo apt-get install redis-tools
   
   # 或者从 Redis 源码编译
   git clone https://github.com/redis/redis.git
   cd redis
   make
   # redis-benchmark 位于 src/ 目录
   ```

2. **启动 BoltDB 服务器**
   ```bash
   cd /Users/lbp/IdeaProjects/BoltDB
   go run cmd/boltreon/main.go -addr=:6379 -dir=./data
   go run cmd/boltreon/main.go -addr :6379 -log-level DEBUG
   ```

3. **测试连接**（可选）
   ```bash
   # 使用测试脚本
   cd cmd/boltreon
   ./test_connection.sh
   
   # 或手动测试
   redis-cli -h 127.0.0.1 -p 6379 PING
   redis-cli -h 127.0.0.1 -p 6379 CONFIG GET "*"
   ```

## 基本使用

### 1. 快速测试（默认配置）

```bash
redis-benchmark -h 127.0.0.1 -p 6379
```

这会运行默认的测试套件，包括：
- 100,000 个请求
- 50 个并发客户端
- 测试 SET、GET、INCR、LPUSH、RPUSH、LPOP、RPOP、SADD、SPOP、LPUSH、LRANGE 等命令

### 2. 指定测试参数

```bash
# 测试 10,000 个请求，100 个并发客户端
redis-benchmark -h 127.0.0.1 -p 6379 -n 10000 -c 100

# 只测试 SET 命令
redis-benchmark -h 127.0.0.1 -p 6379 -t set

# 只测试 GET 命令
redis-benchmark -h 127.0.0.1 -p 6379 -t get

# 测试多个命令
redis-benchmark -h 127.0.0.1 -p 6379 -t set,get,incr
```

### 3. 常用参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-h` | 服务器主机地址 | 127.0.0.1 |
| `-p` | 服务器端口 | 6379 |
| `-n` | 总请求数 | 100000 |
| `-c` | 并发客户端数 | 50 |
| `-t` | 要测试的命令（逗号分隔） | 所有命令 |
| `-d` | SET/GET 值的数据大小（字节） | 3 |
| `-k` | 是否使用 keepalive | 1 (是) |
| `-r` | 使用随机键，而不是固定键 | 1000000 |
| `-P` | 使用管道（pipeline），每次发送的请求数 | 1 |
| `-q` | 安静模式，只显示 QPS | 否 |
| `--csv` | 以 CSV 格式输出 | 否 |
| `-l` | 循环模式，持续运行 | 否 |

## 测试场景示例

### 场景 1: 基础性能测试

```bash
# 测试 SET 和 GET 命令，100,000 个请求，50 个并发
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -t set,get
```

### 场景 2: 高并发测试

```bash
# 测试高并发场景，1,000 个并发客户端
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 1000 -t set,get
```

### 场景 3: 大数据量测试

```bash
# 测试大值（1KB）的 SET/GET
redis-benchmark -h 127.0.0.1 -p 6379 -n 10000 -c 50 -t set,get -d 1024
```

### 场景 4: Pipeline 测试

```bash
# 使用 pipeline，每次发送 10 个请求
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -P 10 -t set,get
```

### 场景 5: 特定数据类型测试

```bash
# 测试 Hash 命令
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -t hset,hget,hgetall

# 测试 List 命令
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -t lpush,rpush,lpop,rpop

# 测试 Set 命令
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -t sadd,spop,smembers

# 测试 Sorted Set 命令
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -t zadd,zrange,zrank
```

### 场景 6: 持续压力测试

```bash
# 循环模式，持续运行直到手动停止
redis-benchmark -h 127.0.0.1 -p 6379 -l -t set,get
```

### 场景 7: 随机键测试

```bash
# 使用随机键，避免缓存影响
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -r 1000000 -t set,get
```

## 输出结果解读

### 标准输出示例

```
====== SET ======
  100000 requests completed in 1.23 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1
  host configuration "save": 
  host configuration "appendonly": no
  multi-thread: no

0.00% <= 0.1 milliseconds
50.00% <= 0.2 milliseconds
95.00% <= 0.3 milliseconds
99.00% <= 0.4 milliseconds
100.00% <= 0.5 milliseconds
81300.81 requests per second
```

### 关键指标说明

- **requests completed**: 完成的请求总数
- **parallel clients**: 并发客户端数
- **bytes payload**: 每个请求的数据大小
- **百分比延迟**: 不同百分位的响应时间
- **requests per second**: 每秒处理的请求数（QPS）

## 性能优化建议

### 1. 调整服务器参数

在启动 BoltDB 时，可以调整 BadgerDB 的参数（需要修改代码）：

```go
// 在 internal/store/define.go 中调整 BadgerDB 选项
opts := badger.DefaultOptions(dbPath)
opts.ValueLogFileSize = 1024 * 1024 * 1024 // 1GB
opts.MaxTableSize = 64 * 1024 * 1024       // 64MB
opts.NumCompactors = 4                     // 压缩器数量
opts.NumMemtables = 5                      // Memtable 数量
```

### 2. 系统优化

```bash
# 增加文件描述符限制
ulimit -n 65535

# 调整内核参数（Linux）
sudo sysctl -w vm.overcommit_memory=1
sudo sysctl -w net.core.somaxconn=1024
```

### 3. 测试环境隔离

- 在独立的机器上运行测试，避免影响其他服务
- 关闭不必要的后台程序
- 使用 SSD 存储以获得更好的性能

## 对比测试

### 与 Redis 对比

```bash
# 测试 Redis（如果已安装）
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -t set,get > redis_results.txt

# 测试 BoltDB
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -t set,get > boltreon_results.txt

# 对比结果
diff redis_results.txt boltreon_results.txt
```

## 自动化测试脚本

创建一个测试脚本 `benchmark.sh`：

```bash
#!/bin/bash

# 启动 BoltDB 服务器（后台运行）
go run cmd/boltreon/main.go -addr=:6379 -dir=./data &
SERVER_PID=$!

# 等待服务器启动
sleep 2

# 运行测试
echo "=== SET/GET 测试 ==="
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -t set,get

echo "=== Hash 测试 ==="
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -t hset,hget

echo "=== List 测试 ==="
redis-benchmark -h 127.0.0.1 -p 6379 -n 100000 -c 50 -t lpush,rpush

# 清理
kill $SERVER_PID
```

## 注意事项

1. **数据持久化**: BoltDB 使用 BadgerDB 进行持久化，写入性能可能低于纯内存的 Redis
2. **首次运行**: 第一次运行可能较慢，因为需要初始化数据库
3. **磁盘 I/O**: 确保有足够的磁盘空间和良好的 I/O 性能
4. **内存使用**: BadgerDB 会使用内存缓存，注意监控内存使用情况
5. **并发安全**: BoltDB 使用 BadgerDB 的事务机制，确保并发安全

## 故障排查

### 连接失败

如果遇到 "Server closed the connection" 错误，请按以下步骤排查：

#### 1. 检查服务器是否运行

```bash
# 检查端口是否监听
netstat -an | grep 6379
# 或
lsof -i :6379
```

#### 2. 测试基本连接

```bash
# 使用 redis-cli 测试
redis-cli -h 127.0.0.1 -p 6379 PING
# 应该返回: PONG

# 测试 CONFIG GET
redis-cli -h 127.0.0.1 -p 6379 CONFIG GET "*"
# 应该返回配置数组
```

#### 3. 使用调试客户端

```bash
# 使用内置的调试客户端测试
cd cmd/boltreon
go run debug_client.go CONFIG GET "*"
go run debug_client.go PING
go run debug_client.go SET test test
go run debug_client.go GET test
```

#### 4. 使用测试脚本模拟 redis-benchmark

```bash
# 使用测试脚本模拟 redis-benchmark 的行为
cd cmd/boltreon
./test_benchmark.sh
```

这个脚本会测试：
- `CONFIG GET *` 命令
- `PING` 命令
- 连续发送多个命令（模拟 redis-benchmark 的行为）

#### 5. 检查服务器日志

启动服务器时查看是否有错误信息：
```bash
go run cmd/boltreon/main.go -addr :6379 -dir ./data
```

#### 5. 检查防火墙设置

```bash
# macOS
sudo pfctl -sr | grep 6379

# Linux
sudo iptables -L | grep 6379
```

### 性能异常

1. 检查磁盘 I/O: `iostat -x 1`
2. 检查 CPU 使用: `top` 或 `htop`
3. 检查内存使用: `free -h` (Linux) 或 `vm_stat` (macOS)
4. 检查网络延迟: `ping 127.0.0.1`

## 参考资源

- [Redis Benchmark 官方文档](https://redis.io/docs/management/optimization/benchmarks/)
- [BadgerDB 性能调优](https://dgraph.io/docs/badger/get-started/#performance-tuning)

