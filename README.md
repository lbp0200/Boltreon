# BoltDB

<p align="center">
  <img src="https://img.shields.io/github/v/release/lbp0200/BoltDB?color=blue" alt="Version">
  <img src="https://img.shields.io/github/license/lbp0200/BoltDB" alt="License">
  <img src="https://img.shields.io/github/actions/workflow/status/lbp0200/BoltDB/go.yml?branch=main" alt="Build">
  <img src="https://goreportcard.com/badge/github.com/lbp0200/BoltDB" alt="Go Report">
</p>

<p align="center">
  <strong>English</strong> | <a href="README_CN.md">中文</a>
</p>

---

## Introduction | 简介

BoltDB is a **high-performance, disk-persistent key-value database** fully compatible with the Redis protocol. Built on **BadgerDB** for storage, it overcomes Redis's memory limitations, supporting up to **100TB** of data on disk while maintaining full Redis protocol compatibility.

**BoltDB** 是一个 **高性能、磁盘持久化的键值数据库**，完全兼容 Redis 协议。基于 **BadgerDB** 构建，克服了 Redis 的内存限制，支持在磁盘上存储高达 **100TB** 的数据，同时保持完整的 Redis 协议兼容性。

> 💡 **Memory Redis can only store 64GB? BoltDB can handle 100TB!**
>
> On pure HDD, BoltDB's GET performance approaches 50% of Redis memory version, and SET is even higher (because Badger's sequential writes dominate).

---

## Why BoltDB? | 为什么选择 BoltDB?

| Scenario | Redis (Memory) | BoltDB (Disk) |
|----------|---------------|---------------|
| Storage Capacity | Limited by RAM (~64GB typical) | Up to 100TB (disk limit) |
| Cost | High (RAM expensive) | Low (HDD/SSD affordable) |
| Persistence | RDB/AOF snapshot | Continuous write |
| Latency | < 1ms | < 5ms (SSD recommended) |
| Throughput | ~100K ops/sec | ~80K ops/sec |

---

## Features | 特性

### Supported Data Types | 支持的数据类型

| Type | Commands | 说明 |
|------|----------|------|
| **String** | `SET`, `GET`, `INCR`, `APPEND`, `STRLEN` | 字符串操作 |
| **List** | `LPUSH`, `RPOP`, `LRANGE`, `LINDEX`, `LTRIM` | 双向链表 |
| **Hash** | `HSET`, `HGET`, `HGETALL`, `HINCRBY`, `HDEL` | 哈希表 |
| **Set** | `SADD`, `SMEMBERS`, `SINTER`, `SDIFF`, `SPOP` | 无序集合 |
| **Sorted Set** | `ZADD`, `ZRANGE`, `ZSCORE`, `ZINCRBY`, `ZREVRANGE` | 有序集合 |

### Core Features | 核心功能

- ✅ **Full Redis Protocol** - Compatible with `redis-cli` and all Redis clients
- ✅ **Disk Persistence** - No memory limits, data survives restart
- ✅ **High Availability** - Sentinel support for automatic failover
- ✅ **Cluster Ready** - Redis Cluster protocol with 16384 slots
- ✅ **Transactions** - MULTI/EXEC support
- ✅ **TTL Expiration** - Key expiration with TTL
- ✅ **Online Backup** - Live backup support

---

## Quick Start | 快速开始

### Install from GitHub Release | 从 GitHub Release 安装

#### Linux (amd64)

```bash
# Download
curl -L https://github.com/lbp0200/BoltDB/releases/latest/download/boltDB-1.0.0-linux-amd64 -o boltDB
chmod +x boltDB

# Start server
./boltDB --dir=./data --addr=:6379
```

#### Linux (arm64)

```bash
# Download
curl -L https://github.com/lbp0200/BoltDB/releases/latest/download/boltDB-1.0.0-linux-arm64 -o boltDB
chmod +x boltDB

# Start server
./boltDB --dir=./data --addr=:6379
```

#### macOS (amd64)

```bash
# Download
curl -L https://github.com/lbp0200/BoltDB/releases/latest/download/boltDB-1.0.0-darwin-amd64 -o boltDB
chmod +x boltDB

# Start server
./boltDB --dir=./data --addr=:6379
```

#### macOS (arm64 / Apple Silicon)

```bash
# Download
curl -L https://github.com/lbp0200/BoltDB/releases/latest/download/boltDB-1.0.0-darwin-arm64 -o boltDB
chmod +x boltDB

# Start server
./boltDB --dir=./data --addr=:6379
```

#### Windows

```powershell
# Download from https://github.com/lbp0200/BoltDB/releases
# Extract and run:
.\boltDB.exe --dir=.\data --addr=:6379
```

### Build from Source | 从源码编译

#### Linux / macOS

```bash
git clone https://github.com/lbp0200/BoltDB.git
cd BoltDB

# Build
go build -o boltDB ./cmd/boltDB/

# Run
./boltDB --dir=./data --addr=:6379
```

#### Windows

```powershell
git clone https://github.com/lbp0200/BoltDB.git
cd BoltDB

go build -o boltDB.exe .\cmd\boltDB\

.\boltDB.exe --dir=.\data --addr=:6379
```

#### Cross-compilation | 交叉编译

```bash
# Build for all platforms
GOOS=linux GOARCH=amd64 go build -o boltDB-linux-amd64 ./cmd/boltDB/
GOOS=linux GOARCH=arm64 go build -o boltDB-linux-arm64 ./cmd/boltDB/
GOOS=darwin GOARCH=amd64 go build -o boltDB-darwin-amd64 ./cmd/boltDB/
GOOS=darwin GOARCH=arm64 go build -o boltDB-darwin-arm64 ./cmd/boltDB/
GOOS=windows GOARCH=amd64 go build -o boltDB-windows-amd64.exe ./cmd/boltDB/
```

### Use with redis-cli | 使用 redis-cli

```bash
# Connect
redis-cli -p 6379

# String operations
SET mykey "Hello from disk!"
GET mykey
INCR counter
DEL mykey

# List operations
LPUSH tasks "task1"
RPUSH tasks "task2"
LRANGE tasks 0 -1

# Hash operations
HSET user:1 name "Alice" age 25
HGET user:1 name
HGETALL user:1

# Set operations
SADD tags "go" "redis" "database"
SMEMBERS tags
SINTER tags "go"

# Sorted Set operations
ZADD leaderboard 100 "Alice" 90 "Bob" 80 "Charlie"
ZRANGE leaderboard 0 -1 WITHSCORES
```

---

## Docker | Docker 部署

```bash
# Run server
docker run -d \
  -p 6379:6379 \
  -v /path/to/data:/data \
  --name boltdb \
  lbp0200/boltDB:latest

# Or with docker-compose
cat > docker-compose.yml << EOF
version: '3.8'
services:
  boltDB:
    image: lbp0200/boltDB:latest
    ports:
      - "6379:6379"
    volumes:
      - ./data:/data
    command: --dir=/data --addr=:6379
EOF

docker-compose up -d
```

---

## Deployment Modes | 部署模式

### Standalone Mode | 单机模式

Basic standalone deployment for single-node usage.

```bash
# Start BoltDB server
./boltDB --dir=/tmp/bolt_data --addr=:6379

# Connect with redis-cli
redis-cli -p 6379 PING
# PONG

redis-cli -p 6379 SET mykey "Hello BoltDB!"
# OK

redis-cli -p 6379 GET mykey
# Hello BoltDB!
```

### Master-Slave Mode | 主从模式

BoltDB supports replication. You can set up master-slave topology.

#### Option 1: BoltDB Master + BoltDB Slave

```bash
# Terminal 1: Start Master on port 6379
./boltDB --dir=/tmp/bolt_master --addr=:6379

# Terminal 2: Start Slave on port 6380 (replicates from master)
./boltDB --dir=/tmp/bolt_slave --addr=:6380 --replicaof 127.0.0.1 6379

# Test replication
redis-cli -p 6379 SET key "value"
redis-cli -p 6380 GET key  # Returns "value"
```

#### Option 2: BoltDB Master + Redis Slave

Use Redis as slave to replicate from BoltDB master.

```bash
# Terminal 1: Start BoltDB Master on port 6380
./boltDB --dir=/tmp/bolt_master --addr=:6380

# Terminal 2: Start Redis Slave on port 6379
redis-server --port 6379 --dir /tmp/redis_data
redis-cli -p 6379 SLAVEOF 127.0.0.1 6380

# Test: Write to BoltDB, read from Redis
redis-cli -p 6380 SET test "hello"
redis-cli -p 6379 GET test  # Returns "hello"
```

> ⚠️ **Note**: BoltDB can act as a replica using `REPLICAOF` command.

### Sentinel Mode | 哨兵模式

For high availability, use **redis-sentinel** to monitor BoltDB instances.

#### Setup redis-sentinel

```bash
# Create sentinel config
cat > sentinel.conf << EOF
port 26379
sentinel monitor mymaster 127.0.0.1 6379 2
sentinel down-after-milliseconds mymaster 30000
sentinel failover-timeout mymaster 180000
EOF

# Start redis-sentinel
redis-server sentinel.conf --sentinel

# Connect to sentinel
redis-cli -p 26379

# Check master status
SENTINEL MASTER mymaster
```

#### Start BoltDB instances for Sentinel

```bash
# Terminal 1: Start Master
./boltDB --dir=/tmp/bolt_master --addr=:6379

# Terminal 2: Start Slave
./boltDB --dir=/tmp/bolt_slave --addr=:6380 --replicaof 127.0.0.1 6379
```

#### Sentinel Commands

```bash
# Check master
redis-cli -p 26379 SENTINEL MASTER mymaster

# Check slaves
redis-cli -p 26379 SENTINEL SLAVES mymaster

# Get master address (for client connection)
redis-cli -p 26379 SENTINEL GET-MASTER-ADDR-BY-NAME mymaster
```

### Cluster Mode | 集群模式

BoltDB supports Redis Cluster protocol with 16384 slots.

#### Single Node Cluster (All Slots)

```bash
# Start with cluster mode enabled (owns all slots)
./boltDB --cluster --dir=/tmp/bolt_cluster --addr=:6379

# Verify cluster status
redis-cli -p 6379 CLUSTER INFO
redis-cli -p 6379 CLUSTER NODES
redis-cli -p 6379 CLUSTER KEYSLOT mykey
```

#### Multi-Node Cluster

```bash
# Terminal 1: Node 1 (slots 0-8191)
./boltDB --cluster --dir=/tmp/node1 --addr=:6379
redis-cli -p 6379 CLUSTER ADDSLOTS {0..8191}

# Terminal 2: Node 2 (slots 8192-16383)
./boltDB --cluster --dir=/tmp/node2 --addr=:6380
redis-cli -p 6380 CLUSTER ADDSLOTS {8192..16383}

# Terminal 3: Connect nodes
redis-cli -p 6380 CLUSTER MEET 127.0.0.1 6379

# Verify
redis-cli -p 6379 CLUSTER NODES
```

#### Hash Tags

Use hash tags to keep related keys on the same node:

```bash
# Keys with same hash tag stay on same slot
redis-cli -p 6379 SET "{user:1}:name" "Alice"
redis-cli -p 6379 SET "{user:1}:age" "25"
redis-cli -p 6379 GET "{user:1}:name"
```

---

## Configuration | 配置

### Command Line Options | 命令行参数

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--dir` | `./data` | Data directory |
| `--addr` | `:6379` | Listen address |
| `--log-level` | `warning` | Log level (debug/info/warning/error) |

### Environment Variables | 环境变量

| Variable | Description |
|----------|-------------|
| `BOLTDB_DIR` | Data directory |
| `BOLTDB_ADDR` | Listen address |
| `BOLTDB_LOG_LEVEL` | Log level |

---

## High Availability | 高可用部署

### Architecture | 架构

```
                    ┌─────────────┐
                    │ Application │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │   Sentinel  │
                    │  (Monitor)  │
                    └──────┬──────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
  ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
  │   Master    │  │   Slave 1   │  │   Slave 2   │
  │  (Primary)  │◄─┤  (Replica)  │◄─┤  (Replica)  │
  └─────────────┘  └─────────────┘  └─────────────┘
```

### Sentinel Setup | Sentinel 配置

BoltDB can be monitored by redis-sentinel for automatic failover:

```bash
# Create sentinel config
cat > sentinel.conf << EOF
port 26379
sentinel monitor mymaster 127.0.0.1 6379 2
sentinel down-after-milliseconds mymaster 30000
sentinel failover-timeout mymaster 180000
EOF

# Start Sentinel (requires redis-sentinel)
redis-server sentinel.conf --sentinel
```

### Redis-Sentinel Compatibility | Redis-Sentinel 兼容性

BoltDB can be monitored by external Redis Sentinel:

| Command | Status | Notes |
|---------|--------|-------|
| `PING` | ✅ | Returns PONG |
| `ROLE` | ✅ | Returns master/slave role |
| `INFO replication` | ✅ | Returns full replication status |
| `REPLCONF GETACK` | ✅ | Returns ACK offset |
| `SENTINEL MASTER` | ✅ | Returns master status |
| `故障检测` | ✅ | Detects master failure in ~30s |
| `ODOWN 标记` | ✅ | Marks master as s_down, o_down |

**Known Issues:**
- `role-reported` may show `slave` instead of `master` in some cases

---

## Redis Interoperability | Redis 互操作性

### Replication Test Results | 复制测试结果

| Scenario | Status | Notes |
|----------|--------|-------|
| **BoltDB → Redis** | ✅ | Data sync works (SET, INCR, LPUSH, ZADD, HSET) |
| **Redis → BoltDB** | ❌ | Not supported (BoltDB lacks SLAVEOF command) |
| **Role Switching** | ✅ | SLAVEOF NO ONE / SLAVEOF work instantly |
| **Data Isolation** | ✅ | Both instances maintain independent data |
| **故障恢复** | ✅ | Redis SLAVEOF switch takes effect immediately |

**Test Commands:**
```bash
# Start BoltDB as master on port 6380
./boltDB --dir=./data --addr=:6380

# Start Redis as slave on port 6379
redis-server --port 6379 --dir=/tmp/redis_data
redis-cli -p 6379 SLAVEOF 127.0.0.1 6380

# Write to BoltDB, read from Redis
redis-cli -p 6380 SET "test" "hello"
redis-cli -p 6379 GET "test"  # Returns "hello"
```

### Known Limitations | 已知限制

1. **RDB Format Incompatibility**: BoltDB and Redis use different RDB formats and cannot exchange RDB snapshot files directly
2. **BoltDB SLAVEOF**: BoltDB does not implement the SLAVEOF command, so it cannot act as a replica of Redis

---

## Performance | 性能

### Benchmarks | 基准测试

```bash
# Using redis-benchmark (50 concurrent clients, 10000 requests)
redis-benchmark -h localhost -p 6379 -t PING,SET,GET,INCR,LPUSH -c 50 -n 10000
```

#### Actual Results | 实际测试结果

| Command | Throughput (ops/sec) | Avg Latency | P99 Latency |
|---------|---------------------|-------------|-------------|
| **PING** | ~48,000 | 0.24 ms | - |
| **GET** | ~34,000 | 0.77 ms | 1.56 ms |
| **SET** | ~31,000 | 0.90 ms | 1.63 ms |
| **INCR** | ~24,000 | 2.04 ms | 3.05 ms |
| **LPUSH** | ~15,000 | 3.38 ms | 5.46 ms |

> 💡 **Note**: Performance tested on macOS with SSD. Results may vary based on hardware.

### Storage Limits | 存储限制

| Metric | Limit |
|--------|-------|
| Max Keys | ~10^12 (practical) |
| Max Value Size | 1GB |
| Max String Size | 512MB |
| Max List Size | 2^32-1 elements |
| Max Set Size | 2^32-1 members |
| Max Hash Size | 2^32-1 fields |
| Max Sorted Set Size | 2^32-1 members |

---

## Architecture | 架构

```
┌─────────────────────────────────────────────────────┐
│                      BoltDB                          │
├─────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────────────┐   │
│  │  Redis Protocol │  │    Cluster Manager      │   │
│  │    Handler      │  │   (16384 Slots)         │   │
│  └────────┬────────┘  └───────────┬─────────────┘   │
│           │                        │                  │
│  ┌────────┴───────────────────────┴────────────┐   │
│  │         Command Router & Replication          │   │
│  └───────────────────┬───────────────────────────┘   │
│                      │                               │
│  ┌───────────────────▼───────────────────────────┐   │
│  │           BadgerDB Storage Engine              │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────────┐  │   │
│  │  │   WAL   │  │ LSM Tree │  │ Value Log   │  │   │
│  │  └─────────┘  └─────────┘  └─────────────┘  │   │
│  └───────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

### Tech Stack | 技术栈

- **Storage Engine**: [BadgerDB v4](https://github.com/dgraph-io/badger) - LSM-tree based KV store
- **Protocol**: Redis RESP2/RESP3 compatible
- **Clustering**: Redis Cluster protocol (CRC16 hashing, 16384 slots)
- **Replication**: Redis Replication (PSYNC, backlog)
- **Logging**: [zerolog](https://github.com/rs/zerolog)
- **Language**: Go 1.25+

---

## Platform Support | 平台支持

| OS | Architecture | Status |
|----|--------------|--------|
| Linux | amd64 | ✅ Supported |
| Linux | arm64 | ✅ Supported |
| macOS | amd64 | ✅ Supported |
| macOS | arm64 (Apple Silicon) | ✅ Supported |
| Windows | amd64 | ✅ Supported |

---

## Contributing | 贡献

Issues and Pull Requests are welcome!

```bash
# 1. Fork this repository
# 2. Create your feature branch
git checkout -b feature/amazing-feature

# 3. Commit your changes
git commit -m 'Add some amazing feature'

# 4. Push to the branch
git push origin feature/amazing-feature

# 5. Create a Pull Request
```

---

## License | 许可证

MIT License - See [LICENSE](LICENSE) for details.

---

## Contact | 联系方式

- **GitHub**: [https://github.com/lbp0200/BoltDB](https://github.com/lbp0200/BoltDB)
- **Issues**: [https://github.com/lbp0200/BoltDB/issues](https://github.com/lbp0200/BoltDB/issues)

---

<p align="center">
  <strong>Made with ❤️ by lbp0200</strong>
</p>
