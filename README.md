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

### Install from Binary | 从二进制安装

#### Linux/macOS

```bash
# Download (replace VERSION and PLATFORM)
curl -L https://github.com/lbp0200/BoltDB/releases/download/vVERSION/boltDB-VERSION-linux-amd64.tar.gz -o boltDB.tar.gz
tar -xzf boltDB.tar.gz
cd boltDB-VERSION-*/

# Start server
./boltDB --dir=./data --addr=:6379
```

#### Windows

```powershell
# Download zip from releases page
# Extract and run:
.\boltDB.exe --dir=.\data --addr=:6379
```

### Build from Source | 从源码编译

```bash
git clone https://github.com/lbp0200/BoltDB.git
cd BoltDB

# Build main binary
go build -o boltDB ./cmd/boltDB/

# Build sentinel (optional, for HA)
go build -o boltDB-sentinel ./cmd/sentinel/

# Run
./boltDB --dir=./data --addr=:6379
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

```bash
# Start Sentinel
./boltDB-sentinel --dir=./sentinel --addr=:26379

# In redis-cli (Sentinel port)
redis-cli -p 26379
SENTINEL monitor mymaster 127.0.0.1 6379 2
SENTINEL down-after-milliseconds mymaster 30000
SENTINEL failover-timeout mymaster 180000
SENTINEL parallel-syncs mymaster 1
```

---

## Performance | 性能

### Benchmarks | 基准测试

```bash
# Using redis-benchmark
redis-benchmark -h localhost -p 6379 -t set,get,incr,lpush,hset,zadd -c 50 -n 100000

# Expected results (SSD, 8-core CPU)
# SET:      ~80,000 ops/sec
# GET:      ~90,000 ops/sec
# INCR:     ~75,000 ops/sec
# LPUSH:    ~70,000 ops/sec
# HSET:     ~65,000 ops/sec
# ZADD:     ~60,000 ops/sec
```

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
