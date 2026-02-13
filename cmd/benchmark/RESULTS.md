# BoltDB Benchmark Results

## 测试环境
- **平台**: macOS Darwin 24.6.0 x86_64
- **Go版本**: 1.25.7
- **Redis版本**: 8.2.1
- **存储引擎**: BadgerDB v4
- **测试日期**: 2026-02-12

## 测试方法
使用 `redis-benchmark` 对 BoltDB 进行压力测试:
```bash
redis-benchmark -h 127.0.0.1 -p 6388 -t <command> -n <requests> -c <clients>
```

## 测试结果

### 基础命令性能

| 命令 | 吞吐量 (ops/sec) | P50 延迟 (msec) | 备注 |
|------|------------------|-----------------|------|
| **PING** | ~48,000 | 0.24 | 基本延迟测试 |
| **SET** | ~31,000 | 0.39 | 写入性能 |
| **GET** | ~46,000 | 0.25 | 读取性能 |

### 命令说明
- **PING**: 基本连接延迟测试
- **SET**: 字符串写入操作
- **GET**: 字符串读取操作

## 性能分析

### 优势
1. **高并发读取**: GET 操作可达 46,000+ ops/sec
2. **低延迟**: PING 延迟约 0.24ms
3. **稳定的写入**: SET 操作约 31,000 ops/sec

### 待优化
1. **事务冲突**: INCR 等计数器操作在高并发下可能出现 "Transaction Conflict" 错误
2. **写入性能**: 相比纯内存 Redis，写入性能有差距（BadgerDB 磁盘持久化开销）
3. **复合命令**: LPUSH、HSET、ZADD 等复合数据结构操作需要进一步优化

## 与 Redis 对比（参考值）

| 命令 | BoltDB (ops/sec) | Redis 8 (ops/sec) | 差距 |
|------|-------------------|-------------------|------|
| PING | ~48,000 | ~80,000 | ~40% |
| SET | ~31,000 | ~55,000 | ~44% |
| GET | ~46,000 | ~70,000 | ~34% |

**说明**: BoltDB 使用磁盘持久化（BadgerDB），性能差距主要来自:
1. 磁盘 I/O 开销
2. LSM-tree 压缩
3. 写放大（Write Amplification）

## 运行测试

```bash
# 1. 编译 BoltDB
go build -o ./build/boltDB ./cmd/boltDB/main.go

# 2. 启动 BoltDB
./build/boltDB -addr=:6388 -dir=/tmp/bolt_test

# 3. 运行基准测试
redis-benchmark -h 127.0.0.1 -p 6388 -t PING,SET,GET -n 10000 -c 50

# 4. 清理
pkill -9 boltDB
```

## 已知问题

~~1. **Transaction Conflict**: INCR、LPUSH 等命令在高并发下可能返回 "Transaction Conflict" 错误~~
   - ~~原因: 事务处理逻辑不够完善~~
   - **状态: 已修复** ✅

修复方法:
- 在 `internal/store/string.go` 中为 INCR、INCRBY 添加重试机制
- 在 `internal/store/list.go` 中为 LPUSH、RPUSH 添加重试机制
- 使用指数退避 + 随机抖动避免雷群效应

2. **缓存并发问题**: LRU 缓存更新存在竞态条件
   - 状态: 已修复

## 结论

BoltDB 在提供磁盘持久化的同时，仍能保持较高的吞吐量。对于读多写少的场景，性能表现良好。写入性能受限于磁盘 I/O，建议在 SSD 上运行以获得更好性能。

后续优化方向:
1. 完善事务处理逻辑
2. 优化写入路径
3. 增加缓存命中率
4. 支持更多数据结构的高效操作
