# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BoltDB is a disk-persistent Redis-compatible database written in Go, using BadgerDB for storage. It overcomes Redis's memory limitations by supporting up to 100TB of data on disk while maintaining Redis 8 protocol compatibility.

## Common Commands

```bash
# Build and run development server
go run cmd/boltDB/main.go -dir=/tmp/bolt_db_data

# Build binary
go build -o ./build/boltDB cmd/boltDB/main.go

# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run specific package tests
go test -v ./internal/store/...
go test -v ./cmd/integration/...

# Run cluster tests
go test -v ./cmd/integration/... -run "Cluster"

# Download dependencies
go mod tidy
```

## Architecture

```
cmd/boltDB/main.go    → Entry point with CLI args (-addr, -dir, -log-level, -cluster)
cmd/sentinel/         → Sentinel instance for HA
cmd/integration/      → Integration tests (uses real server + go-redis client)
internal/
  ├── server/          → Redis protocol command handler (SET, GET, HSET, etc.)
  ├── store/           → BadgerDB storage layer (String, List, Hash, Set, SortedSet, TimeSeries, JSON)
  ├── cluster/         → Redis Cluster with 16384 slots, CRC-16/XModem hashing
  ├── replication/     → Master-slave replication, PSYNC, RDB transmission, backlog
  ├── sentinel/        → Sentinel failover implementation
  ├── proto/           → RESP protocol parser/writer
  └── logger/          → zerolog structured logging
```

## Key Patterns

- **Storage**: BadgerDB with key prefixes (`string:key`, `list:key:*`, `hash:key`, `set:key`, `zset:key`)
- **Data Types**: Defined in `internal/store/define.go` (KeyTypeString, KeyTypeList, KeyTypeHash, KeyTypeSet, KeyTypeSortedSet, KeyTypeTimeSeries, KeyTypeJSON)
- **Thread Safety**: Uses `sync.RWMutex` for shared state protection
- **Cluster**: 16384 slots with CRC-16/XModem hashing, supports hash tags `{tag}` for colocation, MOVED redirects
- **Replication**: PSYNC protocol with 1MB default backlog buffer, RDB snapshot generation

## Cluster Mode

BoltDB supports Redis Cluster mode with the `-cluster` flag. When enabled:
- Each node owns a subset of the 16384 slots
- Key distribution uses CRC-16/XModem hashing
- Hash tags `{tag}` ensure related keys stay on the same node
- MOVED redirects are returned for keys belonging to other nodes

### Supported Cluster Commands
- `CLUSTER INFO` - Cluster status information
- `CLUSTER NODES` - List all nodes in the cluster
- `CLUSTER SLOTS` - Get slot range allocations
- `CLUSTER MYID` - Get current node ID
- `CLUSTER KEYSLOT <key>` - Get slot number for a key
- `CLUSTER MEET <ip> <port>` - Add a node to the cluster
- `CLUSTER ADDSLOTS <slot>...` - Assign slots to current node
- `CLUSTER SETSLOT <slot> <IMPORTING|MIGRATING|STABLE|NODE> ...` - Slot migration
- `CLUSTER FORGET <nodeid>` - Remove a node
- `CLUSTER REPLICATE <nodeid>` - Make this node a replica

### Slot Redirect Behavior
- Commands return `MOVED <slot> <addr>` when key belongs to another node
- Single-key commands: SET, GET, DEL, EXISTS, TYPE, INCR, DECR, etc.
- Multi-key commands: MGET, MSET, DEL (multiple keys)

## Redis-Sentinel Compatibility

BoltDB supports being managed by redis-sentinel. Key compatibility features:

### Required Commands
- **ROLE**: Returns `["master", offset]` or `["slave", host, port, "connected", offset]`
- **INFO replication**: Returns complete replication status with `master_link_status`, `connected_slaves`, etc.
- **PING**: Returns `PONG`
- **REPLCONF**: Supports `LISTENING-PORT`, `CAPA`, `ACK`, `GETACK`
- **PSYNC**: Full PSYNC protocol with RDB snapshot transmission

### Replication Flow
1. Slave connects and sends `PSYNC ? -1`
2. Master responds with `+FULLRESYNC <replid> <offset>`
3. Master sends RDB snapshot (Bulk String format)
4. Master propagates write commands to slaves via backlog
5. Slave acknowledges with `REPLCONF ACK <offset>`

### Master-Slave Setup
```bash
# Start master on port 6379
./build/boltDB -dir=/tmp/master

# Start slave on port 6380
./build/boltDB -dir=/tmp/slave -replicaof 127.0.0.1 6379
```

## Testing

### Unit Tests (`internal/store/`)
- Test storage layer directly without network
- Use `t.TempDir()` for temporary databases
- Each test creates its own store instance

### Integration Tests (`cmd/integration/`)
- Test Redis protocol compatibility end-to-end
- Uses go-redis client to connect to real server
- Each test function calls `setupTestServer(t)` and `defer teardownTestServer(t)`
- Test structure:
  - `setupTestServer`: Creates temp DB, starts server on random port
  - `teardownTestServer`: Closes client, listener, and DB
- **Important**: TTL tests use `time.Duration` (nanoseconds), not `int64` seconds

### Test Commands

| Test Type | Location | Scope |
|-----------|----------|-------|
| Unit | `internal/cluster/*_test.go` | Cluster logic only |
| Unit | `internal/store/*_test.go` | Storage layer only |
| Integration | `cmd/integration/*_test.go` | Full protocol stack |
| Cluster Integration | `cmd/integration/cluster_test.go` | Cluster commands and redirects |

### Cluster Integration Tests
Run with: `go test -v ./cmd/integration/... -run "Cluster"`

Tests include:
- `TestClusterBasic` - Basic commands (INFO, NODES, SLOTS, MYID)
- `TestClusterKeySlot` - KEYSLOT calculation
- `TestClusterSlotRedirect` - MOVED redirect behavior
- `TestClusterMeet/AddSlots/SetSlot/Forget/Replicate` - Cluster management
- `TestClusterDataCommands` - Data operations in cluster mode
- `TestClusterHashTag` - Hash tag support

## Logging

Uses zerolog with configurable levels via `-log-level` flag or `BOLTDB_LOG_LEVEL` env variable (default: WARNING).

## Dependencies

Key imports: `github.com/dgraph-io/badger/v4`, `github.com/redis/go-redis/v9`, `github.com/rs/zerolog`.

## Important Notes

- 尽量兼容redis，如果实在做不到，在README文件中说明
- 尽量做全测试，GitHub Actions中只执行单元测试，集成测试放入`cmd/integration`中，包括所有支持的redis命令，主从模式、哨兵模式、集群模式。
- 编译生成的文件，都放入build文件夹，防止污染git
- When implementing Redis commands, return `int64` for counts (DEL, INCR, etc.)
- For TTL commands, `ExpiresAt()` returns `uint64` nanoseconds Unix timestamp
- go-redis client wraps TTL responses in `time.Duration` (multiply by precision)
- Use `#nosec G115` for int64 conversions that are bounded by practical limits

## Redis-Sentinel Setup

BoltDB can be monitored by redis-sentinel. Example configuration:

```bash
# sentinel.conf
port 26379
sentinel monitor mymaster 127.0.0.1 6379 2
sentinel down-after-milliseconds mymaster 30000
sentinel failover-timeout mymaster 180000
```

Start sentinel:
```bash
redis-server sentinel.conf --sentinel
```

BoltDB nodes should be started as:
```bash
# Master
./build/boltDB -addr=:6379 -dir=/tmp/master

# Slave
./build/boltDB -addr=:6380 -dir=/tmp/slave -replicaof 127.0.0.1 6379
```

Sentinel will monitor these nodes and perform automatic failover.

## Cluster Setup

### Single Node (Standalone Mode)
```bash
./build/boltDB -addr=:6379 -dir=/tmp/bolt1
```

### Cluster Mode (Single Node with All Slots)
```bash
./build/boltDB -cluster -addr=:6379 -dir=/tmp/bolt1
redis-cli -p 6379 CLUSTER INFO
redis-cli -p 6379 CLUSTER NODES
redis-cli -p 6379 CLUSTER KEYSLOT mykey
redis-cli -p 6379 SET testkey "value"
```

### Multi-Node Cluster (Manual Setup)
```bash
# Node 1 (owns slots 0-8191)
./build/boltDB -cluster -addr=:6379 -dir=/tmp/node1
redis-cli -p 6379 CLUSTER ADDSLOTS {0..8191}

# Node 2 (owns slots 8192-16383)
./build/boltDB -cluster -addr=:6380 -dir=/tmp/node2
redis-cli -p 6380 CLUSTER ADDSLOTS {8192..16383}

# Connect nodes
redis-cli -p 6380 CLUSTER MEET 127.0.0.1 6379
```

### Hash Tag Usage
```bash
# Keys with same hash tag stay on same slot
redis-cli -p 6379 SET "{user:1}:name" "John"
redis-cli -p 6379 SET "{user:1}:age" "30"
```
