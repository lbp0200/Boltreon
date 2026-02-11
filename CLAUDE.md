# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BoltDB is a disk-persistent Redis-compatible database written in Go, using BadgerDB for storage. It overcomes Redis's memory limitations by supporting up to 100TB of data on disk while maintaining Redis 8 protocol compatibility.

## Common Commands

```bash
# Build and run development server
go run cmd/boltDB/main.go -dir=./data

# Build binary
go build -o boltDB cmd/boltDB/main.go

# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run specific package tests
go test -v ./internal/store/...
go test -v ./cmd/integration/...

# Download dependencies
go mod tidy
```

## Architecture

```
cmd/boltDB/main.go    → Entry point with CLI args (-addr, -dir, -log-level)
cmd/sentinel/         → Sentinel instance for HA
cmd/integration/      → Integration tests (uses real server + go-redis client)
internal/
  ├── server/          → Redis protocol command handler (SET, GET, HSET, etc.)
  ├── store/           → BadgerDB storage layer (String, List, Hash, Set, SortedSet)
  ├── cluster/         → Redis Cluster with 16384 slots, CRC-16/XModem hashing
  ├── replication/     → Master-slave replication, PSYNC, backlog
  ├── sentinel/        → Sentinel failover implementation
  ├── proto/           → RESP protocol parser/writer
  └── logger/          → zerolog structured logging
```

## Key Patterns

- **Storage**: BadgerDB with key prefixes (`string:key`, `list:key:*`, `hash:key`, `set:key`, `zset:key`)
- **Data Types**: Defined in `internal/store/define.go` (KeyTypeString, KeyTypeList, KeyTypeHash, KeyTypeSet, KeyTypeSortedSet)
- **Thread Safety**: Uses `sync.RWMutex` for shared state protection
- **Cluster**: 16384 slots with CRC-16/XModem hashing, supports hash tags `{tag}` for colocation
- **Replication**: PSYNC protocol with 1MB default backlog buffer

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
| Unit | `internal/store/*_test.go` | Storage layer only |
| Integration | `cmd/integration/*_test.go` | Full protocol stack |

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
