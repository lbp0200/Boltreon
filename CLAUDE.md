# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

BoltDB is a disk-persistent Redis-compatible database written in Go, using BadgerDB for storage. It overcomes Redis's memory limitations by supporting up to 100TB of data on disk while maintaining Redis protocol compatibility.

## Common Commands

```bash
# Build and run development server
go run cmd/boltDB/main.go -dir=./data

# Build binary
go build -o boltDB cmd/boltDB/main.go

# Run tests
go test ./...

# Download dependencies
go mod tidy
```

## Architecture

```
cmd/boltDB/main.go    → Entry point with CLI args (-addr, -dir, -log-level)
cmd/sentinel/         → Sentinel instance for HA
internal/
  ├── server/          → Redis protocol command handler (SET, GET, HSET, etc.)
  ├── store/           → BadgerDB storage layer (String, List, Hash, Set, SortedSet)
  ├── cluster/         → Redis Cluster with 16384 slots, CRC-16/XModem hashing
  ├── replication/      → Master-slave replication, PSYNC, backlog
  ├── sentinel/         → Sentinel failover implementation
  ├── proto/           → RESP protocol parser/writer
  └── logger/          → zerolog structured logging
```

## Key Patterns

- **Storage**: BadgerDB with key prefixes (`string:key`, `list:key:*`, `hash:key`, `set:key`, `zset:key`)
- **Data Types**: Defined in `internal/store/define.go` (KeyTypeString, KeyTypeList, KeyTypeHash, KeyTypeSet, KeyTypeSortedSet)
- **Thread Safety**: Uses `sync.RWMutex` for shared state protection
- **Cluster**: 16384 slots with CRC-16/XModem hashing, supports hash tags `{tag}` for colocation
- **Replication**: PSYNC protocol with 1MB default backlog buffer

## Logging

Uses zerolog with configurable levels via `-log-level` flag or `BOLTDB_LOG_LEVEL` env variable (default: WARNING).

## Dependencies

Key imports: `github.com/dgraph-io/badger/v4`, `github.com/redis/go-redis/v9`, `github.com/rs/zerolog`.
