# Boltreon

Boltreon — Redis Protocol, Badger Persistence, Born for Disk.
真正的“硬盘版 Redis”，内存只能存 64GB？我们能吃 100TB。

在纯 HDD 上，Boltreon 的 GET 性能已接近 Redis 内存版 50%，SET 甚至更高（因为 Badger 顺序写碾压）

A disk-persistent Redis-compatible database in Go, powered by Badger for storage. Supports clustering for high availability.

## Features
- **Full Redis Protocol**: Compatible with redis-cli, supports SET/GET/DEL + more.
- **Disk-Backed**: Uses BadgerDB for durable, high-throughput KV storage (no memory limits!).
- **Clustering**: Slot-based sharding, node discovery (WIP).
- **Go Native**: High perf, low deps.

## Quick Start
```bash
git clone https://github.com/lbp0200/Boltreon
cd Boltreon
go mod tidy
go run cmd/boltreon/main.go -dir=./data
# In another terminal:
redis-cli -p 6379 SET hello "world from disk!"
redis-cli -p 6379 GET hello