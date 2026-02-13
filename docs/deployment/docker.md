# Deploy BoltDB with Docker

## Quick Reference

| File | Description |
|------|-------------|
| `deploy/docker/Dockerfile` | Multi-stage build for BoltDB |
| `deploy/docker/docker-compose.yml` | Main compose file with all examples |
| `deploy/docker/docker-compose.standalone.yml` | Standalone mode |
| `deploy/docker/docker-compose.master-slave.yml` | Master-Slave replication |
| `deploy/docker/docker-compose.cluster.yml` | Cluster mode (3 nodes) |
| `deploy/docker/docker-compose.sentinel.yml` | High availability with Sentinel |
| `deploy/docker/sentinel.conf` | Redis Sentinel configuration |

## Build Image

```bash
# Build from source
cd deploy/docker
docker build -t lbp0200/bolt:latest .

# Or pull from Docker Hub (when available)
docker pull lbp0200/bolt:latest
```

## Quick Start Examples

```bash
cd deploy/docker

# Standalone
docker-compose -f docker-compose.yml -f docker-compose.standalone.yml up -d

# Master-Slave
docker-compose -f docker-compose.yml -f docker-compose.master-slave.yml up -d

# Cluster
docker-compose -f docker-compose.yml -f docker-compose.cluster.yml up -d

# Sentinel (HA)
docker-compose -f docker-compose.yml -f docker-compose.sentinel.yml up -d
```

## Prerequisites

- Docker installed
- Docker Compose (optional, for orchestration)

## Quick Start

### Basic Usage

```bash
# Run BoltDB container
docker run -d \
  --name boltdb \
  -p 6379:6379 \
  -v /tmp/bolt:/data \
  lbp0200/bolt

# Verify it's running
docker ps
```

### With Custom Configuration

```bash
docker run -d \
  --name boltdb \
  -p 6380:6379 \
  -v /var/lib/bolt:/data \
  -e BOLTDB_LOG_LEVEL=info \
  lbp0200/bolt -addr=:6379 -dir=/data
```

## Docker Compose

### Basic Setup

Create a `docker-compose.yml` file:

```yaml
version: '3.8'

services:
  bolt:
    image: lbp0200/bolt
    container_name: boltdb
    ports:
      - "6379:6379"
    volumes:
      - bolt-data:/data
    restart: unless-stopped
    command: ["-log-level", "info"]

volumes:
  bolt-data:
```

Start the container:

```bash
docker-compose up -d
```

### Cluster Mode

```yaml
version: '3.8'

services:
  bolt-node1:
    image: lbp0200/bolt
    ports:
      - "7001:6379"
    volumes:
      - node1-data:/data
    command: ["-cluster", "-addr", ":6379", "-dir", "/data"]
    restart: unless-stopped

  bolt-node2:
    image: lbp0200/bolt
    ports:
      - "7002:6379"
    volumes:
      - node2-data:/data
    command: ["-cluster", "-addr", ":6379", "-dir", "/data"]
    depends_on:
      - bolt-node1
    restart: unless-stopped

volumes:
  node1-data:
  node2-data:
```

### Master-Slave Replication

```yaml
version: '3.8'

services:
  bolt-master:
    image: lbp0200/bolt
    ports:
      - "6379:6379"
    volumes:
      - master-data:/data
    command: ["-dir", "/data"]
    restart: unless-stopped

  bolt-slave:
    image: lbp0200/bolt
    ports:
      - "6380:6379"
    volumes:
      - slave-data:/data
    command: ["-addr", ":6379", "-dir", "/data", "-replicaof", "bolt-master", "6379"]
    depends_on:
      - bolt-master
    restart: unless-stopped

volumes:
  master-data:
  slave-data:
```

### With Sentinel (High Availability)

```yaml
version: '3.8'

services:
  bolt-master:
    image: lbp0200/bolt
    ports:
      - "6379:6379"
    volumes:
      - master-data:/data
    command: ["-dir", "/data"]
    restart: unless-stopped
    networks:
      - bolt-net

  bolt-slave1:
    image: lbp0200/bolt
    ports:
      - "6380:6379"
    volumes:
      - slave1-data:/data
    command: ["-addr", ":6379", "-dir", "/data", "-replicaof", "bolt-master", "6379"]
    depends_on:
      - bolt-master
    restart: unless-stopped
    networks:
      - bolt-net

  redis-sentinel:
    image: redis:7-alpine
    ports:
      - "26379:26379"
    volumes:
      - ./sentinel.conf:/usr/local/etc/redis/sentinel.conf
    command: redis-sentinel /usr/local/etc/redis/sentinel.conf
    depends_on:
      - bolt-master
      - bolt-slave1
    restart: unless-stopped
    networks:
      - bolt-net

networks:
  bolt-net:
```

Create `sentinel.conf`:

```conf
port 26379
sentinel monitor mymaster bolt-master 6379 2
sentinel down-after-milliseconds mymaster 30000
sentinel failover-timeout mymaster 180000
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BOLTDB_LOG_LEVEL` | Log level (debug/info/warn/error) | `warning` |
| `BOLTDB_ADDR` | Server address | `:6379` |
| `BOLTDB_DIR` | Data directory | `./data` |

## Data Persistence

Docker volumes are used for data persistence:

```bash
# View volumes
docker volume ls | grep bolt

# Inspect volume
docker volume inspect boltdb_bolt-data
```

## Management Commands

```bash
# View logs
docker logs boltdb

# Follow logs
docker logs -f boltdb

# Stop container
docker stop boltdb

# Start container
docker start boltdb

# Restart container
docker restart boltdb

# Execute commands in container
docker exec -it boltdb redis-cli

# Connect to BoltDB
docker exec -it boltdb redis-cli -p 6379 PING
```

## Networking

### Expose on Specific IP

```bash
docker run -d \
  -p 192.168.1.100:6379:6379 \
  -v /tmp/bolt:/data \
  lbp0200/bolt
```

### Use Host Network

```bash
docker run -d \
  --network host \
  -v /tmp/bolt:/data \
  lbp0200/bolt
```

## Security

### Run as Non-Root User

```bash
docker run -d \
  --user 1000:1000 \
  -p 6379:6379 \
  -v /tmp/bolt:/data \
  lbp0200/bolt
```

### Enable TLS (Future)

TLS support is planned for future versions.

## Building Custom Image

```bash
# Clone and build
git clone https://github.com/lbp0200/BoltDB.git
cd BoltDB

# Build Docker image
docker build -t my-boltdb .

# Run
docker run -d -p 6379:6379 -v /tmp/bolt:/data my-boltdb
```

## Health Check

Add health check to your compose file:

```yaml
services:
  bolt:
    image: lbp0200/bolt
    healthcheck:
      test: ["CMD", "redis-cli", "PING"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s
```

## Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs boltdb

# Check if port is in use
docker port boltdb
```

### Permission Issues

```bash
# Fix volume permissions
docker run --rm -v /tmp/bolt:/data alpine chown -R 1000:1000 /data
```
