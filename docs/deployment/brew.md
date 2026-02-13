# Install BoltDB via Homebrew (macOS)

## Prerequisites

- macOS with Homebrew installed
- Terminal access

## Installation

### Option 1: From Custom Tap (Recommended)

```bash
# Add the BoltDB tap
brew tap lbp0200/bolt

# Install BoltDB
brew install bolt
```

### Option 2: Build from Source

```bash
# Clone the repository
git clone https://github.com/lbp0200/BoltDB.git
cd BoltDB

# Build
go build -o ./build/boltDB cmd/boltDB/main.go

# Or install directly
go install ./cmd/boltDB
```

## Starting BoltDB

```bash
# Start with default settings (port 6379, data dir ./data)
bolt

# Start with custom settings
bolt -addr=:6380 -dir=/var/lib/bolt -log-level info

# Start in cluster mode
bolt -cluster -addr=:6379 -dir=/var/lib/bolt
```

## Configuration Options

| Flag | Description | Default |
|------|-------------|---------|
| `-addr` | Server address | `:6379` |
| `-dir` | Data directory | `./data` |
| `-log-level` | Log level (debug/info/warn/error) | `warning` |
| `-cluster` | Enable cluster mode | `false` |
| `-replicaof` | Master address (for slave mode) | - |

## Upgrading

```bash
# Upgrade BoltDB
brew upgrade bolt

# Or reinstall
brew reinstall bolt
```

## Uninstalling

```bash
# Remove BoltDB
brew uninstall bolt

# Remove tap (optional)
brew untap lbp0200/bolt
```

## Troubleshooting

### Port Already in Use

If port 6379 is already in use, specify a different port:

```bash
bolt -addr=:6380
```

### Permission Denied

If you encounter permission errors, ensure the data directory is writable:

```bash
mkdir -p /var/lib/bolt
chmod 755 /var/lib/bolt
bolt -dir=/var/lib/bolt
```
