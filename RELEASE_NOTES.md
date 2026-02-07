# Release v0.2.0

## Features
- Add ECHO, DBSIZE, FLUSHDB, FLUSHALL commands
- Add SRandMemberN support for SRANDMEMBER with count parameter

## Fixes
- Fix BadgerDB transaction conflicts by separating read/write operations
- Fix GetSet, APPEND, SetRange, INCRBYFLOAT transaction handling
- Fix LPUSHX/RPUSHX, HSet, HDel, Persist operations
- Fix cache invalidation on delete (Del, DelString)

## Downloads
Pre-built binaries:
- boltreon-v0.2.0-linux-amd64
- boltreon-v0.2.0-linux-arm64
- boltreon-v0.2.0-darwin-amd64
- boltreon-v0.2.0-darwin-arm64
- boltreon-v0.2.0-windows-amd64.exe

## Installation
```bash
# macOS (Homebrew)
brew install lbp0200/Botreon/boltreon

# Linux/Darwin (curl)
curl -L https://github.com/lbp0200/Botreon/releases/download/v0.2.0/boltreon-v0.2.0-linux-amd64 -o boltreon
chmod +x boltreon

# Windows
# Download boltreon-v0.2.0-windows-amd64.exe
```

## Quick Start
```bash
./boltreon -dir=/tmp/boltreon-data -addr=:6379
redis-cli -p 6379 ping
```
