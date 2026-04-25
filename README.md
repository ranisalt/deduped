# deduped — File deduplication daemon

A C++ daemon and CLI tool that identifies duplicate files and replaces them with hardlinks to reduce storage consumption. Made for archival systems, backup storage, and large media libraries where files are immutable after initial write.

## Features

- **BLAKE3-based content hashing** with cached digest validation
- **Metadata-aware caching**: Hashes reused only if file size, mtime, inode, device, and permissions are unchanged
- **Hardlink deduplication**: Confirmed duplicates replaced with hardlinks to reduce storage footprint
- **Race condition hardening**: Safe against concurrent file mutations; digests validated at hardlink time
- **Atomic lock mechanism**: Single-instance daemon enforcement via atomic directory creation
- **inotify-based watching**: Real-time file event monitoring with automatic deduplication
- **Report-only mode**: Inspect planned deduplication without filesystem changes (CLI default)
- **Persistent SQLite index**: Faster lookups and crash-recovery capability with operation logging

## Installation

### From Source

```bash
git clone https://github.com/yourusername/deduped.git
cd deduped
cmake --preset default
cmake --build build
```

Binaries are available at:
- `build/Debug/deduped` (daemon process)
- `build/Debug/deduped-cli` (CLI tool)

### Docker

See [Docker usage](#docker-usage) below.

## Usage

### CLI (one-shot mode)

```bash
# Scan and report duplicates (dry-run, no changes)
deduped-cli --db /path/to/dbdir /path/to/data

# Scan multiple roots in one run
deduped-cli --db /path/to/dbdir /path/to/data /path/to/another

# Apply hardlinks to deduplicate (create hardlinks)
deduped-cli --db /path/to/dbdir --apply /path/to/data
```

### Daemon (persistent watching)

```bash
# Start daemon with persistent database in /config, watch /data (applies by default)
deduped --config /config /data

# Watch multiple roots by passing one positional argument per root
deduped --config /config /data /another
```

In containers, the daemon automatically applies deduplication. To run in report-only mode, override the entrypoint or run the binary without `--apply`.

Environment variables (must be provided at runtime via docker-compose or docker run):
- `DEDUPED_CONFIG`: Directory for SQLite database (e.g., `/config`)
- `DEDUPED_DATA`: For the container entrypoint, one or more root directories passed as a comma-delimited list (e.g., `/data` or `/data,/another`). The entrypoint expands this into one CLI positional argument per root. Root paths cannot contain commas.
- `PUID`: User ID for daemon process (for host permission mapping)
- `PGID`: Group ID for daemon process (for host permission mapping)
- `TZ`: Timezone (optional, default: `Etc/UTC`)
- `UMASK`: File creation mask (optional, default: `022`)

## Docker usage

### Quick start with docker-compose (Recommended)

Edit `docker-compose.yml` to set your data directory and user IDs, then:

```bash
docker-compose up -d
docker-compose logs -f deduped
```

The compose file includes all required environment variables. Update the bind-mount path for `/data`.

### Docker CLI

```bash
# Build the image
docker build -t deduped:latest .

# Run daemon with explicit environment variables
docker run -d \
  --name deduped \
  -e PUID=1000 \
  -e PGID=1000 \
  -e DEDUPED_CONFIG=/config \
  -e DEDUPED_DATA=/data \
  -v deduped-config:/config \
  -v /path/to/your/data:/data \
  --restart unless-stopped \
  deduped:latest

# Multiple roots must be passed as one comma-delimited env value
docker run -d \
  --name deduped \
  -e PUID=1000 \
  -e PGID=1000 \
  -e DEDUPED_CONFIG=/config \
  -e DEDUPED_DATA=/data,/another \
  -v deduped-config:/config \
  -v /path/to/your/data:/data \
  -v /path/to/another:/another \
  --restart unless-stopped \
  deduped:latest

# Run CLI commands (one-shot)
docker run --rm \
  -e DEDUPED_CONFIG=/config \
  -e DEDUPED_DATA=/data \
  -v deduped-config:/config \
  -v /path/to/your/data:/data \
  deduped:latest \
  deduped-cli --db /config /data
```

### Docker volumes

- **`/config`**: Persistent named volume containing SQLite database (`deduped.db`) and lock directory
- **`/data`**: Bind-mount for target data directory where deduplication occurs

### User and group mapping (PUID/PGID)

The Docker image respects `PUID` and `PGID` environment variables to map the container's process to your host user. Set these to match your host user when bind-mounting directories.

Find your user/group IDs:

```bash
id your_username
# Output: uid=1000(your_username) gid=1000(your_username) groups=1000(...)
```

Then set them in docker-compose.yml or docker run:

```bash
docker run -e PUID=1000 -e PGID=1000 ...
```

## Safety

### What deduped won't do

- Symlinks are detected and never replaced with hardlinks
- Files on different filesystems are never hardlinked
- CLI requires explicit `--apply` flag; default is report-only (daemon in container applies by default)
- Hardlinking requires files on the same device and passage of all pre-flight checks

### Pre-flight checks

Before creating any hardlink, deduped verifies:
1. Both files are regular files (not symlinks)
2. Both files are on the same device (filesystem)
3. Files are not already hardlinked (same inode)
4. Canonical file hash matches expected digest (TOCTOU guard)
5. Duplicate file hash matches expected digest (TOCTOU guard)

### Crash safety

Hardlink operations are logged to the SQLite database before execution. If the process crashes mid-operation:
- Temporary backup files remain in place
- Database log records the operation state
- Subsequent daemon restart detects the incomplete operation and logs it
- Corrupted files are preserved; manual recovery is possible

## Configuration

Environment variables must be provided at runtime. The image has no built-in defaults:

| Variable | Description |
|----------|-------------|
| `DEDUPED_CONFIG` | **(Required)** Directory for SQLite database (e.g., `/config`) |
| `DEDUPED_DATA` | **(Required)** One or more root directories, comma-delimited for the container entrypoint (e.g., `/data` or `/data,/another`). The entrypoint expands this into one CLI positional argument per root. Root paths themselves cannot contain commas. |
| `PUID` | **(Required for Docker)** User ID for process execution (match your host user ID) |
| `PGID` | **(Required for Docker)** Group ID for process execution (match your host group ID) |
| `TZ` | *(Optional)* Timezone (default: `Etc/UTC`) |
| `UMASK` | *(Optional)* File creation mask (default: `022`) |

## Hashing

Deduped uses **BLAKE3** for file content hashing with these properties:
- Streaming hashing with 1 MiB chunks
- 256-bit (32-byte) digest
- BLAKE3 is the only hashing algorithm

## Building and development

### Build for testing

```bash
cmake --preset with-tests
cmake --build build
ctest --preset with-tests
```

The test suite covers unit logic, integration workflows, binary entrypoints, crash recovery, and watcher functionality.

### Multi-architecture docker builds

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t deduped:latest .
```

## Database schema

Deduped stores:
- **Files**: path, size, modification time, inode, device, mode, uid, gid
- **Digests**: BLAKE3 hash and last-seen timestamp
- **Operations**: planned and completed hardlink operations with timestamps

Schema versioning allows future migrations without data loss.

## Troubleshooting

### Permission denied on bind-mounted data

Ensure `PUID` and `PGID` match your host user:

```bash
# Find your IDs
id $USER

# Update docker-compose.yml or docker run:
-e PUID=1000 -e PGID=1000
```

### Cross-device error

If `/data` spans multiple filesystems, hardlinking will be skipped with a message like:
```
cross-device: cannot hardlink
```

This is by design for safety. Move files to the same device or use `cp` for cross-device deduplication.

### Database lock

Only one daemon instance can run on a given `/config` directory (enforced via lock directory). If you see:

```
Lock already exists - daemon already running?
```

Either:
1. Stop the existing daemon: `docker stop deduped`
2. Or remove the stale lock: `rm -rf /path/to/config/deduped.lockdir`

## License

GPL-3.0. See LICENSE file.

## Contributing

Contributions welcome! All changes must include:
- Unit tests for new logic
- Integration tests for end-to-end workflows
- Verification that all tests pass

## Support

- **GitHub issues**: Report bugs and feature requests
