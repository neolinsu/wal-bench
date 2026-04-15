# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

**wal-bench** -- a Rust/Tokio benchmark that evaluates sync-write performance (local SSD, NFS, or any filesystem) by mimicking a WAL (Write-Ahead Log) workload. Each concurrent task sequentially appends variable-sized records to its own file, calling fsync/fdatasync after every write.

## Build & Run

```bash
cargo build --release
./target/release/wal-bench --dir /mnt/nfs/wal-test -c 8 -m channel --direct --sync-mode fdatasync
```

Rust edition 2024 -- `gen` is a reserved keyword; use `r#gen` when calling `rand::Rng` methods.

## Architecture

Multi-module binary with four I/O modes controlled by `-m`:

| Source file | Purpose |
|---|---|
| `src/main.rs` | Entry point, runtime setup, results reporting |
| `src/common.rs` | Args, types (TaskResult, SyncEvent, AlignedBuf), helpers |
| `src/mode_std.rs` | std I/O mode |
| `src/mode_tokio.rs` | tokio I/O mode |
| `src/mode_channel.rs` | channel I/O mode |
| `src/mode_thread.rs` | thread I/O mode |

### I/O modes

- **std** -- blocking `std::fs` write+sync directly on tokio worker threads. Concurrency is limited by worker thread count (set via `--worker-cores`) since each write blocks the thread.
- **tokio** -- `tokio::fs` async write+sync, which internally uses `spawn_blocking`. True concurrency beyond worker thread count, but per-op overhead from the blocking pool.
- **channel** -- dedicated `std::thread` per task for I/O, communicating with tokio coroutines via `tokio::sync::mpsc`. Best option when you need pinned I/O thread affinity (`--io-cores`).
- **thread** -- pure `std::thread` per task running the entire write loop. No tokio involvement in the write path. Supports `--io-cores` pinning.

### Key implementation details

- **O_DIRECT** (`--direct`): records are padded to 512-byte alignment; a reusable `AlignedBuf` (manually allocated via `std::alloc`) avoids per-write allocation.
- **fallocate**: files are pre-allocated to avoid extent/metadata updates on each fdatasync (this is why bare fdatasync without fallocate shows ~2ms+ latency even on local SSD).
- **Core affinity** (`core_affinity` crate): `--worker-cores` pins tokio worker threads; `--io-cores` pins channel/thread-mode I/O threads. Tokio does not support pinning its internal blocking pool threads.
- **COW dual-write** (`--replica-dir`): writes each record to both primary and replica directory in parallel. Reports combined, primary, and replica latency separately.
- **Sync tracing** (`--trace-sync`): records per-fdatasync start/end timestamps (nanoseconds) to `sync-trace.tsv` in the output dir. Supported in channel and thread modes.
- **Stagger** (`--stagger-us N`): delays task k by k*N microseconds before starting its write loop, reducing flush contention from synchronized starts.
- **Spread dirs** (`--spread-dirs`): places each task's WAL file in its own subdirectory (`sub-NNNN/wal-NNNN.log`) to distribute XFS allocation groups.
- **Cleanup** (`--cleanup`): removes WAL files (and spread subdirectories) after benchmark completes.

## Metrics

Reports wall time, throughput (MB/s), IOPS, and fsync latency percentiles (p50/p90/p99/p99.9) using `hdrhistogram`, plus per-task breakdown.
