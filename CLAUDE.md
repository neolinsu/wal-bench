# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

**nfs-wal-bench** -- a Rust/Tokio benchmark that evaluates NFS v4 (or local) sync-write performance by mimicking a WAL (Write-Ahead Log) workload. Each concurrent task sequentially appends variable-sized records to its own file, calling fsync/fdatasync after every write.

## Build & Run

```bash
cargo build --release
./target/release/nfs-wal-bench --dir /mnt/nfs/wal-test -c 8 -m channel --direct --sync-data
```

Rust edition 2024 -- `gen` is a reserved keyword; use `r#gen` when calling `rand::Rng` methods.

## Architecture

Single-file binary (`src/main.rs`) with three I/O modes controlled by `-m`:

- **std** -- blocking `std::fs` write+sync directly on tokio worker threads. Concurrency is limited by `-t` (worker thread count) since each write blocks the thread.
- **tokio** -- `tokio::fs` async write+sync, which internally uses `spawn_blocking`. True concurrency beyond worker thread count, but per-op overhead from the blocking pool.
- **channel** -- dedicated `std::thread` per task for I/O, communicating with tokio coroutines via `std::sync::mpsc` (records) and `tokio::sync::mpsc` (latencies). Best option when you need pinned I/O thread affinity (`--io-cores`).

Key implementation details:
- **O_DIRECT** (`--direct`): records are padded to 512-byte alignment; a reusable `AlignedBuf` (manually allocated via `std::alloc`) avoids per-write allocation.
- **fallocate**: files are pre-allocated to avoid extent/metadata updates on each fdatasync (this is why bare fdatasync without fallocate shows ~2ms+ latency even on local SSD).
- **Core affinity** (`core_affinity` crate): `--worker-cores` pins tokio worker threads; `--io-cores` pins channel-mode I/O threads. Tokio does not support pinning its internal blocking pool threads.

## Metrics

Reports wall time, throughput (MB/s), IOPS, and fsync latency percentiles (p50/p90/p99/p99.9) using `hdrhistogram`, plus per-task breakdown.
