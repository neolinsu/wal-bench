# COW Dual-Write WAL Benchmark Design

## Overview

Add a Copy-on-Write (COW) dual-write mode to wal-bench. When `--replica-dir` is provided, each WAL write is duplicated to both a primary directory (`--dir`) and a replica directory (`--replica-dir`). Both syncs happen in parallel, and latency is measured for each side independently plus a combined max.

This simulates a dual-write replication pattern where the caller must wait for both primary and replica to confirm before proceeding.

## CLI Changes

New optional argument:

```
--replica-dir <PATH>   Optional second directory for COW dual-write mode
```

Behavior:
- When omitted: tool behaves identically to today (single-dir mode).
- When provided: enables COW dual-write mode.
- Both `--dir` and `--replica-dir` are created if they don't exist.
- `--cleanup` removes files from both directories.
- Startup banner prints the replica dir and indicates COW mode is active.
- COW mode is orthogonal to `--io-mode` (works with std, tokio, and channel).

## Write & Sync Flow

For each WAL write in COW mode:

1. Build the WAL record once into the existing buffer (no change from today).
2. Write the same buffer to both primary and replica files.
3. Sync both files in parallel, then wait for both to complete.
4. Measure three latencies per write:
   - `primary_lat`: time for primary write+sync
   - `replica_lat`: time for replica write+sync
   - `combined_lat`: max(primary_lat, replica_lat)

### std mode

Double the tokio worker thread count when COW is enabled (so each task effectively gets two worker threads available). For each write:

1. `tokio::spawn` a blocking task for replica write+sync.
2. Do primary write+sync on the current worker thread.
3. `.await` the spawned replica task.
4. Collect primary_lat from local measurement, replica_lat from the spawned task.
5. combined_lat = max(primary_lat, replica_lat).

Worker core affinity: if `--worker-cores` is set, the doubled threads pin to the same core set (round-robin, same as today).

### tokio mode

For each write:

1. `tokio::join!` on primary async write+sync and replica async write+sync.
2. Each branch measures its own latency independently.
3. combined_lat = max(primary_lat, replica_lat).

### channel mode

Spawn a second dedicated I/O thread per task for the replica. For each write:

1. Send the buffer to both primary and replica I/O threads.
2. Await both acknowledgements (each returns its sync latency).
3. combined_lat = max(primary_lat, replica_lat).

The replica I/O thread respects `--io-cores` pinning (round-robin, same as the primary I/O thread).

## Metrics & Reporting

### TaskResult changes

`TaskResult` gains two new fields:
- `primary_latencies_us: Vec<u64>` -- per-write primary sync latency
- `replica_latencies_us: Vec<u64>` -- per-write replica sync latency

The existing `latencies_us` field holds the combined (max) latency.

### Output format

In COW mode, three histogram blocks are printed:

```
Fsync latency - combined (us):
  min:    ...
  p50:    ...
  p90:    ...
  p99:    ...
  p99.9:  ...
  max:    ...

Fsync latency - primary (us):
  min:    ...
  p50:    ...
  p90:    ...
  p99:    ...
  p99.9:  ...
  max:    ...

Fsync latency - replica (us):
  min:    ...
  p50:    ...
  p90:    ...
  p99:    ...
  p99.9:  ...
  max:    ...
```

When `--replica-dir` is not set, output is identical to today (single histogram, no labels).

Throughput and IOPS are based on the combined latency (wall-clock perspective). Per-task breakdown is unchanged.

## File naming

Both primary and replica use the same file names: `wal-XXXX.log`. They are distinguished by their parent directory.

## Preallocation

Both primary and replica files are preallocated using the same `estimate_prealloc` logic (unchanged).
