# wal-bench

A Rust benchmark that evaluates WAL (Write-Ahead Log) (sync-) write performance on filesystems.

Each concurrent task sequentially appends variable-sized records to its own file, calling fsync/fdatasync after every write.

## Build

```bash
cargo build --release
```

## Usage

```bash
# Basic NFS benchmark: 8 concurrent writers, O_DIRECT, fdatasync
./target/release/wal-bench --dir /mnt/nfs/wal-test -c 8 -m channel --direct --sync-data

# Pure kernel threads, pinned to cores 4-5
./target/release/wal-bench --dir /tmp/wal-test -c 2 -m thread --io-cores 4,5 --direct --sync-data

# COW dual-write to two directories
./target/release/wal-bench --dir /mnt/nfs-west/wal -c 4 -m channel --replica-dir /mnt/nfs-east/wal --direct --sync-data

# With sync tracing for latency analysis
./target/release/wal-bench --dir /tmp/wal-test -c 2 -m thread --direct --sync-data --trace-sync
```

## I/O Modes

| Mode | Flag | Description |
|---|---|---|
| **std** | `-m std` | Blocking `std::fs` write+sync on tokio worker threads |
| **tokio** | `-m tokio` | Async `tokio::fs` write+sync via `spawn_blocking` |
| **channel** | `-m channel` | Dedicated `std::thread` per task, communicating via `tokio::sync::mpsc` |
| **thread** | `-m thread` | Pure `std::thread` per task, no tokio in the write path |

## Options

| Flag | Default | Description |
|---|---|---|
| `--dir` | (required) | Directory to write WAL files into |
| `-c`, `--concurrency` | 4 | Number of concurrent writer tasks |
| `-n`, `--writes-per-task` | 1000 | Total writes per task |
| `-m`, `--io-mode` | std | I/O mode: std, tokio, channel, thread |
| `--direct` | false | Open files with O_DIRECT |
| `--sync-data` | false | Use fdatasync instead of fsync |
| `--min-record-size` | 64 | Minimum WAL record size (bytes) |
| `--max-record-size` | 8192 | Maximum WAL record size (bytes) |
| `--warmup` | 0 | Warmup writes per task (excluded from stats) |
| `--worker-cores` | all | Pin tokio worker threads (e.g. `0-3`) |
| `--io-cores` | none | Pin I/O threads for channel/thread modes (e.g. `4-7`) |
| `--replica-dir` | none | Enable COW dual-write to a second directory |
| `--trace-sync` | false | Write per-fdatasync timestamps to `sync-trace.tsv` |
| `--stagger-us` | 0 | Stagger task starts by N*task_id microseconds |
| `--spread-dirs` | false | Place each task's file in its own subdirectory |
| `--cleanup` | false | Remove WAL files after benchmark |

## Output

Reports wall time, throughput (MB/s), IOPS, and fsync latency percentiles (p50/p90/p99/p99.9), plus per-task breakdown. With `--replica-dir`, latencies are reported separately for combined, primary, and replica.

## License

[MIT](LICENSE)
