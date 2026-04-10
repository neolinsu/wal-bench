use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use core_affinity::CoreId;
use hdrhistogram::Histogram;
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use tokio::io::AsyncWriteExt;

/// Parse a core range like "0-3" or "0,2,4" or "0-3,6,8-10" into a Vec<CoreId>.
fn parse_cores(s: &str) -> Result<Vec<CoreId>, String> {
    let mut cores = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if let Some((a, b)) = part.split_once('-') {
            let start: usize = a.trim().parse().map_err(|e| format!("invalid core: {e}"))?;
            let end: usize = b.trim().parse().map_err(|e| format!("invalid core: {e}"))?;
            for id in start..=end {
                cores.push(CoreId { id });
            }
        } else {
            let id: usize = part.parse().map_err(|e| format!("invalid core: {e}"))?;
            cores.push(CoreId { id });
        }
    }
    if cores.is_empty() {
        return Err("empty core list".into());
    }
    Ok(cores)
}

/// Benchmark sync-write performance mimicking WAL workload.
/// Each Tokio task appends variable-sized records to its own file,
/// calling fsync after every write.
#[derive(Parser, Debug)]
struct Args {
    /// Directory to write files into (point this at an NFS mount)
    #[arg(short, long)]
    dir: PathBuf,

    /// Replica directory for COW dual-write mode (enables writing to both --dir and --replica-dir)
    #[arg(long)]
    replica_dir: Option<PathBuf>,

    /// Number of concurrent writer tasks
    #[arg(short = 'c', long, default_value_t = 4)]
    concurrency: usize,

    /// Total number of writes per task
    #[arg(short = 'n', long, default_value_t = 1000)]
    writes_per_task: usize,

    /// Number of warmup writes per task (excluded from stats)
    #[arg(long, default_value_t = 0)]
    warmup: usize,

    /// Minimum WAL record size in bytes
    #[arg(long, default_value_t = 64)]
    min_record_size: usize,

    /// Maximum WAL record size in bytes
    #[arg(long, default_value_t = 8192)]
    max_record_size: usize,

    /// Pin tokio worker threads to these CPU cores (e.g. "0-3" or "0,2,4")
    #[arg(long)]
    worker_cores: Option<String>,

    /// Pin I/O threads (channel mode) to these CPU cores (e.g. "4-7")
    #[arg(long)]
    io_cores: Option<String>,

    /// Max blocking threads for tokio runtime (default: 512)
    #[arg(long)]
    max_blocking_threads: Option<usize>,

    /// I/O mode: "std" for std::fs sync I/O, "tokio" for tokio::fs async I/O,
    /// "channel" for dedicated I/O threads communicating via channels
    #[arg(short = 'm', long, default_value = "std")]
    io_mode: IoMode,

    /// Open files with O_DIRECT (bypass page cache)
    #[arg(long, default_value_t = false)]
    direct: bool,

    /// Use fdatasync instead of fsync (skip metadata sync)
    #[arg(long, default_value_t = false)]
    sync_data: bool,

    /// Remove WAL files after benchmark completes
    #[arg(long, default_value_t = false)]
    cleanup: bool,
}

#[derive(Debug, Clone, ValueEnum)]
enum IoMode {
    Std,
    Tokio,
    Channel,
}

const ALIGN: usize = 512;

/// Build a WAL-like record in-place into `buf`:
/// 4-byte length prefix + 8-byte sequence number + payload.
/// Returns the number of bytes to write (padded to ALIGN when direct).
fn build_record_into(buf: &mut [u8], seq: u64, payload: &[u8], direct: bool) -> usize {
    let total_len = 8 + payload.len();
    let raw_len = 4 + total_len;
    let write_len = if direct {
        (raw_len + ALIGN - 1) / ALIGN * ALIGN
    } else {
        raw_len
    };
    buf[..4].copy_from_slice(&(total_len as u32).to_le_bytes());
    buf[4..12].copy_from_slice(&seq.to_le_bytes());
    buf[12..12 + payload.len()].copy_from_slice(payload);
    // Zero padding for O_DIRECT alignment
    if direct && write_len > raw_len {
        buf[raw_len..write_len].fill(0);
    }
    write_len
}

/// Reusable aligned buffer for O_DIRECT writes.
struct AlignedBuf {
    ptr: *mut u8,
    capacity: usize,
}

impl AlignedBuf {
    fn new(capacity: usize) -> Self {
        use std::alloc::{Layout, alloc_zeroed};
        let cap = (capacity + ALIGN - 1) / ALIGN * ALIGN;
        let layout = Layout::from_size_align(cap, ALIGN).expect("invalid layout");
        let ptr = unsafe { alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "allocation failed");
        Self { ptr, capacity: cap }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.capacity) }
    }

    fn as_slice(&self, len: usize) -> &[u8] {
        assert!(len <= self.capacity);
        unsafe { std::slice::from_raw_parts(self.ptr, len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        use std::alloc::{Layout, dealloc};
        let layout = Layout::from_size_align(self.capacity, ALIGN).expect("invalid layout");
        unsafe { dealloc(self.ptr, layout) };
    }
}

unsafe impl Send for AlignedBuf {}

/// Open a file with optional O_DIRECT, returning a std::fs::File.
/// Pre-allocates space so that fdatasync doesn't need to update extents/size.
fn open_direct(path: &std::path::Path, direct: bool, prealloc_bytes: u64) -> std::fs::File {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;
    let mut opts = std::fs::OpenOptions::new();
    opts.create(true).truncate(true).write(true);
    if direct {
        opts.custom_flags(libc::O_DIRECT);
    }
    let file = opts.open(path)
        .unwrap_or_else(|e| panic!("failed to open {}: {e}", path.display()));
    if prealloc_bytes > 0 {
        let ret = unsafe {
            libc::fallocate(file.as_raw_fd(), 0, 0, prealloc_bytes as libc::off_t)
        };
        if ret != 0 {
            panic!("fallocate failed: {}", std::io::Error::last_os_error());
        }
    }
    file
}

/// Sync file: fdatasync if sync_data, else fsync.
fn do_sync(file: &std::fs::File, sync_data: bool) {
    if sync_data {
        file.sync_data().expect("fdatasync failed");
    } else {
        file.sync_all().expect("fsync failed");
    }
}

/// Estimate preallocation size using max record size.
/// Over-allocates slightly but avoids extent metadata updates mid-benchmark.
fn estimate_prealloc(args: &Args) -> u64 {
    let max_record = 4 + 8 + args.max_record_size;
    let per_write = if args.direct {
        (max_record + ALIGN - 1) / ALIGN * ALIGN
    } else {
        max_record
    };
    let total_writes = args.writes_per_task + args.warmup;
    (per_write * total_writes) as u64
}

struct TaskResult {
    task_id: usize,
    total_bytes: u64,
    measured_bytes: u64,
    elapsed: Duration,
    latencies_us: Vec<u64>,
}

fn writer_task_std(args: &Args, task_id: usize) -> TaskResult {
    use std::io::Write;

    let path = args.dir.join(format!("wal-{task_id:04}.log"));
    let prealloc = estimate_prealloc(args);
    let mut file = open_direct(&path, args.direct, prealloc);

    let mut rng = StdRng::from_entropy();
    let mut total_bytes: u64 = 0;
    let mut measured_bytes: u64 = 0;
    let mut latencies_us = Vec::with_capacity(args.writes_per_task);
    let max_record_raw = 4 + 8 + args.max_record_size;
    let max_write_len = (max_record_raw + ALIGN - 1) / ALIGN * ALIGN;

    // Pre-allocate payload and write buffers
    let mut payload_buf = vec![0u8; args.max_record_size];
    let mut record_buf = vec![0u8; max_write_len];
    let mut abuf = if args.direct { Some(AlignedBuf::new(max_write_len)) } else { None };

    let total_writes = args.warmup + args.writes_per_task;
    let start = Instant::now();

    for seq in 0..total_writes as u64 {
        let payload_len = rng.r#gen_range(args.min_record_size..=args.max_record_size);
        rng.fill_bytes(&mut payload_buf[..payload_len]);

        let write_len;
        if let Some(ref mut abuf) = abuf {
            // O_DIRECT: build directly into aligned buffer
            write_len = build_record_into(abuf.as_mut_slice(), seq, &payload_buf[..payload_len], true);
            let t0 = Instant::now();
            file.write_all(abuf.as_slice(write_len)).expect("write failed");
            do_sync(&file, args.sync_data);
            let lat = t0.elapsed();
            if seq >= args.warmup as u64 {
                latencies_us.push(lat.as_micros() as u64);
                measured_bytes += write_len as u64;
            }
        } else {
            write_len = build_record_into(&mut record_buf, seq, &payload_buf[..payload_len], false);
            let t0 = Instant::now();
            file.write_all(&record_buf[..write_len]).expect("write failed");
            do_sync(&file, args.sync_data);
            let lat = t0.elapsed();
            if seq >= args.warmup as u64 {
                latencies_us.push(lat.as_micros() as u64);
                measured_bytes += write_len as u64;
            }
        };

        total_bytes += write_len as u64;
    }

    let elapsed = start.elapsed();

    TaskResult {
        task_id,
        total_bytes,
        measured_bytes,
        elapsed,
        latencies_us,
    }
}

async fn writer_task_tokio(args: &Args, task_id: usize) -> TaskResult {
    let path = args.dir.join(format!("wal-{task_id:04}.log"));
    let mut opts = tokio::fs::OpenOptions::new();
    opts.create(true).truncate(true).write(true);
    if args.direct {
        opts.custom_flags(libc::O_DIRECT);
    }
    let mut file = opts.open(&path).await
        .unwrap_or_else(|e| panic!("failed to open {}: {e}", path.display()));

    // Pre-allocate file space
    {
        use std::os::unix::io::AsRawFd;
        let prealloc = estimate_prealloc(args);
        if prealloc > 0 {
            let fd = file.as_raw_fd();
            let ret = unsafe { libc::fallocate(fd, 0, 0, prealloc as libc::off_t) };
            if ret != 0 {
                panic!("fallocate failed: {}", std::io::Error::last_os_error());
            }
        }
    }

    let mut rng = StdRng::from_entropy();
    let mut total_bytes: u64 = 0;
    let mut measured_bytes: u64 = 0;
    let mut latencies_us = Vec::with_capacity(args.writes_per_task);
    let max_record_raw = 4 + 8 + args.max_record_size;
    let max_write_len = (max_record_raw + ALIGN - 1) / ALIGN * ALIGN;

    let mut payload_buf = vec![0u8; args.max_record_size];
    let mut record_buf = vec![0u8; max_write_len];
    let mut abuf = if args.direct { Some(AlignedBuf::new(max_write_len)) } else { None };

    let total_writes = args.warmup + args.writes_per_task;
    let start = Instant::now();

    for seq in 0..total_writes as u64 {
        let payload_len = rng.r#gen_range(args.min_record_size..=args.max_record_size);
        rng.fill_bytes(&mut payload_buf[..payload_len]);

        let write_len;
        if let Some(ref mut abuf) = abuf {
            write_len = build_record_into(abuf.as_mut_slice(), seq, &payload_buf[..payload_len], true);
            let t0 = Instant::now();
            file.write_all(abuf.as_slice(write_len)).await.expect("write failed");
            if args.sync_data {
                file.sync_data().await.expect("fdatasync failed");
            } else {
                file.sync_all().await.expect("fsync failed");
            }
            let lat = t0.elapsed();
            if seq >= args.warmup as u64 {
                latencies_us.push(lat.as_micros() as u64);
                measured_bytes += write_len as u64;
            }
        } else {
            write_len = build_record_into(&mut record_buf, seq, &payload_buf[..payload_len], false);
            let t0 = Instant::now();
            file.write_all(&record_buf[..write_len]).await.expect("write failed");
            if args.sync_data {
                file.sync_data().await.expect("fdatasync failed");
            } else {
                file.sync_all().await.expect("fsync failed");
            }
            let lat = t0.elapsed();
            if seq >= args.warmup as u64 {
                latencies_us.push(lat.as_micros() as u64);
                measured_bytes += write_len as u64;
            }
        };

        total_bytes += write_len as u64;
    }

    let elapsed = start.elapsed();

    TaskResult {
        task_id,
        total_bytes,
        measured_bytes,
        elapsed,
        latencies_us,
    }
}

async fn writer_task_channel(
    args: &Args,
    task_id: usize,
    io_core: Option<CoreId>,
) -> TaskResult {
    let path = args.dir.join(format!("wal-{task_id:04}.log"));
    let direct = args.direct;
    let sync_data = args.sync_data;
    let prealloc = estimate_prealloc(args);

    let max_record_raw = 4 + 8 + args.max_record_size;
    let max_write_len = (max_record_raw + ALIGN - 1) / ALIGN * ALIGN;

    // Double-buffer scheme: sender fills a buffer and sends it to IO thread,
    // IO thread writes it and sends the buffer back for reuse. No per-write allocation.
    let (tx, rx) = std::sync::mpsc::sync_channel::<(Vec<u8>, usize)>(1);
    let (ret_tx, mut ret_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1);

    let io_thread = std::thread::spawn(move || {
        if let Some(core) = io_core {
            core_affinity::set_for_current(core);
        }
        use std::io::Write;

        let mut file = open_direct(&path, direct, prealloc);
        let mut abuf = if direct { Some(AlignedBuf::new(max_write_len)) } else { None };

        while let Ok((buf, len)) = rx.recv() {
            if let Some(ref mut abuf) = abuf {
                abuf.as_mut_slice()[..len].copy_from_slice(&buf[..len]);
                file.write_all(abuf.as_slice(len)).expect("write failed");
            } else {
                file.write_all(&buf[..len]).expect("write failed");
            }
            do_sync(&file, sync_data);
            if ret_tx.blocking_send(buf).is_err() {
                break;
            }
        }
    });

    let mut rng = StdRng::from_entropy();
    let mut total_bytes: u64 = 0;
    let mut measured_bytes: u64 = 0;
    let mut latencies_us = Vec::with_capacity(args.writes_per_task);
    let mut payload_buf = vec![0u8; args.max_record_size];
    // Two pre-allocated buffers for double-buffering
    let mut send_buf = vec![0u8; max_write_len];
    let mut spare_buf = Some(vec![0u8; max_write_len]);

    let total_writes = args.warmup + args.writes_per_task;
    let start = Instant::now();

    for seq in 0..total_writes as u64 {
        let payload_len = rng.r#gen_range(args.min_record_size..=args.max_record_size);
        rng.fill_bytes(&mut payload_buf[..payload_len]);
        let write_len = build_record_into(&mut send_buf, seq, &payload_buf[..payload_len], direct);
        total_bytes += write_len as u64;

        // Measure perceived latency including channel overhead
        let t0 = Instant::now();
        tx.send((send_buf, write_len)).expect("send failed");
        // Get the buffer back from the IO thread (reuse it)
        send_buf = ret_rx.recv().await.expect("recv failed");
        let lat = t0.elapsed();

        if seq >= args.warmup as u64 {
            latencies_us.push(lat.as_micros() as u64);
            measured_bytes += write_len as u64;
        }
    }

    drop(tx);
    drop(spare_buf.take());
    io_thread.join().expect("io thread panicked");

    let elapsed = start.elapsed();

    TaskResult {
        task_id,
        total_bytes,
        measured_bytes,
        elapsed,
        latencies_us,
    }
}

fn main() {
    let args = Args::parse();

    if args.min_record_size > args.max_record_size {
        eprintln!(
            "error: --min-record-size ({}) must be <= --max-record-size ({})",
            args.min_record_size, args.max_record_size
        );
        std::process::exit(1);
    }

    let mut rt_builder = tokio::runtime::Builder::new_multi_thread();
    rt_builder.enable_all();

    if let Some(ref cores_str) = args.worker_cores {
        let cores = parse_cores(cores_str).expect("invalid --worker-cores");
        rt_builder.worker_threads(cores.len());
        let idx = std::sync::atomic::AtomicUsize::new(0);
        rt_builder.on_thread_start(move || {
            let i = idx.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let core = cores[i % cores.len()];
            core_affinity::set_for_current(core);
        });
    }

    if let Some(b) = args.max_blocking_threads {
        rt_builder.max_blocking_threads(b);
    }
    let rt = rt_builder.build().expect("failed to build tokio runtime");

    rt.block_on(run(args));
}

async fn run(args: Args) {
    std::fs::create_dir_all(&args.dir).expect("failed to create output directory");
    if let Some(ref replica_dir) = args.replica_dir {
        std::fs::create_dir_all(replica_dir).expect("failed to create replica directory");
    }

    let mode_str = match args.io_mode {
        IoMode::Std => "std",
        IoMode::Tokio => "tokio",
        IoMode::Channel => "channel",
    };

    let worker_cores_str = args.worker_cores.as_deref().unwrap_or("all");
    let io_cores_str = args.io_cores.as_deref().unwrap_or("none");
    let cow_str = match &args.replica_dir {
        Some(rd) => format!("enabled ({})", rd.display()),
        None => "disabled".to_string(),
    };

    println!(
        "Starting WAL sync-write benchmark\n  dir:            {}\n  concurrency:    {}\n  writes/task:    {}\n  warmup/task:    {}\n  record size:    {}..{} bytes\n  io mode:        {}\n  O_DIRECT:       {}\n  sync mode:      {}\n  worker cores:   {}\n  io cores:       {}\n  COW mode:       {}",
        args.dir.display(),
        args.concurrency,
        args.writes_per_task,
        args.warmup,
        args.min_record_size,
        args.max_record_size,
        mode_str,
        args.direct,
        if args.sync_data { "fdatasync" } else { "fsync" },
        worker_cores_str,
        io_cores_str,
        cow_str,
    );

    // Parse io_cores once, share across tasks
    let io_cores: Option<Vec<CoreId>> = args.io_cores.as_ref().map(|s| {
        parse_cores(s).expect("invalid --io-cores")
    });

    let args = std::sync::Arc::new(args);
    let io_cores = std::sync::Arc::new(io_cores);
    let wall_start = Instant::now();

    let mut handles = Vec::with_capacity(args.concurrency);
    for id in 0..args.concurrency {
        let args = args.clone();
        let io_cores = io_cores.clone();
        match args.io_mode {
            IoMode::Std => {
                handles.push(tokio::spawn(async move { writer_task_std(&args, id) }));
            }
            IoMode::Tokio => {
                handles.push(tokio::spawn(async move { writer_task_tokio(&args, id).await }));
            }
            IoMode::Channel => {
                let io_core = io_cores.as_ref().as_ref().map(|c| c[id % c.len()]);
                handles.push(tokio::spawn(async move {
                    writer_task_channel(&args, id, io_core).await
                }));
            }
        }
    }

    let mut results = Vec::with_capacity(args.concurrency);
    for h in handles {
        results.push(h.await.expect("task panicked"));
    }

    let wall_elapsed = wall_start.elapsed();

    // Aggregate stats
    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut grand_total_bytes: u64 = 0;
    let mut grand_measured_bytes: u64 = 0;
    let total_writes: u64 = results.iter().map(|r| r.latencies_us.len() as u64).sum();

    for r in &results {
        grand_total_bytes += r.total_bytes;
        grand_measured_bytes += r.measured_bytes;
        for &lat in &r.latencies_us {
            hist.record(lat).ok();
        }
    }

    let throughput_mbs = grand_measured_bytes as f64 / 1024.0 / 1024.0 / wall_elapsed.as_secs_f64();
    let iops = total_writes as f64 / wall_elapsed.as_secs_f64();

    println!("\n=== Results ===");
    println!("Wall time:       {wall_elapsed:.2?}");
    println!("Total writes:    {total_writes} (excluding {} warmup)", args.warmup * args.concurrency);
    println!("Total data:      {:.2} MB (measured), {:.2} MB (including warmup)",
        grand_measured_bytes as f64 / 1024.0 / 1024.0,
        grand_total_bytes as f64 / 1024.0 / 1024.0);
    println!("Throughput:      {throughput_mbs:.2} MB/s");
    println!("IOPS:            {iops:.0}");
    println!();
    println!("Fsync latency (us):");
    println!("  min:    {:>10}", hist.min());
    println!("  p50:    {:>10}", hist.value_at_percentile(50.0));
    println!("  p90:    {:>10}", hist.value_at_percentile(90.0));
    println!("  p99:    {:>10}", hist.value_at_percentile(99.0));
    println!("  p99.9:  {:>10}", hist.value_at_percentile(99.9));
    println!("  max:    {:>10}", hist.max());

    // Per-task summary
    println!("\nPer-task breakdown:");
    println!("  {:>6}  {:>10}  {:>14}  {:>10}", "task", "writes", "measured bytes", "time");
    for r in &results {
        println!(
            "  {:>6}  {:>10}  {:>14}  {:>10.2?}",
            r.task_id, r.latencies_us.len(), r.measured_bytes, r.elapsed,
        );
    }

    if args.cleanup {
        println!("\nCleaning up WAL files...");
        for id in 0..args.concurrency {
            let path = args.dir.join(format!("wal-{id:04}.log"));
            if let Err(e) = std::fs::remove_file(&path) {
                eprintln!("  warning: failed to remove {}: {e}", path.display());
            }
        }
        println!("Done.");
    }
}
