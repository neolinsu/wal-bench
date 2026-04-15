use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};

/// Parse a core range like "0-3" or "0,2,4" or "0-3,6,8-10" into a Vec<CoreId>.
pub fn parse_cores(s: &str) -> Result<Vec<core_affinity::CoreId>, String> {
    let mut cores = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if let Some((a, b)) = part.split_once('-') {
            let start: usize = a.trim().parse().map_err(|e| format!("invalid core: {e}"))?;
            let end: usize = b.trim().parse().map_err(|e| format!("invalid core: {e}"))?;
            for id in start..=end {
                cores.push(core_affinity::CoreId { id });
            }
        } else {
            let id: usize = part.parse().map_err(|e| format!("invalid core: {e}"))?;
            cores.push(core_affinity::CoreId { id });
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
pub struct Args {
    /// Directory to write files into (point this at an NFS mount)
    #[arg(short, long)]
    pub dir: PathBuf,

    /// Replica directory for COW dual-write mode (enables writing to both --dir and --replica-dir)
    #[arg(long)]
    pub replica_dir: Option<PathBuf>,

    /// Number of concurrent writer tasks
    #[arg(short = 'c', long, default_value_t = 4)]
    pub concurrency: usize,

    /// Total number of writes per task
    #[arg(short = 'n', long, default_value_t = 1000)]
    pub writes_per_task: usize,

    /// Number of warmup writes per task (excluded from stats)
    #[arg(long, default_value_t = 0)]
    pub warmup: usize,

    /// Minimum WAL record size in bytes
    #[arg(long, default_value_t = 64)]
    pub min_record_size: usize,

    /// Maximum WAL record size in bytes
    #[arg(long, default_value_t = 8192)]
    pub max_record_size: usize,

    /// Pin tokio worker threads to these CPU cores (e.g. "0-3" or "0,2,4")
    #[arg(long)]
    pub worker_cores: Option<String>,

    /// Pin I/O threads (channel mode) to these CPU cores (e.g. "4-7")
    #[arg(long)]
    pub io_cores: Option<String>,

    /// Max blocking threads for tokio runtime (default: 512)
    #[arg(long)]
    pub max_blocking_threads: Option<usize>,

    /// I/O mode: "std" for std::fs sync I/O, "tokio" for tokio::fs async I/O,
    /// "channel" for dedicated I/O threads communicating via channels,
    /// "thread" for pure kernel threads
    #[arg(short = 'm', long, default_value = "std")]
    pub io_mode: IoMode,

    /// Open files with O_DIRECT (bypass page cache)
    #[arg(long, default_value_t = false)]
    pub direct: bool,

    /// Sync mode: fsync, fdatasync, dsync (O_DSYNC on open, no explicit sync), none (no sync)
    #[arg(long, default_value = "fsync")]
    pub sync_mode: SyncMode,

    /// Remove WAL files after benchmark completes
    #[arg(long, default_value_t = false)]
    pub cleanup: bool,

    /// Spread files across per-task subdirectories to distribute XFS allocation groups
    #[arg(long, default_value_t = false)]
    pub spread_dirs: bool,

    /// Write per-fdatasync timestamp trace to sync-trace.tsv
    #[arg(long, default_value_t = false)]
    pub trace_sync: bool,

    /// Stagger task starts by N microseconds per task (task k sleeps k*N us)
    #[arg(long, default_value_t = 0)]
    pub stagger_us: u64,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum IoMode {
    Std,
    Tokio,
    Channel,
    Thread,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SyncMode {
    /// fsync after every write
    Fsync,
    /// fdatasync after every write (skip metadata sync)
    Fdatasync,
    /// O_DSYNC on open -- kernel issues FUA per write, no explicit sync call
    Dsync,
    /// No sync at all (baseline measurement)
    None,
}

/// Compute WAL file path. With `spread_dirs`, each task gets its own subdirectory
/// so XFS places inodes in different allocation groups.
pub fn wal_path(dir: &std::path::Path, task_id: usize, spread_dirs: bool) -> PathBuf {
    if spread_dirs {
        dir.join(format!("sub-{task_id:04}")).join(format!("wal-{task_id:04}.log"))
    } else {
        dir.join(format!("wal-{task_id:04}.log"))
    }
}

pub const ALIGN: usize = 512;

/// Build a WAL-like record in-place into `buf`:
/// 4-byte length prefix + 8-byte sequence number + payload.
/// Returns the number of bytes to write (padded to ALIGN when direct).
pub fn build_record_into(buf: &mut [u8], seq: u64, payload: &[u8], direct: bool) -> usize {
    let total_len = 8 + payload.len();
    let raw_len = 4 + total_len;
    let write_len = if direct {
        raw_len.div_ceil(ALIGN) * ALIGN
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
pub struct AlignedBuf {
    ptr: *mut u8,
    capacity: usize,
}

impl AlignedBuf {
    pub fn new(capacity: usize) -> Self {
        use std::alloc::{Layout, alloc_zeroed};
        let cap = capacity.div_ceil(ALIGN) * ALIGN;
        let layout = Layout::from_size_align(cap, ALIGN).expect("invalid layout");
        let ptr = unsafe { alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "allocation failed");
        Self { ptr, capacity: cap }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.capacity) }
    }

    pub fn as_slice(&self, len: usize) -> &[u8] {
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
unsafe impl Sync for AlignedBuf {}

/// Open a file with optional O_DIRECT and O_DSYNC, returning a std::fs::File.
/// Pre-allocates space so that fdatasync doesn't need to update extents/size.
pub fn open_direct(path: &std::path::Path, direct: bool, sync_mode: SyncMode, prealloc_bytes: u64) -> std::fs::File {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;
    let mut opts = std::fs::OpenOptions::new();
    opts.create(true).truncate(true).write(true);
    let mut flags = 0;
    if direct {
        flags |= libc::O_DIRECT;
    }
    if sync_mode == SyncMode::Dsync {
        flags |= libc::O_DSYNC;
    }
    if flags != 0 {
        opts.custom_flags(flags);
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

/// Sync file according to the chosen sync mode.
/// Dsync and None are no-ops (durability handled by O_DSYNC or skipped).
pub fn do_sync(file: &std::fs::File, sync_mode: SyncMode) {
    match sync_mode {
        SyncMode::Fsync => file.sync_all().expect("fsync failed"),
        SyncMode::Fdatasync => file.sync_data().expect("fdatasync failed"),
        SyncMode::Dsync | SyncMode::None => {}
    }
}

/// Estimate preallocation size using max record size.
/// Over-allocates slightly but avoids extent metadata updates mid-benchmark.
pub fn estimate_prealloc(args: &Args) -> u64 {
    let max_record = 4 + 8 + args.max_record_size;
    let per_write = if args.direct {
        max_record.div_ceil(ALIGN) * ALIGN
    } else {
        max_record
    };
    let total_writes = args.writes_per_task + args.warmup;
    (per_write * total_writes) as u64
}

pub struct SyncEvent {
    pub start_ns: u64,
    pub end_ns: u64,
}

pub struct TaskResult {
    pub task_id: usize,
    pub total_bytes: u64,
    pub measured_bytes: u64,
    pub elapsed: Duration,
    pub latencies_us: Vec<u64>,
    pub primary_latencies_us: Vec<u64>,
    pub replica_latencies_us: Vec<u64>,
    pub primary_trace: Vec<SyncEvent>,
    pub replica_trace: Vec<SyncEvent>,
}
