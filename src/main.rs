mod common;
mod mode_std;
mod mode_tokio;
mod mode_channel;
mod mode_thread;

use std::time::Instant;

use clap::Parser;
use hdrhistogram::Histogram;

use common::*;

fn print_histogram(label: &str, hist: &Histogram<u64>) {
    println!("Fsync latency{} (us):", label);
    println!("  min:    {:>10}", hist.min());
    println!("  p50:    {:>10}", hist.value_at_percentile(50.0));
    println!("  p90:    {:>10}", hist.value_at_percentile(90.0));
    println!("  p99:    {:>10}", hist.value_at_percentile(99.0));
    println!("  p99.9:  {:>10}", hist.value_at_percentile(99.9));
    println!("  max:    {:>10}", hist.max());
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
        let thread_count = if args.replica_dir.is_some() {
            cores.len() * 2
        } else {
            cores.len()
        };
        rt_builder.worker_threads(thread_count);
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
        IoMode::Thread => "thread",
    };

    let worker_cores_str = args.worker_cores.as_deref().unwrap_or("all");
    let io_cores_str = args.io_cores.as_deref().unwrap_or("none");
    let cow_str = match &args.replica_dir {
        Some(rd) => format!("enabled ({})", rd.display()),
        None => "disabled".to_string(),
    };

    println!(
        "Starting WAL sync-write benchmark\n  dir:            {}\n  concurrency:    {}\n  writes/task:    {}\n  warmup/task:    {}\n  record size:    {}..{} bytes\n  io mode:        {}\n  O_DIRECT:       {}\n  sync mode:      {}\n  worker cores:   {}\n  io cores:       {}\n  COW mode:       {}\n  spread dirs:    {}",
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
        args.spread_dirs,
    );

    // Parse io_cores once, share across tasks
    let io_cores: Option<Vec<core_affinity::CoreId>> = args.io_cores.as_ref().map(|s| {
        parse_cores(s).expect("invalid --io-cores")
    });

    // Pre-create per-task subdirectories when --spread-dirs is enabled
    if args.spread_dirs {
        for id in 0..args.concurrency {
            let subdir = args.dir.join(format!("sub-{id:04}"));
            std::fs::create_dir_all(&subdir)
                .unwrap_or_else(|e| panic!("failed to create {}: {e}", subdir.display()));
            if let Some(ref rd) = args.replica_dir {
                let rsubdir = rd.join(format!("sub-{id:04}"));
                std::fs::create_dir_all(&rsubdir)
                    .unwrap_or_else(|e| panic!("failed to create {}: {e}", rsubdir.display()));
            }
        }
    }

    let args = std::sync::Arc::new(args);
    let io_cores = std::sync::Arc::new(io_cores);
    let wall_start = Instant::now();

    let mut results = Vec::with_capacity(args.concurrency);

    if matches!(args.io_mode, IoMode::Thread) {
        let mut thread_handles = Vec::with_capacity(args.concurrency);
        for id in 0..args.concurrency {
            let args = args.clone();
            let io_cores = io_cores.clone();
            let io_core = io_cores.as_ref().as_ref().map(|c| c[id % c.len()]);
            thread_handles.push(std::thread::spawn(move || {
                mode_thread::writer_task_thread(&args, id, io_core)
            }));
        }
        for h in thread_handles {
            results.push(h.join().expect("thread panicked"));
        }
    } else {
        let mut handles = Vec::with_capacity(args.concurrency);
        for id in 0..args.concurrency {
            let args = args.clone();
            let io_cores = io_cores.clone();
            match args.io_mode {
                IoMode::Std => {
                    handles.push(tokio::spawn(async move { mode_std::writer_task_std(&args, id).await }));
                }
                IoMode::Tokio => {
                    handles.push(tokio::spawn(async move { mode_tokio::writer_task_tokio(&args, id).await }));
                }
                IoMode::Channel => {
                    let io_core = io_cores.as_ref().as_ref().map(|c| c[id % c.len()]);
                    handles.push(tokio::spawn(async move {
                        mode_channel::writer_task_channel(&args, id, io_core).await
                    }));
                }
                IoMode::Thread => unreachable!(),
            }
        }
        for h in handles {
            results.push(h.await.expect("task panicked"));
        }
    }

    let wall_elapsed = wall_start.elapsed();

    // Aggregate stats
    let cow_mode = args.replica_dir.is_some();
    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut primary_hist = Histogram::<u64>::new(3).unwrap();
    let mut replica_hist = Histogram::<u64>::new(3).unwrap();
    let mut grand_total_bytes: u64 = 0;
    let mut grand_measured_bytes: u64 = 0;
    let total_writes: u64 = results.iter().map(|r| r.latencies_us.len() as u64).sum();

    for r in &results {
        grand_total_bytes += r.total_bytes;
        grand_measured_bytes += r.measured_bytes;
        for &lat in &r.latencies_us {
            hist.record(lat).ok();
        }
        for &lat in &r.primary_latencies_us {
            primary_hist.record(lat).ok();
        }
        for &lat in &r.replica_latencies_us {
            replica_hist.record(lat).ok();
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
    if cow_mode {
        print_histogram(" - combined", &hist);
        println!();
        print_histogram(" - primary", &primary_hist);
        println!();
        print_histogram(" - replica", &replica_hist);
    } else {
        print_histogram("", &hist);
    }

    // Per-task summary
    println!("\nPer-task breakdown:");
    println!("  {:>6}  {:>10}  {:>14}  {:>10}", "task", "writes", "measured bytes", "time");
    for r in &results {
        println!(
            "  {:>6}  {:>10}  {:>14}  {:>10.2?}",
            r.task_id, r.latencies_us.len(), r.measured_bytes, r.elapsed,
        );
    }

    if args.trace_sync {
        use std::io::Write;
        let trace_path = args.dir.join("sync-trace.tsv");
        let mut f = std::fs::File::create(&trace_path)
            .unwrap_or_else(|e| panic!("failed to create {}: {e}", trace_path.display()));
        writeln!(f, "task\tsource\tstart_ns\tend_ns").unwrap();
        for r in &results {
            for ev in &r.primary_trace {
                writeln!(f, "{}\tprimary\t{}\t{}", r.task_id, ev.start_ns, ev.end_ns).unwrap();
            }
            for ev in &r.replica_trace {
                writeln!(f, "{}\treplica\t{}\t{}", r.task_id, ev.start_ns, ev.end_ns).unwrap();
            }
        }
        println!("\nSync trace written to {}", trace_path.display());
    }

    if args.cleanup {
        println!("\nCleaning up WAL files...");
        for id in 0..args.concurrency {
            let path = wal_path(&args.dir, id, args.spread_dirs);
            if let Err(e) = std::fs::remove_file(&path) {
                eprintln!("  warning: failed to remove {}: {e}", path.display());
            }
            if args.spread_dirs {
                let subdir = args.dir.join(format!("sub-{id:04}"));
                let _ = std::fs::remove_dir(&subdir);
            }
        }
        if let Some(ref replica_dir) = args.replica_dir {
            for id in 0..args.concurrency {
                let path = wal_path(replica_dir, id, args.spread_dirs);
                if let Err(e) = std::fs::remove_file(&path) {
                    eprintln!("  warning: failed to remove {}: {e}", path.display());
                }
                if args.spread_dirs {
                    let subdir = replica_dir.join(format!("sub-{id:04}"));
                    let _ = std::fs::remove_dir(&subdir);
                }
            }
        }
        println!("Done.");
    }
}
