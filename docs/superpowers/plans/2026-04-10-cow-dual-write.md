# COW Dual-Write Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `--replica-dir` flag that enables COW dual-write mode — each WAL write goes to both primary and replica directories with parallel sync, measuring latencies independently.

**Architecture:** Single-file modification to `src/main.rs`. Add `--replica-dir` CLI arg. Each of the three I/O modes (std, tokio, channel) gets COW support: std uses `tokio::spawn` for replica sync, tokio uses `tokio::join!`, channel spawns a second I/O thread. `TaskResult` gains per-side latency vectors. Reporting prints three histograms in COW mode.

**Tech Stack:** Rust, Tokio, clap, hdrhistogram (all existing dependencies — no new crates)

---

### Task 1: Add `--replica-dir` CLI arg and startup banner

**Files:**
- Modify: `src/main.rs:37-97` (Args struct, run function banner)

- [ ] **Step 1: Add the `replica_dir` field to `Args`**

Add after the `dir` field (line 40):

```rust
/// Replica directory for COW dual-write mode (enables writing to both --dir and --replica-dir)
#[arg(long)]
replica_dir: Option<PathBuf>,
```

- [ ] **Step 2: Update `run()` to create replica dir and print banner**

In `run()`, after `std::fs::create_dir_all(&args.dir)` (line 482), add:

```rust
if let Some(ref replica_dir) = args.replica_dir {
    std::fs::create_dir_all(replica_dir).expect("failed to create replica directory");
}
```

Update the startup banner println to include COW mode info. After the `io cores` line, add:

```rust
let cow_str = match &args.replica_dir {
    Some(rd) => format!("enabled ({})", rd.display()),
    None => "disabled".to_string(),
};
```

Add `\n  COW mode:       {}` to the format string and `cow_str` as the final argument.

- [ ] **Step 3: Build to verify it compiles**

Run: `cargo build 2>&1`
Expected: successful build, no errors

- [ ] **Step 4: Verify help output shows new flag**

Run: `cargo run -- --help 2>&1 | grep -A1 replica`
Expected: `--replica-dir` appears with description

- [ ] **Step 5: Commit**

```bash
git add src/main.rs
git commit -m "feat: add --replica-dir CLI arg and COW mode banner"
```

---

### Task 2: Extend `TaskResult` with per-side latencies

**Files:**
- Modify: `src/main.rs:203-209` (TaskResult struct)

- [ ] **Step 1: Add new fields to `TaskResult`**

Add two fields after `latencies_us`:

```rust
struct TaskResult {
    task_id: usize,
    total_bytes: u64,
    measured_bytes: u64,
    elapsed: Duration,
    latencies_us: Vec<u64>,
    primary_latencies_us: Vec<u64>,
    replica_latencies_us: Vec<u64>,
}
```

- [ ] **Step 2: Update all existing `TaskResult` construction sites**

In `writer_task_std` (around line 266), `writer_task_tokio` (around line 352), and `writer_task_channel` (around line 439), add the new fields to each `TaskResult` return:

```rust
primary_latencies_us: Vec::new(),
replica_latencies_us: Vec::new(),
```

These are empty in single-dir mode. COW mode will populate them in later tasks.

- [ ] **Step 3: Build to verify it compiles**

Run: `cargo build 2>&1`
Expected: successful build, no errors

- [ ] **Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: add primary/replica latency fields to TaskResult"
```

---

### Task 3: Update reporting to show three histograms in COW mode

**Files:**
- Modify: `src/main.rs:544-577` (results reporting in `run()`)

- [ ] **Step 1: Extract histogram printing into a helper function**

Add this function before `run()`:

```rust
fn print_histogram(label: &str, hist: &Histogram<u64>) {
    println!("Fsync latency{} (us):", label);
    println!("  min:    {:>10}", hist.min());
    println!("  p50:    {:>10}", hist.value_at_percentile(50.0));
    println!("  p90:    {:>10}", hist.value_at_percentile(90.0));
    println!("  p99:    {:>10}", hist.value_at_percentile(99.0));
    println!("  p99.9:  {:>10}", hist.value_at_percentile(99.9));
    println!("  max:    {:>10}", hist.max());
}
```

- [ ] **Step 2: Replace existing histogram printing with COW-aware logic**

Replace the histogram aggregation and printing block (after `let mut grand_measured_bytes`) with:

```rust
let cow_mode = args.replica_dir.is_some();
let mut hist = Histogram::<u64>::new(3).unwrap();
let mut primary_hist = Histogram::<u64>::new(3).unwrap();
let mut replica_hist = Histogram::<u64>::new(3).unwrap();

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
```

Replace the histogram println block with:

```rust
if cow_mode {
    print_histogram(" - combined", &hist);
    println!();
    print_histogram(" - primary", &primary_hist);
    println!();
    print_histogram(" - replica", &replica_hist);
} else {
    print_histogram("", &hist);
}
```

- [ ] **Step 3: Build and run in single-dir mode to verify output is unchanged**

Run: `cargo build 2>&1`
Expected: successful build

Run: `cargo run -- --dir /tmp/wal-test -c 1 -n 10 2>&1`
Expected: output looks the same as before (single histogram, no "combined/primary/replica" labels)

- [ ] **Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: add COW-aware histogram reporting with print_histogram helper"
```

---

### Task 4: Implement COW dual-write for std mode

**Files:**
- Modify: `src/main.rs:211-273` (writer_task_std)
- Modify: `src/main.rs:448-478` (main function, double worker threads)

- [ ] **Step 1: Double tokio worker threads when COW is enabled**

In `main()`, after parsing `--worker-cores` into `cores` and calling `rt_builder.worker_threads(cores.len())`, change the thread count logic. Replace the worker_cores block:

```rust
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
```

Note: the affinity uses `i % cores.len()` which already round-robins the doubled threads over the same core set.

- [ ] **Step 2: Add replica file and COW logic to `writer_task_std`**

Change `writer_task_std` signature to accept `&Args` (unchanged). After opening the primary file, conditionally open the replica file:

```rust
let replica_file = args.replica_dir.as_ref().map(|rd| {
    let rpath = rd.join(format!("wal-{task_id:04}.log"));
    open_direct(&rpath, args.direct, prealloc)
});
```

Add a second `AlignedBuf` for the replica if both `direct` and `replica_dir` are set:

```rust
let mut replica_abuf = if args.direct && replica_file.is_some() {
    Some(AlignedBuf::new(max_write_len))
} else {
    None
};
```

Add `primary_latencies_us` and `replica_latencies_us` vectors:

```rust
let mut primary_latencies_us = Vec::with_capacity(if replica_file.is_some() { args.writes_per_task } else { 0 });
let mut replica_latencies_us = Vec::with_capacity(if replica_file.is_some() { args.writes_per_task } else { 0 });
```

- [ ] **Step 3: Implement the per-write COW logic in std mode**

Replace the inner write loop body. The key change: when `replica_file` is Some, after building the record, spawn a tokio task for the replica write+sync and do the primary write+sync locally, then await the replica task.

For the O_DIRECT branch (when `abuf` is Some), after `build_record_into`:

```rust
if let Some(ref mut abuf) = abuf {
    write_len = build_record_into(abuf.as_mut_slice(), seq, &payload_buf[..payload_len], true);

    if let (Some(ref replica_file), Some(ref mut replica_abuf)) = (&replica_file, &mut replica_abuf) {
        // Copy record into replica's aligned buffer
        replica_abuf.as_mut_slice()[..write_len].copy_from_slice(abuf.as_slice(write_len));

        // Clone what we need for the spawned task
        use std::io::Write;
        use std::os::unix::io::{AsRawFd, FromRawFd};
        let replica_fd = replica_file.as_raw_fd();
        let sync_data = args.sync_data;
        // SAFETY: we duplicate the fd so the spawned task has its own handle
        let dup_fd = unsafe { libc::dup(replica_fd) };
        assert!(dup_fd >= 0, "dup failed");
        let replica_data = abuf.as_slice(write_len).to_vec();

        let t0 = Instant::now();

        let replica_handle = tokio::spawn(async move {
            let t_replica = Instant::now();
            let mut rfile = unsafe { std::fs::File::from_raw_fd(dup_fd) };
            rfile.write_all(&replica_data).expect("replica write failed");
            do_sync(&rfile, sync_data);
            // Prevent rfile from closing the dup'd fd? No — let it close, it's a dup.
            t_replica.elapsed().as_micros() as u64
        });

        file.write_all(abuf.as_slice(write_len)).expect("write failed");
        do_sync(&file, args.sync_data);
        let primary_lat = t0.elapsed().as_micros() as u64;

        let replica_lat = replica_handle.await.expect("replica task panicked");
        let combined_lat = primary_lat.max(replica_lat);

        if seq >= args.warmup as u64 {
            latencies_us.push(combined_lat);
            primary_latencies_us.push(primary_lat);
            replica_latencies_us.push(replica_lat);
            measured_bytes += write_len as u64;
        }
    } else {
        let t0 = Instant::now();
        file.write_all(abuf.as_slice(write_len)).expect("write failed");
        do_sync(&file, args.sync_data);
        let lat = t0.elapsed();
        if seq >= args.warmup as u64 {
            latencies_us.push(lat.as_micros() as u64);
            measured_bytes += write_len as u64;
        }
    }
}
```

Wait — using `dup` + `File::from_raw_fd` on each write is messy. Better approach: since std mode blocks the tokio thread anyway, we can use a simpler pattern. Keep the replica file as a `std::sync::Arc<std::sync::Mutex<std::fs::File>>` — no, that's also heavy.

Simplest correct approach: the replica file must be `Send + 'static` to go into `tokio::spawn`. Wrap it in `Arc<Mutex<File>>`:

Before the loop, wrap the replica file:

```rust
let replica_file = args.replica_dir.as_ref().map(|rd| {
    let rpath = rd.join(format!("wal-{task_id:04}.log"));
    std::sync::Arc::new(std::sync::Mutex::new(open_direct(&rpath, args.direct, prealloc)))
});
```

Then in the write loop, for the COW path:

```rust
if let Some(ref replica_file) = replica_file {
    let replica_file = replica_file.clone();
    let record_copy = abuf.as_slice(write_len).to_vec();
    let sync_data = args.sync_data;

    let t0 = Instant::now();

    let replica_handle = tokio::spawn(async move {
        use std::io::Write;
        let t_replica = Instant::now();
        let mut rfile = replica_file.lock().unwrap();
        rfile.write_all(&record_copy).expect("replica write failed");
        do_sync(&rfile, sync_data);
        t_replica.elapsed().as_micros() as u64
    });

    file.write_all(abuf.as_slice(write_len)).expect("write failed");
    do_sync(&file, args.sync_data);
    let primary_lat = t0.elapsed().as_micros() as u64;

    let replica_lat = replica_handle.await.expect("replica task panicked");
    let combined_lat = primary_lat.max(replica_lat);

    if seq >= args.warmup as u64 {
        latencies_us.push(combined_lat);
        primary_latencies_us.push(primary_lat);
        replica_latencies_us.push(replica_lat);
        measured_bytes += write_len as u64;
    }
}
```

Apply the same pattern for the non-O_DIRECT branch (when `abuf` is None), using `record_buf[..write_len].to_vec()` instead.

- [ ] **Step 4: Update TaskResult construction in `writer_task_std`**

```rust
TaskResult {
    task_id,
    total_bytes,
    measured_bytes,
    elapsed,
    latencies_us,
    primary_latencies_us,
    replica_latencies_us,
}
```

- [ ] **Step 5: Build and test std mode in single-dir (no regression)**

Run: `cargo build 2>&1`
Expected: successful build

Run: `cargo run -- --dir /tmp/wal-test -c 2 -n 20 -m std --cleanup 2>&1`
Expected: runs successfully, single histogram output, no COW labels

- [ ] **Step 6: Test std mode with `--replica-dir`**

Run: `cargo run -- --dir /tmp/wal-test --replica-dir /tmp/wal-test-replica -c 2 -n 20 -m std --cleanup 2>&1`
Expected: COW mode banner, three histograms (combined, primary, replica), files created in both dirs

- [ ] **Step 7: Commit**

```bash
git add src/main.rs
git commit -m "feat: implement COW dual-write for std I/O mode"
```

---

### Task 5: Implement COW dual-write for tokio mode

**Files:**
- Modify: `src/main.rs:275-359` (writer_task_tokio)

- [ ] **Step 1: Open replica file in `writer_task_tokio`**

After opening the primary file and preallocating, add:

```rust
let mut replica_file = if let Some(ref rd) = args.replica_dir {
    let rpath = rd.join(format!("wal-{task_id:04}.log"));
    let mut ropts = tokio::fs::OpenOptions::new();
    ropts.create(true).truncate(true).write(true);
    if args.direct {
        ropts.custom_flags(libc::O_DIRECT);
    }
    let rfile = ropts.open(&rpath).await
        .unwrap_or_else(|e| panic!("failed to open {}: {e}", rpath.display()));
    // Pre-allocate replica file
    {
        use std::os::unix::io::AsRawFd;
        let prealloc = estimate_prealloc(&args);
        if prealloc > 0 {
            let fd = rfile.as_raw_fd();
            let ret = unsafe { libc::fallocate(fd, 0, 0, prealloc as libc::off_t) };
            if ret != 0 {
                panic!("fallocate failed: {}", std::io::Error::last_os_error());
            }
        }
    }
    Some(rfile)
} else {
    None
};
```

Add latency vectors and a second aligned buffer:

```rust
let mut primary_latencies_us = Vec::with_capacity(if replica_file.is_some() { args.writes_per_task } else { 0 });
let mut replica_latencies_us = Vec::with_capacity(if replica_file.is_some() { args.writes_per_task } else { 0 });
let mut replica_abuf = if args.direct && replica_file.is_some() {
    Some(AlignedBuf::new(max_write_len))
} else {
    None
};
```

- [ ] **Step 2: Implement per-write COW logic using `tokio::join!`**

In the write loop, after building the record, when `replica_file` is Some:

For the O_DIRECT branch:

```rust
if let Some(ref mut abuf) = abuf {
    write_len = build_record_into(abuf.as_mut_slice(), seq, &payload_buf[..payload_len], true);

    if let (Some(ref mut rfile), Some(ref mut rabuf)) = (&mut replica_file, &mut replica_abuf) {
        rabuf.as_mut_slice()[..write_len].copy_from_slice(abuf.as_slice(write_len));
        let sync_data = args.sync_data;

        let t0 = Instant::now();
        let (primary_res, replica_res) = tokio::join!(
            async {
                let t = Instant::now();
                file.write_all(abuf.as_slice(write_len)).await.expect("write failed");
                if sync_data { file.sync_data().await.expect("fdatasync failed"); }
                else { file.sync_all().await.expect("fsync failed"); }
                t.elapsed().as_micros() as u64
            },
            async {
                let t = Instant::now();
                rfile.write_all(rabuf.as_slice(write_len)).await.expect("replica write failed");
                if sync_data { rfile.sync_data().await.expect("replica fdatasync failed"); }
                else { rfile.sync_all().await.expect("replica fsync failed"); }
                t.elapsed().as_micros() as u64
            }
        );

        if seq >= args.warmup as u64 {
            primary_latencies_us.push(primary_res);
            replica_latencies_us.push(replica_res);
            latencies_us.push(primary_res.max(replica_res));
            measured_bytes += write_len as u64;
        }
    } else {
        let t0 = Instant::now();
        file.write_all(abuf.as_slice(write_len)).await.expect("write failed");
        if args.sync_data { file.sync_data().await.expect("fdatasync failed"); }
        else { file.sync_all().await.expect("fsync failed"); }
        let lat = t0.elapsed();
        if seq >= args.warmup as u64 {
            latencies_us.push(lat.as_micros() as u64);
            measured_bytes += write_len as u64;
        }
    }
}
```

Apply the same pattern for the non-O_DIRECT branch using `record_buf`.

Note: `tokio::join!` requires mutable borrows of both `file` and `rfile`. Since they are separate variables, this works — no borrow conflict.

- [ ] **Step 3: Update TaskResult construction**

Same as Task 4 Step 4 — include `primary_latencies_us` and `replica_latencies_us`.

- [ ] **Step 4: Build and test tokio mode**

Run: `cargo build 2>&1`
Expected: successful build

Run: `cargo run -- --dir /tmp/wal-test --replica-dir /tmp/wal-test-replica -c 2 -n 20 -m tokio --cleanup 2>&1`
Expected: three histograms, no errors

- [ ] **Step 5: Commit**

```bash
git add src/main.rs
git commit -m "feat: implement COW dual-write for tokio I/O mode"
```

---

### Task 6: Implement COW dual-write for channel mode

**Files:**
- Modify: `src/main.rs:361-446` (writer_task_channel)

- [ ] **Step 1: Spawn second I/O thread for replica**

In `writer_task_channel`, after setting up the primary I/O thread, conditionally spawn a replica I/O thread with its own channel pair:

```rust
let replica_channels = args.replica_dir.as_ref().map(|rd| {
    let rpath = rd.join(format!("wal-{task_id:04}.log"));
    let direct_r = direct;
    let sync_data_r = sync_data;
    let prealloc_r = prealloc;
    let max_write_len_r = max_write_len;

    let (rtx, rrx) = std::sync::mpsc::sync_channel::<(Vec<u8>, usize)>(1);
    let (rret_tx, rret_rx) = tokio::sync::mpsc::channel::<(Vec<u8>, u64)>(1);

    // Replica I/O thread returns (buffer, latency_us) instead of just buffer
    let replica_io_core = io_core; // same pinning strategy
    let replica_thread = std::thread::spawn(move || {
        if let Some(core) = replica_io_core {
            core_affinity::set_for_current(core);
        }
        use std::io::Write;

        let mut file = open_direct(&rpath, direct_r, prealloc_r);
        let mut abuf = if direct_r { Some(AlignedBuf::new(max_write_len_r)) } else { None };

        while let Ok((buf, len)) = rrx.recv() {
            let t = Instant::now();
            if let Some(ref mut abuf) = abuf {
                abuf.as_mut_slice()[..len].copy_from_slice(&buf[..len]);
                file.write_all(abuf.as_slice(len)).expect("replica write failed");
            } else {
                file.write_all(&buf[..len]).expect("replica write failed");
            }
            do_sync(&file, sync_data_r);
            let lat = t.elapsed().as_micros() as u64;
            if rret_tx.blocking_send((buf, lat)).is_err() {
                break;
            }
        }
    });

    (rtx, rret_rx, replica_thread)
});
```

- [ ] **Step 2: Update the primary I/O thread to also return latency**

Change the primary channel return type from `Vec<u8>` to `(Vec<u8>, u64)`:

```rust
let (ret_tx, mut ret_rx) = tokio::sync::mpsc::channel::<(Vec<u8>, u64)>(1);
```

In the primary I/O thread, measure latency and send it back:

```rust
while let Ok((buf, len)) = rx.recv() {
    let t = Instant::now();
    if let Some(ref mut abuf) = abuf {
        abuf.as_mut_slice()[..len].copy_from_slice(&buf[..len]);
        file.write_all(abuf.as_slice(len)).expect("write failed");
    } else {
        file.write_all(&buf[..len]).expect("write failed");
    }
    do_sync(&file, sync_data);
    let lat = t.elapsed().as_micros() as u64;
    if ret_tx.blocking_send((buf, lat)).is_err() {
        break;
    }
}
```

- [ ] **Step 3: Implement per-write COW logic in the send/recv loop**

```rust
let mut primary_latencies_us = Vec::with_capacity(if replica_channels.is_some() { args.writes_per_task } else { 0 });
let mut replica_latencies_us = Vec::with_capacity(if replica_channels.is_some() { args.writes_per_task } else { 0 });

for seq in 0..total_writes as u64 {
    let payload_len = rng.r#gen_range(args.min_record_size..=args.max_record_size);
    rng.fill_bytes(&mut payload_buf[..payload_len]);
    let write_len = build_record_into(&mut send_buf, seq, &payload_buf[..payload_len], direct);
    total_bytes += write_len as u64;

    let t0 = Instant::now();

    // Send to primary
    tx.send((send_buf, write_len)).expect("send failed");

    // Send to replica (need a second buffer)
    if let Some((ref rtx, _, _)) = replica_channels {
        let mut replica_buf = spare_buf.take().unwrap_or_else(|| vec![0u8; max_write_len]);
        replica_buf[..write_len].copy_from_slice(&send_buf); // send_buf was moved — need to handle this
    }
    // ...
}
```

Wait — `send_buf` is moved into `tx.send()`. We need to copy before sending. Revised approach:

```rust
for seq in 0..total_writes as u64 {
    let payload_len = rng.r#gen_range(args.min_record_size..=args.max_record_size);
    rng.fill_bytes(&mut payload_buf[..payload_len]);
    let write_len = build_record_into(&mut send_buf, seq, &payload_buf[..payload_len], direct);
    total_bytes += write_len as u64;

    let t0 = Instant::now();

    if let Some((ref rtx, ref mut rret_rx, _)) = replica_channels {
        // Copy record to replica buffer before sending primary (which moves send_buf)
        let mut replica_buf = spare_buf.take().unwrap_or_else(|| vec![0u8; max_write_len]);
        replica_buf[..write_len].copy_from_slice(&send_buf[..write_len]);
        rtx.send((replica_buf, write_len)).expect("replica send failed");

        // Send to primary
        tx.send((send_buf, write_len)).expect("send failed");

        // Await both
        let (primary_ret, primary_lat) = ret_rx.recv().await.expect("recv failed");
        let (replica_ret, replica_lat) = rret_rx.recv().await.expect("replica recv failed");

        send_buf = primary_ret;
        spare_buf = Some(replica_ret);

        let combined_lat = primary_lat.max(replica_lat);

        if seq >= args.warmup as u64 {
            latencies_us.push(combined_lat);
            primary_latencies_us.push(primary_lat);
            replica_latencies_us.push(replica_lat);
            measured_bytes += write_len as u64;
        }
    } else {
        tx.send((send_buf, write_len)).expect("send failed");
        let (ret, primary_lat) = ret_rx.recv().await.expect("recv failed");
        send_buf = ret;

        if seq >= args.warmup as u64 {
            latencies_us.push(primary_lat);
            measured_bytes += write_len as u64;
        }
    }
}
```

- [ ] **Step 4: Clean up replica thread on exit**

After `drop(tx)` and `io_thread.join()`:

```rust
if let Some((rtx, _, replica_thread)) = replica_channels {
    drop(rtx);
    replica_thread.join().expect("replica io thread panicked");
}
```

- [ ] **Step 5: Update TaskResult construction**

Same as previous tasks — include `primary_latencies_us` and `replica_latencies_us`.

- [ ] **Step 6: Build and test channel mode**

Run: `cargo build 2>&1`
Expected: successful build

Run: `cargo run -- --dir /tmp/wal-test --replica-dir /tmp/wal-test-replica -c 2 -n 20 -m channel --cleanup 2>&1`
Expected: three histograms, no errors

- [ ] **Step 7: Commit**

```bash
git add src/main.rs
git commit -m "feat: implement COW dual-write for channel I/O mode"
```

---

### Task 7: Update cleanup to remove replica files

**Files:**
- Modify: `src/main.rs:588-597` (cleanup block in `run()`)

- [ ] **Step 1: Add replica cleanup**

After the existing cleanup loop, add:

```rust
if let Some(ref replica_dir) = args.replica_dir {
    for id in 0..args.concurrency {
        let path = replica_dir.join(format!("wal-{id:04}.log"));
        if let Err(e) = std::fs::remove_file(&path) {
            eprintln!("  warning: failed to remove {}: {e}", path.display());
        }
    }
}
```

- [ ] **Step 2: Build and test cleanup**

Run: `cargo run -- --dir /tmp/wal-test --replica-dir /tmp/wal-test-replica -c 2 -n 10 -m std --cleanup 2>&1`
Expected: "Cleaning up WAL files..." message, files removed from both dirs

Verify files are gone:

Run: `ls /tmp/wal-test/wal-*.log /tmp/wal-test-replica/wal-*.log 2>&1`
Expected: "No such file or directory" for both

- [ ] **Step 3: Commit**

```bash
git add src/main.rs
git commit -m "feat: clean up replica files with --cleanup flag"
```

---

### Task 8: End-to-end validation across all modes

- [ ] **Step 1: Test all three modes with COW**

Run each and verify three histograms appear, no panics:

```bash
cargo run --release -- --dir /tmp/wal-test --replica-dir /tmp/wal-test-replica -c 4 -n 100 -m std --cleanup 2>&1
cargo run --release -- --dir /tmp/wal-test --replica-dir /tmp/wal-test-replica -c 4 -n 100 -m tokio --cleanup 2>&1
cargo run --release -- --dir /tmp/wal-test --replica-dir /tmp/wal-test-replica -c 4 -n 100 -m channel --cleanup 2>&1
```

- [ ] **Step 2: Test all three modes without COW (no regression)**

```bash
cargo run --release -- --dir /tmp/wal-test -c 4 -n 100 -m std --cleanup 2>&1
cargo run --release -- --dir /tmp/wal-test -c 4 -n 100 -m tokio --cleanup 2>&1
cargo run --release -- --dir /tmp/wal-test -c 4 -n 100 -m channel --cleanup 2>&1
```

- [ ] **Step 3: Test with O_DIRECT and fdatasync**

```bash
cargo run --release -- --dir /tmp/wal-test --replica-dir /tmp/wal-test-replica -c 2 -n 50 -m channel --direct --sync-data --cleanup 2>&1
```

- [ ] **Step 4: Final commit if any fixes were needed**

```bash
git add src/main.rs
git commit -m "fix: address issues found during end-to-end COW validation"
```
