use std::time::Instant;

use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use tokio::io::AsyncWriteExt;

use crate::common::*;

async fn do_sync_tokio(file: &tokio::fs::File, sync_mode: SyncMode) {
    match sync_mode {
        SyncMode::Fsync => file.sync_all().await.expect("fsync failed"),
        SyncMode::Fdatasync => file.sync_data().await.expect("fdatasync failed"),
        SyncMode::Dsync | SyncMode::None => {}
    }
}

pub async fn writer_task_tokio(args: &Args, task_id: usize) -> TaskResult {
    let path = wal_path(&args.dir, task_id, args.spread_dirs);
    let mut opts = tokio::fs::OpenOptions::new();
    opts.create(true).truncate(true).write(true);
    {
        let mut flags = 0;
        if args.direct { flags |= libc::O_DIRECT; }
        if args.sync_mode == SyncMode::Dsync { flags |= libc::O_DSYNC; }
        if flags != 0 { opts.custom_flags(flags); }
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

    // Open replica file if COW mode is enabled
    let mut replica_file = if let Some(ref rd) = args.replica_dir {
        let rpath = wal_path(rd, task_id, args.spread_dirs);
        let mut ropts = tokio::fs::OpenOptions::new();
        ropts.create(true).truncate(true).write(true);
        {
            let mut flags = 0;
            if args.direct { flags |= libc::O_DIRECT; }
            if args.sync_mode == SyncMode::Dsync { flags |= libc::O_DSYNC; }
            if flags != 0 { ropts.custom_flags(flags); }
        }
        let rfile = ropts.open(&rpath).await
            .unwrap_or_else(|e| panic!("failed to open {}: {e}", rpath.display()));
        // Pre-allocate replica file
        {
            use std::os::unix::io::AsRawFd;
            let prealloc = estimate_prealloc(args);
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

    let mut rng = StdRng::from_entropy();
    let mut total_bytes: u64 = 0;
    let mut measured_bytes: u64 = 0;
    let mut latencies_us = Vec::with_capacity(args.writes_per_task);
    let max_record_raw = 4 + 8 + args.max_record_size;
    let max_write_len = max_record_raw.div_ceil(ALIGN) * ALIGN;

    let mut payload_buf = vec![0u8; args.max_record_size];
    let mut record_buf = vec![0u8; max_write_len];
    let mut abuf = if args.direct { Some(AlignedBuf::new(max_write_len)) } else { None };

    let mut primary_latencies_us = Vec::with_capacity(if replica_file.is_some() { args.writes_per_task } else { 0 });
    let mut replica_latencies_us = Vec::with_capacity(if replica_file.is_some() { args.writes_per_task } else { 0 });
    let mut replica_abuf = if args.direct && replica_file.is_some() {
        Some(AlignedBuf::new(max_write_len))
    } else {
        None
    };
    let mut replica_record_buf = if replica_file.is_some() && !args.direct { vec![0u8; max_write_len] } else { vec![] };

    let total_writes = args.warmup + args.writes_per_task;
    let start = Instant::now();

    for seq in 0..total_writes as u64 {
        let payload_len = rng.r#gen_range(args.min_record_size..=args.max_record_size);
        rng.fill_bytes(&mut payload_buf[..payload_len]);

        let write_len;
        if let Some(ref mut abuf) = abuf {
            write_len = build_record_into(abuf.as_mut_slice(), seq, &payload_buf[..payload_len], true);

            if let (Some(rfile), Some(rabuf)) = (&mut replica_file, &mut replica_abuf) {
                rabuf.as_mut_slice()[..write_len].copy_from_slice(abuf.as_slice(write_len));
                let sync_mode = args.sync_mode;

                let (primary_lat, replica_lat) = tokio::join!(
                    async {
                        let t = Instant::now();
                        file.write_all(abuf.as_slice(write_len)).await.expect("write failed");
                        do_sync_tokio(&file, sync_mode).await;
                        t.elapsed().as_micros() as u64
                    },
                    async {
                        let t = Instant::now();
                        rfile.write_all(rabuf.as_slice(write_len)).await.expect("replica write failed");
                        do_sync_tokio(&rfile, sync_mode).await;
                        t.elapsed().as_micros() as u64
                    }
                );

                if seq >= args.warmup as u64 {
                    primary_latencies_us.push(primary_lat);
                    replica_latencies_us.push(replica_lat);
                    latencies_us.push(primary_lat.max(replica_lat));
                    measured_bytes += write_len as u64;
                }
            } else {
                let t0 = Instant::now();
                file.write_all(abuf.as_slice(write_len)).await.expect("write failed");
                do_sync_tokio(&file, args.sync_mode).await;
                let lat = t0.elapsed();
                if seq >= args.warmup as u64 {
                    latencies_us.push(lat.as_micros() as u64);
                    measured_bytes += write_len as u64;
                }
            }
        } else {
            write_len = build_record_into(&mut record_buf, seq, &payload_buf[..payload_len], false);

            if let Some(ref mut rfile) = replica_file {
                replica_record_buf[..write_len].copy_from_slice(&record_buf[..write_len]);
                let sync_mode = args.sync_mode;

                let (primary_lat, replica_lat) = tokio::join!(
                    async {
                        let t = Instant::now();
                        file.write_all(&record_buf[..write_len]).await.expect("write failed");
                        do_sync_tokio(&file, sync_mode).await;
                        t.elapsed().as_micros() as u64
                    },
                    async {
                        let t = Instant::now();
                        rfile.write_all(&replica_record_buf[..write_len]).await.expect("replica write failed");
                        do_sync_tokio(&rfile, sync_mode).await;
                        t.elapsed().as_micros() as u64
                    }
                );

                if seq >= args.warmup as u64 {
                    primary_latencies_us.push(primary_lat);
                    replica_latencies_us.push(replica_lat);
                    latencies_us.push(primary_lat.max(replica_lat));
                    measured_bytes += write_len as u64;
                }
            } else {
                let t0 = Instant::now();
                file.write_all(&record_buf[..write_len]).await.expect("write failed");
                do_sync_tokio(&file, args.sync_mode).await;
                let lat = t0.elapsed();
                if seq >= args.warmup as u64 {
                    latencies_us.push(lat.as_micros() as u64);
                    measured_bytes += write_len as u64;
                }
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
        primary_latencies_us,
        replica_latencies_us,
        primary_trace: Vec::new(),
        replica_trace: Vec::new(),
    }
}
