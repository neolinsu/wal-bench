use std::time::Instant;

use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

use crate::common::*;

pub async fn writer_task_std(args: &Args, task_id: usize) -> TaskResult {
    use std::io::Write;

    let path = wal_path(&args.dir, task_id, args.spread_dirs);
    let prealloc = estimate_prealloc(args);
    let mut file = open_direct(&path, args.direct, args.sync_mode, prealloc);

    let replica_file = args.replica_dir.as_ref().map(|rd| {
        let rpath = wal_path(rd, task_id, args.spread_dirs);
        std::sync::Arc::new(std::sync::Mutex::new(open_direct(&rpath, args.direct, args.sync_mode, prealloc)))
    });

    let mut rng = StdRng::from_entropy();
    let mut total_bytes: u64 = 0;
    let mut measured_bytes: u64 = 0;
    let mut latencies_us = Vec::with_capacity(args.writes_per_task);
    let max_record_raw = 4 + 8 + args.max_record_size;
    let max_write_len = max_record_raw.div_ceil(ALIGN) * ALIGN;

    // Pre-allocate payload and write buffers
    let mut payload_buf = vec![0u8; args.max_record_size];
    let mut record_buf = vec![0u8; max_write_len];
    let mut abuf = if args.direct { Some(AlignedBuf::new(max_write_len)) } else { None };
    let mut primary_latencies_us = Vec::with_capacity(if replica_file.is_some() { args.writes_per_task } else { 0 });
    let mut replica_latencies_us = Vec::with_capacity(if replica_file.is_some() { args.writes_per_task } else { 0 });

    let total_writes = args.warmup + args.writes_per_task;
    let start = Instant::now();

    for seq in 0..total_writes as u64 {
        let payload_len = rng.r#gen_range(args.min_record_size..=args.max_record_size);
        rng.fill_bytes(&mut payload_buf[..payload_len]);

        let write_len;
        if let Some(ref mut abuf) = abuf {
            // O_DIRECT: build directly into aligned buffer
            write_len = build_record_into(abuf.as_mut_slice(), seq, &payload_buf[..payload_len], true);

            if let Some(ref replica_arc) = replica_file {
                let replica_arc = replica_arc.clone();
                let record_copy = abuf.as_slice(write_len).to_vec();
                let sync_mode = args.sync_mode;

                let t0 = Instant::now();

                let replica_handle = tokio::spawn(async move {
                    use std::io::Write;
                    let t_replica = Instant::now();
                    let mut rfile = replica_arc.lock().unwrap();
                    rfile.write_all(&record_copy).expect("replica write failed");
                    do_sync(&rfile, sync_mode);
                    t_replica.elapsed().as_micros() as u64
                });

                file.write_all(abuf.as_slice(write_len)).expect("write failed");
                do_sync(&file, args.sync_mode);
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
                do_sync(&file, args.sync_mode);
                let lat = t0.elapsed();
                if seq >= args.warmup as u64 {
                    latencies_us.push(lat.as_micros() as u64);
                    measured_bytes += write_len as u64;
                }
            }
        } else {
            write_len = build_record_into(&mut record_buf, seq, &payload_buf[..payload_len], false);

            if let Some(ref replica_arc) = replica_file {
                let replica_arc = replica_arc.clone();
                let record_copy = record_buf[..write_len].to_vec();
                let sync_mode = args.sync_mode;

                let t0 = Instant::now();

                let replica_handle = tokio::spawn(async move {
                    use std::io::Write;
                    let t_replica = Instant::now();
                    let mut rfile = replica_arc.lock().unwrap();
                    rfile.write_all(&record_copy).expect("replica write failed");
                    do_sync(&rfile, sync_mode);
                    t_replica.elapsed().as_micros() as u64
                });

                file.write_all(&record_buf[..write_len]).expect("write failed");
                do_sync(&file, args.sync_mode);
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
                file.write_all(&record_buf[..write_len]).expect("write failed");
                do_sync(&file, args.sync_mode);
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
