use std::time::{Duration, Instant};

use core_affinity::CoreId;
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

use crate::common::*;

pub fn writer_task_thread(
    args: &Args,
    task_id: usize,
    io_core: Option<CoreId>,
) -> TaskResult {
    use std::io::Write;

    if let Some(core) = io_core {
        core_affinity::set_for_current(core);
    }

    if args.stagger_us > 0 && task_id > 0 {
        std::thread::sleep(Duration::from_micros(args.stagger_us * task_id as u64));
    }

    let path = wal_path(&args.dir, task_id, args.spread_dirs);
    let direct = args.direct;
    let sync_mode = args.sync_mode;
    let trace_sync = args.trace_sync;
    let prealloc = estimate_prealloc(args);
    let max_record_raw = 4 + 8 + args.max_record_size;
    let max_write_len = max_record_raw.div_ceil(ALIGN) * ALIGN;

    let mut file = open_direct(&path, direct, sync_mode, prealloc);
    let mut abuf = if direct { Some(AlignedBuf::new(max_write_len)) } else { None };
    let mut record_buf = vec![0u8; max_write_len];

    // Replica: dedicated thread with mpsc channel
    let replica = args.replica_dir.as_ref().map(|rd| {
        let rpath = wal_path(rd, task_id, args.spread_dirs);
        let (tx, rx) = std::sync::mpsc::sync_channel::<(Vec<u8>, usize)>(0); // rendezvous
        let (ret_tx, ret_rx) = std::sync::mpsc::sync_channel::<u64>(0);
        let replica_core = io_core;
        let direct_r = direct;
        let sync_mode_r = sync_mode;
        let trace_r = trace_sync;
        let t0 = Instant::now();

        let thread = std::thread::spawn(move || {
            if let Some(core) = replica_core {
                core_affinity::set_for_current(core);
            }
            let mut rfile = open_direct(&rpath, direct_r, sync_mode_r, prealloc);
            let mut rabuf = if direct_r { Some(AlignedBuf::new(max_write_len)) } else { None };
            let mut trace = Vec::new();

            while let Ok((buf, len)) = rx.recv() {
                let t = Instant::now();
                if let Some(ref mut rabuf) = rabuf {
                    rabuf.as_mut_slice()[..len].copy_from_slice(&buf[..len]);
                    rfile.write_all(rabuf.as_slice(len)).expect("replica write failed");
                } else {
                    rfile.write_all(&buf[..len]).expect("replica write failed");
                }
                do_sync(&rfile, sync_mode_r);
                let end = Instant::now();
                let lat = end.duration_since(t).as_micros() as u64;
                if trace_r {
                    trace.push(SyncEvent {
                        start_ns: t.duration_since(t0).as_nanos() as u64,
                        end_ns: end.duration_since(t0).as_nanos() as u64,
                    });
                }
                let _ = ret_tx.send(lat);
            }
            trace
        });

        (tx, ret_rx, thread)
    });

    let mut rng = StdRng::from_entropy();
    let mut total_bytes: u64 = 0;
    let mut measured_bytes: u64 = 0;
    let mut latencies_us = Vec::with_capacity(args.writes_per_task);
    let mut primary_latencies_us = Vec::new();
    let mut replica_latencies_us = Vec::new();
    let mut primary_trace = Vec::new();
    let mut payload_buf = vec![0u8; args.max_record_size];

    let total_writes = args.warmup + args.writes_per_task;
    let t0 = Instant::now();
    let start = Instant::now();

    for seq in 0..total_writes as u64 {
        let payload_len = rng.r#gen_range(args.min_record_size..=args.max_record_size);
        rng.fill_bytes(&mut payload_buf[..payload_len]);

        let write_len = if let Some(ref mut abuf) = abuf {
            build_record_into(abuf.as_mut_slice(), seq, &payload_buf[..payload_len], true)
        } else {
            build_record_into(&mut record_buf, seq, &payload_buf[..payload_len], false)
        };
        total_bytes += write_len as u64;

        // Send to replica thread before primary write
        if let Some((ref tx, _, _)) = replica {
            let buf = if let Some(ref abuf) = abuf {
                abuf.as_slice(write_len).to_vec()
            } else {
                record_buf[..write_len].to_vec()
            };
            tx.send((buf, write_len)).expect("replica send failed");
        }

        // Primary write + sync
        let t = Instant::now();
        if let Some(ref abuf) = abuf {
            file.write_all(abuf.as_slice(write_len)).expect("write failed");
        } else {
            file.write_all(&record_buf[..write_len]).expect("write failed");
        }
        do_sync(&file, sync_mode);
        let end = Instant::now();
        let primary_lat = end.duration_since(t).as_micros() as u64;

        if trace_sync {
            primary_trace.push(SyncEvent {
                start_ns: t.duration_since(t0).as_nanos() as u64,
                end_ns: end.duration_since(t0).as_nanos() as u64,
            });
        }

        if let Some((_, ref ret_rx, _)) = replica {
            let replica_lat = ret_rx.recv().expect("replica recv failed");
            let combined_lat = primary_lat.max(replica_lat);
            if seq >= args.warmup as u64 {
                latencies_us.push(combined_lat);
                primary_latencies_us.push(primary_lat);
                replica_latencies_us.push(replica_lat);
                measured_bytes += write_len as u64;
            }
        } else if seq >= args.warmup as u64 {
            latencies_us.push(primary_lat);
            measured_bytes += write_len as u64;
        }
    }

    let replica_trace = if let Some((tx, _, thread)) = replica {
        drop(tx);
        thread.join().expect("replica thread panicked")
    } else {
        Vec::new()
    };

    let elapsed = start.elapsed();

    TaskResult {
        task_id,
        total_bytes,
        measured_bytes,
        elapsed,
        latencies_us,
        primary_latencies_us,
        replica_latencies_us,
        primary_trace,
        replica_trace,
    }
}
