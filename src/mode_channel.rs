use std::time::{Duration, Instant};

use core_affinity::CoreId;
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

use crate::common::*;

pub async fn writer_task_channel(
    args: &Args,
    task_id: usize,
    io_core: Option<CoreId>,
) -> TaskResult {
    let path = wal_path(&args.dir, task_id, args.spread_dirs);
    let direct = args.direct;
    let sync_data = args.sync_data;
    let trace_sync = args.trace_sync;
    let prealloc = estimate_prealloc(args);
    let t0 = Instant::now();

    let max_record_raw = 4 + 8 + args.max_record_size;
    let max_write_len = max_record_raw.div_ceil(ALIGN) * ALIGN;

    // Async channel to I/O thread: non-blocking send from tokio task,
    // blocking_recv on the I/O thread side.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(Vec<u8>, usize)>(1);
    let (ret_tx, mut ret_rx) = tokio::sync::mpsc::channel::<(Vec<u8>, u64)>(1);

    let io_thread = std::thread::spawn(move || {
        if let Some(core) = io_core {
            core_affinity::set_for_current(core);
        }
        use std::io::Write;

        let mut file = open_direct(&path, direct, prealloc);
        let mut abuf = if direct { Some(AlignedBuf::new(max_write_len)) } else { None };
        let mut trace = Vec::new();

        while let Some((buf, len)) = rx.blocking_recv() {
            let t = Instant::now();
            if let Some(ref mut abuf) = abuf {
                abuf.as_mut_slice()[..len].copy_from_slice(&buf[..len]);
                file.write_all(abuf.as_slice(len)).expect("write failed");
            } else {
                file.write_all(&buf[..len]).expect("write failed");
            }
            do_sync(&file, sync_data);
            let end = Instant::now();
            let lat = end.duration_since(t).as_micros() as u64;
            if trace_sync {
                trace.push(SyncEvent {
                    start_ns: t.duration_since(t0).as_nanos() as u64,
                    end_ns: end.duration_since(t0).as_nanos() as u64,
                });
            }
            if ret_tx.blocking_send((buf, lat)).is_err() {
                break;
            }
        }
        trace
    });

    let replica_channels = args.replica_dir.as_ref().map(|rd| {
        let rpath = wal_path(rd, task_id, args.spread_dirs);
        let direct_r = direct;
        let sync_data_r = sync_data;
        let prealloc_r = prealloc;
        let max_write_len_r = max_write_len;

        let (rtx, mut rrx) = tokio::sync::mpsc::channel::<(Vec<u8>, usize)>(1);
        let (rret_tx, rret_rx) = tokio::sync::mpsc::channel::<(Vec<u8>, u64)>(1);

        let replica_io_core = io_core; // same pinning strategy
        let replica_thread = std::thread::spawn(move || {
            if let Some(core) = replica_io_core {
                core_affinity::set_for_current(core);
            }
            use std::io::Write;

            let mut file = open_direct(&rpath, direct_r, prealloc_r);
            let mut abuf = if direct_r { Some(AlignedBuf::new(max_write_len_r)) } else { None };
            let mut trace = Vec::new();

            while let Some((buf, len)) = rrx.blocking_recv() {
                let t = Instant::now();
                if let Some(ref mut abuf) = abuf {
                    abuf.as_mut_slice()[..len].copy_from_slice(&buf[..len]);
                    file.write_all(abuf.as_slice(len)).expect("replica write failed");
                } else {
                    file.write_all(&buf[..len]).expect("replica write failed");
                }
                do_sync(&file, sync_data_r);
                let end = Instant::now();
                let lat = end.duration_since(t).as_micros() as u64;
                if trace_sync {
                    trace.push(SyncEvent {
                        start_ns: t.duration_since(t0).as_nanos() as u64,
                        end_ns: end.duration_since(t0).as_nanos() as u64,
                    });
                }
                if rret_tx.blocking_send((buf, lat)).is_err() {
                    break;
                }
            }
            trace
        });

        (rtx, rret_rx, replica_thread)
    });
    let mut replica_channels = replica_channels;

    let mut rng = StdRng::from_entropy();
    let mut total_bytes: u64 = 0;
    let mut measured_bytes: u64 = 0;
    let mut latencies_us = Vec::with_capacity(args.writes_per_task);
    let mut primary_latencies_us = Vec::with_capacity(if replica_channels.is_some() { args.writes_per_task } else { 0 });
    let mut replica_latencies_us = Vec::with_capacity(if replica_channels.is_some() { args.writes_per_task } else { 0 });
    let mut payload_buf = vec![0u8; args.max_record_size];
    // Two pre-allocated buffers for double-buffering
    let mut send_buf = vec![0u8; max_write_len];
    let mut spare_buf = Some(vec![0u8; max_write_len]);

    let total_writes = args.warmup + args.writes_per_task;

    if args.stagger_us > 0 && task_id > 0 {
        let delay = Duration::from_micros(args.stagger_us * task_id as u64);
        tokio::time::sleep(delay).await;
    }

    let start = Instant::now();

    for seq in 0..total_writes as u64 {
        let payload_len = rng.r#gen_range(args.min_record_size..=args.max_record_size);
        rng.fill_bytes(&mut payload_buf[..payload_len]);
        let write_len = build_record_into(&mut send_buf, seq, &payload_buf[..payload_len], direct);
        total_bytes += write_len as u64;

        if let Some((ref rtx, ref mut rret_rx, _)) = replica_channels {
            // Copy record to replica buffer BEFORE sending primary (which moves send_buf)
            let mut replica_buf = spare_buf.take().unwrap_or_else(|| vec![0u8; max_write_len]);
            replica_buf[..write_len].copy_from_slice(&send_buf[..write_len]);

            // Async send to both I/O threads — non-blocking
            rtx.send((replica_buf, write_len)).await.expect("replica send failed");
            tx.send((send_buf, write_len)).await.expect("send failed");

            // Await both results in parallel
            let (primary_res, replica_res) = tokio::join!(
                ret_rx.recv(),
                rret_rx.recv()
            );
            let (primary_ret, primary_lat) = primary_res.expect("recv failed");
            let (replica_ret, replica_lat) = replica_res.expect("replica recv failed");

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
            tx.send((send_buf, write_len)).await.expect("send failed");
            let (ret, primary_lat) = ret_rx.recv().await.expect("recv failed");
            send_buf = ret;

            if seq >= args.warmup as u64 {
                latencies_us.push(primary_lat);
                measured_bytes += write_len as u64;
            }
        }
    }

    drop(tx);
    drop(spare_buf.take());
    let primary_trace = io_thread.join().expect("io thread panicked");

    let replica_trace = if let Some((rtx, _, replica_thread)) = replica_channels {
        drop(rtx);
        replica_thread.join().expect("replica io thread panicked")
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
