//! Benchmarks for `SharedQuote` — the wait-free price-feed primitive.
//!
//! Compare a single-threaded load/store against a 4-reader / 1-writer
//! contended workload. The monitor loop is 1 kHz and the opportunity loop
//! is 1 ms, so a read must stay well under 100 ns.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use std::thread;
use xemm_rust::util::price::SharedQuote;

fn single_threaded_load(c: &mut Criterion) {
    let q = SharedQuote::empty();
    q.store(100.0, 100.05);
    c.bench_function("shared_quote_load_uncontended", |b| {
        b.iter(|| {
            let pair = black_box(q.load());
            black_box(pair);
        })
    });
}

fn single_threaded_store(c: &mut Criterion) {
    let q = SharedQuote::empty();
    c.bench_function("shared_quote_store_uncontended", |b| {
        b.iter(|| {
            q.store(black_box(100.0), black_box(100.05));
        })
    });
}

/// Measures one reader's load rate while a dedicated writer thread is
/// issuing back-to-back stores. The reported time is per-load for the
/// benched reader.
fn contended_load(c: &mut Criterion) {
    let q = SharedQuote::empty();
    q.store(100.0, 100.05);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Spawn a background writer for the duration of the bench.
    let writer_q = Arc::clone(&q);
    let writer_stop = Arc::clone(&stop);
    let writer = thread::spawn(move || {
        let mut i = 0u64;
        while !writer_stop.load(std::sync::atomic::Ordering::Relaxed) {
            if i & 1 == 0 {
                writer_q.store(100.0, 100.05);
            } else {
                writer_q.store(200.0, 200.10);
            }
            i = i.wrapping_add(1);
        }
    });

    c.bench_function("shared_quote_load_contended_1w", |b| {
        b.iter(|| {
            let pair = black_box(q.load());
            black_box(pair);
        })
    });

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = writer.join();
}

criterion_group!(
    benches,
    single_threaded_load,
    single_threaded_store,
    contended_load
);
criterion_main!(benches);
