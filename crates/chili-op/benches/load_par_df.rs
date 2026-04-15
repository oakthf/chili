//! B1, B2, B3 — HDB load benchmarks.
//!
//! - B1 `load_cold_2000p`: build a 2000-partition single-table HDB, then time
//!   `EngineState::load_par_df`. Note that the first run after fixture creation
//!   is "warm" because fixture creation touches every file — a truly cold page
//!   cache requires macOS `purge` (root) or Linux `echo 3 > drop_caches`. We
//!   call this "warm" for honesty. Criterion averages over many iterations so
//!   steady-state is what we measure.
//!
//! - B2 `load_warm_2000p`: identical setup, illustrating that B1 and B2 are
//!   effectively the same under criterion's iteration model — but kept as a
//!   separate bench so downstream phase-N improvements can diff both names
//!   consistently.
//!
//! - B3 `load_multitable_5x200p`: 5 tables × 200 partitions each, for measuring
//!   proposal C (parallel per-table scans).

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

mod common;
use common::{TempHdb, build_hdb, build_multitable_hdb, make_engine};

fn bench_load_2000p(c: &mut Criterion) {
    // Build fixture once — reused across all iterations.
    let tmp = TempHdb::new("load_2000p");
    build_hdb(&tmp, "t", 2000, 3, 1);

    let mut group = c.benchmark_group("load");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(15));
    group.bench_function(BenchmarkId::new("load_cold_2000p", 2000), |b| {
        b.iter(|| {
            let engine = make_engine();
            engine.load_par_df(tmp.path()).unwrap();
            engine
        });
    });
    group.bench_function(BenchmarkId::new("load_warm_2000p", 2000), |b| {
        b.iter(|| {
            let engine = make_engine();
            engine.load_par_df(tmp.path()).unwrap();
            engine
        });
    });
    group.finish();
}

fn bench_load_multitable(c: &mut Criterion) {
    let tmp = TempHdb::new("load_5x200");
    build_multitable_hdb(&tmp, 5, 200, 3, 1);

    let mut group = c.benchmark_group("load");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));
    group.bench_function(BenchmarkId::new("load_multitable_5x200p", "5x200"), |b| {
        b.iter(|| {
            let engine = make_engine();
            engine.load_par_df(tmp.path()).unwrap();
            engine
        });
    });
    group.finish();
}

criterion_group!(benches, bench_load_2000p, bench_load_multitable);
criterion_main!(benches);
