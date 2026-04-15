//! B4, B5, B6 — Query scan benchmarks.
//!
//! - B4 `query_eq_single_date`: single-partition exact match via scan_partition
//! - B5 `query_narrow_range_5d`: 5-partition inclusive range via scan_partition_by_range
//! - B6 `query_wide_range_500d`: 500-partition range via scan_partition_by_range
//!
//! All three run against the same 2000-partition fixture. The engine is built
//! once per bench (cost amortized in setup), then query iteration is what we
//! measure.

use std::time::Duration;

use chili_core::{EngineState, SpicyObj, Stack};
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

mod common;
use common::{TempHdb, build_hdb, make_engine};

fn eval(engine: &EngineState, query: &str) {
    let mut stack = Stack::new(None, 0, 0, "");
    let obj = engine
        .eval(&mut stack, &SpicyObj::String(query.to_owned()), "bench.chi")
        .unwrap();
    black_box(obj);
}

fn load_bench_engine(tmp: &TempHdb) -> EngineState {
    let engine = make_engine();
    engine.load_par_df(tmp.path()).unwrap();
    engine
}

fn bench_scan(c: &mut Criterion) {
    let tmp = TempHdb::new("scan_2000p");
    // 2000 partitions × 5 symbols × 100 rows = 1M rows total
    build_hdb(&tmp, "t", 2000, 5, 100);
    let engine = load_bench_engine(&tmp);

    let mut group = c.benchmark_group("scan");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    // B4 — single partition exact equality
    // 2024.01.03 = day index 1 in our fixture (start=2024-01-02)
    group.bench_function("query_eq_single_date", |b| {
        b.iter(|| eval(&engine, "select from t where date=2024.01.03"));
    });

    // B5 — narrow 5-day inclusive range
    group.bench_function("query_narrow_range_5d", |b| {
        b.iter(|| {
            eval(
                &engine,
                "select from t where date>=2024.01.03, date<=2024.01.07",
            )
        });
    });

    // B6 — 500-day wide range (hits scan_partition_by_range over ~500 partitions)
    // 500 days from 2024.01.02 is mid-2025
    group.bench_function("query_wide_range_500d", |b| {
        b.iter(|| {
            eval(
                &engine,
                "select from t where date>=2024.01.02, date<=2025.05.15",
            )
        });
    });

    group.finish();
}

criterion_group!(benches, bench_scan);
criterion_main!(benches);
