//! B7, B8 — Eval-path hot path benchmarks.
//!
//! - B7 `query_groupby_agg`: group-by + aggregation (tests filter fusion, with_capacity, collect_schema shortcut)
//! - B8 `query_select_star`: raw select * to measure IPC + eval overhead unrelated to polars compute

use std::time::Duration;

use chili_core::{EngineState, SpicyObj, Stack};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

mod common;
use common::{build_hdb, make_engine, TempHdb};

fn eval(engine: &EngineState, query: &str) {
    let mut stack = Stack::new(None, 0, 0, "");
    let obj = engine
        .eval(&mut stack, &SpicyObj::String(query.to_owned()), "bench.chi")
        .unwrap();
    black_box(obj);
}

fn bench_eval(c: &mut Criterion) {
    let tmp = TempHdb::new("eval_100p");
    // 100 partitions × 50 symbols × 500 rows = 2.5M rows total — enough for group-by
    // to be meaningful but small enough that load is fast.
    build_hdb(&tmp, "t", 100, 50, 500);
    let engine = make_engine();
    engine.load_par_df(tmp.path()).unwrap();

    let mut group = c.benchmark_group("eval");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    // B7 — group-by + aggregation across 10 days
    group.bench_function("query_groupby_agg", |b| {
        b.iter(|| {
            eval(
                &engine,
                "select mean price, sum volume by symbol from t where date>=2024.01.02, date<=2024.01.11",
            )
        });
    });

    // B8 — select * on a single partition (measures eval/IPC overhead with minimal compute)
    group.bench_function("query_select_star", |b| {
        b.iter(|| eval(&engine, "select from t where date=2024.01.03"));
    });

    group.finish();
}

criterion_group!(benches, bench_eval);
criterion_main!(benches);
