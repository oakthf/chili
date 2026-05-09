//! B11 — Partition-write throughput.
//!
//! Measures `write_partition_py` loop, which is the pattern used by chili-py's
//! `Engine::wpar`. This bench surfaces the `fs::canonicalize` syscall cost
//! (proposal O) and any file-creation overhead.
//!
//! Sprint 15 — added codec A/B variants (`wpar_1k_rows_codec_*`) to support
//! the Parquet codec public-API decision (ADR 0005). On-disk-size capture
//! is a separate non-criterion path (the `size_capture` module test, run
//! with `--ignored` flag) per audit recommendation — criterion's
//! `Measurement` trait is overkill for file-size tracking.

use std::time::Duration;

use chili_core::SpicyObj;
use chili_op::{ParquetWriteConfig, write_partition_native};
use criterion::{Criterion, criterion_group, criterion_main};
use polars::io::parquet::write::ParquetCompression;
use polars::prelude::*;

mod common;
use common::{TempHdb, make_row};

fn write_loop(tmp: &TempHdb, df: &DataFrame, options: Option<ParquetWriteConfig>) {
    for i in 0..5 {
        write_partition_native(
            tmp.path(),
            &SpicyObj::Date(19724 + i),
            "t",
            df,
            &[],
            false,
            false,
            options.clone(),
        )
        .unwrap();
    }
}

fn bench_wpar(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    let symbols = ["AAPL", "MSFT", "SPY", "GOOG", "TSLA"];
    let df: DataFrame = make_row(&symbols, 200); // 1000 rows per partition

    // Pre-Sprint-15 baseline shape — preserved for cadence-metrics
    // continuity. None ⇒ polars default (ZSTD as of 0.53; verified
    // empirically against 0.8.2 wheel — see ADR 0005).
    group.bench_function("wpar_1k_rows_fresh_hdb", |b| {
        b.iter_with_setup(
            || TempHdb::new("wpar_bench"),
            |tmp| write_loop(&tmp, &df, None),
        );
    });

    // Sprint 15 codec A/B — same fixture, explicit codec override.
    let codecs: &[(&str, ParquetCompression)] = &[
        ("zstd", ParquetCompression::Zstd(None)),
        ("snappy", ParquetCompression::Snappy),
        ("lz4_raw", ParquetCompression::Lz4Raw),
        ("uncompressed", ParquetCompression::Uncompressed),
    ];

    for (name, compression) in codecs.iter() {
        let config = ParquetWriteConfig {
            compression: Some(*compression),
            row_group_size: None,
        };
        let label = format!("wpar_1k_rows_codec_{}", name);
        group.bench_function(&label, |b| {
            b.iter_with_setup(
                || TempHdb::new(&format!("wpar_bench_{}", name)),
                |tmp| write_loop(&tmp, &df, Some(config.clone())),
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_wpar);
criterion_main!(benches);
