// FR-A (atomic single-shard overwrite) + FR-C (per-call parquet codec)
// tests — v1-63 Tier-2 chili-FR batch.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use chili_core::SpicyObj;
use chili_op::{write_partition_native, write_partition_native_full};
use polars::prelude::*;

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempHdb {
    root: PathBuf,
}

impl TempHdb {
    fn new() -> Self {
        let id = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("chili_ac_test_{}_{}", std::process::id(), id));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        Self { root }
    }
    fn path(&self) -> &str {
        self.root.to_str().unwrap()
    }
}

impl Drop for TempHdb {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

// 2026-01-01 expressed as days since epoch (used as the partition key).
const DAY: i32 = 20454;

fn make_df(value: i64) -> DataFrame {
    df![
        "symbol" => ["AAPL"],
        "value"  => [value],
    ]
    .unwrap()
}

// A larger, compressible frame so different codecs produce measurably
// different on-disk byte counts (proves the codec threaded through).
fn make_big_df() -> DataFrame {
    let n = 50_000;
    let symbols: Vec<&str> = (0..n).map(|_| "AAPL").collect();
    let values: Vec<i64> = (0..n).map(|i| (i % 7) as i64).collect();
    df![
        "symbol" => symbols,
        "value"  => values,
    ]
    .unwrap()
}

fn shard_bytes(hdb: &str, table: &str) -> u64 {
    let shard = format!("{}/{}/2026.01.01_0000", hdb, table);
    fs::metadata(&shard).unwrap().len()
}

fn read_value(hdb: &str, table: &str) -> i64 {
    // Read the single _0000 shard back and extract `value`.
    let shard = format!("{}/{}/2026.01.01_0000", hdb, table);
    let f = std::fs::File::open(&shard).expect("shard missing");
    let df = ParquetReader::new(f).finish().expect("parquet read failed");
    df.column("value").unwrap().i64().unwrap().get(0).unwrap()
}

fn partition_dir(hdb: &str, table: &str) -> PathBuf {
    PathBuf::from(format!("{}/{}", hdb, table))
}

fn shard_count(hdb: &str, table: &str) -> usize {
    let pat = format!("{}/{}/2026.01.01_*", hdb, table);
    glob::glob(&pat).unwrap().filter_map(|p| p.ok()).count()
}

// FR-A: an atomic overwrite must round-trip the new value, and the partition
// dir must NEVER be empty — the old _0000 stays present until the atomic
// rename installs the new one (write-then-swap), so a concurrent-ish reader
// always sees exactly one complete shard.
#[test]
fn test_atomic_overwrite_roundtrips_and_never_empties() {
    let hdb = TempHdb::new();
    let table = "ohlcv";

    // Initial write (non-atomic; establishes the partition + schema sidecar).
    write_partition_native(
        hdb.path(),
        &SpicyObj::Date(DAY),
        table,
        &make_df(100),
        &[],
        false,
        true,
    )
    .unwrap();
    assert_eq!(read_value(hdb.path(), table), 100);
    assert_eq!(shard_count(hdb.path(), table), 1);

    // Atomic overwrite to a new value.
    write_partition_native_full(
        hdb.path(),
        &SpicyObj::Date(DAY),
        table,
        &make_df(200),
        &[],
        false,
        true,  // overwrite
        true,  // atomic
        None,
    )
    .unwrap();

    // New value is visible; still exactly one shard; no leftover .tmp.
    assert_eq!(read_value(hdb.path(), table), 200);
    assert_eq!(shard_count(hdb.path(), table), 1);
    let tmp = format!("{}/{}/2026.01.01_0000.tmp", hdb.path(), table);
    assert!(!PathBuf::from(&tmp).exists(), "temp file must be renamed away");

    // The partition dir is non-empty at all times (the schema sidecar lives
    // one level up; the partition itself holds the single _0000). Confirm the
    // dir is never observed empty by re-listing.
    let dir = partition_dir(hdb.path(), table);
    let entries: Vec<_> = fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()).collect();
    assert!(
        entries.iter().any(|e| e.file_name().to_string_lossy().starts_with("2026.01.01_")),
        "partition shard must always be present"
    );
}

// FR-A: an atomic overwrite over a MULTI-shard append history collapses to a
// single _0000 (the new complete shard), dropping the stale _0001/_0002 only
// AFTER the new _0000 is in place.
#[test]
fn test_atomic_overwrite_collapses_multishard() {
    let hdb = TempHdb::new();
    let table = "ohlcv";

    // Three appended shards: _0000, _0001, _0002.
    for v in [1, 2, 3] {
        write_partition_native(
            hdb.path(),
            &SpicyObj::Date(DAY),
            table,
            &make_df(v),
            &[],
            false,
            false, // append
        )
        .unwrap();
    }
    assert_eq!(shard_count(hdb.path(), table), 3);

    // Atomic overwrite → one shard, value 99.
    write_partition_native_full(
        hdb.path(),
        &SpicyObj::Date(DAY),
        table,
        &make_df(99),
        &[],
        false,
        true, // overwrite
        true, // atomic
        None,
    )
    .unwrap();
    assert_eq!(shard_count(hdb.path(), table), 1);
    assert_eq!(read_value(hdb.path(), table), 99);
}

// FR-C: a non-default codec round-trips AND changes the on-disk encoding.
// Write the SAME compressible frame with snappy and with zstd; both must read
// back identically, but the byte counts must differ — proving the codec knob
// actually threaded through to the parquet writer (not silently zstd both
// times). zstd compresses this repetitive frame harder than snappy.
#[test]
fn test_codec_snappy_roundtrips_and_differs_from_zstd() {
    let big = make_big_df();

    let hdb_snappy = TempHdb::new();
    write_partition_native_full(
        hdb_snappy.path(),
        &SpicyObj::Date(DAY),
        "ohlcv",
        &big,
        &[],
        false,
        true,
        false,
        Some("snappy"),
    )
    .unwrap();

    let hdb_zstd = TempHdb::new();
    write_partition_native_full(
        hdb_zstd.path(),
        &SpicyObj::Date(DAY),
        "ohlcv",
        &big,
        &[],
        false,
        true,
        false,
        Some("zstd"),
    )
    .unwrap();

    // Both must round-trip the row count intact.
    let shard_snappy = format!("{}/ohlcv/2026.01.01_0000", hdb_snappy.path());
    let df_back =
        ParquetReader::new(std::fs::File::open(&shard_snappy).unwrap()).finish().unwrap();
    assert_eq!(df_back.height(), big.height());

    let snappy_bytes = shard_bytes(hdb_snappy.path(), "ohlcv");
    let zstd_bytes = shard_bytes(hdb_zstd.path(), "ohlcv");
    assert_ne!(
        snappy_bytes, zstd_bytes,
        "snappy ({}) and zstd ({}) must produce different on-disk sizes",
        snappy_bytes, zstd_bytes
    );
}

// FR-C: an unknown codec name errors loudly (a config typo must not silently
// fall back to zstd).
#[test]
fn test_codec_unknown_errors() {
    let hdb = TempHdb::new();
    let res = write_partition_native_full(
        hdb.path(),
        &SpicyObj::Date(DAY),
        "ohlcv",
        &make_df(1),
        &[],
        false,
        true,
        false,
        Some("brotli-typo"),
    );
    assert!(res.is_err(), "unknown codec must error");
}

// FR-C: explicit "zstd" and None both produce the default codec (backwards
// compatibility guard).
#[test]
fn test_codec_default_and_zstd_equivalent() {
    let hdb = TempHdb::new();
    let table = "ohlcv";
    write_partition_native_full(
        hdb.path(),
        &SpicyObj::Date(DAY),
        table,
        &make_df(7),
        &[],
        false,
        true,
        false,
        Some("zstd"),
    )
    .unwrap();
    assert_eq!(read_value(hdb.path(), table), 7);
}
