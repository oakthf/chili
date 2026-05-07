# Iceberg compatibility evaluation for chili HDB (Sprint 12 research synthesis)

**Author:** Sprint 12 perf-pass-3 + Iceberg eval — research synthesis only.
**Date:** 2026-05-08
**Source:** `docs/research/kdb_alternatives.md` §3.2 (item 5: Apache Iceberg + Parquet ascendance trend).
**Status:** Research output. NOT an ADR — no decision proposed. The synthesis output is a recommendation framework that future sprints (or a user-driven session) can convert to ADR if mdata or another consumer requests Iceberg compatibility.

---

## Question

Should chili-2's HDB layout add Apache Iceberg metadata so that chili-
written partitions are readable by Iceberg-consuming tools (DuckDB,
Spark, Trino, ClickHouse, Athena, Snowflake)?

## Background

- chili's current HDB: `{root}/{table}/{partition}/{shard}.parquet` (per
  `crates/chili-op/src/io.rs` `write_partition_native` and chili-core's
  `load_par_df` recursive walker).
- Storage primitive: Parquet files written via polars-parquet.
- Read path: chili-core's `LazyFrame::scan_parquet` (per Sprint 9 P2
  profile finding — the per-table polars-plan setup is what's hot).
- Apache Iceberg: open table format adding **manifest lists**,
  **manifests**, **snapshots**, and **metadata.json** files on top of
  Parquet data files. Provides ACID transactions, time-travel,
  schema evolution, and is the de-facto interchange format for the
  modern data-lake stack (Iceberg 1.10.x as of Dec 2025).
- Strategic context (kdb_alternatives.md §3.2 item 5): "kdb+'s
  splayed-table on-disk format is becoming an outlier. Any shop
  adopting a data-lake/lakehouse architecture is structurally pulled
  away from kdb+. **Chili's storage layer is also Parquet-based,
  putting it on the right side of this trend.**"

The strategic framing is favorable to chili — it's already on Parquet.
The question is whether to LEAN INTO it by adding Iceberg metadata on
top.

---

## What "Iceberg compatibility" requires

To make chili-written HDB readable by Iceberg-consuming tools:

1. **Per-partition manifest writes.** Each `wpar` call writes a
   manifest file alongside the Parquet shards listing the data files,
   schema, partition spec, and statistics (min/max per column).
2. **Manifest list per snapshot.** A `metadata/snap-{N}.avro`
   pointing at the manifests in the current snapshot.
3. **Table metadata.json.** Top-level table schema + snapshot list +
   partition spec + sort order.
4. **Catalog registration** (optional for direct file access; required
   for many tools): an external catalog (Glue, REST, Nessie, file-
   based) tracks the table's `metadata.json` location.

Implementation surface: ~3 new chili-core / chili-op modules
(`iceberg_writer`, `iceberg_metadata`, `iceberg_catalog_local`),
~500-1000 lines of Rust + dependency on the Apache Iceberg Rust
client (`iceberg-rust` crate, currently 0.x experimental as of
2026-05-08).

---

## Cost / benefit

### Cost (~10-15pp implementation + ongoing maintenance)

- ~500-1000 lines of Rust to implement Iceberg writer integrated
  with chili-op's `wpar` path.
- ~3-5pp follow-up sprint per Iceberg version bump (the format
  evolves; chili would track upstream).
- Per-partition manifest write doubles or triples the number of file
  writes per `wpar` call. Sprint 7 Part B measured `wpar_1k_rows_fresh_hdb`
  at 9.0752 ms (claude-2); Iceberg manifest writes would add maybe
  1-3 ms per partition (rough estimate based on typical Iceberg
  manifest serialization cost).
- Mandatory dependency on `iceberg-rust` crate (still 0.x; pre-1.0
  API stability risk).

### Benefit (gated on consumer demand)

- chili HDB readable by DuckDB, Spark, Trino, ClickHouse, Athena,
  Snowflake (and any future Iceberg-consuming tool).
- ACID transactions across multi-partition writes (currently chili's
  `wpar` is per-partition; failures mid-multi-partition leave
  inconsistent state).
- Time-travel reads: query "as-of" a previous snapshot.
- Schema evolution: add columns without rewriting partitions.

The benefit is **all gated on a consumer requesting it**. If mdata
(chili's only declared downstream user as of Sprint 7 delivery) doesn't
need Iceberg compatibility, the cost is pure overhead.

---

## Recommendation

**Defer to a user-driven sprint when a concrete consumer surface
emerges.** Specifically:

1. **mdata next-version checkpoint:** when mdata's roadmap mentions
   "we want to be readable by Spark / DuckDB / lakehouse tooling,"
   reopen this evaluation and convert to ADR 0005.
2. **External chili adopter checkpoint:** if a non-mdata consumer
   asks for Iceberg compatibility (e.g., a Trino-on-S3 shop wanting
   chili-built HDBs as a source), reopen.
3. **Chili-as-data-lake-source pivot:** if chili's strategic
   positioning shifts from "embedded Python q-syntax engine" to
   "lakehouse-aligned columnar engine," Iceberg becomes a
   first-class roadmap item.

Until then: **chili's Parquet-only HDB is the right shape**. The
strategic positioning paper's framing ("on the right side of the
Parquet trend") is sufficient — chili doesn't need Iceberg metadata
to BENEFIT from Parquet's ascendance; chili just needs to keep
producing Parquet files (which it does).

### What to do NOW (zero-cost)

- Document this evaluation (this file). Future sprints have a
  reference instead of re-deriving the analysis.
- In the chili-py wheel delivery handoff, note that chili HDB is
  Parquet-only and provide the file layout (`{root}/{table}/{partition}/
  {shard}.parquet`) so consumers can directly read individual Parquet
  shards via DuckDB / pyarrow / pandas without needing Iceberg.

### What to NOT do

- DON'T add iceberg-rust as a dependency speculatively. Wait for
  consumer demand.
- DON'T speculate on Iceberg API stability / version pinning before
  there's a use case.
- DON'T preemptively refactor `wpar` to be "Iceberg-shaped." Premature.

---

## Cross-references

- `docs/research/kdb_alternatives.md` §3.2 item 5 — the strategic
  framing.
- `docs/research/competitive_position_2026-05-06.md` — chili's defensible
  position framing.
- `docs/sync/mdata_chili_2026-05-08_delivery.md` — current mdata
  delivery (no Iceberg ask).
- Sprint 12 retro: `../sim/sprint_12_retro.md` (lands at Sprint 12 wrap).
- iceberg-rust upstream (for reference): https://github.com/apache/iceberg-rust
