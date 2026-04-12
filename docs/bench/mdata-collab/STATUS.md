# Chili ↔ mdata Collaboration Status

**Purpose**: Single source of truth for phase coordination between chili and mdata.
Both sides read this file at the start of every invocation and know what to do next.

**Read from mdata side**: `project_chili_wishlist_collaboration.md` in mdata memory.
**Read from chili side**: this file + commit log for Phase N shipping signal.

---

## Phase tracker

| Phase | Chili status | mdata deliverable | mdata status |
|---|---|---|---|
| 0 — Bench infra | **SHIPPED 2026-04-11** | baseline benches | N/A (chili-side only) |
| 1 — Build-system (F/G/N) | **SHIPPED 2026-04-11** ✓ | N/A | N/A |
| 2 — Load path (J/B/C) | **SHIPPED 2026-04-11** ✓ (multi-table load −39.8%) | N/A | N/A |
| 3 — Scan parallelism (E) | **SHIPPED 2026-04-11** ✓ (wide-range −58.5%) | N/A | N/A |
| 4 — Eval micro (I/K/L) | **SHIPPED 2026-04-12** ✓ (eval −22%, scan −4 to −6%) | N/A | N/A |
| 5 — Parse cache (D) | **SHIPPED 2026-04-12** ✓ (cache hit −99.7% / 370× faster, eval/select_star −24%) | N/A | N/A |
| 6 — GIL release (A) | **SHIPPED 2026-04-12** ✓ (concurrent 519→3168 q/s = 6.1×) | N/A | N/A |
| 7 — Write path (O) | **SHIPPED 2026-04-12** ✓ (cache works; bench flat — code is free, low impact in microbench) | N/A | N/A |
| 8 — Validation | **SHIPPED 2026-04-12** ✓ (181 tests, full bench sweep, CHANGELOG + README + summary.md) | N/A | N/A |
| **9 — Column pruning (WL 2.1)** | **VERIFIED 2026-04-12 — NO CHANGE NEEDED** ✓ (polars 0.53 already pushes projection down through both scan_partition and scan_partition_by_range; multi-partition 81% saving on 1-col vs 11-col verified empirically) | Q1-Q12 rerun NOT needed for this phase | fixtures DONE 2026-04-11; rerun deferred to Phase 10 |
| **10 — Symbol pushdown (WL 2.2)** | IN PROGRESS — investigating whether polars row-group stats are pruning by symbol | **Q1-Q12 rerun** when chili signals ready | waits on chili |
| **11 — Fork guard (WL 1.2)** | PENDING | **validate error message** | waits on chili |
| **12 — Lifecycle API (WL 3.1)** | PENDING | **swap `ChiliGateway.close()`** | waits on chili |
| **13 — Structured errors (WL 3.3)** | PENDING | **swap RuntimeError catches** | waits on chili |
| **14 — Observability (WL 3.2)** | PENDING | **wire into rest prometheus** | waits on chili |
| **15 — Quantized helper (WL 3.4)** | PENDING | **schema doc drop + validation** | schema doc **DONE 2026-04-11** at `artifacts/quantized_schema.md`; validation waits on chili |
| **16 — Broker bindings (WL 1.1)** | PENDING | **parity test + pubsub swap-in** | parity test skeleton **DONE 2026-04-11** at `artifacts/broker_parity_test.py`; swap-in waits on chili |
| **17 — Agg pushdown (WL 2.3)** | PENDING | **Q11 rerun** | waits on chili |

---

## Proactive drops from mdata (2026-04-11)

mdata's Claude pre-delivered the non-benchmark artifacts for phases 9, 15, and 16
so chili can build against them without waiting on a round-trip. All benchmark
re-runs still wait for the corresponding chili phase to ship.

| Path (under `docs/bench/mdata-collab/`) | Size | Phase | Purpose |
|---|---|---|---|
| `fixtures/ohlcv_1d_*.0000` × 20 | ~5 MB | 9, 10, 17 | Realistic ohlcv_1d partitions (2021-04 → 2025-10) for chili column-pruning benchmarks. **Refreshed 2026-04-11 to Int64 storage schema** after `scripts/quantize_hdb.py --execute` ran against the live `ohlcv_1d` HDB. |
| `fixtures/ohlcv_1m_*.0000` × 5 | ~117 MB | 9, 10, 17 | Realistic ohlcv_1m partitions for chili column-pruning at minute resolution. Still **Float64** — `ohlcv_1m` rewrite has not been run yet. |
| `fixtures/ohlcv_*_schema` × 2 | ~2 KB | — | Chili partition schema sentinels matching the two tables. |
| `tests/test_chili_pepper_patterns.py` | 16 KB | 9–15 | Verbatim copy of mdata's pepper pattern pin tests. |
| `tests/test_chili_gateway_quantized.py` | 10 KB | 9–15 | Verbatim copy of mdata's Int64 round-trip tests. |
| `tests/README.md` | — | — | License + port guidance for chili-side usage. |
| `artifacts/quantized_schema.md` | — | 15 | Scale factor, per-column map, volume-not-scaled edge, reference dequantize path. |
| `artifacts/broker_parity_test.py` | — | 16 | Parameterized `unix` vs `chili` parity test — acceptance contract for Phase 16 bindings. |

**Still outstanding (mdata side, independent of chili):**

- `scripts/quantize_hdb.py --execute` run against `ohlcv_1d` on 2026-04-11.
  1,251 partitions rewritten in 17.2 s (direct Polars write, bypassing
  `chili.Engine.wpar` whose append-shard semantics were incompatible with
  in-place rewrites). Fixtures in this directory refreshed to Int64. The
  `ohlcv_1m` table (890 partitions, ~19 GB) has not yet been run — deferred
  pending a decision on whether the direct-Polars approach is acceptable at
  that scale. Empirical size delta on `ohlcv_1d`: **+6.7%** (324 MB → 346 MB),
  not the 28% reduction the Sprint I empirical study predicted. Int64 scaled
  prices compress less well under zstd than Float64 mantissas — the
  precision/storage tradeoff is against us on this specific workload.

---

## Drop zone contract

mdata writes deliverables into subdirectories of this file's parent:

```
docs/bench/mdata-collab/
    STATUS.md                 ← this file
    benchmarks/               ← mdata CSV deliverables
    fixtures/                 ← mdata parquet fixtures
    tests/                    ← mdata test files (verbatim copies)
    artifacts/                ← miscellaneous (parity tests, schema docs)
```

Full deliverable specification is in mdata's memory at
`project_chili_wishlist_collaboration.md` (ref: drop zone section).

---

## Phase 1 result summary (for reference — reflects pre-Phase-2 state)

All benches measured against `pre-all` baseline captured 2026-04-11 before any
chili optimization work. `post-phase1` captured after F (release profile) + G
(mimalloc) + N (#[inline] annotations) landed.

| Bench | pre-all | post-phase1 | Change |
|---|---:|---:|---:|
| scan/query_eq_single_date | 2.81 ms | 2.22 ms | **-20.9%** |
| scan/query_narrow_range_5d | 11.05 ms | 9.80 ms | **-11.3%** |
| scan/query_wide_range_500d | 988.63 ms | 918.86 ms | **-7.1%** |
| eval/query_groupby_agg | 7.66 ms | 4.55 ms | **-40.7%** |
| eval/query_select_star | 1.52 ms | 0.62 ms | **-59.5%** |
| write/wpar_1k_rows_fresh_hdb | 10.46 ms | 9.03 ms | **-13.6%** |
| parse/parse_repeat_same_query | 374 µs | 143 µs | **-61.8%** |
| parse/parse_unique_query_per_iter | 266 µs | 92 µs | **-65.5%** |
| load/load_cold_2000p | 6.27 ms | 5.05 ms | **-19.5%** |
| load/load_warm_2000p | 6.25 ms | 5.02 ms | **-19.6%** |
| load/load_multitable_5x200p | 2.87 ms | 2.54 ms | **-11.4%** |

Python concurrent bench (pre-A, GIL still held) — captured 2026-04-11 before any
optimization work. Retained as reference for Phase 6:

| Metric | pre-all |
|---|---:|
| Single-thread throughput | 564 q/s |
| 8-thread concurrent throughput | 519 q/s |
| Speedup vs serial-estimate | **0.92×** (worse than serial) |

---

## When a phase completes (signal protocol)

Chili's Claude writes the next-phase entry to the Phase Tracker above AND updates
the matching task status. Example:

```diff
- | 9 — Column pruning (WL 2.1) | PENDING | **Q1-Q12 rerun + fixture drop** | waiting |
+ | 9 — Column pruning (WL 2.1) | **SHIPPED 2026-04-NN** commit abc1234 | Q1-Q12 rerun | waiting |
```

When mdata's Claude completes a deliverable, it appends:

```diff
- | 9 — Column pruning (WL 2.1) | SHIPPED 2026-04-NN commit abc1234 | Q1-Q12 rerun | waiting |
+ | 9 — Column pruning (WL 2.1) | SHIPPED 2026-04-NN commit abc1234 | Q1-Q12 rerun | **DONE 2026-04-NN** csv at benchmarks/q1-q12_post_phase9.csv |
```

Chili reads the delta and moves to the next phase.
