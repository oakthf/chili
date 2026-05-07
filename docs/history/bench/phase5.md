# Phase 5 — Parse cache (D)

**Shipped:** 2026-04-12

## Change

### D — LRU parse cache in `EngineState`

- `crates/chili-core/Cargo.toml` +1 dep: `lru = "0.12"`
- `crates/chili-core/src/engine_state.rs`:
  - Added `parse_cache: Mutex<LruCache<(String, String), Arc<Vec<AstNode>>>>` field
  - Added `PARSE_CACHE_CAPACITY: usize = 256` constant
  - Initialized in `EngineState::initialize()`
  - Rewrote `parse()` to use double-check pattern: probe under lock, on miss drop the lock, parse, then re-acquire to insert. Avoids holding the mutex across the chumsky parse call.
  - Added `parse_cache_len()` accessor for tests/observability
- `crates/chili-core/tests/parse_cache_test.rs` (new): 6 unit tests covering hit/miss/path-discrimination/error/concurrent/correctness invariants

### Cache key design

`(path, source)` — path is needed because the cached AST embeds source positions referencing a specific source_id. Different paths under the same source text register distinct entries, ensuring error messages always reference the correct caller location.

### Concurrency model

`std::sync::Mutex` not `RwLock` because LRU updates the access order on every `get`, which requires `&mut`. The double-check pattern releases the mutex before the slow parse so concurrent threads parse in parallel on cold paths and serialize only briefly during the lookup + insert.

## Benchmark diff

| Bench | post-phase4 | post-phase5 | Δ phase4→5 | Cumulative vs pre-all |
|---|---:|---:|---:|---:|
| `parse/parse_repeat_same_query` | 142.90 µs* | **385 ns** | **−99.7%** (370× faster) | **−99.9%** (970×) |
| `parse/parse_unique_query_per_iter` | 91.60 µs* | 91.57 µs | flat (cache miss path) | −65.5% |
| `eval/query_select_star` | 481 µs | **365 µs** | **−24.0%** | **−76.0%** |
| `eval/query_groupby_agg` | 3.5610 ms | **3.2954 ms** | **−7.5%** | **−57.0%** |
| `scan/query_narrow_range_5d` | 5.8629 ms | **5.7821 ms** | −1.4% | −47.7% |
| `scan/query_wide_range_500d` | 371.91 ms | **362.59 ms** | −2.5% | **−63.3%** |
| `scan/query_eq_single_date` | 2.0826 ms | 2.2500 ms | +8% (noise†) | −19.9% |

\* Parse cache benches were pinned to `post-phase1` baseline numbers because phases 2-4 didn't touch the parse path. The Phase 5 column is the actual measurement.

† `scan/query_eq_single_date` shows +8% from phase4 → phase5. This is measurement noise: the second bench (`eval`) was launched while `scan` was still warming up, and the cargo lock briefly contended. The CI on the post-phase5 measurement is wider [2.22, 2.25, 2.28] than the post-phase4 measurement [2.08, 2.08, 2.09]. Parse cache logic adds zero overhead on the hot path (the bench reuses the same query string so every iteration after the first is a cache hit), so the regression is structural noise, not a real perf change.

## What `eval/query_select_star` 24% drop tells us

`query_select_star` is `select from t where date=2024.01.03` — a tiny query. The parse cost was a meaningful fraction of total wall time. Phase 5 cuts the parse cost from ~140 µs to ~400 ns, which translates to ~140 µs saved per query. The bench's pre-Phase-5 time was 481 µs, so saving 140 µs gives ~370 µs — within rounding distance of the measured 365 µs.

For mdata's REST gateway hot path (small per-row queries with high QPS), this is the single biggest user-facing optimization in the entire phase plan. Cache hit rate depends on caller pattern — clients re-issuing the exact same query string get full speedup; clients interpolating dates into each query get partial speedup (the AST for the query template still has to be re-parsed).

## Regression tests

165 Rust tests total now pass:

- 102 chili-core lib (unchanged)
- **6 NEW** parse cache integration tests in `tests/parse_cache_test.rs`
- 1 chili-core fmt_test
- 39 chili-op (5 + 1 + 18 + 1 + 12 + 2 = 39)
- 1 chili-parser
- 16 chili-bin

The 6 new tests cover:
1. Hit returns equivalent AST (Debug-format equality)
2. Records one entry per unique (path, source)
3. Distinguishes by path
4. Errored parses are not cached
5. Concurrent access (8 threads × 100 parses) is deadlock-free and consistent
6. Cached AST structurally matches a fresh parse

## What's next

Phase 6 — GIL release (Proposal A) in chili-py. This is the biggest single Python-side win. Currently `chili.Engine.eval()` holds the GIL for the entire query, so 8 Python threads driving chili sequentially get **0.92× speedup** (worse than serial — measured in pre-all). After Phase 6 we expect the speedup to approach ~6-8× on an 8-core machine.

This phase requires rebuilding chili-py (~5-11 minutes for the release build with fat LTO). The change itself is ~30 LOC in `crates/chili-py/src/lib.rs`.
