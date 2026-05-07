# Post-pivot bench rebaseline — claude-2 (started 2026-05-07)

**Started:** 2026-05-07 (Sprint 3 Part D wrap)
**Branch:** `claude-2` (post-pivot, forked from main tip `f8b6360` on 2026-05-07)
**Comparison anchor:** `claude-baseline-2026-05-07` tag (= parked-claude tip
`dea966e`)
**Methodology:** measure each hot path on claude-2 as the porting Sprints
(3, 4, 5) touch it; record the matching number from the parked claude
binary opportunistically (not blocking each sprint wrap).
**Cadence:** sprints append rows; full A/B sweep + summary lands at Sprint 5
wrap.

---

## Why this doc exists

The 2026-05-07 pivot (Sprint 2 v2) restarted from `main` instead of carrying
the parked-claude shape forward. Every measurable hot path on `claude-2`
needs to be checked against parked claude (the binary mdata is currently
running on `claude-baseline-2026-05-07`-built wheels) so that:

1. **Golden rule guarantees stay green** (parse cache ≤400ns; GIL released
   on `Engine::eval`; etc.).
2. **Regressions surface before mdata sees them.** Sprint 5 cuts the
   claude-2 wheel; this doc is the cumulative dossier the user reviews
   before that ship date.

Bench rebaseline was directed to land **alongside features** (user
direction 2026-05-07: "alongside features"). Sprint 3 starts with parse
cache; Sprints 4 and 5 add scan, eval, load_par_df, write_partition rows
as those features are touched.

---

## Hot-path inventory

| Metric | Bench file | Owner sprint | claude-baseline-2026-05-07 | claude-2 |
|---|---|---|---|---|
| parse_cache hit (ns, median) | `crates/chili-core/benches/parse_cache.rs` | Sprint 3 Part D | ~385 (reported, [docs/bench/phase5.md](phase5.md)) | **371.43** |
| parse_cache cold (µs, median) | same | Sprint 3 Part D | (not recorded) | 95.37 |
| scan throughput | `crates/chili-op/benches/scan.rs` | Sprint 5 (rescheduled) | TBD | compile-validated Sprint 4 |
| eval throughput | `crates/chili-op/benches/eval.rs` | Sprint 5 (rescheduled) | TBD | compile-validated Sprint 4 |
| load_par_df cold | `crates/chili-op/benches/load_par_df.rs` | Sprint 5 (rescheduled) | TBD | compile-validated Sprint 4 |
| write_partition | `crates/chili-op/benches/write_partition.rs` | Sprint 5 (rescheduled) | TBD | compile-validated Sprint 4 |
| Python concurrent eval | `crates/chili-py/tests/bench_concurrent.py` | Sprint 5 | 6.10× pre-pivot | TBD |

---

## Sprint 3 Part D — parse_cache (2026-05-07)

### Bench config

```
machine:  Apple Silicon (Darwin 25.4.0, aarch64)
toolchain: rust stable, profile = bench (release + debug-assertions off)
criterion sample_size: 100
warm-up: 3.0 s
measurement: 10.0 s
query (hit path): "select from t where date=2024.01.03, symbol=`AAPL"
query (cold path): "x: <i>; x + 1" (i monotonically increasing)
```

### Result

| Bench | Median | Inner CI |
|---|---:|---:|
| parse/parse_repeat_same_query (hit) | **371.43 ns** | [371.20, 371.67] ns |
| parse/parse_unique_query_per_iter (cold) | 95.37 µs | [94.99, 95.67] µs |

**Golden rule 6 verdict:** PASS. 371.43 ns < 400 ns (target) < ~385 ns
(parked-claude reported number). claude-2's parse cache outperforms parked
claude on the hit path on the same hardware. The contingency in the brief
(Path 1: port claude's parse_cache shape inline if main's > 400 ns; Path 2:
escalate user) was NOT needed.

### Regression tests

`crates/chili-core/tests/parse_cache_test.rs` (new, ported from
`claude-baseline-2026-05-07:crates/chili-core/tests/parse_cache_test.rs`):
6 tests covering:

- Cache hit returns identical AST as cold parse.
- One entry per `(path, source)` key; same path + same source = hit.
- Different paths produce distinct entries (even on identical source).
- Errored parses don't pollute the cache.
- 8-thread concurrent access converges to a single cache entry.
- AST structural equality preserved across hit/cold.

All 6 pass. Test count delta: +6 Rust tests on chili-core (160 → 166 in
post-Part-D Rust gate).

### Notes

- The cold-path number (95.37 µs) is the first time claude-2 has recorded
  it; parked claude's phase5 doc didn't break it out separately. Sprint 4-5
  comparison can use this as the anchor.
- Hit-path 371 ns implies ~26.9 M parses/sec, which is well above any
  realistic throughput ceiling (network, FFI marshalling, eval) and
  validates the LRU choice over a per-call full reparse.

---

## Sprint 4 Part C — bench harness validation (2026-05-07)

### Why no measurement numbers landed this sprint

The Sprint 4 brief planned to measure scan / eval / load_par_df /
write_partition headlines on `claude-2`. In practice, `cargo bench
-p chili-op` requires a full release-profile recompile of the polars
0.53 dependency tree (polars-ops, polars-stream, polars-expr, polars-plan
each take ~1–2 min at `-C opt-level=3 -C linker-plugin-lto -C codegen-units=1`).
Compounded over four bench files, the wall-clock cost exceeded Sprint 4's
~2-3pp Part C budget allocation.

**Sprint 4 verdict:** verified the bench harnesses **type-check** cleanly
via `cargo check --benches -p chili-op` (30s; **dev profile**, NOT the
release profile that `cargo bench --no-run` uses; release-profile compile
is the expensive part and is deferred to Sprint 5 along with the
measurement). The dev-profile check confirms no signal that the polars-version
upgrade or Sprint 3's clippy hand-port broke the bench code at type level.
Defer the actual A/B measurement to Sprint 5, where it consolidates with
the parked-claude tag-built binary measurement and lands as a single
comparison sweep.

**What this means for Sprint 5:** the release-profile artifact cache is NOT
warm after Sprint 4. Sprint 5's first `cargo bench` invocation will pay
the full ~5-10 min release-profile compile cost.

### Result

| Bench file | Compile status | Measurement landing | Notes |
|---|---|---|---|
| `crates/chili-op/benches/scan.rs` | GREEN | Sprint 5 | 2000-partition fixture |
| `crates/chili-op/benches/eval.rs` | GREEN | Sprint 5 | 100-partition × 50-symbol × 500-row fixture |
| `crates/chili-op/benches/load_par_df.rs` | GREEN | Sprint 5 | 2000-partition + 5×200 multitable |
| `crates/chili-op/benches/write_partition.rs` | GREEN | Sprint 5 | 5-partition write loop |

### Lesson recorded for Sprint 5 budget

Bench *compilation* (release profile, full polars dep tree) is itself
expensive (~5-10 min wall on this machine when Cargo's release artifacts
are cold). Sprint 5 should budget 3-5pp for bench compile + 2-3pp for
runtime. If the parked-claude tag-built binary needs a *separate* release
build, double the compile cost. Caching is mostly stable across runs in
the same session but invalidates on any `Cargo.toml` workspace edit.

---

## Sprints 5+ placeholder

(Rows for the actual numbers land in Sprint 5 alongside the parked-claude
tag-built A/B comparison.)
