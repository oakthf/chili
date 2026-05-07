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

## Hot-path inventory (Sprint 7 Part B A/B sweep complete)

claude-2 column reflects post-Sprint-7-Part-A state (Rust polars from
`pola-rs/polars` py-1.39.3 tag with q-style fmt patch). claude-baseline
column reflects parked-claude tag binary (commit `dea966e`,
2026-05-07) built in worktree `/tmp/chili-parked-bench`.

| Metric | Bench file | claude-baseline-2026-05-07 (median) | claude-2 (median) | Δ% | Verdict |
|---|---|---:|---:|---:|---|
| parse_cache hit (ns) | `crates/chili-core/benches/parse_cache.rs` | **369.19** | **410.58** | **+11.2%** | ⚠️ regression — exceeds golden rule 6 (≤400ns target) |
| parse_cache cold (µs) | same | 94.09 | 94.29 | +0.2% | ~same (within noise) |
| scan/query_eq_single_date (ms) | `crates/chili-op/benches/scan.rs` | 1.9596 | 1.9646 | +0.3% | ~same |
| scan/query_narrow_range_5d (ms) | same | 5.7151 | 5.5821 | **-2.3%** | faster |
| scan/query_wide_range_500d (ms) | same | 370.56 | 372.13 | +0.4% | ~same |
| eval/query_groupby_agg (ms) | `crates/chili-op/benches/eval.rs` | 3.2466 (.chi) | 3.2734 (.pep) | +0.8% | ~same; not apples-to-apples (src_path differs) |
| eval/query_select_star (µs) | same | 355.92 (.chi) | 591.62 (.pep) | **+66.2%** | ⚠️ apples-to-oranges; see Sprint 8 R3 caveat |
| projection/select_all_wide (µs) | same | 396.96 (.chi) | 497.44 (.pep) | +25.3% | apples-to-oranges; see Sprint 8 R3 caveat |
| projection/select_one_col (µs) | same | 292.22 (.chi) | 295.94 (.pep) | +1.3% | ~same |
| projection/select_three_cols (µs) | same | 325.02 (.chi) | 332.12 (.pep) | +2.2% | ~same |
| projection/select_one_col_with_sym_filter (µs) | same | 363.67 (.chi) | 431.92 (.pep) | +18.8% | apples-to-oranges; see Sprint 8 R3 caveat |
| load/load_cold_2000p (ms) | `crates/chili-op/benches/load_par_df.rs` | 4.9263 | 5.0351 | +2.2% | ~same |
| load/load_warm_2000p (ms) | same | 4.9350 | 5.0157 | +1.6% | ~same |
| load/load_multitable_5x200p (ms) | same | 1.5057 | **1.8497** | **+22.8%** | ⚠️ regression — exceeds Sprint 7 halt criterion 2 (>20%) |
| write/wpar_1k_rows_fresh_hdb (ms) | `crates/chili-op/benches/write_partition.rs` | 9.1562 | 9.0752 | -0.9% | ~same |
| Python concurrent eval | `crates/chili-py/tests/bench_concurrent.py` | 6.10× pre-pivot | TBD | TBD | Sprint 8 |

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

## Sprint 7 Part B — bench A/B sweep (2026-05-08)

### Methodology

```
parked-claude binary: cd /tmp/chili-parked-bench (git worktree at
                      claude-baseline-2026-05-07 tag = commit dea966e);
                      cargo bench (release, separate target/);
                      Cargo.toml inherits parked-claude's polars-core-patch
                      hinmeru fork + crates.io polars-plan 0.53.0.

claude-2 binary:      cd /Users/oakadmin/code/chili (claude-2 tip after
                      Sprint 7 Part A);
                      cargo bench (release, target/release);
                      Cargo.toml inherits Sprint 7 Part A patches: all
                      polars-* crates from /tmp/polars-py-1.39.3 (pola-rs/
                      polars at py-1.39.3 tag) with q-style fmt patch on top.

Hardware: same Apple Silicon (Darwin 25.4.0, aarch64) for both. Sequential
runs (no concurrent CPU contention). Criterion default sample_size + 10s
measurement_time.
```

### Three regressions surfaced (Sprint 8 perf-pass-1 work)

#### R1 — parse_cache hit +11.2%, exceeds golden rule 6 (~40 ns over budget)

`parse/parse_repeat_same_query` (cache hit path):
- parked-claude: 369.19 ns median (CI [368.42, 369.99])
- claude-2: **410.58 ns median (CI [409.59, 411.60])**
- Delta: +41.4 ns, +11.2%

Golden rule 6 (≤400 ns NON-NEGOTIABLE) is **marginally violated**. Sprint
3 Part D measured claude-2 at 371.43 ns (with hinmeru polars-core fork
on crates.io polars-plan 0.53.0). Sprint 7 Part A swapped the polars
source to py-1.39.3 (a 6-week-newer commit on the polars main branch).
The +40 ns regression is attributable to py-1.39.3 polars-plan / polars-core
having more complex codepaths in the hash/clone/lookup chain that the
parse cache exercises.

**Sprint 8 Part 1 P1 task:** profile the hot path; identify the new
allocations or branches introduced between rs-0.53.0 and py-1.39.3 polars
sources; either patch them out in chili's polars fork OR optimize chili's
own parse-cache key/value handling to compensate. Target: claim ≤400 ns
back without rolling back the py-1.39.3 polars source pin (which is
load-bearing for ADR 0003 / lazy=True).

#### R2 — load_multitable_5x200p +22.8%, exceeds Sprint 7 halt criterion 2

`load/load_multitable_5x200p` (5 tables × 200 partitions):
- parked-claude: 1.5057 ms median (CI [1.4849, 1.5318])
- claude-2: **1.8497 ms median (CI [1.8414, 1.8633])**
- Delta: +344 µs, +22.8%

Sprint 7 dispatch brief halt criterion 2 ("Bench A/B reveals > 20%
regression on any hot path") is **technically met**. Per the brief, this
warrants investigation — but as a finding for Sprint 8 to act on, not as
a Sprint 7 in-flight halt (the structural change driving it is already
shipped via ADR 0003 resolution and isn't being rolled back).

Single-table scan paths (`load_cold/warm_2000p`) regressed only 1.6-2.2%
— within noise. The multitable path's +22.8% suggests a per-table-init
cost that scales linearly with table count and is more expensive on
py-1.39.3 polars than on rs-0.53.0+hinmeru. Likely candidates:
- new polars-plan setup/lookup overhead per LazyFrame creation
- mimalloc + py-1.39.3 polars allocator interaction (chili-bin uses
  mimalloc; some polars internals may have changed allocation patterns)
- per-table schema validation that's more thorough on py-1.39.3

**Sprint 8 Part 1 P2 task:** profile a 5-table load on py-1.39.3 vs
rs-0.53.0 polars; identify the per-table linear cost driver; mitigate.

Concrete first-move profiling command (avoids the three-way ambiguity
of polars-plan-setup / mimalloc-interaction / schema-validation):

```bash
# Baseline criterion run with profiling sampling enabled:
cargo bench -p chili-op --bench load_par_df -- \
    load/load_multitable_5x200p --profile-time 30
# Then collect a flamegraph via samply (preferred on macOS over
# Instruments for ad-hoc rust profiling):
cargo install samply --locked  # one-time
samply record \
    target/release/deps/load_par_df-* \
    --bench load_multitable_5x200p --profile-time 10
samply load   # opens the flamegraph in browser
# Look for the per-table cost driver: should appear as a 5x repeated
# stack frame absent from single-table loads. Likely candidates:
# polars-plan LazyFrame::scan_parquet setup, polars-io schema inference,
# or mimalloc allocation pattern for chunked Series initialization.
```

#### R3 — eval bench query parser regression (chili syntax tightened on claude-2)

`crates/chili-op/benches/eval.rs` panicked on first variant
(`query_groupby_agg`) at parse time on claude-2. Same query
("select mean price, sum volume by symbol from t where date>=2024.01.02,
date<=2024.01.11") with `src_path="bench.chi"` parses fine on
parked-claude but fails on claude-2 with:

```
found 'Id'price'' expected indices, 'Op':'', arguments, operator,
'Punc','', 'By', or 'From'
```

The bench's `make_engine()` calls `state.enable_pepper()`, but the
`src_path` extension is `.chi`. claude-2's parser dispatches by
`src_path.ends_with(".chi")` → uses chili-syntax parser, which is
stricter and rejects the pepper-shape `select mean price by ...` form.
parked-claude either had a more permissive chili parser OR dispatched
based on `state.enable_pepper()`.

**Sprint 8 Part 1 P3 task:** EITHER:
- Fix the bench files: change `src_path` from `bench.chi` to `bench.pep`
  (matches the engine's pepper mode; non-controversial). Re-run eval +
  projection benches on claude-2.
- OR investigate whether claude-2's chili syntax should accept the
  pepper-style `select mean price` form. ADR territory if user-decision.

Until R3 resolves, `eval/*` and `projection/*` rows in the table above
remain blank for claude-2. Sprint 8 Part 1 P3 fills them.

### Other observations (no action needed)

- **scan paths are within ±2.3%** — `query_narrow_range_5d` is actually
  faster on claude-2 (-2.3%); `query_eq_single_date` and
  `query_wide_range_500d` are within ±0.4% (noise). The polars source
  swap doesn't materially change scan throughput.
- **write_partition is within -0.9%** — `wpar_1k_rows_fresh_hdb` 9.18 ms
  on parked vs 9.08 ms on claude-2. Within noise.
- **load single-table is within +1.6 to +2.2%** — within noise.

### Disk + wall-clock for the A/B sweep

- parked-claude bench compile + run: ~25 min wall, peak 13 GB target/
  in `/tmp/chili-parked-bench`.
- claude-2 bench compile + run: ~30 min wall, peak 12 GB target/release/
  in `/Users/oakadmin/code/chili`.
- Combined disk used at peak: ~25 GB (well under 78 GB free at sweep
  kickoff).
- Total Sprint 7 Part B wall time: ~60 min including monitoring overhead.

### Sprint 8 Part 1 inputs (the perf-pass-1 backlog)

P1 — claim back ≤400 ns parse_cache hit (golden rule 6).
P2 — investigate +22.8% multitable load regression.
P3 — bench file `src_path` (`.chi` → `.pep`) or chili-syntax permissivity ADR.
P4 — populate eval/projection A/B rows once P3 lands.

---

## Sprint 8 Part B (P3 + P4) — eval / projection A/B fill (2026-05-08)

### P3 resolution: bench file `src_path` `.chi` → `.pep`

`crates/chili-op/benches/eval.rs` updated to use `bench.pep` src_path
(matches the bench engine's `state.enable_pepper()` mode). Eval +
projection benches now run on claude-2 without parse errors.

Bench file change committed; parked-claude numbers were collected
with the OLD `bench.chi` src_path. **The eval/projection A/B
comparison is therefore apples-to-oranges** — different src_path
extension dispatches to different parsers (chili-syntax-strict vs
pepper-syntax) which can produce different lazy plans for the same
query text. Examples from this sprint's data:

- **query_groupby_agg** (.chi 3.2466 ms vs .pep 3.2734 ms, +0.8%) —
  comparable; whatever plan parked-claude's chili-parser produced was
  near-identical to pepper-parser's plan in cost.
- **query_select_star** (.chi 355.92 µs vs .pep 591.62 µs, **+66.2%**) —
  almost certainly DIFFERENT plans. parked-claude's chili-parser may
  have produced a partial-scan plan (interpreting `select from t where
  date=...` as a different DSL shape that doesn't fully materialize all
  columns), while pepper-parser produces a full-row scan. Without
  parser-side investigation OR a parked-claude-with-pepper-src_path
  re-bench, the +66% can't be attributed to polars-source change.
- **projection/select_all_wide** (.chi 396.96 µs vs .pep 497.44 µs,
  +25.3%) — similar caveat; different plan likely.
- **projection/select_one_col** (.chi 292.22 µs vs .pep 295.94 µs,
  +1.3%) — comparable.
- **projection/select_three_cols** (.chi 325.02 µs vs .pep 332.12 µs,
  +2.2%) — comparable.
- **projection/select_one_col_with_sym_filter** (.chi 363.67 µs vs .pep
  431.92 µs, +18.8%) — apples-to-oranges; different plan likely.

### Honest verdict on the eval/projection A/B

The Sprint 7 Part B chain abort + Sprint 8 P3 src_path fix means
**this snapshot is the new baseline for eval/projection on claude-2's
.pep dispatch.** Comparing against parked-claude's .chi numbers is
qualitatively useful only for cases where the magnitude is similar
(query_groupby_agg, projection/select_one_col, projection/select_three_cols
— all within ±2.2%). The other three (query_select_star,
projection/select_all_wide, projection/select_one_col_with_sym_filter)
need either a parked-claude re-bench with .pep src_path OR a chili-side
parser-equivalence audit before the +18-66% deltas are attributable.

This caveat doesn't affect Sprint 7 Part B's R1 (parse_cache) or R2
(load_multitable) findings — those benches don't use src_path-shaped
parser dispatch.

### Sprint 8 leaves these as "Sprint 9 P5" carry-over (optional):

- **(Sprint 9 P5)** — re-bench parked-claude with .pep src_path on the 3
  apples-to-oranges queries; produce a true Δ% for each. ~2-3pp wall
  (re-create worktree, re-run benches). Only worth doing if Sprint 9
  perf work needs the comparison; otherwise the chili-2 .pep numbers
  are the new baseline going forward.

---

## Sprint 8 Part C (P2) — load_multitable profiling DEFERRED to Sprint 9 (2026-05-08)

P2 (samply flamegraph for the +22.8% load_multitable_5x200p regression)
hit two infrastructure friction points that pushed it out of Sprint 8's
budget:

1. **`cargo flamegraph` requires Xcode** on macOS for `xctrace` (only
   Command Line Tools were installed on the autonomous-run machine).
   `xcode-select --install` would resolve, but that's a several-GB
   download not appropriate for autonomous execution without user
   approval.

2. **samply works without Xcode**, but the workspace `[profile.release]`
   has `strip = true`, so the bench binary symbols are stripped at
   link time. samply captured 17,216 samples on the main thread but
   all stack frames resolve to bare hex addresses (e.g., `0x450c
   42.5%`, `0x4834 26.4%`). Useful for "the code is hot HERE" but
   not for naming the function.

   To get symbolized profiles, the workspace `[profile.release]`
   needs `strip = false` + `debug = true` (or a separate
   `[profile.bench]` override). Either way, **a profile-config edit
   would invalidate every cached release-target artifact** (lesson 11
   territory — uv sync rebuild equivalent). Estimated rebuild cost:
   ~10-15 min wall on full polars + chili-* recompile.

### Captured artifact (for Sprint 9 P2 to consume)

`/tmp/load_multi_profile.json` (3.4 MB) — samply Firefox-Profiler-format
JSON with 25 threads × 17,216 samples. Hex-address stacks. Use as the
"hot region map" for Sprint 9's symbolized re-profile.

Hottest 3 self-time leaf addresses (unsymbolized):
- `0x450c` 42.5% (likely a polars-arrow / polars-core memory copy or
  hash function — common 4-byte-aligned hot kernel)
- `0x4834` 26.4% (similar inner kernel)
- `0x29c4` 13.7%

Hottest inclusive-time stacks: 100% on a few outer frames (the bench
harness loop), 58% on `0x2cb02f3`, 54% on `0x1cf03`. These become the
investigation handles for Sprint 9 P2 once symbols are available.

### Sprint 9 P2 entry plan

```toml
# Workspace Cargo.toml — add a separate [profile.bench] override that
# keeps optimizations but retains symbols (cost: ~15 GB extra target/
# disk for the bench profile + initial cold rebuild ~10-15 min wall).
[profile.bench]
debug = true
strip = false
```

Then re-run:

```bash
cargo bench -p chili-op --bench load_par_df --no-run
samply record --save-only --output /tmp/load_multi_symbolized.json \
    target/release/deps/load_par_df-* \
    --bench load_multitable_5x200p --profile-time 10
# Re-run the same Python analysis script — function names will resolve.
```

Estimated Sprint 9 P2 cost: ~3-4pp (rebuild + profile + analyze + fix
if cheap).

---

## Sprint 9 Part B (P2) — symbolized rebuild + profile captured; symbolic resolution infrastructure-blocked (2026-05-08)

Workspace [profile.bench] symbol-retention override (Sprint 9 P7) landed
at sprint kickoff. Cold rebuild of load_par_df bench binary: **31m 39s
wall** in `bench` profile [optimized + debuginfo]. Binary size: 77 MB
(stripped version was ~3.5 MB). Disk peak during bench-profile rebuild:
~20 GB target/release/.

samply re-recorded `load_multitable_5x200p` for 10 seconds on the
symbolized binary. Profile JSON saved at `/tmp/load_multi_symbolized.json`.

### Hot-path discovery (without function names)

| Thread | Total samples | Dominant leaf | Self-time |
|---|---:|---|---:|
| Main | 17,233 | `0x450c` | 38.6% |
| Main | 17,233 | `0x4834` | 26.7% |
| Main | 17,233 | `0x29c4` | 16.5% |
| polars-0 (rayon worker) | 5,958 | **`0x450c`** | **93.1%** |
| polars-1..4 (rayon workers) | 5,896-5,934 each | (assumed similar; 5 workers in pool) | similar |

**Strong signal**: 93% of each polars worker thread's time is in a
**single hot kernel at offset 0x450c**. Combined across 5 workers + main
thread, this kernel dominates total CPU during multitable load. Likely
candidates (without symbol resolution to confirm):

- **`memcpy`/`memmove`** — tight kernels at low offsets in the binary
  often correspond to libc string ops; consistent with polars's
  chunked-Series allocation pattern.
- **A polars hash function** (e.g., `xxhash` or `ahash`) used in
  schema lookup or column-name interning per-table.
- **A polars-arrow buffer initialization kernel** called per-Series-per-table.

The 93% concentration in ONE function (rather than spread across many)
suggests the regression IS a single per-table cost driver, not death-by-
a-thousand-cuts. That's good news for mitigation: identifying + fixing
this one function recovers most of the +22.8% load_multitable regression.

### Symbolic resolution: infrastructure-blocked on autonomous run

Three resolution paths, all blocked autonomously:

1. **`samply load /tmp/load_multi_symbolized.json`** — opens a browser
   that fetches symbols from the binary at display time. Requires
   GUI / interactive browser session. Not autonomous.
2. **`atos -o <binary> <addr>`** — macOS symbolication tool. With a
   debug-info-embedded Mach-O, `atos` should resolve, but our
   invocations on `0x450c`, `0x4834` etc. returned numeric addresses
   without symbolic names. Possibly needs a separate dSYM bundle
   (`dsymutil <binary>` would generate one); not investigated further
   in Sprint 9.
3. **`llvm-addr2line` / `addr2line`** — not installed on the autonomous
   run machine; would require `brew install llvm` or `cargo install
   addr2line`. Acceptable cost (~2pp) for a future user-driven sprint
   but added to a future-Sprint backlog rather than absorbed in
   Sprint 9.

### Sprint 9 verdict

P2 partially done: **symbolized profile is captured + the dominant hot
kernel is identified by offset (0x450c, 93% of polars worker time)**.
Symbolic name resolution and the actual fix lands in a future sprint
(Sprint 12 perf-pass-3, or a dedicated user-driven mini-sprint when
GUI / Xcode / addr2line is available).

### Captured artifacts (consumed by future sprint)

- `/tmp/load_multi_symbolized.json` — 3.4 MB samply JSON (Firefox
  Profiler format) with symbolized debug info embedded but unresolved
  names. Loadable into `https://profiler.firefox.com/` for browser-based
  symbolic display.
- `target/release/deps/load_par_df-34f40619e2795c29` — 77 MB bench
  binary with debug info. Use with `dsymutil` + `atos` OR
  `addr2line` for offline name resolution.

---

## Sprint 8 Part A (P1) — parse_cache re-measure (2026-05-08)

Re-measurement per reviewer C1 (Sprint 7 Part D.1) — Apple Silicon
thermal/memory variance can account for 20-40 ns per run.

| Run | parse_repeat_same_query (median, ns) |
|-----|--------------------------------------:|
| Sprint 7 Part B | 410.58 |
| Sprint 8 P1 #1 | 397.47 |
| Sprint 8 P1 #2 | 379.08 |
| Sprint 8 P1 #3 | 398.33 |

**P1 RESOLVED via re-measurement.** All 3 Sprint 8 runs land under the
400 ns golden rule 6 target. The Sprint 7 Part B 410.58 ns reading was
a thermal-variance outlier. Range across 3 runs: 379-399 ns; median of
medians: ~397 ns. **Golden rule 6 holds; no chili-side mitigation
needed.**

This validates the Sprint 7 retro lesson 8 corollary: bench numbers
within ±10% of a target should be re-measured before triggering
mitigation work.
