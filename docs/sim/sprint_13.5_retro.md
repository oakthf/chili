# Sprint 13.5 retro — bench infrastructure + state audit (pre-Sprint-14 measurement)

**Wrap:** 2026-05-09
**Predicted:** 8–12 pp
**Actual:** ~10 pp
**Variance:** 0 % vs midpoint (10) — at-band
**Owner:** coordinator-solo (main Claude); no subagent dispatch (no chili source code touched).
**Plan reference:** [`../history/sprints/sprint_13.5_dispatch_brief_2026-05-09.md`](../history/sprints/sprint_13.5_dispatch_brief_2026-05-09.md)

---

## Wrap status: 9/9 gates green — Sprint 14 P3.2b proceeds

**Binary success criterion (from brief):** all 5 deliverables committed AND
profile shows GIL hold ≥ 40 % of wall time on the concurrent-load shape.
**Result: GIL hold ≈ 92.5 % cumulative-thread-time on 4-worker direct-FFI
shape; throughput shape (1.0× scaling) confirms; halt threshold cleared
decisively.**

---

## Scope shipped

**Code:** none (chili source untouched per brief invariant). Only bench
files + docs.

**New files:**

- `crates/chili-py/tests/bench_concurrent.py` (commit `65bcb7d`) — 4-shape
  Python concurrent throughput harness. Surfaces both the GIL-released
  path (`load_partitioned_df` → `fn_call` → `py.detach`) and the GIL-held
  path (`engine.engine.load_par_df` direct FFI binding).
- `crates/chili-op/benches/categorical_eval.rs` (commit `65bcb7d`) —
  criterion bench: `repeated` vs `distinct` symbol filter for P3.4 fate
  evidence.
- `crates/chili-op/Cargo.toml` (commit `65bcb7d`) — `[[bench]]
  categorical_eval` entry.
- `docs/sync/load_par_df_state_audit.md` (commit `<this wrap>`) — 200-line
  standalone state-audit doc for Sprint 14 readiness; verdict GREEN.

**Updated docs:**

- `docs/bench/post_pivot_baseline_2026-05-07.md` — Sprint 13.5 section
  added with B.1 / B.2 / B.3 / C / readiness-summary tables.
- `docs/sim/sprints_index.md` — Sprint 13.5 row updated to Wrapped.
- `docs/sim/cadence_metrics.md` — row 13.5 appended.

**Tests:** unchanged. Pre/post Sprint 13.5: 166 Rust (`cargo test --workspace
--exclude chili-py`) + 65 chili-py pytest. ✓

**Bench delta (claude-2 HEAD vs Sprint 13 baseline):**

- `parse_cache hit`: 397 ns (Sprint 8) → **377 ns** (Sprint 13.5). −5.0 %
  (within ±10 % thermal-noise band per lesson 15). **Golden rule 6 holds.**
- `load_multitable_5x200p`: 1.92 ms (Sprint 13 post-revert) → **1.92 ms**
  (Sprint 13.5). 0 % (identical, confirms revert restored byte-equivalence).
- `categorical_filter`: NEW. repeated 357.7 µs vs distinct 359.2 µs — Δ
  0.4 % within noise.

---

## Lessons (durable)

### 1. Bench infrastructure first; pre-specifying gate thresholds without measurement is the Sprint 13 anti-pattern

**Rule.** Before any optimization sprint that targets a measurable
property (concurrency throughput, latency, allocation rate), run a
measurement-only sprint that produces the bench harness, baseline
numbers, and profile evidence on the SAME shape the optimization will
target. Only after the baseline is in hand do you set the bench-gate
threshold for the implementation sprint, and only with reference to the
observed range.

**Why.** Sprint 13 set bench-gate `load_multitable_5x200p ≤ 1.65 ms`
upfront based on inference, not measurement. The implementation hit
zero gain, the brief's rollback criterion fired, the sprint reverted.
Sprint 13.5 inverted the order: bench harness first, then capture
baseline + profile, THEN at wrap recommend Sprint 14's gate threshold
based on observed `concurrent_load_direct ≈ 4.85 K calls/s` baseline
and `concurrent_load ≈ 13.1 K calls/s` ceiling — i.e. "Sprint 14
should aim for `concurrent_load_direct` to approach the `concurrent_load`
shape, ±10 % of 13 K calls/s at N=4." That target is grounded; the
Sprint 13 target was speculative.

**Apply where.** Any sprint whose binary success criterion is a
quantitative bench delta. Especially load-bearing on optimization-shape
sprints (perf-pass, FFI changes, lock-contention reductions). Pair with
lesson 2 (verify-before-claim) and lesson 15 (re-measure within ±10 %).

**Cost saved.** Sprint 13's revert cost was ~3 pp wasted plus the +60 %
regression-bug round; if the bench infrastructure had landed first,
Sprint 13's brief would have pre-quantified the 17.7 % `Box::new` upper
bound on chili-side allocation gain and the sprint would have been
descoped to an audit-only pass. **Avoidable cost: ~3 pp + a sprint of
trust delta.** Worth promoting to durable rule.

### 2. The user-facing API path may already release the GIL — verify which FFI binding the typical caller hits before wrapping the FFI in `py.detach`

**Rule.** Before proposing "release the GIL on FFI method X" as an
optimization, trace the call path from the user-facing Python wrapper
through the FFI binding. If the wrapper routes through a different
GIL-releasing intermediate (`fn_call`, `eval`), the typical caller may
already see GIL release — and the optimization only matters for callers
that bypass the wrapper.

**Why.** Sprint 13.5 Part D found that
`chili.engine.ChiliEngine.load_partitioned_df()` routes through
`engine.engine.fn_call("load", [hdb])` rather than calling
`engine.engine.load_par_df()` directly. `fn_call` releases the GIL
(`lib.rs:527`). The bench data confirms it: `concurrent_load` (fn_call
path) scales to 13.1 K calls/s at N=4; `concurrent_load_direct`
(direct FFI) stays flat at 4.85 K calls/s. Sprint 14 P3.2b "release GIL
on `load_par_df`" is a no-op for typical Python callers; it only
unblocks callers that reach the FFI directly. This shapes Sprint 14's
positioning: the change is correctness-symmetry (every FFI method
should release the GIL by default) more than user-visible perf.

**Apply where.** Any FFI-surface optimization or audit. Especially in
projects where the binding crate exposes both raw FFI methods and
higher-level Python wrappers that route through registered functions.

**Cost saved.** Without this audit, Sprint 14's claimed gain ("~2.7×
concurrent throughput uplift") would have been miscommunicated to mdata
as "fixes a perf bottleneck for your existing `load_partitioned_df`
calls" when in fact those calls are already GIL-released. Misleading
delivery doc avoidance: ~1 pp wasted plus user-trust hit. Worth
promoting.

### 3. (candidate, not yet promoted) `addr2line` on a `strip = true` Rust .so still resolves chili-side symbols on macOS — symbol table is preserved even when debuginfo is stripped

**Rule.** On macOS arm64, `[profile.release] strip = true` in workspace
Cargo.toml strips debuginfo but **not** the runtime symbol table.
`addr2line -e <chili.so> -f -C 0x<offset>` works for symbol resolution;
samply does not auto-pick these up but the manual fallback is fast.

**Why.** Sprint 12 P2 (lesson 17) concluded "macOS samply autonomous-run
produces unsymbolicated profiles." Sprint 13.5 Part C found that 274,862
mangled Rust symbols are present in the stripped 116 MB chili-py .so;
addr2line resolved 8 of 8 attempted chili-side leaf addresses cleanly
(`build_par_df_entry`, `chrono::format::strftime::StrftimeItems::next`,
etc.). Lesson 17 is more nuanced than "no symbols ever" — it's "samply
doesn't auto-resolve, but addr2line as a manual fallback works."

**Apply where.** Any future profiling sprint on macOS. Always try
addr2line on top-N hot offsets before concluding "symbolication blocked."

**Cost saved.** Avoids triggering a rebuild with `strip = false` (~5–10 min
lesson 8 cost) when the symbols are already resolvable. **Saves ~3 pp
per occurrence.** Single occurrence so far; promote on second
observation.

---

## A.2.2 descope note (per user direction 2026-05-09)

The original audited proposal (`docs/history/proposals/perf_alternatives_post_mimalloc_2026-05-08.md`
§A.2.2) proposed releasing the `vars` write-lock around heavy DataFrame
ops in `upsert_var` / `insert_var` (`engine_state.rs:277-382`). The
only feasible implementation is **clone-then-swap**: clone the inner
DataFrame outside the lock, perform `df.extend()` on the clone, swap
back inside a brief lock window.

**User concern (load-bearing for the descope):**

> "The A.2.2 clone then swap is a 'write' component performance enhancement,
> at a cost of magnifying memory size. Also clone is a heavy read and
> write — although it is not I/O, you still need to clone — I am not
> sure that justify the concurrent gain. I will say descope A.2.2
> entirely first. Note down my concern and we will do profiling first
> in the future before we relook at this."

**Concrete cost:** for mdata's typical 5 M-row table, clone-then-swap
allocates 200–500 MB transient memory per upsert call. That's a
significant memory-pressure cost paid for an unmeasured concurrency
gain. The borrow-checker constraint that forces clone-then-swap
(`vars.get_mut(id)` returns `&mut SpicyObj` borrow inside the lock
guard, can't release lock while borrow live) is structural; without
clone there's no way to release the lock around `df.extend()`.

**Reopening criterion:** profile evidence showing lock contention on
`vars.write()` is dominant on a representative concurrent upsert
workload. Until then, A.2.2 stays descoped.

This is documented here in the retro (per user direction "note down my
concern") and in the audit's §3.8 (`docs/sync/load_par_df_state_audit.md`).
Future Claude reading the proposals doc should not re-propose A.2.2
without this evidence.

---

## P3.4 fate (post Sprint-13.5 evidence)

The categorical_eval bench (Part A.2 / B.3) measured `repeated` (357.7 µs)
vs `distinct` (359.2 µs) — Δ 0.4 %, statistically indistinguishable.
**P3.4 (Categorical mapping cache) is deferred indefinitely.** No
measurable target at the chili-eval level on the current polars version.
Reopen only with profile evidence on a real workload that surfaces
per-call rebuild cost the bench did not reproduce. Following the same
"profile-evidence-before-implementation" lesson 1 above, ADR 0005 stays
dropped.

---

## Pp accounting

| Item                                                         | Predicted | Actual |
|--------------------------------------------------------------|----------:|-------:|
| Brief authoring + commit + audit pass                        | 1.0       | ~1.0 (pre-Sprint-13.5 work) |
| A.1 `bench_concurrent.py` + smoke-test debug (`avg`→`mean`)  | 2–3       | ~1.0 |
| A.2 `categorical_eval.rs` + Cargo.toml entry                 | 1.0       | ~0.5 |
| Pre-commit gate + commit Part A                              | 0.5       | ~0.3 |
| B.1 0.8.2 baseline (clean venv install + sweep)              | 0.5–1     | ~0.5 |
| B.2 HEAD wheel build (release) + baseline                    | 1–2       | ~1.5 (5 m 48 s wall) |
| B.3 Rust criterion benches (parse_cache + load_par_df + categorical_eval) | 2–4 | ~2.5 (16 + 21 + ~2 min compile + bench) |
| C samply concurrent profile + module-level + addr2line resolve | 1.5–2 | ~1.5 |
| D state audit doc                                            | 1–2       | ~1.0 |
| E wrap (retro + cadence + sprints_index + brief move)        | 1.0       | ~1.0 |
| **Total**                                                    | **8–12**  | **~10** |

At-band (mid-point 10). Closest historical comparable: **Sprint 9**
(predicted 5–10, actual ~2 — but Sprint 9 was symbolization-blocked at
infrastructure level so it halted early). Sprint 13.5 unblocked
addr2line via the symbol-table-preserved-despite-strip finding (lesson 3
candidate), so it didn't hit Sprint 9's halt.

Pattern: cadence_metrics row 13.5 fits the **measurement-implementation
shape** (Sprint 8 predicted 6–12, actual ~4; Sprint 13.5 predicted
8–12, actual ~10). The two new bench-binary compiles (B.2 wheel +
B.3 load_par_df bench rebuild) cost ~3.5 pp wall combined — roughly
the lesson 8 floor.

---

## What surprised

- **Sprint 14's premise was already empirically supported by the
  throughput shape alone, before any profile.** `concurrent_load_direct`
  flat at 4.85 K calls/s × N ∈ {1,2,4,8} with p99 latency stacking
  linearly is a textbook GIL-serialization fingerprint. The samply
  profile confirmed (92.5 % kernel time) but didn't add new information
  beyond "yes, the throughput shape was diagnostic." Future readiness
  audits could short-circuit profile capture if the throughput shape is
  this clean.
- **HEAD wheel ≡ 0.8.2 wheel within ±5 %.** Sprint 13's revert restored
  byte-equivalence on the bench-relevant code paths; B.2 vs B.1 deltas
  were thermal-noise. Useful as a Sprint 14 A/B reference: any post-
  Sprint-14 throughput delta is a clean isolation of the P3.2b change.
- **The user-facing wrapper-route-through-`fn_call` finding (lesson 2
  above) was unexpected.** I went into Part D expecting to confirm
  the audit doc's "GIL is held during direct FFI." The finding that
  the wrapper path already releases the GIL is the more interesting
  takeaway — and reframes Sprint 14 from "perf optimization for typical
  callers" to "FFI-symmetry correctness for direct-FFI callers." mdata
  delivery doc for Sprint 14 should be careful about this framing.
- **`addr2line` on a stripped .so worked** (lesson 3 candidate). I
  expected to fall back to "frames unresolved, document as
  Sprint-14+-followup" per Sprint 12 P2 lesson 17. Sprint 12 P2's
  conclusion was over-narrow; the symbol table survives `strip = true`
  on macOS arm64.
- **The `0x450c` collision.** Sprint 12 P2 found polars-internal
  `0x450c` accounting for 93.1 % of polars-worker time. Sprint 13.5
  Part C found `libsystem_kernel.dylib` hot offset `0x450c` accounting
  for 46.6 % of worker-thread time. **Different libraries, same
  numerical offset.** Easy mistake to conflate them in the retro
  narrative; flagged in the bench-doc Part C "Notes on profile
  resolution" section to prevent confusion in future readings.

---

## Cross-references

- **Plan:** [`../history/sprints/sprint_13.5_dispatch_brief_2026-05-09.md`](../history/sprints/sprint_13.5_dispatch_brief_2026-05-09.md) (post-ratification move)
- **Cadence metrics row 13.5:** [`cadence_metrics.md`](cadence_metrics.md)
- **Sprints index:** [`sprints_index.md`](sprints_index.md)
- **State audit (Part D output):** [`../sync/load_par_df_state_audit.md`](../sync/load_par_df_state_audit.md)
- **Bench results (Parts B + C output):** [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md) §"Sprint 13.5"
- **Sprint 13 retro (the lesson 2 trigger):** [`sprint_13_retro.md`](sprint_13_retro.md)
- **Audited proposal (revised plan input):** [`../history/proposals/perf_alternatives_post_mimalloc_2026-05-08.md`](../history/proposals/perf_alternatives_post_mimalloc_2026-05-08.md)
- **Implementation commits:** `65bcb7d` (Part A bench files), `<wrap-commit>` (Parts B/C/D docs + retro)
- **Related artifacts (uncommitted, /tmp):** `/tmp/sprint_13.5_baseline_0_8_2_concurrent.json`, `/tmp/sprint_13.5_baseline_head_concurrent.json`, `/tmp/sprint_13.5_concurrent_load_profile.json`, `/tmp/sprint_13.5_head_dist/chili_sauce-0.8.2-cp310-abi3-macosx_11_0_arm64.whl`

---

## Sprint 14 hand-off

**Proceed with P3.2b (release GIL on `engine.load_par_df` direct FFI).**
All 9 readiness gates green (table in `post_pivot_baseline_2026-05-07.md`).

**Recommended Sprint 14 scope:**

1. Wrap `engine.load_par_df` in `py.detach(...)` (`crates/chili-py/src/lib.rs:532`),
   following the `fn_call` pattern at line 527.
2. Same change for `clear_par_df` (`lib.rs:539`) for symmetry — same
   audit applies (Part D §5.2).
3. Bench gate: `concurrent_load_direct` N=4 should approach
   `concurrent_load`'s shape — Sprint 14.0 baseline ~4.85 K calls/s,
   target post-implementation ≥ 12 K calls/s (within ±10 % of the
   `concurrent_load` ceiling 13.1 K calls/s).
4. Re-bench all 4 shapes via `tests/bench_concurrent.py` against the
   Sprint 14 wheel for symmetry confirmation.
5. Reviewer dispatch (`code-reviewer`) per lesson 7 — Sprint 14 touches
   FFI surface, lesson 7 binds.

**Out of scope for Sprint 14:**

- A.2.2 vars-write-lock release (descoped indefinitely; see this retro).
- A.2.4 Parquet codec tuning (deferred to Sprint 15).
- P3.4 Categorical cache (deferred indefinitely; see this retro).
- ADR 0005 Categorical cache invalidation (dropped).
- Polars-internal kernel optimization (`0x450c` 93.1 % polars-worker
  time, Sprint 12 P2) — blocked on user-driven P0 (GitHub-host the
  polars fork).
