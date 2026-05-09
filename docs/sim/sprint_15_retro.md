# Sprint 15 retro — Parquet codec + row_group_size public API + 0.8.3 wheel cut (A.2.4)

**Wrap:** 2026-05-09
**Predicted:** 7–11 pp (post-audit; original was 6–10)
**Actual:** ~9 pp
**Variance:** 0 % vs midpoint (9) — at-band
**Owner:** coordinator-solo + `code-reviewer` subagent dispatch (Part D, lesson 7).
**Plan reference:** [`../history/sprints/sprint_15_dispatch_brief_2026-05-09.md`](../history/sprints/sprint_15_dispatch_brief_2026-05-09.md) (audited; 3-agent parallel review at brief stage).

---

## Wrap status: BINARY SUCCESS — bench gate met + reviewer ship-as-is + 0.8.3 wheel cut

**Binary success criterion (from brief):** ≥ 3 codec variants benched with
measured (write throughput, on-disk size); new API exercised end-to-end
via pytest assertion of round-trip equivalence; default code-path produces
byte-identical output to 0.8.2.

**Result: ALL THREE met.** 4 codec variants benched (Snappy / Zstd /
Lz4Raw / Uncompressed) with wall + size measurements; 7 new pytest tests
including the load-bearing
`TestParquetWriteConfig::test_default_args_byte_equivalent_to_0_8_2`
which asserts sha256 match + size equality against the 0.8.2 wheel
output; 0.8.3 wheel built and bundles Sprint 14 + Sprint 15 changes.

---

## Scope shipped

**Code:**

- `crates/chili-op/src/io.rs` — new `pub struct ParquetWriteConfig` +
  `parse_compression_name` helper. `write_partition` FFI takes 2
  trailing positional optional `SpicyObj` args (compression-name +
  row_group_size); `write_partition_native` adds
  `Option<ParquetWriteConfig>` trailing arg.
- `crates/chili-op/src/util.rs` — refactored
  `write_parquet_to_filepath_with_row_group_size` →
  `write_parquet_to_filepath_with_options(filepath, df,
  Option<ParquetCompression>, Option<usize>)`. Doc-comment now
  correctly states default codec is **ZSTD** (verified empirically;
  reviewer NIT-promoted-to-MINOR fold-in).
- `crates/chili-op/src/lib.rs` — re-export `ParquetWriteConfig`.
- `crates/chili-op/src/built_in_fn.rs` — `wpar` arg_num 7→9; param
  list grew with `compression`, `row_group_size`.
- `crates/chili-op/benches/write_partition.rs` — added 4 codec A/B
  variants on the existing wpar bench.
- `crates/chili-op/benches/common/mod.rs` +
  `crates/chili-op/tests/partition_filter_test.rs` — `None` arg
  threading on the 4 external `write_partition_native` callsites.
- `crates/chili-op/tests/eval_test.rs` — pre-existing latent bug fix
  (CHILI_SYNTAX env-var leak between tests; surfaced by Sprint 15
  recompile shuffling parallel-test order).
- `crates/chili-py/chili/engine.py` — `write_partitioned_df` +
  `overwrite_partition` add 2 keyword-only kwargs (`compression`,
  `row_group_size`). `overwrite_partition` parity per audit MAJOR.
- `crates/chili-py/tests/test_engine.py` — new `TestParquetWriteConfig`
  class with 6 tests + 1 mirror in `TestOverwritePartition`.

**Docs:**

- NEW `docs/decisions/0005-parquet-write-defaults.md` — the ADR.
  Documents the empirically-verified ZSTD default + override semantics +
  future-default-change protocol + explicit "provisional aspects" callout
  for the 2-arg-positional FFI shape.
- `docs/bench/post_pivot_baseline_2026-05-07.md` — new "Sprint 15 —
  Parquet codec A/B" section with codec wall-time table + on-disk size
  table + halt-criterion verdict.
- `docs/sync/ideas.md` — 3 future-perf ideas captured pre-Sprint-15
  (per-table mutex on par_df, RCU on par_df, coalesce concurrent loads
  on same hdb_path) — surfaced from Sprint 14 retro discussion.
- `docs/sync/mdata_chili_2026-05-09_delivery.md` — 0.8.3 delivery doc
  (Sprint 14 + Sprint 15 bundle).
- This retro.
- `docs/sim/cadence_metrics.md` — row 15 appended.
- `docs/sim/sprints_index.md` — Sprint 15 row → Wrapped.
- `CLAUDE.md` — state line: chili-py 0.8.2 → 0.8.3; ADR list grew to
  include 0005.

**Versions:**

- `crates/chili-py/Cargo.toml`: 0.8.2 → 0.8.3.
- `crates/chili-py/pyproject.toml`: 0.8.2 → 0.8.3.

**Wheel:** NEW `dist/chili_sauce-0.8.3-cp310-abi3-macosx_11_0_arm64.whl`.

**Tests:** Rust 166 (no change). chili-py pytest 65 → **72** (+7 new):

| Class | Test | Purpose |
|---|---|---|
| `TestOverwritePartition` | `test_overwrite_partition_with_zstd` | Mirror parity |
| `TestParquetWriteConfig` | `test_default_args_byte_equivalent_to_0_8_2` | sha256 regression check vs 0.8.2 wheel |
| `TestParquetWriteConfig` | `test_compression_routing_per_codec` | parquet-metadata codec field per variant |
| `TestParquetWriteConfig` | `test_zstd_smaller_than_snappy` | Compression ratio (zstd < snappy explicit) |
| `TestParquetWriteConfig` | `test_compression_uncompressed_larger_than_default` | Codec correctness (uncompressed > default) |
| `TestParquetWriteConfig` | `test_row_group_size_override` | Row-group-count assertion |
| `TestParquetWriteConfig` | `test_invalid_compression_raises` | Halt-criterion: silent fallback prevented |

**Bench delta:** new codec variants `wpar_1k_rows_codec_{zstd, snappy,
lz4_raw, uncompressed}` on the existing `write_partition` criterion
bench. Per-codec wall + on-disk size in
`docs/bench/post_pivot_baseline_2026-05-07.md` Sprint 15 section.

---

## Reviewer findings (Part D — `code-reviewer` subagent)

Dispatched per lesson 7 (FFI surface + new public API). Subagent token
usage: 49,062 tokens / 91 s wall.

| Item | Verdict | Action |
|---|---|---|
| Default-args byte-equivalence code path | OK | n/a |
| `Option<ParquetWriteConfig>` lifetime across `py.detach` | OK | n/a |
| `Option<ParquetCompression>` Copy-vs-clone on user_compression | NIT — should drop `.clone()` | Folded — fixed 4 sites (3 `user_compression.clone()` removals + 1 bench) |
| `parse_compression_name` error path | OK | n/a |
| **`"none"` alias for `Uncompressed` is a silent footgun** | **MINOR** | **Folded — alias removed; explicit comment added explaining why** |
| `wpar` arg_num 7→9 breaking change | OK (no chili-script callers found) | n/a |
| `overwrite_partition` parity | OK (already in scope from audit MAJOR) | n/a |
| `eval_test.rs` env-var fix structural fragility | MINOR | Acknowledged — track as follow-up; not blocking |
| **`util.rs:147` doc comment says "Snappy" not ZSTD** | **NIT-promoted-to-MINOR** | **Folded — corrected to "ZSTD (verified empirically; ADR 0005)"** |
| Bench `write_loop` redundant clone | NIT | Not folded — `Option<ParquetCompression>` is `Copy`, the clone is now removed by the bigger fold-in |
| Version bump deferred to Part E | OK | n/a (bumped at wrap) |

Verdict: **ship after 2 minors folded.** Both folded; verified gate
green post-fold (cargo fmt + clippy + 166 Rust tests + 72 pytest).

---

## Lessons (durable)

### 1. External-dependency defaults are load-bearing claims and must be empirically verified before drafting

**Rule.** Any sentence in a brief or audit of the form "the default for
[external dependency] is X" is a load-bearing claim under
`~/.claude/rules/verify-before-claim.md`. Before stating it, run the
5-minute verification (a `pq.metadata` check, `printenv`,
`cargo --print=cfg`, etc.). Both the brief author AND the audit
agents must run this check independently — inheriting the wrong premise
across a chain of agents is the failure mode.

**Why.** Sprint 15's brief assumed the default Parquet codec was Snappy
("default stays Snappy" was repeated language). Three audit agents
inherited the same wrong premise without re-verifying. The
implementation surfaced the bug at the first failing pytest:
`test_compression_zstd_smaller_than_default` failed with
`zstd=5878 == default=5878` because the default IS zstd. A 30-second
`pq.metadata` check at draft time would have caught this. The wasted
cost was ~0.5 pp (one failed test round + retest) plus the
inherited-assumption risk: if the brief had said "default is Snappy" in
the ADR text and the test hadn't caught it, future-Claude reading the
ADR would have inherited a wrong fact about the codebase.

**Apply where.** Any sprint that touches an external library's API
defaults: parquet codecs, polars features, pyo3 derive macros, rayon
thread-pool defaults, etc. Generalizes Sprint 13 lesson 2 to specifically
include "external-dependency defaults" as a load-bearing-claim category.

**Cost saved.** ~0.5 pp per recurrence (a failed test round) plus the
inherited-assumption risk. Worth promoting to a durable rule. **Promotion
candidate:** this is the second observation of the verify-before-claim
shape (Sprint 13 was the first); promote to
`docs/standards/iteration_lessons.md` as lesson 18.

### 2. (no second durable lesson this sprint)

The largest other theme is "audit-revealed gaps were correctly fixed" —
the 3-agent parallel audit caught 3 majors (overwrite_partition parity,
byte-equivalence baseline prerequisite, 4 missing call sites) AND the
reviewer at Part D caught 2 minors (`"none"` alias footgun + doc-comment
correction). Both pre-execution and post-execution review cycles paid
off. This validates the
`~/.claude/rules/self-audit-on-plans.md` + lesson 7 (reviewer-before-retro)
structural pairing — they catch different categories of issue.

The Sprint 14 lesson 1 candidate (dev-profile vs release-profile bench
A/B) was NOT applicable here because Part B benched via `cargo bench`
which always uses the release profile by default. No relevance.

---

## Pp accounting

| Item                                                              | Predicted | Actual |
|-------------------------------------------------------------------|----------:|-------:|
| Brief authoring + 3-agent audit + appendix fold-in + commit       | 1.5       | ~1.0 (audit found 3 majors; appendix added cleanly) |
| Part A.0 baseline capture (sha256 of 0.8.2 reference parquet)     | 0.3       | ~0.3 |
| Part A.1 ParquetWriteConfig struct + parse_compression_name       | 1.0       | ~0.7 |
| Part A.2 util.rs refactor + 4 external call sites                 | 0.5       | ~0.5 |
| Part A.3 wpar FFI + arg_num 7→9 + built_in_fn.rs param list       | 0.5       | ~0.3 |
| Part A.4 chili-py wrapper kwargs + overwrite_partition parity     | 0.5       | ~0.5 |
| Part A.5 7 pytest tests (TestParquetWriteConfig + mirror)         | 1.5       | ~1.5 |
| **eval_test.rs env-var leak debug + fix** (unplanned)             | 0         | ~0.5 (test ordering bug surfaced by recompile) |
| **Snappy-vs-Zstd default-codec discovery + test rework**          | 0         | ~0.5 (lesson 1 above) |
| Part B bench A/B (cargo bench compile + runtime + size capture)   | 1.5       | ~1.5 (release-bench compile ~19 min wall) |
| Part C ADR 0005 draft                                             | 1.0       | ~0.8 |
| Part D code-reviewer dispatch + 2-minor fold-in + gate re-run     | 1.5       | ~1.0 (subagent: 49K tokens / 91 s) |
| Part E.1 version bump (Cargo.toml + pyproject.toml)               | 0.1       | ~0.1 |
| Part E.2 build 0.8.3 release wheel + smoke test                   | 0.5       | ~0.4 |
| Part E.3 mdata delivery doc                                       | 0.5       | ~0.5 |
| Part E.4 wrap (retro + cadence + index + brief move + CLAUDE.md)  | 1.0       | ~1.0 |
| **Total**                                                         | **7–11**  | **~9** |

At-band (mid-point 9). Two 0.5 pp unplanned drains (env-var bug + ZSTD
default discovery) absorbed cleanly within the audit-driven +1.7 pp
buffer.

Pattern: matches **Sprint 5** shape (predicted 10–15, actual ~10 — new
public API + ADR + wheel cut + delivery doc). Sprint 15 ran narrower
(just ParquetWriteConfig, not whole lazy-frame API) so came in at the
low end of the comparable historical band, consistent with mid-band of
its own predicted band.

---

## What surprised

- **The default Parquet codec is ZSTD, not Snappy** (lesson 1 above).
  Both the brief and 3-agent audit assumed Snappy. The actual default
  surfaced from a failing pytest within 30 seconds of running the
  full suite. Reframed multiple test assertions and the ADR around
  this finding.
- **The Sprint 13 lesson 2 trap is general.** "Speculative claims need
  profile-evidence verification" generalized here: any external
  dependency default is a load-bearing claim. Sprint 15's lesson 1 is
  basically Sprint 13 lesson 2 specialized to one common case.
- **Pre-existing latent test ordering bug in `eval_test.rs`.** The env
  var `CHILI_SYNTAX="pepper"` was set in `pepper_tests::eval_case01`
  but never restored. Running test binaries in parallel had been
  hiding this — the recompile from Sprint 15's API change shuffled
  parallel scheduling and unmasked the bug. Caught in <2 minutes via
  isolated-test-passes-but-suite-fails diagnostic. Fix is 1 line.
- **Audit + reviewer dispatched at different sprint phases caught
  non-overlapping issues.** Audit (pre-execution): structural gaps
  (overwrite_partition parity, byte-equivalence baseline, missing call
  sites). Reviewer (post-implementation): localized footguns (`"none"`
  alias, doc-comment correctness). Different categories; both worth
  the dispatch cost.
- **Compression ratios on this fixture: ZSTD 31.5 % / Snappy 59.4 % /
  LZ4 59.2 % / uncompressed 100 %.** ZSTD compresses ~2× better than
  Snappy on this OHLCV-like fixture at zero wall-time penalty. Strong
  empirical case for keeping ZSTD as the default. ADR 0005 documents
  this.
- **All compressed codecs landed within ±1.5 % wall.** Codec selection
  is essentially free at the 1000-row × 5-partition fixture scale —
  filesystem I/O + parquet metadata generation dominate the write
  cost. mdata-shape (much larger writes) might shift this; out of
  scope for this sprint.

---

## Cross-references

- **Plan:** [`../history/sprints/sprint_15_dispatch_brief_2026-05-09.md`](../history/sprints/sprint_15_dispatch_brief_2026-05-09.md) (post-ratification move; includes the 3-agent audit appendix)
- **Cadence metrics row 15:** [`cadence_metrics.md`](cadence_metrics.md)
- **Sprints index:** [`sprints_index.md`](sprints_index.md)
- **ADR 0005:** [`../decisions/0005-parquet-write-defaults.md`](../decisions/0005-parquet-write-defaults.md)
- **Bench A/B (Part B output):** [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md) §"Sprint 15"
- **Sprint 14 retro (the wheel-bundle question):** [`sprint_14_retro.md`](sprint_14_retro.md)
- **Future-perf ideas (recorded pre-Sprint-15):** [`../sync/ideas.md`](../sync/ideas.md)
- **mdata delivery (post-wrap):** [`../sync/mdata_chili_2026-05-09_delivery.md`](../sync/mdata_chili_2026-05-09_delivery.md)
- **Implementation commits:** `35c215c` (Sprint 14 brief), `e7efc09` (Sprint 14 wrap), `6b83dbf` (Sprint 15 brief + ideas), `<wrap-commit>` (Sprint 15 code + docs + retro + 0.8.3 wheel)
- **Related artifacts (uncommitted, /tmp):** `/tmp/sprint_15_baseline_0_8_2_parquet.hash`, `/tmp/sprint_15_write_codec_bench.log`, `/tmp/sprint_14_post_dist/...whl` (pre-Sprint-15 wheel from Sprint 14 testing).

---

## Sprint 16 hand-off

**No specific sprint scoped yet.** Per CLAUDE.md "User-driven backlog,"
the open items are:

- (P0) GitHub-host the polars fork at `/tmp/polars-py-1.39.3` — replace
  `path = "/tmp/polars-py-1.39.3"` with `git = "..." + tag = "..."` in
  workspace + chili-py `[patch.crates-io]` blocks. Without it, fresh
  clones break at `cargo build`.
- (P1) KDB-X CE comparison once GA + interactive registration available.
- (P2) mdata sign-off on 0.8.3 delivery (this sprint).
- (P3) Sprint 13 P2 Box::new mitigation — deferred indefinitely per
  Sprint 13 lesson 2.

**Future-perf ideas captured in `docs/sync/ideas.md`** (no scope yet;
trigger requires profile evidence on real workloads):

- Per-table mutex on `par_df` (vs single HashMap RwLock).
- RCU on `par_df` (write to clone, swap with `arc_swap`).
- Coalesce concurrent loads on the same `hdb_path`.

**Sprint 16+ candidate scopes** (post mdata-feedback):

- Struct-shaped FFI for Parquet write options (ADR 0005 §6 — stop using
  positional optional args before adding a 10th).
- Polars 0.54 / 0.55 bump scoping (current pin 0.53.0 — bump triggers
  ADR 0005 §5 default-codec-change protocol if the upstream default
  changes).

---

## mdata delivery banner

**0.8.3 ships:**

1. **Sprint 14 (FFI symmetry):** `engine.engine.load_par_df` and
   `clear_par_df` direct-FFI calls now release the GIL. Typical Python
   callers via `engine.load_partitioned_df(...)` already saw GIL
   release via the fn_call route (Sprint 13.5 lesson 2); no behavior
   change for them.
2. **Sprint 15 (new ParquetWriteConfig API):** `engine.write_partitioned_df`
   and `engine.overwrite_partition` now accept `compression=` and
   `row_group_size=` keyword-only args. Default behavior preserved
   byte-equivalently with 0.8.2 (sha256
   `9682bed9...` for the canonical fixture).

**Not a bug fix.** No urgency to upgrade. Default behavior for old call
sites is identical to 0.8.2.

**Recommended adoption:** if mdata's storage-budget constraints can
benefit from explicit ZSTD (which is already the default — see ADR
0005), no change needed. If experimenting with LZ4 or Snappy for
compression-cpu vs ratio trade-offs, opt in via `compression="lz4_raw"`
(or `"snappy"`) on the write path.

See `docs/sync/mdata_chili_2026-05-09_delivery.md` for installation
protocol + banner notes.