# Sprint 15 dispatch brief — Parquet codec + row-group-size public API + 0.8.3 wheel cut (A.2.4)

**Kickoff:** 2026-05-09 — user-ratified post Sprint 14 retro ("let's continue with Sprint 15 and bundle the result before wheel cut").
**Owner:** coordinator-solo + `code-reviewer` subagent dispatch at Part D (lesson 7 binds — FFI surface + new public API).
**Type:** implementation (new public API; net-additive; no behavior change at default).
**Predicted pp:** 6–10.
**Plan reference:** [`sprint_14_retro.md`](sprint_14_retro.md) §"Sprint 15 hand-off"; user direction 2026-05-09 to bundle wheel cut with Sprint 15.
**ADR references:** **ADR 0005** drafted by this sprint (Parquet write defaults + override semantics).

---

## Sprint objective

Expose Parquet `compression` + `row_group_size` as user-controllable
options on `engine.write_partitioned_df` (Python) and on the
`write_partition` engine function (chili / pepper). Net-additive: at
default values, behavior is byte-equivalent to current 0.8.2 output.
ADR 0005 documents the convention (default stays Snappy + auto
row-group sizing; future default change requires mdata sign-off per
golden rule 4).

Bundle Sprint 14's GIL release into a single **0.8.3 wheel** at wrap.

**Binary success criterion:** post-implementation `write_partition`
criterion bench shows:
- ≥ 3 codec variants benched (Snappy / Zstd-3 / Lz4Raw at minimum) with
  measured (write throughput, on-disk size) per codec.
- New API exercised end-to-end via a chili-py pytest test asserting
  Snappy and Zstd outputs are read-equivalent (round-trip).
- Default code-path produces byte-identical output to 0.8.2 for the
  unchanged-args case (regression check).

---

## Why now

- Sprint 13.5 deferred A.2.4 explicitly to a Sprint 15 owner; Sprint 14
  is now ratified.
- mdata's write-side ingest path is the primary external consumer; a
  user-controllable codec enables ZSTD experiments without forking
  chili.
- 0.8.3 wheel is the natural bundling vehicle for Sprint 14 + Sprint 15
  changes (no user-visible benefit from Sprint 14 alone for typical
  callers per Sprint 13.5 lesson 2).
- Existing `write_partition_native` already takes a 7-arg signature; one
  more arg slot for `parquet_options: Option<&ParquetWriteOptionsLite>`
  is a small additive change.

---

## Scope — Part A: ParquetWriteConfig surface

### A.1 New public type — `ParquetWriteConfig` (chili-op)

Lightweight options struct exposed at `crates/chili-op/src/io.rs` (or
new module `crates/chili-op/src/parquet_options.rs` if it grows past
50 lines). Fields:

```rust
#[derive(Debug, Clone, Default)]
pub struct ParquetWriteConfig {
    /// Compression codec. None = polars default (Snappy in polars 0.53).
    pub compression: Option<ParquetCompression>,
    /// Row group size override. None = auto (current behavior:
    /// computed from sort_columns + height when sort_columns non-empty,
    /// otherwise polars default 262144).
    pub row_group_size: Option<usize>,
}
```

`ParquetCompression` is re-exported from `polars::io::parquet::write::ParquetCompression`
to keep the surface small.

### A.2 Threading through `write_partition_native`

Add a trailing `Option<&ParquetWriteConfig>` arg. When `None` →
preserve current behavior byte-exactly (this is the regression-check
gate). When `Some(cfg)` → apply codec + row_group_size overrides.

`write_parquet_to_filepath_with_row_group_size` in `util.rs:149` is
already the choke-point; extend it to accept an `Option<ParquetCompression>`
or refactor into a new `write_parquet_to_filepath_with_options(filepath,
df, &ParquetWriteConfig)`. Prefer the refactor — clean signature, fewer
methods.

### A.3 Threading through the chili / pepper FFI (`fn write_partition`)

The FFI `wpar` call (`io.rs:287`) currently takes 7 SpicyObj args. Add
**positional optional args 8 + 9**: compression-name (string sym; e.g.
`` `snappy ``, `` `zstd ``, `` `lz4_raw ``, `` `none ``) and
row_group_size (i64 or null). Treat absent / null args as "preserve
default."

Rationale for keeping it positional rather than introducing a struct
arg over the FFI: chili / pepper are positional-arg languages; adding
a struct adds the SpicyObj struct shape which doesn't yet exist.
Positional optional with `Null` sentinels matches the existing pattern
(see `partition` arg which accepts `Date | I64 | Null`).

### A.4 Threading through the chili-py wrapper

`chili.engine.ChiliEngine.write_partitioned_df` adds two kwargs:

```python
def write_partitioned_df(
    self,
    df: pl.DataFrame,
    hdb_path: str,
    table: str,
    date: Any,
    sort_columns: Optional[list[str]] = None,
    rechunk: bool = False,
    overwrite: bool = False,
    *,
    compression: Optional[str] = None,        # "snappy" | "zstd" | "lz4_raw" | "none"
    row_group_size: Optional[int] = None,
) -> int:
```

Routes through `fn_call("wpar", [..., compression_or_null, row_group_size_or_null])`.

Compression-name parsing happens in chili-op `wpar` (string-to-`ParquetCompression`
mapping). Default `Zstd(None)` accepts polars' default level (3 for
zstd in polars 0.53).

### A.5 Tests

- chili-py pytest: 1 new test under `TestWritePartition` (or new class)
  asserting:
  - default case writes successfully (no kwargs) and is read-equivalent
    to `polars.read_parquet`-ed output.
  - explicit `compression="zstd"` produces a smaller file than default
    on the same DataFrame.
  - explicit `compression="none"` produces a larger file than default.
  - explicit `row_group_size=1024` produces a parquet with ≥10 row
    groups on a 10000-row DataFrame.
  - invalid compression name raises a clear error.

Existing 65 pytest must continue to pass.

---

## Scope — Part B: bench A/B

### B.1 Extend `crates/chili-op/benches/write_partition.rs`

Existing bench measures `write_partition_native` at default options.
Add a parameterized criterion group with 3 codec variants on the same
fixture (50p × 100sym × 200rps; the existing `write_partition` bench
shape):

- `snappy` (default; baseline)
- `zstd_3` (polars default zstd level)
- `lz4_raw`

For each: capture **wall time per write** (criterion median) +
**on-disk size in bytes** (manually via `std::fs::metadata` after
each write; report as a separate `metric` block).

### B.2 Document in post_pivot_baseline_2026-05-07.md

New "Sprint 15 — Parquet codec A/B" section. Table format:

| Codec   | Wall (ms/write) | On-disk size (MB) | Δ vs Snappy default |
|---------|-----------------|-------------------|---------------------|
| snappy  | (baseline)      | (baseline)        | 0 % / 0 MB          |
| zstd-3  | …               | …                 | + …% / − …%         |
| lz4_raw | …               | …                 | …                   |

### B.3 Halt criterion

If the default-args case (no compression / row_group_size override)
produces a non-byte-identical output vs 0.8.2 wheel output for the
same DataFrame → **HALT**. The regression check is load-bearing.

---

## Scope — Part C: ADR 0005

### C.1 New file `docs/decisions/0005_parquet_write_defaults.md`

Documents:

- **Decision:** default Parquet compression remains Snappy in 0.8.3;
  default `row_group_size` remains polars-default (262144) when
  sort_columns is empty, else the existing auto-computed clamp
  (1024..32768, target 16 row groups).
- **Rationale:** Snappy preserves byte-equivalence with 0.8.2 (and
  earlier mdata-shipped data); changing the default would require
  re-writing every existing partition for round-trip equality, which
  is out of scope. ZSTD as an option is now available for new mdata
  experiments.
- **Override semantics:** users opt in via `compression=` /
  `row_group_size=` kwargs.
- **Future-default-change protocol:** any change to the chili-side
  default codec triggers the golden rule 4 territory (storage schema
  decision); requires mdata sign-off + a documented re-write path for
  existing HDBs.
- **Status:** ACCEPTED.

### C.2 Index update

Add ADR 0005 to the docs map in CLAUDE.md (line 125 ADRs sentence) and
to `docs/decisions/README.md` if such an index exists (else just the
filename in the directory listing).

---

## Scope — Part D: code-reviewer dispatch

Per **lesson 7**: dispatch `code-reviewer` BEFORE writing the retro.
Lesson 7 binds because Sprint 15 touches the FFI surface and adds a
new public API.

**Reviewer prompt should specifically check:**

1. The default-args case is byte-equivalent to 0.8.2 (regression check).
2. The new `Option<&ParquetWriteConfig>` arg in `write_partition_native`
   doesn't accidentally break any internal caller — grep for all
   `write_partition_native` call sites and confirm.
3. The chili-py kwarg ordering is `*` keyword-only (forces explicit
   naming; prevents future positional-arg drift).
4. Compression-string parsing is case-insensitive AND has a clear error
   on unknown codec name.
5. ADR 0005 cross-references resolve; "ACCEPTED" status matches what's
   shipped.
6. No accidental change to `EngineState::load_par_df` or the fn_call
   FFI path (Sprint 14's GIL release must stay GREEN).

---

## Scope — Part E: wrap + 0.8.3 wheel cut

### E.1 Bump version

- Workspace `Cargo.toml`: `version = "0.8.0"` stays (workspace-level;
  not user-visible).
- `crates/chili-py/Cargo.toml`: bump `version = "0.8.2"` → `"0.8.3"`.
  Note that the chili-py `name = "chili-sauce"` distribution
  externalizes this version.

### E.2 Cut the wheel

```sh
cd crates/chili-py
uv run --no-sync maturin build --release --out /Users/oakadmin/code/chili/dist
```

Verify the produced wheel:
- Filename: `chili_sauce-0.8.3-cp310-abi3-macosx_11_0_arm64.whl`
- Run `unzip -l <wheel>` — confirm `engine_state.abi3.so` is present.
- Quick smoke test in a clean venv: `python -c "import chili; e =
  chili.ChiliEngine(); e.write_partitioned_df(...)"` round-trip.

### E.3 mdata delivery doc update

Edit `docs/sync/mdata_chili_2026-05-08_delivery.md` (or create a new
sibling `docs/sync/mdata_chili_2026-05-09_delivery.md` if the 0.8.2
banner is still load-bearing). Frame: "0.8.3 ships Sprint 14 (FFI
symmetry — releases GIL on direct-FFI `load_par_df` + `clear_par_df`;
typical Python callers via `load_partitioned_df` already saw GIL
release through `fn_call`) + Sprint 15 (new `compression` /
`row_group_size` kwargs on `write_partitioned_df`). NOT a bug fix.
Default behavior preserves 0.8.2 byte-equivalence." Per Sprint 14
retro § Open question option 3 (user-ratified).

### E.4 Standard wrap

- Pre-commit gate green: `cargo fmt --all -- --check && cargo clippy
  --all-targets -- -D warnings && cargo test --workspace --exclude
  chili-py`.
- chili-py pytest: 65 + new tests (see A.5).
- Bench A/B documented in `docs/bench/post_pivot_baseline_2026-05-07.md`.
- Author retro at `docs/sim/sprint_15_retro.md`.
- Append cadence row 15.
- Update `docs/sim/sprints_index.md` Sprint 15 row.
- Move dispatch brief to `docs/history/sprints/`.
- Update CLAUDE.md state line: chili-py 0.8.2 → 0.8.3; ADR list grows
  to include 0005.
- HALT until user ratifies retro.

---

## Out of scope (defer)

| Item | Reason |
|---|---|
| **Default-codec change** | ADR 0005 forbids it without mdata sign-off (golden rule 4 territory). |
| **`SinkDestination::File` (sink-API) refactor** | Out of scope; current `ParquetWriter` codepath is sufficient. |
| **Streaming writes / multi-file output per partition** | Sprint 15+ idea, not committed. |
| **Per-table mutex on `par_df`** | Idea recorded in `docs/sync/ideas.md` 2026-05-09; trigger requires profile evidence. |
| **RCU on `par_df`** | Same as above; idea-only. |
| **Coalesce concurrent loads** | Same as above; idea-only. |
| **A.2.2 vars-write-lock release** | Descoped indefinitely per Sprint 13.5 retro. |
| **P3.4 Categorical mapping cache** | Deferred indefinitely per Sprint 13.5 categorical_eval bench. |
| **Polars-internal kernel optimization** | Blocked on user P0. |

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | `crates/chili-op/src/io.rs` (or new `parquet_options.rs`) — `ParquetWriteConfig` + threading | edit / new |
| 2 | `crates/chili-op/src/util.rs` — `write_parquet_to_filepath_with_options` refactor | edit |
| 3 | `crates/chili-op/src/io.rs:287` — `write_partition` accepts 2 new positional optional args | edit |
| 4 | `crates/chili-py/chili/engine.py:221-260` — `write_partitioned_df` adds 2 kwargs | edit |
| 5 | `crates/chili-py/tests/test_engine.py` — new tests under `TestWritePartition` (or new class) | edit |
| 6 | `crates/chili-op/benches/write_partition.rs` — codec A/B variants | edit |
| 7 | `crates/chili-py/Cargo.toml` — version bump 0.8.2 → 0.8.3 | edit |
| 8 | `docs/decisions/0005_parquet_write_defaults.md` | new |
| 9 | `docs/bench/post_pivot_baseline_2026-05-07.md` — Sprint 15 codec A/B section | edit |
| 10 | `dist/chili_sauce-0.8.3-cp310-abi3-macosx_11_0_arm64.whl` | new (artifact, committed to dist/) |
| 11 | `docs/sync/mdata_chili_2026-05-09_delivery.md` (or update existing) | new / edit |
| 12 | `docs/sim/sprint_15_retro.md` | new (post-sprint) |
| 13 | `docs/sim/cadence_metrics.md` — row 15 | edit (post-sprint) |
| 14 | `docs/sim/sprints_index.md` — Sprint 15 row | edit |
| 15 | `CLAUDE.md` — state line + ADR list | edit |

---

## Lead allocation

**Coordinator-solo for Parts A, B, C, E.** **`code-reviewer` subagent
dispatch for Part D** (lesson 7). Budget allocation:

- Part A API + impl: ~3 pp (struct + 3 call-site threading + 1 codepoint
  for codec-string parsing).
- Part B bench A/B: ~1.5 pp (extending existing bench; 3 codec
  variants).
- Part C ADR draft: ~1 pp.
- Part D reviewer dispatch: ~1.5 pp (similar shape to Sprint 14).
- Part E wrap + wheel cut + delivery doc + version bump: ~2 pp.

No worktree (single sprint, no parallel execution).

---

## Mid-checkpoint plan

At ~50 % predicted-pp consumed (~4 pp), post a status:

- Has Part A compiled clean? Tests pass?
- Has Part B bench data been captured?
- ETA to Part D reviewer dispatch?

Halt-and-escalate criteria:

1. **Default-output regression** — if the no-kwargs codepath produces a
   different parquet byte stream from 0.8.2 → halt.
2. **API-design pivot** — if mid-implementation reveals the positional-
   arg approach for chili FFI is structurally wrong (e.g., breaks
   serde9 byte-stream encoding) → halt for user direction on the struct
   route.
3. **Bench delta out of expected band** — if zstd-3 is not at least 30 %
   smaller than snappy on the bench fixture (the well-established
   parquet-zstd ratio for typical OHLCV-shape data), something is wrong
   with the codec-routing path → halt for diagnosis.
4. **Watchdog approaching** — 5h ≥ 80 % AND remaining > 6 pp.

---

## Wrap (per ceremony)

(See Part E above.)

---

## Pp accounting reference

Closest historical comparable sprints from `docs/sim/cadence_metrics.md`:

- **Sprint 5** (polars pin + ADR 0003 PyLazyFrame DSL incompat + chili
  0.8.0-claude2.1 wheel cut + mdata delivery handoff) — predicted
  10–15, actual ~10. Sprint 15 is similar shape (new public API + ADR +
  wheel cut + delivery doc) but smaller surface (Parquet kwargs vs
  whole lazy-frame transfer); expect mid- to low-band actual.
- **Sprint 7 Part A** (ADR 0003 resolution + chili 0.8.1 wheel cut) —
  predicted 8–15, actual ~12. Comparable wheel-cut + ADR shape; Sprint
  15 narrower implementation but adds bench A/B work; expect comparable.
- **Sprint 14** (P3.2b GIL release) — predicted 5–9, actual ~5.
  Cautionary tale that small audited changes can run very fast; Sprint
  15 has more concrete deliverables (new public API + ADR + bench) so
  expect higher actual.

Sprint 15 expected at the **mid-band** (7–9 pp), capped above by:
- Lesson 8: maturin compile cost ~3 pp (release wheel build).
- Reviewer dispatch surfacing follow-ups.
- ADR draft with cross-references.

Capped below by: byte-equivalence regression check passing on first
attempt + reviewer finding nothing structural.

---

## Cross-references

- **Sprint 14 retro:** [`sprint_14_retro.md`](sprint_14_retro.md) §"Sprint 15 hand-off"
- **Sprint 13.5 retro (where A.2.4 was originally deferred):** [`sprint_13.5_retro.md`](sprint_13.5_retro.md)
- **Existing write_partition_native:** `crates/chili-op/src/io.rs:287-540`
- **Existing write_parquet_to_filepath_with_row_group_size:** `crates/chili-op/src/util.rs:149`
- **Existing chili-py wrapper:** `crates/chili-py/chili/engine.py:221-260`
- **Existing write_partition bench:** `crates/chili-op/benches/write_partition.rs`
- **Iteration lessons:**
  - Lesson 7 — reviewer-before-retro (Part D).
  - Lesson 8 — maturin compile cost reservation.
  - Lesson 14 — wheel-only install protocol (Part E mdata delivery).
  - Sprint 13 lesson 1 — bench-gate threshold set FROM measurement, not pre-specified (Part B variants).
  - Sprint 14 lesson 1 (candidate, not yet promoted) — release-profile build for A/B. Avoid the dev-profile detour.
- **Hard constraints:**
  - Polars version pinned 0.53.0; do not bump.
  - Storage schema is Int64-quantized (golden rule 4) — relevant: any default-codec change touches this.
  - Default behavior must be byte-equivalent to 0.8.2 (regression check).
- **Cross-project (mdata):** delivery doc update at Part E.3. Frame as
  "FFI symmetry + new ParquetWriteConfig API; not a bug fix; default
  behavior preserved."

---

## Appendix — Independent audit (2026-05-09)

Three parallel audit agents (Explore + code-reviewer + planner) reviewed
this brief before execution per `~/.claude/rules/self-audit-on-plans.md`.
Findings below; original brief preserved unchanged above; this appendix
is the load-bearing addendum the implementer must follow.

### Material corrections

**1. [MAJOR] `overwrite_partition` parity gap.** `chili.engine.ChiliEngine.overwrite_partition`
(`crates/chili-py/chili/engine.py:326-346`) delegates to
`write_partitioned_df(..., overwrite=True)` but does not forward kwargs.
Both audits (Explore + code-reviewer) flagged: once `write_partitioned_df`
gains `compression=` / `row_group_size=` kwargs, `overwrite_partition`
callers have an asymmetric API.

**Fold into Part A.4:** add `compression: Optional[str] = None,
row_group_size: Optional[int] = None` to `overwrite_partition`'s
signature (keyword-only via `*`), thread through to the internal
`write_partitioned_df` call. Trivial change but a public API gap.

**Fold into Part A.5:** add 1 mirror test on `overwrite_partition`
exercising at least the `compression="zstd"` variant.

**2. [MAJOR] Byte-equivalence baseline must be materialized at Part A.0
(new prerequisite step).** All 3 audits flagged: no committed artifact
to diff against. The "no-kwargs path is byte-identical to 0.8.2" halt
criterion in Part B.3 is unverifiable as written.

**Fold as new Part A.0 (before any code changes):**

```sh
# Install 0.8.2 wheel in a clean venv (already exists at dist/)
TEMPDIR=$(mktemp -d) && uv venv --python 3.12 "$TEMPDIR/.venv"
VIRTUAL_ENV="$TEMPDIR/.venv" uv pip install \
  /Users/oakadmin/code/chili/dist/chili_sauce-0.8.2-cp310-abi3-macosx_11_0_arm64.whl \
  'polars==1.39.3'
# Run write_partitioned_df with default args on a canonical fixture
"$TEMPDIR/.venv/bin/python" -c "
import polars as pl
from chili import ChiliEngine
import hashlib, glob
e = ChiliEngine()
df = pl.DataFrame({'sym': ['A', 'B'], 'close': [19000, 38000]})
e.write_partitioned_df(df, '$TEMPDIR/hdb', 'ohlcv', '2024.01.01')
e.shutdown()
# Capture sha256 of the produced parquet
shard = sorted(glob.glob('$TEMPDIR/hdb/ohlcv/2024.01.01_*'))[0]
print('sha256:', hashlib.sha256(open(shard, 'rb').read()).hexdigest())
print('size:', __import__('os').path.getsize(shard))
"
```

Record both `sha256` and file size as named constants in the new
pytest test (`tests/test_engine.py::TestParquetWriteConfig`). The
regression check is then `assert sha256(post_change_output) == BASELINE_SHA256`.

**Code-reviewer caveat:** raw sha256 may be brittle if Parquet metadata
embeds polars version strings. Pragmatic fallback: if the sha256 differs
post-change, ALSO assert `polars.read_parquet` round-trips identically
AND the file size is within ±0 bytes. Both must be true to pass; sha256
divergence alone is a yellow flag, not red.

**3. [MAJOR] All 4 external call sites of `write_partition_native` must
be updated.** Brief Part A.2 mentioned the function but didn't enumerate
external call sites. Explore audit identified:

| File | Line | Context |
|---|---:|---|
| `crates/chili-op/benches/write_partition.rs` | 32 | bench loop |
| `crates/chili-op/benches/common/mod.rs` | 155 | bench harness `build_wide_hdb` |
| `crates/chili-op/benches/common/mod.rs` | 177 | bench harness `write_one_partition` |
| `crates/chili-op/tests/partition_filter_test.rs` | 46 | integration test |

**Fold into Part A.2 deliverables:** all 4 sites pass the new
`Option<&ParquetWriteConfig>` arg as `None` to preserve current behavior.
Compile-error surprise mid-sprint is the avoided cost.

### Additional refinements (minor)

**4. [MINOR] Version bump is in two places.** `crates/chili-py/Cargo.toml`
AND `crates/chili-py/pyproject.toml` both have `version = "0.8.2"`. Bump
both atomically in Part E.1.

**5. [MINOR] Add Part B.4: re-run Sprint 14 `concurrent_load_direct`
against the 0.8.3 wheel.** The 0.8.3 wheel bundles Sprint 14 + Sprint 15
in a single build artifact; Sprint 14 was tested against a Sprint-14-only
wheel. Confirm the GIL-release N=4 gate (≥ 12,000 cps) still holds on
the bundled wheel. Dev profile is OK for shape verification (Sprint 14
lesson 1: ~0.55× release numbers; if N=4 ≥ ~7,000 on dev, the shape is
intact and the absolute number on release will pass).

**6. [MINOR] Reframe Part B.3 halt criterion (zstd-vs-snappy ratio).**
Per Sprint 13 lesson 1, pre-specified ratio thresholds are the wrong
shape. Replace with:
- **Halt** if `lz4_raw` or `zstd` produces output ≥ snappy size (clear
  sign codec selection isn't applied — correctness regression).
- **Record as observation, not gate**: actual zstd-3-vs-snappy ratio.
  Expected ~30 % smaller on OHLCV-shape data; if very different, note
  in retro but do not halt.

**7. [MINOR] On-disk-size measurement: avoid criterion's `Measurement`
trait.** Brief Part B says "report as a separate `metric` block" but
criterion's custom-metric API is 30-50 lines and overkill. Simpler:
use a separate `bench_function` (or even a plain `#[test]`) that writes
once per codec variant outside the timed loop, then reads
`std::fs::metadata(path).len()`. Record manually in the doc table.

**8. [MINOR] Add halt criterion for compression-name parsing.** If the
string-to-`ParquetCompression` mapping silently falls back to Snappy on
an unknown codec name (instead of returning an error), it's a usability
correctness bug. Add as halt criterion 5 in the mid-checkpoint plan:
"unknown compression name silently accepted → halt."

**9. [MINOR] ADR 0005 explicit "provisional" callout.** Code-reviewer
flagged that the positional-arg approach over the chili FFI is a
stopping point, not a permanent design. ADR 0005 should add a
"Provisional aspects" section noting:
- Positional optional args 8+9 are a stopping point; struct-shaped FFI
  is the preferred path when ≥ 3 Parquet write options are needed (e.g.,
  `data_page_size`, `bloom_filter`, `dictionary`).
- Future Sprint 16+ scope: `SpicyObj::Map` variant or named-options
  struct over the FFI.

**10. [MINOR] ADR 0005 mixed-codec HDB read transparency.** One sentence
in ADR 0005: "Polars' Parquet reader is codec-agnostic per file; a
partition written with Zstd is read correctly alongside Snappy-encoded
neighbors. Mixed-codec HDBs are supported by reading; chili does not
enforce same-codec-per-table on write."

### Cross-cutting gates

**Reviewer prompt update (Part D):** the code-reviewer dispatch should
specifically check that:
- All 4 external call sites of `write_partition_native` were updated.
- Both `Cargo.toml` and `pyproject.toml` got the version bump.
- `overwrite_partition` parity is in place.
- The byte-equivalence regression check passes (sha256 OR fallback
  round-trip+size).
- The Sprint 14 concurrent-load gate still holds on the bundled wheel.

### Revised sequencing

```
A.0  Capture 0.8.2 baseline (sha256 + file size of reference parquet)  [NEW prerequisite]
A.1  ParquetWriteConfig struct (chili-op)
A.2  Refactor write_parquet_to_filepath → with_options + update 4 external call sites  [REVISED]
A.3  Add 2 positional optional args to wpar (chili FFI)
A.4  Add compression / row_group_size kwargs to write_partitioned_df + overwrite_partition (chili-py)  [REVISED]
A.5  pytest: 5 write_partitioned_df tests + 1 overwrite_partition mirror + byte-equivalence sha256 assertion
B.1  Extend write_partition criterion bench (3 codec variants)
B.2  Document Part B in post_pivot_baseline_2026-05-07.md
B.3  Halt criteria (revised — codec-routing correctness; ratio observation)  [REVISED]
B.4  Re-run Sprint 14 concurrent_load_direct N=4 on the post-A wheel  [NEW]
C    ADR 0005 (with provisional + mixed-codec callouts)  [REVISED]
D    code-reviewer dispatch (with revised checklist)
E.0  Bump version in Cargo.toml AND pyproject.toml  [REVISED]
E.1-E.4  Wheel cut + delivery doc + standard wrap
```

### Sprint sizing

Audit re-validates 6–10pp band. Adjustments push budget toward upper
band edge:
- Part A.0 baseline capture: +0.3 pp
- Part A.2 4 external call sites: +0.3 pp
- Part A.4 overwrite_partition mirror: +0.3 pp
- Part A.5 6th test: +0.3 pp
- Part B.4 Sprint 14 regression check: +0.5 pp
- Total audit-driven addition: ~1.7 pp

Predicted **7–11pp** post-audit. Mid-band 9. Still in-budget; Sprint
sizing realism is OK.
