# Pre-pivot State Snapshot — 2026-05-07

Frozen reference for the chili `claude` branch state at pivot time. This is the
authoritative baseline for A/B comparison against `claude-2` once Sprints 3-5
ports complete. Per the pivot direction, `claude` branch is parked-historical and
never modified again.

## Branch tips at pivot

| Branch / tag | SHA | Date |
|---|---|---|
| `claude` (= `claude-baseline-2026-05-07` tag) | `dea966e` | 2026-05-07 |
| `main` (= `main-pivot-2026-05-07` tag) | `f8b6360` | 2026-05-06 |
| Fork point (claude ↔ main divergence) | `d7a748b` | 2026-04-13 (chili 0.7.4 release) |

## Tags

- `claude-baseline-2026-05-07` — claude tip, immutable; used for binary build of
  the pre-pivot reference for A/B comparison in Sprints 3-5.
- `main-pivot-2026-05-07` — main tip at pivot time, immutable; provides
  reproducibility for "claude-2 was forked at this main commit."

## Test count on claude (pre-pivot)

Per CLAUDE.md project state line 115 (claude branch):
- 165 Rust tests
- 44 Python tests (35 baseline + 9 direct-DataFrame regression)
- **Total: 209**

Note: cargo test on this fresh shell session needed the workaround
`cargo test --workspace --exclude chili-py` + `.cargo/config.toml [env]` for
PYO3_PYTHON + DYLD_FALLBACK_LIBRARY_PATH per `docs/dev_setup.md`.

Verified count when gate ran during Sprint 2 v2 prep:
- ~162 Rust tests via `cargo test --workspace --exclude chili-py` (split: 1 + 102 +
  12 + 16 + 18 + 2 + 5 + 6). Diff to documented 165 may be due to doctests counted
  separately, or a small drift since the CLAUDE.md count was last updated.
- Python pytest: not run on this session (env not yet set up); 44 documented.

## Key bench numbers (claude baseline, pre-pivot)

Sourced from the bench artifacts copied to this directory (`baseline.md`, `summary.md`,
`phase{1..7,9}.md`, `phase17_*.py`) and CLAUDE.md golden rules:

- **Parse cache hit time:** ~385 ns (golden rule 6). Measured in `chili-core/benches/parse_cache.rs`.
- **GIL-released eval concurrent throughput multiplier:** 6.10× (golden rule 5).
  Measured by `crates/chili-py/tests/bench_concurrent.py`.
- **Storage schema:** Int64-quantized for price columns (golden rule 4). `set_column_scale`
  dequantizes on read.
- **Polars version pin:** 0.53.0 (workspace Cargo.toml golden rule 2).
- **Edition:** 2024 (golden rule 3).

For full per-phase numbers see the per-phase bench files in this directory.

## Key claude-only features at pivot (preview; full inventory in Part B)

These are the additive features that need to be ported onto claude-2 in Sprints 3-4
unless deliberately retired (preview from Sprint 1 inventory; authoritative reverse-
direction inventory is Sprint 2 v2 Part B's deliverable):

- Int64-quantized price storage + `set_column_scale` (golden rule 4).
- GIL-released `Engine::eval` (golden rule 5; 6.10× throughput).
- Structured exception hierarchy (`ChiliError` → `PepperParseError` etc.; Phase 13/WL 3.3).
- Logger built-ins (`.log.{info,warn,debug,error}`).
- mimalloc global allocator in chili-py cdylib.
- parse_cache shape (golden rule 6 invariant; main has its own under different lineage).
- In-process Python pub/sub (`publish(ipc_bytes)` / `subscribe(callback)`) — likely
  retired per ADR 0001.
- Cross-process TCP pub/sub (`publish(handle, bytes)`) — likely retired per ADR 0001.
- `overwrite_partition` separate fn (vs main's `write_partition(overwrite=…)` flag).

## Pre-commit gate state on claude-2 at end of Part A

Documenting per the Sprint 2 v2 brief Part A.4 directive: "Document failures;
don't fix in this sprint — failures inform Sprint 3 port priorities."

| Gate stage | Status | Note |
|---|---|---|
| `cargo fmt --all -- --check` | ✓ GREEN | One pre-existing fmt diff in `chili-parser/tests/chili/test_error.rs` was applied via `cargo fmt --all` in Part A initialization commit (`4fbe5eb`). |
| `cargo clippy --all-targets -- -D warnings` | ✗ FAIL | ~19 pre-existing lints in `chili-core` (and possibly more cascading) inherited from bare main. Claude has the fixes in `9aa358d` but the cherry-pick conflicts on `crates/chili-core/src/engine_state.rs` (FFI-rewrite divergence — same surface that triggered the original Sprint 2 v1 halt). Lints include `needless_borrow`, `too_many_arguments`, `clone_on_copy`, `unnecessary_cast`, `field_reassign_with_default`, `declare_interior_mutable_const`. |
| `cargo test --workspace --exclude chili-py` | ⏸ BLOCKED | Blocked by clippy gate failure — cannot run cleanly until clippy passes. The 162-test pass count from Sprint 2 v2 prep was on `claude` branch (which had all clippy fixes); claude-2 verification will land in Sprint 3. |
| `cd crates/chili-py && uv run maturin develop && uv run pytest` | ⏸ DEFERRED | chili-py FFI surface needs port from claude before tests are meaningful. Sprint 3-4 territory. |

**Successful clippy ports in Part A:** `71e2c41` (chili-parser/tests/utils.rs 11 lints,
clean), `2e08649` (chili-parser/src/token.rs type_complexity allow, applied directly),
`e829bd4` (chili-op clamp, clean cherry-pick), `a8d4014` partial (chili-op tests
arithmetic_test.rs only; chili-py portion deferred).

**Sprint 3 first deliverable:** Port `9aa358d`'s 19 chili-core lints by hand
(not cherry-pick — engine_state.rs divergent shape will fight). After: re-run
gate end-to-end on claude-2; expect green except chili-py-side which depends
on FFI port progress.

## How to use this snapshot

For A/B comparison vs claude-2 in Sprints 3-5:

1. **Build pre-pivot binary:** `git checkout claude-baseline-2026-05-07 && cargo build --release && cd crates/chili-py && uv run maturin build --release`. The resulting wheel is the "claude baseline" reference.
2. **Build claude-2 binary:** `git checkout claude-2 && cargo build --release && cd crates/chili-py && uv run maturin build --release`.
3. **Run identical workloads on each.** Numbers from this snapshot are the
   "expected pre-pivot baseline."
4. **Compare and document deltas** in a future `docs/bench/post_pivot_comparison_<date>.md`.

## Cross-references

- Pivot brief: `../../sim/sprint_2_dispatch_brief_2026-05-07.md`
- Pivot iteration lesson: `../../standards/iteration_lessons.md` lesson 4
- ADR 0001 (pub/sub canonical model): `../../decisions/0001-pub-sub-canonical-model.md`
- New roadmap (port arc): `../../sim/roadmap_2026-05-07.md`
- Sprint 1 forward-direction inventory: `../../research/main_vs_claude_inventory_2026-05-06.md`
