# CLAUDE.md — Chili

Agent-facing map of this repo. Keep terse: this file is loaded into every conversation.

## Project background

This is a local working fork of [purple-chili/chili](https://purple-chili.github.io/) — a kdb+/q-style analytics engine on Polars + Arrow + Parquet, with `chili` (JS-like) and `pepper` (q-like) syntaxes. It reuses `kola` for q interop. Upstream is the canonical project; this repo exists because a separate user project, **mdata** (`~/code/mdata`, market-data warehouse, ~11K US equities), needed Python bindings, GIL-released eval, quantization, pub/sub, `overwrite_partition`, etc. that upstream lacked. The chili author has since picked up a subset of those changes (see `project_chili_background.md` memory for the commit range and table). See `README.md` for performance numbers and feature list.

## Branch policy (load-bearing — post-pivot 2026-05-07)

**No remote.** `git remote -v` is empty by design. Never `git push`, `git pull`, `git fetch`, or re-add a remote. The user manually uploads upstream state into the local `main` branch.

**`main` = upstream / external contributors.** Never commit to it, never check it out to make changes. Treat it as read-only.

**`claude-2` = the only branch you commit to.** Verify with `git rev-parse --abbrev-ref HEAD` before every commit. Forked from `main` tip on 2026-05-07 as part of the pivot from cherry-pick to invert-and-restart (see `docs/standards/iteration_lessons.md` lesson 4 + `docs/history/sprints/sprint_2_dispatch_brief_2026-05-07.md`).

**`claude` = parked-historical, immutable.** Tagged `claude-baseline-2026-05-07` for reproducible historical binary builds. Never commit to it, never delete it. It is the project's pre-pivot reference for A/B comparison + provenance. The full pre-pivot state lived there.

**Merging:** Only `main → claude-2` (when the user uploads new upstream state). Never `claude-2 → main`. Never `claude → claude-2` (use `git checkout claude -- <path>` for selective doc copy if needed during port sprints). If a change must reach `main`, surface it and let the user handle it.

**Tags pinned at pivot:** `claude-baseline-2026-05-07` (claude tip 2026-05-07), `main-pivot-2026-05-07` (main tip 2026-05-07).

## Pre-commit gate

Run before every commit (matches `Taskfile.yml`):

```bash
cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace --exclude chili-py
```

The `--workspace --exclude chili-py` is **load-bearing**: chili-py's pyo3 `extension-module` feature unifies into pyo3 across the workspace and tells pyo3 NOT to emit `-lpython` linker flags, which breaks `cargo test` for chili-core/chili-op standalone test binaries. See `docs/dev_setup.md` for the full env setup (`.cargo/config.toml` with PYO3_PYTHON + DYLD_FALLBACK_LIBRARY_PATH).

For Python bindings work also run, from `crates/chili-py/`:

```bash
uv run maturin develop && uv run pytest
```

Then run the staged-file audit from `~/.claude/rules/git-commit-hygiene.md` — never `git add -A`.

## Common commands

```bash
task build                  # debug build of `chili` binary
task release                # release build
task test                   # cargo test
cargo bench -p chili-op     # core ops benchmarks (scan, eval, load_par_df, write_partition)
cargo bench -p chili-core --bench parse_cache
cd crates/chili-py && uv run python tests/bench_concurrent.py   # Python concurrent throughput
```

## Golden rules

1. **Branch:** `claude-2` only. See above.
2. **Polars version is pinned** in workspace `Cargo.toml` (`0.53.0`). Bumps are coordinated changes — don't unpin in passing.
3. **Edition 2024.** MSRV follows the toolchain in `rust-toolchain.toml` if present; otherwise stable.
4. **Storage schema is Int64-quantized** for price columns. `set_column_scale` dequantizes to Float64 on read. Don't change the on-disk dtype without coordinating with the mdata storage layer.
5. **GIL is released around `Engine::eval`** in `chili-py`. Don't reintroduce GIL-held long ops — the 6.10× concurrent throughput win depends on it.
6. **Parse cache is a hot path.** A cache hit is ~385ns. Don't add allocations or locks on the hit path without benching.

## Workspace layout

| Crate | Purpose |
|---|---|
| `crates/chili-core` | Engine, parse cache, partition loader |
| `crates/chili-op`   | Query operators, scan, eval, write_partition |
| `crates/chili-parser` | chili / pepper parsers (chumsky-based) |
| `crates/chili-bin` | `chili` CLI binary |
| `crates/chili-py`  | PyO3 / maturin Python bindings; ships as the **`chili-sauce`** wheel (import name `chili`). Directory name `chili-py` is legacy. |

## Docs map

Start here: this file (CLAUDE.md docs map below). [`README.md`](README.md) is the upstream user-facing intro (install + features).

- [`CHANGELOG.md`](CHANGELOG.md) — release notes (upstream-aligned; claude-2 internal 0.8.1 wheel documented in delivery doc, not here).
- [`crates/chili-py/README.md`](crates/chili-py/README.md) — Python API surface.
- [`docs/dev_setup.md`](docs/dev_setup.md) — local env (PYO3_PYTHON, DYLD_FALLBACK_LIBRARY_PATH, `.cargo/config.toml`).
- [`docs/bench/post_pivot_baseline_2026-05-07.md`](docs/bench/post_pivot_baseline_2026-05-07.md) — claude-2 post-pivot rebaseline (rolling; through Sprint 15; Sprints 13.5/14/15 A/B numbers appended).
- [`docs/bench/mdata-collab/`](docs/bench/mdata-collab/) — mdata↔chili collaboration: schema, parity, kdb+ comparison.
- [`docs/research/`](docs/research/) — strategic research (kdb+ landscape, alternatives, Shakti, competitive position, claude-only-features inventory, Iceberg eval); start at `competitive_position_2026-05-06.md`.
- [`docs/sim/`](docs/sim/) — sprint cadence: 15 ratified retros (Sprints 1-15, Sprint 13 reverted) + `cadence_metrics.md` + `sprints_index.md`; templates are `_*_template.md`. Closed roadmap at [`docs/history/sim/roadmap_2026-05-07.md`](docs/history/sim/roadmap_2026-05-07.md).
- [`docs/standards/iteration_lessons.md`](docs/standards/iteration_lessons.md) — 17 durable rules promoted from sprint retros.
- [`docs/sync/`](docs/sync/) — `decisions-needed.md` (open: empty) + `ideas.md` (7 entries: 3 perf-architecture + 4 from mdata 0.8.3 feedback) + `mdata_chili_2026-05-09_delivery.md` (0.8.3 handoff; mdata smoke-pass sign-off received 2026-05-09 — see §"mdata response received") + `load_par_df_state_audit.md` (GIL-release safety audit, GREEN). Earlier delivery docs (0.8.1/0.8.2) moved to `docs/history/sync/`.
- [`docs/decisions/`](docs/decisions/) — ADRs 0001-0005 (0003 RESOLVED Sprint 7; 0005 Accepted Sprint 15); see `README.md` for convention.
- [`docs/history/`](docs/history/) — frozen historical docs; never modify, only add.
- [`vendor/polars-core/`](vendor/polars-core/) — hinmeru fmt-patch reference material for reconstructing the chili-side q-style fmt patch when `/tmp/polars-py-1.39.3` is lost. See `vendor/polars-core/README.md` for recovery protocol.

## Rules map

Project-local rules live in `.claude/rules/*.md`; global rules in `~/.claude/rules/*.md`:

Project-local:
- `.claude/rules/sprint-cadence.md` — sprint protocol (briefs, retros, `cadence_metrics.md` row, every-5-sprints housekeeping).

Global:
- `git-commit-hygiene.md` — pre-commit audit, never commit secrets / large / regenerable files.
- `docs-lifecycle.md` — every non-`history/` doc is live; sweep on milestones.
- `claude-md-housekeeping.md` — this file ≤ 200 lines.
- `runtime-estimation.md` — estimate + monitor any task > 30s.
- `shutdown-protocol.md` — on `SHUTDOWN_SIGNAL`, halt + WIP note + CronCreate resume.
- `verify-before-claim.md` — verify load-bearing claims about current state / cause / sequencing / historical fact when verification is < 5 min cost; otherwise mark "unverified, my best inference."
- `self-audit-on-plans.md` — for any plan/proposal with ≥ 3 work items or ≥ 5pp total, dispatch parallel Explore + code-reviewer + planner agents before delivering to user. Promoted to user-level 2026-05-08.

## Agents

- `Explore` — broad codebase searches (>3 queries).
- `debugger` — error / test failure root cause.
- `tester` — write & run tests; verify functionality.
- `refactor` — DRY / simplify / perf.
- `qa` — post-change verification, style, regression check.
- `docs` — README / CHANGELOG / inline docs sync.
- `housekeeper` — milestone sweep (docs lifecycle + memory).
- `code-reviewer` — independent review of staged changes.

## Project state

- Branch: `claude-2` (working, post-pivot) / `main` (read-only upstream mirror, user-managed) / `claude` (parked-historical, tagged `claude-baseline-2026-05-07`).
- Remote: none. Do not re-add.
- Pivoted from `claude` to `claude-2` on 2026-05-07. claude-2 forked from main tip `f8b6360`. **Sprints 3-16 ratified — original 12-sprint roadmap closed + Sprints 13/13.5/14/15/16 post-roadmap. Sprint 13 reverted (0pp gain). Sprint 13.5 ratified (bench infra + state audit GREEN; readiness gates 9/9 green). Sprint 14 ratified (P3.2b GIL release on direct-FFI `load_par_df` + `clear_par_df`; bench gate PASSED at 12,987 cps N=4 = +168% vs 4,841 cps baseline). Sprint 15 ratified (A.2.4 Parquet `compression` + `row_group_size` public API + ADR 0005 + 0.8.3 wheel cut bundling Sprint 14 + Sprint 15). Sprint 16 ratified 2026-05-13 (mdata wishlist v1 bundle: P0 `engine.flush_tplog()` + P3 `engine.add_at_time()` + P2 pepper `::` null-literal disambiguation + 0.8.4 wheel cut + two pre-existing chili bug fixes surfaced by P3 test gate: `job.rs:96 next_run_time` defer + chili-py tz-offset conversion).**
- Pivot rationale: cherry-pick conflict accumulation on FFI-rewrite divergence surface — see `docs/standards/iteration_lessons.md` lesson 4 + `docs/history/sprints/sprint_2_dispatch_brief_2026-05-07.md`.
- Date pin: 2026-05-13.
- Versions on claude-2: chili-py at **0.8.4** (Sprint 16 wheel — adds `engine.flush_tplog()` + `engine.add_at_time()` + pepper `::` disambiguation; sha256 `6e724eef6b526372d82b14fb2c7f6ae0eafb482e2067005f9ba79f3839451f87`; replaces 0.8.3 wheel for mdata); workspace at 0.8.1 (post-2026-05-13 merge of upstream main, commits 7ebb919..b91680f); Python polars pinned to `1.39.3`; Rust polars patched to `pola-rs/polars` at **`py-1.39.3` tag** + chili-side q-style fmt patch (currently a local clone at `/tmp/polars-py-1.39.3` — see user-driven backlog P0 below). The 0.8.1 wheel is BROKEN (segfaults when pyarrow is co-loaded) and must not be shipped.
- Rust toolchain: **stable ≥ 1.95** (main's sysinfo 0.39 dep requires it). No `rust-toolchain.toml` pin — use `rustup update stable`. See `docs/dev_setup.md`.
- Python min: 3.10 (raised from 3.7 by pyo3 0.27 abi3-py310). See `docs/dev_setup.md` for env setup.
- Test count on claude-2 (post-Sprint-16): **172 Rust** (`cargo test --workspace --exclude chili-py`; +2 from Sprint 16 `flush_handle_test.rs`) + **83 chili-py pytest passing + 0 xfailed** (`uv run pytest`; +11 from Sprint 16: 3 `test_tplog_flush.py` + 4 `test_add_at_time.py` + 4 `test_pepper_syntax.py`).
- Bench gate (golden rule 6): parse_cache hit **377 ns** on claude-2 (Sprint 13.5 re-measure; Sprint 8 P1 historical median was 397 ns). **Golden rule 6 holds.** Sprint 13.5 added a Python concurrent throughput harness (`crates/chili-py/tests/bench_concurrent.py`, 4 shapes: single_eval, concurrent_eval, concurrent_load via fn_call, concurrent_load_direct via direct FFI) and a categorical_eval Rust criterion bench (`crates/chili-op/benches/categorical_eval.rs`); all baseline numbers + samply concurrent-load profile findings recorded in `docs/bench/post_pivot_baseline_2026-05-07.md` Sprint 13.5 section.
- ADRs: **0001** (pub/sub canonical, S5) + **0002** (`engine.eval(lazy=True)` Option b, S4 commit `f23e40a`) + **0003 RESOLVED S7 Part A** via option 3b (polars py-1.39.3 fork + q-style fmt patch) — lazy=True path usable end-to-end with predicate pushdown across FFI + **0004** (S10; pepper retains Polars-aligned primitive set, does NOT track k9 minimization) + **0005** (S15; Parquet write defaults — default codec is **ZSTD** verified empirically + validated 2026-05-09 by mdata real-data bench at 4M-row scale; user override via `compression=` / `row_group_size=` keyword-only kwargs; future default change requires mdata sign-off per golden rule 4).
- mdata delivery: **chili-sauce 0.8.3 wheel** at `dist/chili_sauce-0.8.3-cp310-abi3-macosx_11_0_arm64.whl` (Sprint 15 — bundles Sprint 14 + Sprint 15) + handoff doc `docs/sync/mdata_chili_2026-05-09_delivery.md` (install protocol; default behavior byte-equivalent to 0.8.2; not a bug fix). 0.8.2 still at `dist/chili_sauce-0.8.2-...whl` for byte-equivalence baseline reference; earlier delivery docs moved to `docs/history/sync/` (0.8.1 / 0.8.2).
- Wheel-only install protocol: **NEVER** ship editable installs to mdata (lesson 14). The wheel byte-stream is fully self-contained; chili dev on this tree does not affect mdata's installed wheel.
- Next: **Sprint 16+ scope unscoped** — mdata smoke-pass received 2026-05-09 (option 1; 0.8.3 pinned). 7 ideas in `docs/sync/ideas.md`: (3 perf-architecture: per-table mutex on par_df / RCU on par_df / coalesce concurrent loads — trigger needs profile evidence on real workloads) + (4 from mdata 0.8.3 feedback: `load_partitioned_df_eager` returning DataFrame / chili-native pepper read overhead profile / ≥100k-row CI codec fixture mdata-offered / struct-shaped FFI for `ParquetWriteConfig`). None scoped yet. **User-driven backlog**: (P0 — **elevated 2026-05-13** after macOS `/tmp` daily cleanup gutted the local polars clone, requiring re-clone + manual re-port of the q-style fmt patch from `vendor/polars-core/hinmeru-reference/`) GitHub-host the polars fork — replace `path = "/tmp/polars-py-1.39.3"` with `git = "..." + tag = "..."` in workspace + chili-py `[patch.crates-io]` blocks; without it, fresh clones of chili break at `cargo build`. Until done, `vendor/polars-core/README.md` is the recovery protocol. (P1) KDB-X CE comparison once GA + interactive registration available. (P2) **RESOLVED 2026-05-09** — mdata sign-off on 0.8.3 received (smoke pass, option 1; ZSTD default validated at 4M-row scale; full assessment at `~/code/mdata/docs/sync/chili_0.8.3_upgrade_assessment_2026-05-09.md`). (P3) Sprint 13 P2 Box::new mitigation: **deferred indefinitely** per Sprint 13 lesson 2 (no measurable target — 17.7 % chili-side Box::new is bounded above by polars-internal `0x450c` 93.1 % polars-worker time which dominates the regression). A.2.2 vars-write-lock release **descoped indefinitely** per Sprint 13.5 retro (clone-then-swap memory cost vs unmeasured concurrent gain). P3.4 Categorical mapping cache **deferred indefinitely** per Sprint 13.5 categorical_eval bench (0.4 % Δ between repeated and distinct shapes; no measurable target).
- Open items: see `~/.claude/projects/-Users-oakadmin-code-chili/memory/MEMORY.md`.
