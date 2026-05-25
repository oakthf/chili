# CLAUDE.md — Chili

Agent-facing map of this repo. Keep terse: this file is loaded into every conversation.

## Project background

Local working fork of [purple-chili/chili](https://purple-chili.github.io/) — kdb+/q-style analytics engine on Polars + Arrow + Parquet, with `chili` (JS-like) and `pepper` (q-like) syntaxes; reuses `kola` for q interop. Upstream is canonical. This repo exists to serve **mdata** (`~/code/mdata`, market-data warehouse, ~11K US equities) — Python bindings, GIL-released eval, quantization, pub/sub, `overwrite_partition` etc. The chili author has since upstreamed a subset (memory: `project_chili_vision.md`). See `README.md` for perf + feature list.

## Branch policy (load-bearing — post-pivot 2026-05-07)

**No remote.** `git remote -v` is empty by design. Never `git push`, `git pull`, `git fetch`, or re-add a remote. The user manually uploads upstream state into `main`.

- **`main`** — upstream / external; **read-only**. Never commit or check out for edits.
- **`claude-2`** — the **only** branch you commit to. Verify with `git rev-parse --abbrev-ref HEAD` first. Forked from `main` tip 2026-05-07 (rationale: cherry-pick conflict accumulation, `iteration_lessons.md` L4).
- **`claude`** — parked-historical, immutable. Tagged `claude-baseline-2026-05-07` for reproducible historical binary builds. Pre-pivot reference for A/B + provenance.

**Merging:** only `main → claude-2`. Never `claude-2 → main`. Never `claude → claude-2` (use `git checkout claude -- <path>` for selective doc copy). Pivot tags: `claude-baseline-2026-05-07`, `main-pivot-2026-05-07`.

## Pre-commit gate

Matches `Taskfile.yml`:

```bash
cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace --exclude chili-py
```

`--workspace --exclude chili-py` is **load-bearing** — chili-py's pyo3 `extension-module` feature blocks `-lpython` linker flags, breaking standalone test binaries for chili-core/chili-op. Env setup in `docs/dev_setup.md` (`.cargo/config.toml` with PYO3_PYTHON + DYLD_FALLBACK_LIBRARY_PATH).

Python bindings, from `crates/chili-py/`:

```bash
uv run maturin develop && uv run pytest
```

Then the staged-file audit from `~/.claude/rules/git-commit-hygiene.md` — never `git add -A`.

## Common commands

```bash
task build                  # debug build of `chili` binary
task release                # release build
task test                   # cargo test
cargo bench -p chili-op     # core ops benchmarks (scan, eval, load_par_df, write_partition)
cargo bench -p chili-core --bench parse_cache
cd crates/chili-py && uv run python tests/bench_concurrent.py   # Python concurrent throughput

# Canonical wheel build — output to repo-root /dist/ ONLY (never per-crate dist/).
# Delivery docs + CLAUDE.md reference paths as `dist/...` from repo root.
cd crates/chili-py && uv run maturin build --release -o ../../dist
```

## Golden rules

1. **Branch:** `claude-2` only. See above.
2. **Polars version is pinned** in workspace `Cargo.toml` (`0.53.0`). Bumps are coordinated changes — don't unpin in passing.
3. **Edition 2024.** MSRV per `rust-toolchain.toml` if present; otherwise stable.
4. **Storage schema is Int64-quantized** for price columns. `set_column_scale` dequantizes to Float64 on read. Don't change the on-disk dtype without coordinating with mdata storage layer.
5. **GIL is released around `Engine::eval`** in `chili-py`. Don't reintroduce GIL-held long ops — the 6.10× concurrent throughput win depends on it.
6. **Parse cache is a hot path.** A cache hit is ~385ns. Don't add allocations or locks on the hit path without benching.
7. **Wheel output lives at repo-root `/dist/` only.** Build with `-o ../../dist` from `crates/chili-py/`; never let maturin default to per-crate `dist/`. Delete duplicate per-crate dist if it reappears. When cutting a new delivery wheel, remove the prior-version wheel from `/dist/` in the same commit.

## Workspace layout

| Crate | Purpose |
|---|---|
| `crates/chili-core` | Engine, parse cache, partition loader |
| `crates/chili-op`   | Query operators, scan, eval, write_partition |
| `crates/chili-parser` | chili / pepper parsers (chumsky-based) |
| `crates/chili-bin` | `chili` CLI binary |
| `crates/chili-py`  | PyO3 / maturin Python bindings; ships as **`chili-sauce`** wheel (import name `chili`). Directory name `chili-py` is legacy. |

## Docs map

Start here: this file. [`README.md`](README.md) is the upstream user-facing intro.

- [`CHANGELOG.md`](CHANGELOG.md) — release notes (upstream-aligned).
- [`crates/chili-py/README.md`](crates/chili-py/README.md) — Python API surface.
- [`docs/dev_setup.md`](docs/dev_setup.md) — local env (PYO3_PYTHON, DYLD_FALLBACK_LIBRARY_PATH, `.cargo/config.toml`).
- [`docs/bench/post_pivot_baseline_2026-05-07.md`](docs/bench/post_pivot_baseline_2026-05-07.md) — claude-2 post-pivot rebaseline (rolling; through Sprint 17; later sprints had no bench-gate changes worth rebaselining).
- [`docs/bench/mdata-collab/`](docs/bench/mdata-collab/) — mdata↔chili: schema, parity, kdb+ comparison.
- [`docs/research/`](docs/research/) — strategic research (kdb+ landscape, alternatives, Shakti, Iceberg).
- [`docs/sim/`](docs/sim/) — sprint cadence: 24 ratified retros (S13 reverted) + `cadence_metrics.md` + `sprints_index.md`. Templates: `_*_template.md`.
- [`docs/standards/iteration_lessons.md`](docs/standards/iteration_lessons.md) — 24 durable rules from sprint retros (S1–S24; L20 cross-read normative lines; L23 fold-deletions-into-merge-commit; L24 quiescent-bench requirement).
- [`docs/sync/`](docs/sync/) — **live (4 files):** `mdata_chili_2026-05-25_0.9.0_delivery.md` (active — awaiting mdata acceptance), `upstream_handle_lock_proposal_2026-05-14.md` (open with chili-author), `decisions-needed.md` (working dashboard; D-001 resolved), `ideas.md` (8-entry backlog). Subdir: `reproducers/`. Pre-Sprint-24 deliveries/dialogs/wishlists moved to `docs/history/sync/`.
- [`docs/decisions/`](docs/decisions/) — ADRs 0001-0007 (see Project state for status).
- [`docs/history/`](docs/history/) — frozen historical docs; never modify, only add.
- [`vendor/polars-core/`](vendor/polars-core/) — hinmeru fmt-patch reference for reconstructing the chili-side q-style fmt patch when `/tmp/polars-py-1.39.3` is lost. See `vendor/polars-core/README.md` for recovery protocol.

## Rules map

Project-local rules in `.claude/rules/*.md`; global rules in `~/.claude/rules/*.md`:

Project-local:
- `.claude/rules/sprint-cadence.md` — sprint protocol (briefs, retros, `cadence_metrics.md` row, wheel-cut convention, every-5-sprints housekeeping).

Global:
- `git-commit-hygiene.md` — pre-commit audit, never commit secrets / large / regenerable files.
- `docs-lifecycle.md` — every non-`history/` doc is live; sweep on milestones.
- `claude-md-housekeeping.md` — this file ≤ 200 lines; rules ≤ 120; MEMORY ≤ 150.
- `runtime-estimation.md` — estimate + monitor any task > 30s.
- `shutdown-protocol.md` — on `SHUTDOWN_SIGNAL`, halt + WIP note + CronCreate resume.
- `verify-before-claim.md` — verify load-bearing claims when verification is < 5 min cost; otherwise mark "unverified, my best inference."
- `self-audit-on-plans.md` — for any plan/proposal ≥ 3 work items or ≥ 5pp, dispatch parallel Explore + code-reviewer + planner audits before delivering.
- `baseline-doc-audit.md` — 4-task dual-workflow when refreshing a baseline gap-analysis or auditing multi-daemon readiness.
- `work-metrics.md` — token-budget estimation; per-session attribution; ledger schema.

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

- **Branches:** `claude-2` (working) / `main` (RO upstream) / `claude` (parked, tagged `claude-baseline-2026-05-07`). **Remote: none. Do not re-add.**
- **Date pin:** 2026-05-26.
- **Sprints 3-24 ratified** (S13 reverted, 0pp). Detail in `docs/sim/cadence_metrics.md` + `sprint_N_retro.md`.
- **Versions:** **0.9.0** coherently across workspace + chili-py `Cargo.toml` + `pyproject.toml` (Lesson 14: triple-bump or wheel mis-labels). The 0.8.1 wheel is BROKEN (pyarrow segfault) — never ship.
- **Toolchain:** Rust stable ≥ 1.95 (`docs/dev_setup.md`); Python ≥ 3.10 (pyo3 0.27 abi3-py310); Python polars pinned `1.39.3`; Polars-core fork at `github.com/hinmeru/polars-core-patch.git` tag `v0.53.0` (replaces /tmp/polars-py-1.39.3 — closes 6-month P0 backlog).
- **Tests (post-Sprint-24, gate-green):** **189 Rust + 72 chili-py pytest**, 0 xfailed.
- **Bench gate (GR5, S24 matched-shell A/B per L21):** concurrent_eval N=1 1264→1272 cps (+0.7%); N=4 3160→3155 (-0.2%). Within ±1%. GR5 preserved.
- **ADRs:** **0001** pub/sub canonical · **0002** `eval(lazy=True)` Option-b · **0003 RESOLVED** true-lazy via hosted polars-core-patch · **0004** pepper ≠ k9-minimization · **0005 SUPERSEDED S20** · **0006 SUPERSEDED S24** push-model FFI (mdata Revision A) · **0007 SUPERSEDED S24** W3 Python-callable bridge (mdata withdrew + author declined upstreaming).
- **Active mdata delivery:** **0.9.0 SENT 2026-05-25** — `dist/chili_sauce-0.9.0-cp310-abi3-macosx_11_0_arm64.whl`, sha256 `ee85a079cee12531d211a4426fb3fa793176fe918acd0ce566f4c91082d585f4`; handoff `docs/sync/mdata_chili_2026-05-25_0.9.0_delivery.md`. claude-2 effectively ≡ main 0.9.0+ (`git diff main HEAD` shows only docs/, M-1 test, Sprint-16 `::` extension, claude-2-only extra coverage tests).
- **Next:** mdata 0.9.0 acceptance + their v1-36 architecture cleanup (~1700 LOC removable per Revision A §5). No new chili-side features planned. claude-2's role narrows to "main + M-1 + docs + claude-team tooling"; future main merges are routine forward-ports.
- **Open items:** see `~/.claude/projects/-Users-oakadmin-code-chili/memory/MEMORY.md`.

## Cross-project mesh (vantage team-bus)

Per `~/team/oak/vantage/docs/architecture/team_bus.md`. **Outbound** events use atomic-write to `.cross_comms/outbox/<idempotency_key>.json` — write `.tmp` then `os.rename()`. Required envelope: `topic`, `payload`, `idempotency_key`; `contract.*` / `directive.*` / `ratification_*` / `phase.boundary.*` also need `correlation_id`. **Inbound** events arrive at `.cross_comms/inbox/<event_id>.json`; process and `mark_event_processed` (move to `.sent/`). Subscriptions: `.cross_comms/config.json`. Topic ACL registry: `~/team/oak/vantage/mesh/registry/topics.toml`. Token: `.cross_comms/.chili.token` (mode 0600, gitignored).

**Python runtime for bus ops:** always `uv run python` — chili's `/usr/bin/python3` is macOS CLT 3.9 which lacks `datetime.UTC` (added in 3.11). Bare `python3 -c "from datetime import UTC, ..."` fails silently and outbox `ts_utc` ends up empty. The launchd thin client already uses `/opt/homebrew/bin/uv run python`; this binds for ad-hoc publishers too.
