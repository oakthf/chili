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
- [`docs/bench/post_pivot_baseline_2026-05-07.md`](docs/bench/post_pivot_baseline_2026-05-07.md) — claude-2 post-pivot rebaseline (rolling; through Sprint 17; Sprints 13.5/14/15 A/B numbers appended; Sprints 16/17 had no bench-gate changes).
- [`docs/bench/mdata-collab/`](docs/bench/mdata-collab/) — mdata↔chili collaboration: schema, parity, kdb+ comparison.
- [`docs/research/`](docs/research/) — strategic research (kdb+ landscape, alternatives, Shakti, Iceberg eval); historical positioning + inventory docs moved to `docs/history/research/`.
- [`docs/sim/`](docs/sim/) — sprint cadence: 17 ratified retros (Sprints 1-17, Sprint 13 reverted) + `cadence_metrics.md` + `sprints_index.md`; templates are `_*_template.md`. Closed roadmap at [`docs/history/sim/roadmap_2026-05-07.md`](docs/history/sim/roadmap_2026-05-07.md).
- [`docs/standards/iteration_lessons.md`](docs/standards/iteration_lessons.md) — 20 durable rules from sprint retros (S1–15 + S18 mechanism-claim; **L19** Sprint-20 auto-merge-cascade; **L20** 2026-05-19 cross-read-normative-lines / contradictory-ADR-line-survived-6-layers — the mdata-found ADR-0006 §4 resume-coordinate bug).
- [`docs/sync/`](docs/sync/) — live: `mdata_chili_2026-05-24_0.8.9_delivery.md` (active 0.8.9 handoff) + `mdata_wishlist_2026-05-23_remote-eval-surface.md` (turn-9 — W3 satisfied) + `upstream_v0.9_vs_claude-2_0.8.9_gap_analysis_2026-05-24.md` (**outbound for chili-author — full feature-parity gap doc; author left 8 inline comments + we replied + added §10 with mdata's 5 discussion topics**) + `mdata_architecture_handoff_2026-05-24.md` (**mdata's production-deployment snapshot for the chili author — 9 daemons + 2 mermaid diagrams + invariants; pair-pre-read with the gap analysis**) + `decisions-needed.md` (D-001 resolved 2026-05-24) + `ideas.md` (8 entries) + `upstream_handle_lock_proposal_2026-05-14.md` (open upstream-author opinion request, handle write-lock contention). Swept to `docs/history/sync/`: earlier 0.8.1–0.8.7 deliveries + `mdata_push_model_proposal_2026-05-17.md` (satisfied → ADR-0006) + `mdata_chili_2026-05-18_main_merge_signoff.md` (Sprint-20 closed).
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
- Pivoted `claude`→`claude-2` 2026-05-07 (forked `main` tip `f8b6360`; rationale: cherry-pick conflict accumulation — `iteration_lessons.md` L4). **Sprints 3–24 ratified** (S13 reverted, 0pp). Per-sprint detail canonical in `docs/sim/cadence_metrics.md` + `sprint_N_retro.md` (map-not-manual).
- Date pin: 2026-05-25.
- Versions on claude-2: **0.9.0** coherently across workspace + chili-py `Cargo.toml` + `pyproject.toml` (Lesson 14: triple bump or wheel mis-labels). **Sprint 24 main-port (2026-05-25)** brought claude-2 to ≡ main 0.9.0+ — drops push-model (D-1/D-2/D-3), W3 register_fn, flush_tplog, publish_via_handle, roll_tick native, GR4 helpers (per mdata Revision A reframe + chili-author's 8 day-of commits closing all gaps). Preserves M-1 invariant test guard + claude-team docs + Sprint 16 `::` null parse extension. Polars: chili-author hosts the q-style fmt fork at `github.com/hinmeru/polars-core-patch.git` tag `v0.53.0` — replaces our 6-month-old local `/tmp/polars-py-1.39.3` family-clone (closes user-driven P0 backlog). Python polars pinned to `1.39.3`. The 0.8.1 wheel is BROKEN (pyarrow co-load segfault) and must not be shipped.
- Rust toolchain: **stable ≥ 1.95**. See `docs/dev_setup.md`.
- Python min: 3.10 (pyo3 0.27 abi3-py310).
- Tests on claude-2 (post-Sprint-24, **gate-green**): **189 Rust** + **72 chili-py pytest, 0 xfailed**. Net post-Sprint-24 from 215+108: −26 Rust (deleted external_fn / upd_notify / flush_handle / rotate_handle / eval_str tests) + −36 pytest (deleted test_register_fn / test_push_model / test_eval_str / test_publish_via_handle / test_tplog_flush / test_roll_tick).
- Bench gate (GR5): matched-shell A/B (Sprint 23 L21) — concurrent_eval N=1: 0.8.9=1264 → 0.9.0=1272 cps (+0.7%); N=4: 0.8.9=3160 → 0.9.0=3155 cps (-0.2%). Both within ±1%. GR5 preserved.
- ADRs (status only; rationale in each `docs/decisions/000N-*.md`): **0001** pub/sub canonical · **0002** `eval(lazy=True)` Option-b · **0003 RESOLVED** true-lazy via the now-hosted polars-core-patch fork · **0004** pepper ≠ k9-minimization · **0005 SUPERSEDED S20** · **0006 SUPERSEDED S24** push-model FFI (mdata Revision A reframe — `sub.pep` auto-apply replaces `upd_notify_fd`/`drain_upds`/`UpdEvent`/`subscribe(resume_from=)`/`get_var_lazy`) · **0007 SUPERSEDED S24** W3 Python-callable bridge (mdata withdrew + author declined upstreaming).
- mdata delivery: **0.9.0 SENT 2026-05-25** — `dist/chili_sauce-0.9.0-cp310-abi3-macosx_11_0_arm64.whl`, sha256 `ee85a079cee12531d211a4426fb3fa793176fe918acd0ce566f4c91082d585f4`; handoff `docs/sync/mdata_chili_2026-05-25_0.9.0_delivery.md`. Skips 0.8.9 (mdata never adopted W3 wheel — orphaned per Revision A). claude-2 effectively ≡ main 0.9.0+ (`git diff main HEAD` shows only docs/, M-1 test, Sprint-16 `::` extension, claude-2-only extra coverage tests). mdata v1-36 architecture cleanup (~1700 LOC removable per Revision A §5) follows mdata's 24h Pipeline X soak completion. **Invariants unchanged:** wheel-only installs (lesson 14); mdata authoritative on running wheel.
- Next: mdata 0.9.0 acceptance + v1-36 cleanup sprint. No new chili-side features planned. claude-2's role narrows to "main + M-1 + docs + claude-team tooling"; future merges from main are routine forward-ports.
- Open items: see `~/.claude/projects/-Users-oakadmin-code-chili/memory/MEMORY.md`.

## Cross-project mesh (vantage team-bus)

This project participates in vantage's team-bus per `~/team/oak/vantage/docs/architecture/team_bus.md`.

### Producer contract (binding for any agent/script emitting cross-project signals)

Write outbound events to `.cross_comms/outbox/<idempotency_key>.json` using the atomic pattern:

1. Write to `.cross_comms/outbox/<idempotency_key>.json.tmp`
2. `os.rename()` to `.cross_comms/outbox/<idempotency_key>.json` (atomic on POSIX)

Required envelope fields: `topic`, `payload`, `idempotency_key`. For `contract.*`, `directive.*`, `ratification_*`, `phase.boundary.*` also include `correlation_id` — the bus rejects events on those topics without it.

### Inbox

Read events written by the thin client to `.cross_comms/inbox/<event_id>.json`. Each file is a one-shot delivery; process + `mark_event_processed` (move to `.sent/`) when done.

### Topics this project subscribes to

See `.cross_comms/config.json`'s `subscriptions:` list. Topic ACL registry: `~/team/oak/vantage/mesh/registry/topics.toml` (canonical).

### Token

`.cross_comms/.chili.token` (mode 0600, gitignored). If lost: ask the principal to issue a replacement.

### Python runtime — always `uv run python` for bus ops

chili's `/usr/bin/python3` is the macOS Command Line Tools 3.9, which lacks `datetime.UTC` (added in 3.11). Any script that publishes to `.cross_comms/outbox/` and stamps `ts_utc` itself MUST use `uv run python` (or `uv run python3`) so it picks up the root `pyproject.toml`'s `requires-python = ">=3.10"` and chili's venv 3.12. Bare `python3 -c "from datetime import UTC, ..."` will fail silently — bash captures the traceback and the outbox file ends up with `"ts_utc": ""`. Vantage server-stamps to recover, but the convention is for senders to fill it themselves.

The launchd thin client already runs via `/opt/homebrew/bin/uv run python ...`; this rule binds for ad-hoc Claude / shell-script publishers too.
