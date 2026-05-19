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
- [`docs/sync/`](docs/sync/) — `decisions-needed.md` (open: empty) + `ideas.md` (8 entries) + `mdata_chili_2026-05-16_0.8.6_delivery.md` (0.8.6 handoff; Sprint 18 `roll_tick`; closes mdata wishlist v2 P0) + `upstream_handle_lock_proposal_2026-05-14.md` (upstream author proposal re: handle write-lock contention). Earlier delivery docs (0.8.1–0.8.5) and `load_par_df_state_audit` moved to `docs/history/sync/`.
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
- Pivoted from `claude` to `claude-2` on 2026-05-07. claude-2 forked from main tip `f8b6360`. **Sprints 3-17 ratified — original 12-sprint roadmap closed + Sprints 13/13.5/14/15/16/17 post-roadmap. Sprint 13 reverted (0pp gain). Sprint 13.5/14/15 ratified per `cadence_metrics.md`. Sprint 16 ratified 2026-05-13 (mdata wishlist v1 bundle P0+P3+P2; 0.8.4 wheel). Sprint 17 ratified 2026-05-14 (mdata wishlist v1 P1 bundle: Part B `engine.publish_via_handle(h, table, df)` thin-marshalling primitive per Q3 Option B lock-in; Part A `signal_eod` sync→async rewrite fixing latent EOD broadcast suppression — bug was H6 not in audit's H1-H5 hypothesis space; 0.8.5 wheel cut; mdata wishlist v1 closed on chili side). Sprint 18 ratified 2026-05-16 (mdata wishlist v2 P0, thread `mdata-chili-eod-upd-race-2026-05-15`: `EngineState::roll_tick(log_dir, segment_label)` — atomic tplog segment-rollover via same-id writer swap under `handle.write()`, replacing the racy `createLog` close-then-reopen; `prepare_file_writer` extracted from `open_handle`; generic opaque caller-owned segment label (UHF-ready); 0.8.6 wheel; red-first harness surfaced a 2nd silent legacy failure mode [`set_handle` id-reuse misplacement] — design D fixes both). Sprint 19 ratified 2026-05-16 (upstream merge `main`→`claude-2`: IPC remote query `606d1cc` [`fn_call` I64 arm + `engine.py` `open_handle`/`sync`] + chiz package imports `5a6adc5` [`import_source_path`/`resolve_package_*`]; real `git merge` not cherry-pick; `engine_state.rs` auto-merged clean; ADD-2 caught + fixed upstream `sync()` ↔ claude-2 ADR-0002 `eval()` incompat → routed via `fn_call`; clippy-conformance on upstream `resolve_package_version`; merge `f04e9e8` + fixups `b0b5f89`).**
- Pivot rationale: cherry-pick conflict accumulation on FFI-rewrite divergence surface — see `docs/standards/iteration_lessons.md` lesson 4 + `docs/history/sprints/sprint_2_dispatch_brief_2026-05-07.md`.
- Date pin: 2026-05-19.
- Versions on claude-2: **0.8.7** coherently across workspace `[workspace.package]` + chili-py `Cargo.toml` + chili-py `pyproject.toml` (the maturin wheel version is read from **pyproject.toml `[project] version`**, NOT Cargo.toml — both must be bumped; missing the pyproject bump produces a mis-labelled wheel). 0.8.7 = Sprint-21 push-model (D-1/D-2/D-3) wheel cut from claude-2 HEAD post-ratification; sha256 `1608317e23fc33f0ad2f5765fa46c495099ab15661e29fccb2c8ea5848636482`; delivery doc `docs/sync/mdata_chili_2026-05-19_0.8.7_delivery.md`. claude-2 current with upstream `main` through `606d1cc` (Sprint-19 merge `f04e9e8`) + Sprint-20 `1a42b13`. Python polars pinned to `1.39.3`; Rust polars patched to `pola-rs/polars` at **`py-1.39.3` tag** + chili-side q-style fmt patch (local clone at `/tmp/polars-py-1.39.3` — see user-driven backlog P0). The 0.8.1 wheel is BROKEN (segfaults when pyarrow co-loaded) and must not be shipped.
- Rust toolchain: **stable ≥ 1.95** (main's sysinfo 0.39 dep requires it). No `rust-toolchain.toml` pin — use `rustup update stable`. See `docs/dev_setup.md`.
- Python min: 3.10 (raised from 3.7 by pyo3 0.27 abi3-py310). See `docs/dev_setup.md` for env setup.
- Test count on claude-2 (post-Sprint-21, awaiting ratification): **201 Rust** (`cargo test --workspace --exclude chili-py`; Sprint 21: +3 `upd_notify_test.rs` [back-pressure-never-drops/fd-signal/CLOEXEC+nonblock], net 198→201) + **97 chili-py pytest passing + 0 xfailed** (`uv run pytest`; Sprint 21: +7 `test_push_model.py` [D-1 fd+drain real-IPC round-trip, D-3 resume-skip + unclean-restart, D-2 lazy-pushdown, Q5 close-on-exec fork]). Full gate green, 0 failed.
- Bench gate (golden rule 6): parse_cache hit **377 ns** on claude-2 (Sprint 13.5 re-measure; Sprint 8 P1 historical median was 397 ns). **Golden rule 6 holds.** Sprint 13.5 added a Python concurrent throughput harness (`crates/chili-py/tests/bench_concurrent.py`, 4 shapes: single_eval, concurrent_eval, concurrent_load via fn_call, concurrent_load_direct via direct FFI) and a categorical_eval Rust criterion bench (`crates/chili-op/benches/categorical_eval.rs`); all baseline numbers + samply concurrent-load profile findings recorded in `docs/bench/post_pivot_baseline_2026-05-07.md` Sprint 13.5 section.
- ADRs: **0001** (pub/sub canonical, S5; S17 + S18 follow-up notes: `signal_eod` Async rewrite [S17]; `roll_tick` cutover-only / independent of `signal_eod` [S18]) + **0002** (`engine.eval(lazy=True)` Option b, S4 commit `f23e40a`) + **0003 RESOLVED S7 Part A** via option 3b (polars py-1.39.3 fork + q-style fmt patch) — lazy=True path usable end-to-end with predicate pushdown across FFI + **0004** (S10; pepper retains Polars-aligned primitive set, does NOT track k9 minimization) + **0005 SUPERSEDED S20** (S15; Parquet write defaults — default codec **ZSTD** still holds [stock `polars-io 0.53.0` `ParquetCompression::default()` = `Zstd(None)`, re-verified at merge]; the `compression=`/`row_group_size=` override kwargs + `ParquetWriteConfig` + 9-arg `wpar` were **removed** in the Sprint-20 `main` merge for upstream's lean 7-arg `write_partition_native`; codec byte-stable; see ADR-0005 header + `docs/sync/mdata_chili_2026-05-18_main_merge_signoff.md` item-3) + **0006 Accepted S21** (async upd-notification FFI — push-model D-1/D-2/D-3: `upd_notify_fd`/`drain_upds`/`UpdEvent`/`subscribe(resume_from=)`/`get_var_lazy`; POSIX self-pipe + bounded crossbeam(4096) blocking-never-drop; `cursor_*` = per-handle delivery ordinal NOT mdata seq; impl `4c3fe0c`, gate-green, **awaiting ratification**).
- mdata delivery: **chili-sauce 0.8.7 wheel** at `dist/chili_sauce-0.8.7-cp310-abi3-macosx_11_0_arm64.whl` (sha256 `1608317e23fc33f0ad2f5765fa46c495099ab15661e29fccb2c8ea5848636482`; Sprint-21 push-model D-1/D-2/D-3 + carries Sprint-20 lean refactors) + handoff doc `docs/sync/mdata_chili_2026-05-19_0.8.7_delivery.md`. **⚠ DELIVERY: gate CLEARED 2026-05-19 — mdata's (ii)+0.8.7-only nod received (both YES; kwarg-drop landed mdata `b8f279a`; recorded mdata `fc4c800`). Wheel built + sha-verified; cross-comms delivery event drafted at `.cross_comms/drafts/chili_0.8.7_push_model_delivered.json`. ONLY remaining step = the outbox send, pending chili-principal final go-ahead. On receipt mdata sha-verifies == `1608317e…` then runs 769-suite + D-1/D-2/D-3 acceptance + ADR-0005 re-bench.** **mdata runs the IPC-superset lineage** and is authoritative on its running wheel (cites sha `8878907…`); chili does **not** assert a sha for mdata's wheel. chili-local `dist/` shas are **local build artifacts only, not the delivery base** — do not anchor delivery reasoning to them (the first 0.8.7 build attempt incidentally HEAD-rebuilt the local `dist/chili_sauce-0.8.6-…whl` because pyproject.toml wasn't bumped — exactly why local dist shas are not the anchor). 0.8.7 IPC-superset **verified**: `git merge-base --is-ancestor 606d1cc HEAD`=true → strict superset of roll_tick(S18) + IPC-remote-query+chiz(S19) + lean refactors(S20) + push-model(S21); cuts the tip not an ancestor → no structural regression path. Delivery base = the **claude-2 HEAD IPC-superset lineage**, not a frozen sha. Prior delivery docs: 0.8.6 `docs/sync/mdata_chili_2026-05-16_0.8.6_delivery.md`; 0.8.1–0.8.5 in `docs/history/sync/`.
- Wheel-only install protocol: **NEVER** ship editable installs to mdata (lesson 14). The wheel byte-stream is fully self-contained; chili dev on this tree does not affect mdata's installed wheel.
- **Sprint 20 RATIFIED 2026-05-18** (`main`→`claude-2` merge `1a42b13` + 0.8.7-base citation fix `40fa7a4`; adopt hinmeru lean refactors + preserve mdata superset [publish_via_handle/roll_tick/M-1/true-lazy/full-family py-1.39.3 pin]; drop overwrite_partition + wpar kwargs [ADR-0005 superseded]; 5 auto-merge cascades fixed → iteration_lessons Lesson 19; full detail in `docs/sim/sprints_index.md` + `sprint_20_retro.md`). claude-2 now incorporates upstream `main` tip (Sprint-19 `606d1cc`/`5a6adc5` + Sprint-20 `9dfa4d2`/`43faf44`/`ef4bfb2`).
- **Sprint 21 RATIFIED 2026-05-19** — mdata push-model D-1/D-2/D-3 (proposal `docs/sync/mdata_push_model_proposal_2026-05-17.md`; gates Q1–Q5 locked). D-1 GIL-free upd notification (`upd_notify.rs` POSIX self-pipe + bounded crossbeam(4096) blocking-never-drop; message-shape interception in `handle_chili_conn`, zero pyo3 on path); D-3 resumable cursor (`resume_cursor` map+builtin min-over-topics per Q1 Path-1; `subscribe(resume_from=)`; sub.pep off hardcoded 0/latent tick[0]); D-2 `get_var_lazy`. The audit#2-C6 open design lock RESOLVED: fields named `cursor_lo/cursor_hi` (per-handle delivery ordinal, NOT mdata seq) — not `seq_lo/seq_hi`. impl `4c3fe0c` (kickoff `4017674`); ADR-0006; retro + cadence row + sprints_index. Caught+fixed 1 regression (`to_str_vec` errors on non-empty MixedList) via committed pytest guard.
- Next (post-Sprint-21-ratification): cut **0.8.7** wheel from claude-2 HEAD (IPC-superset by construction — roll_tick + IPC-remote-query + chiz + Sprint-20 + Sprint-21) → single combined mdata delivery; brief moves to `docs/history/sprints/`. mdata still owes the explicit **(ii)+0.8.7-only** nod (gates the 0.8.7 *delivery*, not the sprints). mdata wishlist v2 P0 (`roll_tick`) closed Sprint 18; mdata adopts in v1-25.2. New upstream surface available on claude-2: `engine.open_handle()`/`engine.sync()` (IPC remote query — note claude-2-adapted via `fn_call`), `import "@scope/pkg/mod"` (chiz `$CHIZPATH` package imports). 7+ ideas in `docs/sync/ideas.md` all gated on external triggers. **User-driven backlog** (still open, no timeline): (P0) GitHub-host the polars fork — replace `path = "/tmp/polars-py-1.39.3"` with `git = "..." + tag = "..."` in workspace + chili-py `[patch.crates-io]` blocks; without it, fresh clones break at `cargo build`. Until done, `vendor/polars-core/README.md` is the recovery protocol. (P1) KDB-X CE comparison once GA + interactive registration available. (P3) Box::new mitigation + A.2.2 vars-write-lock + P3.4 Categorical cache: all **deferred indefinitely** (Sprint 13/13.5 retros; no measurable targets).
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
