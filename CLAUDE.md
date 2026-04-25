# CLAUDE.md — Chili

Agent-facing map of this repo. Keep terse: this file is loaded into every conversation.

## Project background

This is a local working fork of [purple-chili/chili](https://purple-chili.github.io/) — a kdb+/q-style analytics engine on Polars + Arrow + Parquet, with `chili` (JS-like) and `pepper` (q-like) syntaxes. It reuses `kola` for q interop. Upstream is the canonical project; this repo exists because a separate user project, **mdata** (`~/code/mdata`, market-data warehouse, ~11K US equities), needed Python bindings, GIL-released eval, quantization, pub/sub, `overwrite_partition`, etc. that upstream lacked. The chili author has since picked up a subset of those changes (see `project_chili_background.md` memory for the commit range and table). See `README.md` for performance numbers and feature list.

## Branch policy (load-bearing)

**No remote.** `git remote -v` is empty by design. Never `git push`, `git pull`, `git fetch`, or re-add a remote. The user manually uploads upstream state into the local `main` branch.

**`main` = upstream / external contributors.** Never commit to it, never check it out to make changes. Treat it as read-only.

**`claude` = the only branch you commit to.** Verify with `git rev-parse --abbrev-ref HEAD` before every commit; if absent locally, `git checkout -b claude`.

**Merging:** Only `main → claude` (when the user uploads new upstream state). Never `claude → main`, never any other direction. If a change must reach `main`, surface it and let the user handle it.

## Pre-commit gate

Run before every commit (matches `Taskfile.yml`):

```bash
cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test
```

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

1. **Branch:** `claude` only. See above.
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
| `crates/chili-py`  | PyO3 / maturin Python bindings (`chili-pie`) |

## Docs map

Start here: [`README.md`](README.md) — performance summary + Document Index of all live docs.

- [`CHANGELOG.md`](CHANGELOG.md) — release notes, all shipped phases.
- [`crates/chili-py/README.md`](crates/chili-py/README.md) — Python API surface.
- [`docs/bench/summary.md`](docs/bench/summary.md) — final post-sweep benchmark summary (2026-04-12).
- [`docs/bench/baseline.md`](docs/bench/baseline.md) — pre-sweep baseline (2026-04-11).
- [`docs/bench/phase{1..7,9}.md`](docs/bench/) — per-phase benchmark snapshots.
- [`docs/bench/mdata-collab/`](docs/bench/mdata-collab/) — mdata↔chili collaboration: schema, parity, comparison.
- [`docs/history/`](docs/history/) — frozen historical docs; never modify, only add.

## Rules map

Project-specific guidance lives in user memory; global rules are in `~/.claude/rules/*.md`:

- `git-commit-hygiene.md` — pre-commit audit, never commit secrets / large / regenerable files.
- `docs-lifecycle.md` — every non-`history/` doc is live; sweep on milestones.
- `claude-md-housekeeping.md` — this file ≤ 200 lines.
- `runtime-estimation.md` — estimate + monitor any task > 30s.
- `shutdown-protocol.md` — on `SHUTDOWN_SIGNAL`, halt + WIP note + CronCreate resume.

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

- Branch: `claude` (working) / `main` (read-only upstream mirror, user-managed).
- Remote: none. Do not re-add.
- Upstream `e9092ce..b0f20e5` selectively merged into `claude` on 2026-04-26: bytes-removal FFI rewrite (eval/wpar/overwrite_partition/tick_upd return/accept `pl.DataFrame` directly via `pyo3_polars::PyDataFrame`), pyo3 0.22→0.27, pyo3-polars 0.26, `chrono` workspace dep, `.gitignore` additions. **NOT** merged: `chili-pie → chili-sauce` rename, manylinux 2_28 GH workflows, low-level `PyEngineState`. See `project_chili_background.md`.
- Date pin: 2026-04-26.
- Versions: workspace `0.7.4`, `chili-pie 0.7.5` (Python wheel, post-merge bump). Upstream calls his package `chili-sauce 0.8.0`; we stay on `chili-pie` because mdata/nxcar import it.
- Python min: 3.10 (raised from 3.7 by pyo3 0.27 abi3-py310).
- Test count: 44 Python (35 baseline + 9 new direct-DataFrame regression tests) + 165 Rust = 209.
- Open items: see `~/.claude/projects/-Users-oakadmin-code-chili/memory/MEMORY.md`.
