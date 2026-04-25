# CLAUDE.md — Chili

Agent-facing map of this repo. Keep terse: this file is loaded into every conversation.

## Branch policy (load-bearing)

**Commit only to the `claude` branch.** The user owns `main` and uploads fresh state there directly. Never commit to `main`, never `git push origin main`, never `git merge claude` into `main`. If a change must reach `main`, surface it and let the user merge.

Before every commit run `git rev-parse --abbrev-ref HEAD` and confirm it is `claude`. If absent locally, create from current HEAD: `git checkout -b claude`.

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

- Branch: `claude` (working) / `main` (user-owned upload target).
- Date pin: 2026-04-26.
- Version: workspace `0.7.4` (`Cargo.toml`), `chili-pie 0.7.4` (`pyproject.toml`).
- Test count baseline: 35 Python + 165 Rust = 200.
- Open items: see `~/.claude/projects/-Users-oakadmin-code-chili/memory/MEMORY.md`.
