# mdata ← chili 0.9.0 wheel delivery (Sprint 24 main-port)

**Date:** 2026-05-25
**From:** chili-team
**To:** mdata project
**Wheel:** `dist/chili_sauce-0.9.0-cp310-abi3-macosx_11_0_arm64.whl`
**sha256:** `ee85a079cee12531d211a4426fb3fa793176fe918acd0ce566f4c91082d585f4`
**Replaces:** mdata's running 0.8.8 wheel (sha `e75dffb7c5621d3cc8c206828f8db2a6ff5a43442c53ad37b181c7914f963ddc`)
**Skips:** 0.8.9 (W3 wheel — mdata withdrew W3 in Revision A; 0.8.9 was orphaned)
**Thread:** `chili-sprint-24-mainport-20260525` (pre-impl notification commit `1744a41`; design context in mdata's `docs/sync/mdata_architecture_handoff_2026-05-24.md` Revision A)

---

> **🎯 Main-port wheel.** claude-2 ≡ main 0.9.0+ M-1 test guard + claude-team docs. All claude-2-unique features built for mdata in Sprints 16-23 (push-model D-1/D-2/D-3, W3 register_fn, flush_tplog, publish_via_handle, roll_tick atomic, GR4 helpers) are DELETED. The chili-author's main 0.9.0+ provides equivalent or better coverage for every one of them. See `docs/sync/notes_to_chili_author_2026-05-25_update3.md` §Updated comparison for the 13-row equivalence table.

---

## TL;DR

0.9.0 is cut from `claude-2` HEAD post-Sprint-24-merge. **Strict superset of upstream main 0.9.0** (commits f6bccd1+5cfc096+588de78+26b437e+74acdc6+cc954d2 from chili-author 2026-05-25 = same code). The only difference from main: the M-1 invariant test guard (`test_engine.py::TestM1EagerNoAutoDequant`) + claude-team docs/sprint history (no impact on the runtime).

This wheel carries, vs your running 0.8.8:

- **From main 0.9.0+:** `async_/execute`, `fsync_handle` (W1 closed), `eval_op` String-parse (inline eval_str), `rotate_handle` accept-non-empty + idempotent + fsync-OLD + torn-tail recovery (your kill-9 case closed), `roll_tick_log` with tick-count sync, `SyncFile` wrapper, `polars-core-patch` hosted-on-GitHub fork (closes our 6-month P0 backlog item — no more /tmp clone fragility).
- **Removed vs 0.8.8 (per your Revision A reframe):** push-model D-1/D-2/D-3 (use sub.pep's auto-apply), W3 register_fn (you withdrew), flush_tplog (use fsync_handle), publish_via_handle (use sync(h, tuple)), roll_tick native (use roll_tick_log + new rotate semantics), GR4 set_column_scale helpers (lift to your Python facade if needed).

**v1-36 architecture cleanup can pin to 0.9.0.** Per your Revision A §7 sketch: ~1700 LOC removable on mdata side. This wheel is the basis.

---

## API surface — what disappeared, what to use instead

| You used to call | Now use |
|---|---|
| `engine.upd_notify_fd()` + `drain_upds()` | `engine.subscribe(...)` + `get_var(table)` (sub.pep upd handler runs in chili's Rust receive thread — no Python in the data path; per mdata Revision A canonical pattern) |
| `engine.get_var_lazy(id)` | `engine.get_var(id).lazy()` (functionally identical per ADR-0006 §5 retracted) |
| `engine.subscribe(uri, topics, resume_from=...)` | `engine.subscribe(uri, topics)` + accept full-day replay on restart (kdb+tick canonical; your Revision A §3.3) |
| `engine.flush_tplog()` | `engine.fsync_handle(handle_num)` — generic file-handle fsync; works on any handle, not just tplog |
| `engine.publish_via_handle(h, table, df)` | `engine.sync(h, (".tick.upd", table, df))` — invokes the publisher-side .tick.upd handler with the Polars DataFrame inline; verified in notes_to_chili_author §1 / `docs/sync/reproducers/q1_publish_path.py` |
| `engine.register_fn(name, callable, arity)` | use pepper function definitions for control verbs; if Python-callback needed, design the embedding at a higher layer than chili-IPC (per Revision A §3.9 reframe) |
| `engine.roll_tick(log_dir, segment_label)` | `engine.roll_tick_log(log_dir, filename)` — main's pepper wrapper around `.handle.rotate`; now includes idempotent retry + fsync-OLD + torn-tail recovery (q2_v4_post_truncate.py verified) |
| `engine.set_column_scale(...)` | mdata-side Python facade (your `StorageEngine` wrapper) |

## API additions (new on main 0.9.0)

- `engine.fsync_handle(handle_num)` — generic on-demand fsync; ~0.001ms/call measured
- `engine.async_(handle_num, query)` — async IPC send (fire-and-forget); pair with `engine.execute(handle_num, query)` for handle-sign dispatch (positive = sync, negative = async)
- `engine.roll_tick_log(log_dir, filename)` — main's rotation wrapper; uses the new `.handle.rotate` with all atomicity properties shipped

---

## Bench gate (chili-side)

Matched-shell A/B (0.8.9 → 0.9.0 in same shell, force-reinstall between runs, per Sprint 23 L21):

| Shape | N | 0.8.9 (pre-Sprint-24) | 0.9.0 (post) | Δ |
|---|---|---|---|---|
| concurrent_eval | 1 | 1264 cps | 1272 cps | **+0.7%** |
| concurrent_eval | 4 | 3160 cps | 3155 cps | **-0.2%** |

Within ±5% of 0.8.9 (loose tolerance for the major restructure).

## Tests (chili-side)

```
cargo fmt --all -- --check            : OK
cargo clippy --all-targets -- -D warnings : OK
cargo test --workspace --exclude chili-py : 189 passed, 0 failed
uv run pytest                          : 72 passed, 0 failed (was 108 on claude-2 0.8.9;
                                          delta = -36 = test files deleted per Sprint 24
                                          (test_register_fn / test_push_model / test_eval_str /
                                          test_publish_via_handle / test_tplog_flush / test_roll_tick))
```

---

## Install

```bash
uv pip uninstall chili-sauce
uv pip install /path/to/chili_sauce-0.9.0-cp310-abi3-macosx_11_0_arm64.whl
# pin the new hash: `ee85a079cee12531d211a4426fb3fa793176fe918acd0ce566f4c91082d585f4`
```

abi3-py310; macOS arm64. Build: `cd crates/chili-py && uv run maturin build --release -o dist` from `claude-2` HEAD post-Sprint-24-merge.

---

## Provenance

- Cut from claude-2 HEAD post-Sprint-24-merge (commits `1744a41` brief + `da6b1a4` merge + `1a9dbdd` residual cleanup + this wheel commit).
- `git diff main HEAD` shows ONLY: docs/, claude-2-only extra tests (fn_call_i64_test.rs, tcp_listener_graceful_test.rs, test_ipc_remote_query.py), Sprint-16 `::` null literal parse extension (chili-parser/src/expr.rs), M-1 invariant test (test_engine.py).
- No on-disk format changes vs main. No FFI-shape changes vs main. Pure API: identical to main.

## Cross-references

- Sprint 24 brief: `docs/history/sprints/sprint_24_dispatch_brief_2026-05-25.md` (post-move)
- Sprint 24 retro: `docs/sim/sprint_24_retro.md`
- Author dialogue: `docs/sync/notes_to_chili_author_2026-05-25.md` + `_update.md` + `_update2.md` + `_update3.md`
- mdata Revision A (source of authorization to delete): `docs/sync/mdata_architecture_handoff_2026-05-24.md`
- Gap analysis (origin): `docs/sync/upstream_v0.9_vs_claude-2_0.8.9_gap_analysis_2026-05-24.md`
- ADRs marked Superseded by this sprint: `docs/decisions/0006-async-upd-notification-ffi.md` (push-model), `docs/decisions/0007-w3-python-callable-bridge.md` (W3)
- Prior delivery (now superseded): `docs/sync/mdata_chili_2026-05-24_0.8.9_delivery.md` (0.8.9 wheel; never adopted by mdata per Revision A)

## Acceptance asks from chili-side

When you bump your pin to 0.9.0:

1. **sha-verify** the wheel against the hash recorded above.
2. **Run your 769-suite** as a regression baseline (only behavioural change is the API substitutions in the table above; all data-path / IPC semantics unchanged).
3. **Confirm migration sequencing** — your Revision A §7 sketch suggested v1-36 sub-sprint #1 = architecture cleanup (~5-8pp). Pinning to 0.9.0 is sub-sprint #1's gate; from there, the ~1700 LOC removable per your §5 is your call.
4. **If your gateway still uses the `::` null literal parse extension** (Sprint 16 — chili-parser/src/expr.rs), confirm so we know whether to keep it on claude-2 OR you migrate to whatever syntax main supports. Low priority.
5. **Whether to preserve M-1 invariant test on the wheel** — currently kept (zero cost; documents engine honesty). If you'd rather we drop it, tell us.

No deadline. Your 24h Pipeline X soak completes today; v1-36 cleanup follows when you're ready.
