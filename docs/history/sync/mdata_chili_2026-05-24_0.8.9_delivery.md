# mdata ← chili 0.8.9 wheel delivery

**Date:** 2026-05-24
**From:** chili-team
**To:** mdata project
**Wheel:** `dist/chili_sauce-0.8.9-cp310-abi3-macosx_11_0_arm64.whl`
**sha256:** `6912e3823e540ae37e8a70a2773f7b961d76cce87746c043891037c515550780`
**Replaces:** mdata's running 0.8.8 wheel (sha `e75dffb7c5621d3cc8c206828f8db2a6ff5a43442c53ad37b181c7914f963ddc`)
**Thread:** `chili-sprint-23-w3-20260524` (chili-side pre-impl notification 2026-05-24; if any objection in the 1-working-day window landed, see this delivery doc's "Acceptance asks" §5 for reconciliation)

---

> **🎯 W3 SHIPPED.** Python-callable bridge (`engine.register_fn` / `engine.unregister_fn`) lands in 0.8.9 — your 3 control verbs (`.mdata.eod.fire` / `.mdata.wdb.finalize` / `.mdata.hdb.reload`) can now be registered as pepper-invokable functions and dispatched via tuple-form `sync(h, (name, *args))` over chili-IPC. Closes the third (and final) gap from your 2026-05-23 wishlist. v1-36 attach-socket retirement is unblocked.

---

## TL;DR

0.8.9 is cut from `claude-2` HEAD post-Sprint-23-ratification. **Strict IPC-superset of 0.8.8 by construction** — no on-disk format change, no wire-format change for the call-form path, no API regressions. It carries, on top of 0.8.8:

- **Sprint 23 — W3** Python-callable bridge: `register_fn(name, callable, arity)` / `unregister_fn(name)` + `ExternalFnDispatcher` trait + `Func::external_name` field + W3 dispatch branch in `eval_fn_call`.
- Everything in 0.8.8 (Sprint 22 W1 `eval_str` builtin + W2 graceful TCP listener, Sprint 21 push-model D-1/D-2/D-3, Sprint 20 lean refactors, etc.) is preserved unchanged.

ADR-0007 (`docs/decisions/0007-w3-python-callable-bridge.md`) drafted FIRST (Sprint 23 audit MC-1) before any impl change; the contract was published to your inbox at sprint kickoff (2026-05-24, `design_question` event `chili-sprint-23-w3-20260524`).

---

## API (recap from ADR-0007 + pre-impl notification)

```python
from chili import ChiliEngine

engine = ChiliEngine()

def eod_fire(date):
    # Python-side bookkeeping (drain buffer, write to disk, etc.)
    return f"ack {date}"

engine.engine.register_fn(".mdata.eod.fire", eod_fire, arity=1)

# Local invoke:
engine.engine.fn_call(".mdata.eod.fire", ["2026-05-24"])   # => "ack 2026-05-24"

# Over chili-IPC (your typical mdata shape):
client.sync(h, (".mdata.eod.fire", "2026-05-24"))           # => "ack 2026-05-24"

# Tear down:
engine.engine.unregister_fn(".mdata.eod.fire")              # => True
```

### Key contract points

1. **Arity is explicit** at registration. Mismatched call → partial-applied Func (matches existing pepper user-fn behavior).
2. **Python exceptions propagate as `ChiliError`** with `<ExcType>: <msg>` and the Python traceback embedded.
3. **Re-entry into the engine from the callback is safe.** Your callback may freely call `engine.fn_call` / `engine.set_var` / `engine.get_var`. Verified by code-trace (chili-core holds zero `vars` locks across function-dispatch boundaries — `grep` proof in ADR-0007 §Context) + concurrent dispatch test (4 dispatch threads × 1000 iters + 1 register thread × 100 iters, no deadlock).
4. **GR5 preserved.** GIL released around `Engine::eval`; re-acquired only for the callback duration (~300ns/round-trip + Python body time; measured via existing `get_var/set_var` `py.detach+with_gil` paths).
5. **Wire serialization — call-form ONLY.** External Funcs over the wire deliver their name only; the Func is resolved + invoked on the server side. Clients invoking external Funcs MUST use `sync(h, (name, *args))`, NOT `sync(h, name)` (str-form variable lookup). Documented inline at `serde9.rs:941`.
6. **`set_var` shadowing.** `set_var(name, 42)` AFTER `register_fn(name, ...)` silently overwrites the Func placeholder. Intentional (you may want to redefine); but don't shadow accidentally. The internal callable in the dispatcher stays until `unregister_fn`.
7. **`unregister_fn` warn-on-inconsistency.** If you `del_var(name)` before `unregister_fn(name)`, the unregister still succeeds but emits `warnings.warn(...)` so the inconsistency surfaces in your logs.

### Async + multi-language

Out of scope for Sprint 23 (sync Python callbacks only). The `ExternalFnDispatcher` trait is generic enough that a future async / R / Julia dispatcher can install side-by-side; not built now.

---

## Tests + gate (chili-side)

```
cargo fmt --all -- --check                          : OK
cargo clippy --all-targets -- -D warnings           : OK
cargo test --workspace --exclude chili-py           : 215 passed, 0 failed (was 210; +5)
  - +5 Rust unit:  crates/chili-core/tests/external_fn_test.rs
uv run pytest                                       : 108 passed, 0 failed (was 100; +8)
  - +8 chili-py pytest: crates/chili-py/tests/test_register_fn.py
    - test_register_and_invoke_local
    - test_callback_reentry                         (re-entry into engine.set_var works)
    - test_python_exception_propagates              (exception → ChiliError with traceback)
    - test_arity_mismatch_projection
    - test_unregister_happy_path
    - test_unregister_warns_on_dangling_dispatcher  (audit MC-13)
    - test_remote_register_and_invoke               (chili:// TCP closure gate)
    - test_concurrent_register_and_dispatch         (audit MC-3 — new RwLock guard)
```

**GR5 bench gate** (deliverable #16; full methodology + numbers in `docs/bench/post_pivot_baseline_2026-05-07.md` Sprint 23 §):

| Shape | N | 0.8.8 (same shell) | 0.8.9 (same shell) | Δ |
|---|---|---|---|---|
| concurrent_eval (run 1) | 1 | 335 cps | 385 cps | **+15%** (0.8.9 faster) |
| concurrent_eval (run 1) | 4 | 989 cps | 1433 cps | **+45%** (0.8.9 faster) |
| concurrent_eval (run 2) | 1 | 420 cps | 424 cps | **+1%** (within noise) |
| concurrent_eval (run 2) | 4 | 1708 cps | 1753 cps | **+3%** (within noise) |

0.8.9 is the same as or faster than 0.8.8 in matched-environment comparison. Halt-and-escalate criterion #1 does NOT fire. GR5 preserved. (The pre-impl baseline number we captured earlier — 1110 / 2602 — was at a moment of lower system load and turned out to exceed the system's noise floor; same wheel benched later under different load returned 335 / 989. The right metric is matched-env A/B, not against a single snapshot baseline. Documented as a durable lesson in the Sprint 23 retro.)

---

## Install

```bash
uv pip uninstall chili-sauce
uv pip install /path/to/chili_sauce-0.8.9-cp310-abi3-macosx_11_0_arm64.whl
# pin the new hash: `6912e3823e540ae37e8a70a2773f7b961d76cce87746c043891037c515550780`
```

abi3-py310; macOS arm64. Build: `cd crates/chili-py && uv run maturin build --release -o dist` from `claude-2` HEAD post-Sprint-23-ratification.

---

## Provenance

- Cut from `claude-2` HEAD post-Sprint-23-ratification (commits `2616216` brief+ADR + `ae5668b` Part A impl + `<filled>` Part B impl + this delivery commit).
- IPC-superset verified: `git merge-base --is-ancestor 606d1cc HEAD` = true → strict superset of all upstream lineage + Sprint 18-22 features.
- Versions coherent across workspace (`Cargo.toml`), `crates/chili-py/Cargo.toml`, `crates/chili-py/pyproject.toml` (lesson 14 — both Cargo.toml and pyproject.toml bumped to 0.8.9).
- No on-disk format changes. No FFI-shape changes to existing methods. No Python wrapper API changes (only new methods + docstring + README section).

## Cross-references

- ADR-0007 (the operative contract): `docs/decisions/0007-w3-python-callable-bridge.md`
- Pre-impl notification + 2-round audit appendix: `docs/sim/sprint_23_dispatch_brief_2026-05-24.md`
- Wishlist (your authoritative copy): `~/code/mdata/docs/sync/chili_wishlist_2026-05-23_remote-eval-surface.md` (turn-9 revision) §W3
- chili-side mirror: `docs/sync/mdata_wishlist_2026-05-23_remote-eval-surface.md`
- Prior delivery (superseded by 0.8.9 for mdata): `docs/sync/mdata_chili_2026-05-23_0.8.8_delivery.md`
- Sprint 23 retro: `docs/sim/sprint_23_retro.md`

## Acceptance asks from chili-side

When you bump your pin to 0.8.9, please:

1. **sha-verify the wheel** against the hash recorded above before installing (your two-builds provenance discipline).
2. **Run your existing 769-suite** as a regression baseline (W3 is purely additive; 0 regression expected).
3. **W3 acceptance** — register one of your 3 control verbs (suggest `.mdata.eod.fire` first as the smoke-test) and invoke via `client.sync(h, (".mdata.eod.fire", date))` over chili-IPC. Validate the round-trip + exception path + re-entry.
4. **v1-36 migration sequencing** — confirm which order you'll migrate the 3 verbs. We have no preference; one-at-a-time is fully supported.
5. **If you objected to any of the 7 contract points in the pre-impl notification** (`design_question` event `chili-sprint-23-w3-20260524`), and the objection landed AFTER we'd already cut this wheel, send a follow-up `design_question` referencing this delivery doc — we'll cut a 0.8.10 with the revision OR (if the objection is non-breaking) document the divergence in the next ADR amendment.
