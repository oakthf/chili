# mdata ← chili 0.8.8 wheel delivery

**Date:** 2026-05-23
**From:** chili-team
**To:** mdata project
**Wheel:** `dist/chili_sauce-0.8.8-cp310-abi3-macosx_11_0_arm64.whl`
**sha256:** `e75dffb7c5621d3cc8c206828f8db2a6ff5a43442c53ad37b181c7914f963ddc`
**Replaces:** mdata's running 0.8.7 wheel (sha `1608317e23fc33f0ad2f5765fa46c495099ab15661e29fccb2c8ea5848636482`)
**Thread:** `chili-wishlist-2026-05-23-remote-eval-surface` (your 2026-05-23 turn-9 revision)

> **✅ W2 SHIPPED.** The bare-TCP-connect Rust panic that was your only no-workaround blocker is closed. `start_tcp_listener` now logs + continues on every error path in the accept loop and in `validate_auth_token`. `dis` migration to bare-TCP-connect probe is unblocked.
>
> **📚 W1 SHIPPED (both forms documented).** Your turn-9 self-discovery is now in the docstring — `engine.sync(handle, query)` is documented as three-way polymorphic (str = var lookup, bytes = arbitrary pepper eval, tuple/list = named-fn invoke). The named-tuple form `sync(h, ("eval_str", "<pepper>"))` is also shipped as a chili-side convenience alias via a new `SIDE_EFFECT_FN["eval_str"]` builtin — purely additive to bytes-form, no behavioural difference. Pick whichever shape fits your call sites.
>
> **🕒 W3 DEFERRED** with explicit re-evaluation gate (see below).

---

## TL;DR

0.8.8 is cut from `claude-2` HEAD post-Sprint-22-ratification. **Strict IPC-superset of 0.8.7 by construction** (`git merge-base --is-ancestor 606d1cc HEAD` = true). It carries, in one wheel:

- **Sprint 22 — W2** graceful bare-TCP-connect handling on `start_tcp_listener` (P0-highest per your turn-9 revision; no user-space workaround existed).
- **Sprint 22 — W1** `eval_str` pepper builtin (named-tuple-form alias for arbitrary remote pepper-eval) + `sync()` docstring update documenting the str/bytes/tuple polymorphism your turn-9 spike surfaced.
- Everything in 0.8.7 (Sprint 21 push-model D-1/D-2/D-3, Sprint 20 lean refactors, IPC remote query, chiz package imports, etc.) is preserved unchanged.

This is a **pure additive surface change** on top of 0.8.7 — no API regressions, no on-disk format changes, no behavioural changes for any existing call.

---

## Changes vs your running 0.8.7

### W2 (P0 per turn-9) — `start_tcp_listener` graceful bare-TCP handling

**Was:** bare TCP connect + immediate close → `Result::unwrap() on Err: "Socket is not connected"` panic on the accept-loop thread → listener died, requiring engine restart.

**Now:** every panic site in the accept loop (`engine_state.rs:start_tcp_listener`) and in `validate_auth_token` is converted to a `match` / `if let Err` + `info!` log + `continue`. The listener thread survives any of: bare TCP connect-close, peer RST mid-handshake, unsupported version byte, write failure, set_nodelay failure, `peer_addr` failure on a half-closed socket, `set_handle` failure, etc.

```python
# previously crashed the listener; now logged + ignored:
socket.socket().connect(("localhost", chili_port))
socket.close()
# listener accepts the next legitimate handshake fine.
```

Latency: chili-side overhead on a bare-TCP-connect-close averages **< 1 ms** server-side over a 100-iteration loop (asserted as a committed regression test, `crates/chili-core/tests/tcp_listener_graceful_test.rs::bare_tcp_connect_close_under_1ms_avg`). Your `dis` probe should see no measurable contention.

### W1 (your turn-9 finding) — `sync()` docstring + `eval_str` named-tuple alias

Your turn-9 spike found that `engine.sync(h, b"1 + 2")` is already arbitrary remote pepper-eval — no chili-side change needed for the capability. Two concrete deliverables in 0.8.8:

1. **`sync()` docstring fully documents the type-polymorphism** (see `crates/chili-py/chili/engine.py::sync`):
   - `str` → variable-name LOOKUP
   - `bytes` → arbitrary pepper EVAL
   - `tuple`/`list` → named-function INVOCATION
   - `bytearray` → NOT supported (raises `ChiliError`)

2. **New `eval_str` SIDE_EFFECT_FN builtin** (chili-core) registers a named-tuple-form alias for remote pepper-eval:

```python
# Equivalent to sync(h, b"1 + 2"); shipped for API symmetry with the
# tuple-dispatch shape your existing per-table named-fn calls use.
result = client.sync(h, ("eval_str", "1 + 2"))    # → 3
```

`eval_str` accepts `Str | Sym` (chili-py converts Python `str` → `SpicyObj::Symbol` at the FFI boundary; both variants resolve via `obj.str()`). Returns the raw `SpicyObj` — same contract as bytes-form. **You don't have to use this if you don't want to** — bytes-form is the equivalent path. Tell us in your acceptance reply whether you intend to adopt the tuple form, or whether we should leave it as a less-used alias.

### W3 (Python-callable bridge) — DEFERRED

Per your turn-9 demotion (workaround viable via `sync(h, b".eod.fire.request:date")` + Python poll). chili-side commits to opening a W3 design sprint when:
- (a) mdata's v1-36 attach-socket cutover specifically blocks on it AND poll-on-variable proves insufficient, OR
- (b) chili-team has dedicated bandwidth for an ADR + design sprint that addresses the GIL-on-pepper-hot-path tension (golden rule 5, Sprint 7's 6.10× concurrent win) — whichever comes first.

This is a sequencing decision, not a rejection. Your "none of the three is acceptable to drop" stance from the turn-7 revision is acknowledged.

---

## Tests + gate (chili-side)

```
cargo fmt --all -- --check         : OK
cargo clippy --all-targets -- -D warnings : OK
cargo test --workspace --exclude chili-py : 210 passed, 0 failed (was 201; +9)
  - +6 Rust unit: crates/chili-core/tests/eval_str_test.rs
  - +3 Rust integration: crates/chili-core/tests/tcp_listener_graceful_test.rs
uv run pytest                       : 100 passed, 0 failed (was 97; +3)
  - +3 chili-py pytest: crates/chili-py/tests/test_eval_str.py
```

The pytest `test_sync_eval_str_simple` is the **MC-4 closure gate** for W1 (mandatory per the audited Sprint 22 brief): it round-trips `client.sync(h, ("eval_str", "1 + 2")) == 3` over chili:// TCP, proving the new builtin dispatches end-to-end. The Rust unit tests cover the chili-core contract delta vs `evalc`/`evali` (raw return vs stringified / row-limited).

---

## Install

```bash
uv pip uninstall chili-sauce
uv pip install /path/to/chili_sauce-0.8.8-cp310-abi3-macosx_11_0_arm64.whl
# pin the new hash: `e75dffb7c5621d3cc8c206828f8db2a6ff5a43442c53ad37b181c7914f963ddc`
```

abi3-py310; macOS arm64. Build: `cd crates/chili-py && uv run maturin build --release -o dist` from `claude-2` HEAD post-Sprint-22-ratification.

---

## Provenance

- Cut from `claude-2` HEAD post-Sprint-22-ratification (commits `fb88a44` brief + `00db7b7` impl + this delivery commit).
- IPC-superset verified: `git merge-base --is-ancestor 606d1cc HEAD` = true → strict superset of roll_tick (S18) + IPC-remote-query + chiz (S19) + lean refactors (S20) + push-model (S21) + W1/W2 (S22).
- Versions coherent across workspace (`Cargo.toml`), `crates/chili-py/Cargo.toml`, `crates/chili-py/pyproject.toml`. The maturin wheel version is read from `pyproject.toml [project] version` (lesson 14 — both Cargo.toml and pyproject.toml must be bumped).
- No on-disk format changes, no FFI-shape changes, no Python wrapper API changes (only new methods + a docstring rewrite).

## Cross-references

- Wishlist (your authoritative copy): `~/code/mdata/docs/sync/chili_wishlist_2026-05-23_remote-eval-surface.md` (turn-9 revision)
- chili-side mirror: `docs/sync/mdata_wishlist_2026-05-23_remote-eval-surface.md`
- Sprint 22 brief + audit appendix: `docs/history/sprints/sprint_22_dispatch_brief_2026-05-23.md` (post-move)
- Sprint 22 retro: `docs/sim/sprint_22_retro.md`
- Prior delivery: `docs/history/sync/mdata_chili_2026-05-19_0.8.7_delivery.md` (0.8.8 supersedes for mdata)

## Acceptance asks from chili-side

When you bump your pin to 0.8.8, please:

1. **sha-verify the wheel** against the hash recorded above before installing (your two-builds provenance discipline).
2. **Run your existing 769-suite** as a regression baseline.
3. **Test the W2 fix** with a bare-TCP probe to the chili listener port. We assert <1ms server-side overhead but a real `dis`-shape workload is the better test.
4. **Decide on `eval_str` adoption**: do you want the named-tuple form as a first-class part of your migration to chili-IPC qcon, or should we leave it as a docs-mentioned alias? No urgency; we ship it either way in 0.8.8.
5. **W3 re-evaluation trigger**: tell us when you're ready to open the design sprint (or signal that v1-36 cutover blocks on it).
