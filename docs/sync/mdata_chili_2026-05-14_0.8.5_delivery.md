# mdata ← chili 0.8.5 wheel delivery

**Date:** 2026-05-14
**From:** chili-team
**To:** mdata project
**Wheel:** `dist/chili_sauce-0.8.5-cp310-abi3-macosx_11_0_arm64.whl`
**sha256:** `62e809129827d9f2514e5f5cbb506161f1281f1e7a4e3abd1a9e56f67efb5bf2`
**Replaces:** 0.8.4 (sha256 `6e724eef6b526372d82b14fb2c7f6ae0eafb482e2067005f9ba79f3839451f87`)

---

## TL;DR

Sprint 17 closes the two remaining items from the 2026-05-13 wishlist
(both P1) as chili-sauce 0.8.5. **mdata wishlist v1 is now complete on
the chili side.**

- **P1** `engine.publish_via_handle(h, table, df)` — thin one-shot publish
  primitive over chili-IPC (Q3 lock-in Option B: chili owns marshalling;
  mdata owns RemoteTpClient connection-manager class).
- **P1** Subscriber-side `eod` dispatch fix — `signal_eod` rewritten to
  use Async fire-and-forget broadcast (matching `EngineState::publish`)
  instead of `sync()` (which was rejecting Publishing conn_type and
  silently disconnecting subscribers). Per audit C1: H6, not in original
  hypothesis space.

mdata committed: flip the remaining wishlist-tied xfails to strict-pass
within 1 sprint of receipt + ship the `RemoteTpClient` class on top of
`publish_via_handle`.

---

## Install

Replace your pinned wheel hash:

```bash
# uninstall the prior version
uv pip uninstall chili-sauce

# install the new wheel (pin the sha256 to detect re-builds)
uv pip install --no-deps \
  /Users/oakadmin/code/chili/dist/chili_sauce-0.8.5-cp310-abi3-macosx_11_0_arm64.whl
```

Update your `pyproject.toml` lockfile pin to the new sha256 above.
Cross-verify by running `shasum -a 256` on the wheel after install.

---

## API additions (full surface)

### 1. `engine.publish_via_handle(h: int, table: str, df: pl.DataFrame) -> None` (P1 publish_remote)

Thin one-shot publish primitive. Sends `(`upd; table; df)` over an open
chili-IPC handle (must be `Outgoing`). The remote tp's subscriber dispatch
applies the message as `upd[table, df]` per pepper semantics.

```python
import chili
import polars as pl

mdata_engine = chili.ChiliEngine(pepper=True)

# Caller manages connection lifecycle: open, cache, close.
h = mdata_engine.fn_call(".handle.open", ["chili://tp-host:40001"])

df = pl.DataFrame({"sym": ["AAPL"], "price": [150.0], "size": [100]})
mdata_engine.publish_via_handle(h, "trades", df)

# Re-use the handle for subsequent publishes — no per-call connect/disconnect.
mdata_engine.publish_via_handle(h, "trades", df2)
mdata_engine.publish_via_handle(h, "quotes", df3)

# Caller closes the handle when done.
mdata_engine.fn_call(".handle.close", [h])
```

**Blocking semantics** (per audit C4): `sync()` is a blocking send-and-receive
on chili IPC. `publish_via_handle` does NOT return until the remote tp
has answered. If the remote is slow or unreachable, the handle map's
write lock is held across the network read (inherited from `sync()`;
pre-Sprint-17 concern, not this method's responsibility to fix). Callers
needing client-side cancellation / timeout must implement it above this
layer — mdata's `RemoteTpClient` is the canonical place.

**GIL release**: yes, around the `sync()` call (per Sprint 14 P3.2b
convention).

**Errors**:
- `RuntimeError` (chili-side `MismatchedTypeErr` underlying) if `df` is
  not a polars DataFrame.
- `RuntimeError` (chili-side `InvalidHandleErr` underlying) if `h` has
  no live connection.
- `RuntimeError` if the handle is not `ConnType::Outgoing` (e.g.,
  already promoted to Subscribing via `.handle.subscribing`).

### 2. Subscriber-side `eod` dispatch — bug fix (P1 eod-dispatch)

No new API surface. The fix is in `EngineState::signal_eod` (publisher
side):

**Before (broken):** signal_eod called `self.sync(&h, args)` for each
Publishing handle. `sync()`'s conn_type match had no `Publishing` arm,
so every call returned `EvalErr("cannot sync for Publishing handle")`
and `signal_eod` disconnected the subscriber. EOD broadcast was
completely suppressed.

**After (fixed):** signal_eod uses the same Async fire-and-forget pattern
as `EngineState::publish` (the broker upd path): serialize message via
`serde9::serialize`, iterate Publishing handles, write each via
`utils::write_chili_ipc_msg(rw, &bytes, MessageType::Async)`. The
subscriber's `handle_chili_conn` loop reads the message, calls
`state.eval` → `eval_op` → looks up `eod` symbol head → invokes
`eod[date]`.

**mdata-side test that flips to strict-pass:**
- `tests/rdb/test_rdb_subscriber.py::test_subscriber_eod_shim_triggered_by_publisher_eod`

**chili-side acceptance tests (already passing on the 0.8.5 wheel):**
- `crates/chili-py/tests/test_subscriber_eod_dispatch.py::test_subscriber_eod_shim_triggered_by_publisher_eod`
- `crates/chili-py/tests/test_subscriber_eod_dispatch.py::test_multi_message_subscriber_observes_upd_then_eod` (O1 audit — multi-message regression)

---

## Behavior change to confirm in mdata's regression suite

1. **Any code path that calls `engine.eod(date)` on a publisher with
   active subscribers**: previously, this silently disconnected all
   subscribers. Now, subscribers receive the `(`eod; date)` message and
   their pepper-level `eod` handler fires. **This is a behavioral change
   that mdata's existing subscriber-side code may depend on (either way).**
2. **Any subscriber that defined an `eod` function before this fix**
   never had it invoked. Confirm mdata's subscribers don't have stale
   `eod` shims that should now fire (or are now firing unexpectedly).

If mdata previously worked around this bug with Python-side EOD timer
polling (`tp.config.eod_time` lookup, mentioned in the wishlist), that
work-around can now be retired — chili-side dispatch is authoritative.

---

## Cross-references

- Sprint 17 retro: `docs/sim/sprint_17_retro.md`
- Sprint 17 dispatch brief (audited): `docs/history/sprints/sprint_17_dispatch_brief_2026-05-14.md`
- mdata wishlist source: `~/code/mdata/docs/sync/chili_wishlist_2026-05-13.md`
- mdata Q3 + Q4 lock-in reply: `~/code/mdata/docs/sync/chili_wishlist_2026-05-13_mdata_reply.md`
- ADR 0001 (Pub/sub canonical model — Sprint 17 follow-up note added): `docs/decisions/0001-pub-sub-canonical-model.md`
- Prior delivery (0.8.4 — P0+P2+P3): `docs/sync/mdata_chili_2026-05-13_0.8.4_delivery.md`
- Part B commit: `0062c8e`
- Part A commit: `7b508bd`

---

## Open / pending on chili side

- Sprint 18+ scope unscoped. The wishlist v1 is closed. mdata may submit
  v2 wishlist when ready.
- User-driven P0 backlog (still open): GitHub-host the polars fork —
  workspace `Cargo.toml` still has `path = "/tmp/polars-py-1.39.3"`;
  fresh clones break at `cargo build`. No timeline.
- `docs/sync/ideas.md` carries 7+ entries, all gated on external triggers
  (none ratified for Sprint 18).

---

## Acknowledgements

Wishlist v1 closing was driven by mdata's:
- Concrete failing-test artifacts (`test_subscriber_eod_shim_triggered_by_publisher_eod`).
- Code-cited acceptance criteria.
- 4-question clarification cycle that locked Sprint 17 scope before
  Sprint 17 kickoff (Q3 reversed publish-remote API shape; saved ~10pp
  on Sprint 17 vs the original wishlist preference).

The 3.7× kdb+/TorQ baseline framing remains the most compelling
production-signal chili has received. mdata's continued audit-driven
sprint cadence on top has accelerated the chili-side rate of bug
surfacing — the eod dispatch bug was a Latent for an unknown period
before mdata's acceptance test pinpointed it.
