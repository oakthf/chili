# Upstream proposal — handle-map write-lock held across IPC writes

**Date:** 2026-05-14
**From:** chili-claude-2 working fork (oak@treehouse.finance)
**To:** chili author (upstream `purple-chili/chili`)
**Re:** Concurrency / scalability concern in `EngineState`'s pub/sub + IPC paths
**Status:** Discussion / opinion request — not a PR proposal

---

## TL;DR

In `EngineState`, every path that writes bytes to a connection (`sync`,
`EngineState::publish`, the pre-Sprint-17 `signal_eod`) holds the global
`self.handle.write()` lock across the TCP-write syscall. Under
loopback / single-subscriber loads this is invisible. Under multi-
subscriber or WAN topologies, **one slow / unreachable subscriber freezes
every other handle operation in the engine** until that subscriber's TCP
write resolves (which can be unbounded). I'd like your read on this and
on a proposed fix shape before scoping work.

I also discovered + fixed a related bug on my fork (`signal_eod` was
routing through `sync()` which has no `Publishing` conn_type arm,
silently disconnecting subscribers on every EOD). I'm flagging that
separately below since you may want to pick it up.

---

## Context (for upstream perspective)

The chili-claude-2 fork is a local working fork against `mdata`
(a market-data warehouse, ~11K US equities, kdb+/TorQ-style tickerplant
+ rdb + wdb topology, all in a single process). mdata uses chili-sauce
0.8.x via Python bindings + chili's pub/sub primitives. They reported
3.7× their kdb+/TorQ baseline on chili 0.8.3, then surfaced a v1
wishlist asking for `flush_tplog`, `publish_remote`, subscriber-side
`eod` dispatch, etc. — Sprint 16/17 closed v1.

Sprint 17 surfaced the latent concurrency concern documented here.
**Nothing in mdata's production today triggers it**, so this is "would
you accept a fix?" not "we have a bug report" — but it'll bite the
first many-subscriber or WAN deployment, and I'd rather discuss the
design before scoping.

---

## The latent concurrency debt — three code sites, one shape

### Pattern in source

The shape (taking `EngineState::publish` as the canonical example):

```rust
// crates/chili-core/src/engine_state.rs:1192-1241 (line numbers per
// claude-2 HEAD; upstream main differs only by a few lines)
pub fn publish(&self, table: &str, bytes: &[Vec<u8>]) -> SpicyResult<()> {
    let mut topic_map = self.topic_map.write();
    // ...
    let mut handle = self.handle.write();   // ← write lock on the ENTIRE handle map
    for subscriber in subscribers {
        if let Some(v) = handle.get_mut(&subscriber) {
            // ...
            match &mut v.rw {
                Some(rw) => match crate::write_chili_ipc_msg(rw, bytes, MessageType::Async) {
                    //                                      ^^ TCP write — blocks on backpressure
                    Ok(_) => (),
                    Err(e) => { /* ... */ }
                },
                // ...
            }
        }
    }
    Ok(())
}
```

The write lock on the handle map is held across the inner
`write_chili_ipc_msg(rw, bytes, ...)` call, which writes bytes to the
subscriber's TCP socket. **If any subscriber's TCP receive buffer is
full or unreachable, the write blocks while holding the map's write
lock.** During that block:

- No other broadcasts can proceed (next iteration of the same for-loop
  also blocks).
- No `sync()`, `flush_handle()`, `open_handle()`, `disconnect_handle()`,
  `list_handle()`, `handle_subscriber()`, `handle_publisher()` can run
  on any other thread — they all need the same lock.
- The engine is effectively frozen on handle operations for the
  duration of the stuck TCP write.

### Three sites with this shape

| Path | File:line | Lock | Notes |
|---|---|---|---|
| **`EngineState::publish`** (broker `upd` path) | `engine_state.rs:1192-1241` | `self.handle.write()` for the whole loop | Async msg type; fire-and-forget at the protocol level, but **not at the lock level**. |
| **`EngineState::sync`** | `engine_state.rs:984-1132` | `self.handle.write()` for the whole call | Holds lock across BOTH the request write AND the response read for Sync messages. Worst-case latency = remote tp's eval time. |
| **`EngineState::signal_eod`** (pre-Sprint-17 — see §3) | `engine_state.rs:1230` (now fixed on claude-2) | Was routing through `sync()` which holds map lock | Sprint 17 rewrote this; same lock pattern as `publish` post-fix. |

Other handle-map-touching methods (`flush_handle`, `disconnect_handle`,
`handle_subscriber`, `handle_publisher`, `start_tcp_listener`,
`list_handle`, `open_handle`) all also take `self.handle.write()` but
hold it briefly — they're not in the same risk class.

### Concrete failure scenario

Two subscribers, one healthy and one with a full TCP receive buffer
(e.g., subscriber's Python main thread is mid-eval on a long query and
hasn't consumed from its socket yet). Publisher calls
`engine.publish("trades", df)`:

1. Acquire `self.handle.write()`.
2. Iterate `subscribers` from `topic_map`.
3. Write to subscriber A's socket — returns immediately (healthy).
4. Write to subscriber B's socket — **blocks** waiting for B's
   receive buffer to drain.
5. Step 4 holds the lock. Any other publish, sync, flush, list, etc.
   on any other thread is blocked until B's buffer drains.
6. If B is unreachable (network partition, crashed but not
   TCP-RST'd yet), step 4 may block for minutes — entire engine frozen.

Under chili's typical load (single-process loopback) this never
triggers. WAN deployment + N subscribers, it's a guaranteed problem
sooner or later.

---

## Fix options I've evaluated

I'm interested in your read on whether you'd accept a fix and which
direction you'd prefer. Effort estimates are in pp (5h-window
percentage points, my unit — roughly 1pp ≈ 1 hr of focused work).

### Option A — per-handle `Arc<Mutex>` on `rw` (structural fix; recommended)

Change the `Handle` struct:

```rust
// Before
pub struct Handle {
    pub rw: Option<Box<dyn ReadWrite>>,
    // ...
}

// After
pub struct Handle {
    pub rw: Option<Arc<Mutex<Box<dyn ReadWrite>>>>,
    // ...
}
```

Call sites that need to write:

```rust
// Acquire read lock to find the handle + clone the Arc<Mutex<rw>>
let rw_arc = {
    let handles = self.handle.read();
    handles.get(h).and_then(|h| h.rw.clone())
};
// Release map lock. Now lock JUST this handle's rw.
if let Some(rw_arc) = rw_arc {
    let mut rw = rw_arc.lock();
    write_chili_ipc_msg(&mut **rw, &bytes, MessageType::Async)?;
}
```

**Pros:**
- Surgical — fixes all three sites at once (publish, sync, signal_eod).
- Map operations (find handle by id, insert, remove, list) take only
  brief read or write locks — no longer blocked by TCP writes.
- Concurrent writes to DIFFERENT handles no longer contend.
- Reuses well-understood Rust concurrency primitives.

**Cons:**
- ~30-50 call sites touch `handle.rw` directly. Mechanical refactor
  but tedious.
- New lock-ordering surface (map lock → per-handle lock). Easy to
  reason about (always map first, then handle) but worth a
  code-review pass.
- Slight overhead per write (one extra atomic op for the Arc clone).

**Estimate:** ~5-8pp on my fork. Probably similar upstream depending
on whether you've added handle-touching sites since.

### Option B — take/put pattern on broadcast paths only (cheap; not recommended)

Under the map write lock, `.take()` the `rw: Option<...>` out of each
broadcast target (sets to `None`, returns `Some(box)`). Release the
map lock. Iterate and write. Re-acquire the map lock and put the rw
back via `replace(Some(box))`.

**Pros:**
- ~2-3pp. No struct changes.
- Targeted fix to broadcast paths.

**Cons:**
- **Racy semantics during the take/write/put window**: any concurrent
  code that reads `handle.get(h).rw` sees `None` and errors as if the
  handle is disconnected. This is observably wrong if the same
  handle is used for an unrelated `sync()` on another thread mid-broadcast.
- Doesn't fix `sync()` which has the same pattern.

I evaluated this and decided not to ship it. Mentioned for completeness.

### Option C — per-write timeout on the TCP write

Wrap each `write_chili_ipc_msg` call with a timeout (e.g. 100ms). If
exceeded, mark the handle disconnected and skip. Doesn't fix the
lock-granularity issue but bounds the worst-case lock-hold duration.

**Pros:**
- Cheaper than A (~3-5pp).
- Bounds the symptom (max engine freeze = 100ms × N subscribers).

**Cons:**
- `dyn ReadWrite` is sync-blocking; timeouts require either
  `spawn_blocking + cancellation` (async/sync mixing) or a custom
  Trait-level timeout primitive. Non-trivial.
- Treats symptom not cause. A slow subscriber is now silently dropped
  rather than blocked — which may itself be wrong behavior (data
  loss on transient backpressure).

### Option D — channel + writer thread per Publishing handle

Each Publishing handle gets a dedicated writer thread + bounded
channel. Broadcast = enqueue. Slow subscriber's channel fills →
drop / queue / disconnect based on policy.

**Pros:**
- Best decoupling.
- Natural place to add per-subscriber backpressure policy.

**Cons:**
- Substantial architectural change (~8-12pp).
- N threads per N subscribers. Lifecycle management for clean
  disconnect.
- Probably overkill for chili's current scale envelope.

---

## My recommendation

**Option A**, scoped as one sprint (~7pp midpoint).

The Arc<Mutex<rw>> change is structurally right, well-understood, and
unlocks `sync()` improvements as a side benefit. The other options
either don't actually fix the bug (B is racy, C treats symptom) or are
disproportionate to chili's current scale envelope (D).

That said, **if you'd prefer to leave the current design in place**
(the lock-hold may have been an intentional simplification for the
single-process-loopback use case chili was designed for), I can
document the constraint in CLAUDE.md and ship a "don't deploy
chili pub/sub across WAN" note instead. **Your call.**

---

## Related — Sprint 17 fix to `signal_eod` (upstream-applicable)

While localizing a separate bug (mdata's `eod` dispatch test was
failing), I found that `signal_eod` (`engine_state.rs:1230` in
upstream) calls `self.sync(&h, args)` for each Publishing handle. But
`sync()`'s `match` on `conn_type` (engine_state.rs:984-1132) has NO
`Publishing` arm — every call falls through to the catch-all:

```rust
} else {
    Err(SpicyError::EvalErr(format!(
        "cannot sync for {:?} handle",
        conn_type
    )))
}
```

`signal_eod`'s `if let Err(e)` branch then disconnects the handle.
**Net result: every `signal_eod` call disconnects every subscriber
and silently drops the EOD broadcast.** Latent for at least as long as
the code has been in tree (commit `^8fd6ae3` hinmeru 2025-12-07).

mdata never noticed because they were working around it with a
Python-side EOD timer polling. Their wishlist asked for a
"subscriber-side eod dispatch path" — turned out the publisher side
was the issue.

**My fix** (claude-2 commit `7b508bd`): rewrite `signal_eod` to use
the same Async fire-and-forget broadcast pattern as
`EngineState::publish`. Serialize the args once, iterate Publishing
handles, write directly via `utils::write_chili_ipc_msg(rw, &bytes,
MessageType::Async)`. The subscriber's `handle_chili_conn` loop
dispatches via `state.eval → eval_op` and the `eod` symbol head
invokes the pepper-level `eod` function.

The diff is small (~30 LOC); happy to send a PR if you'd like. Note
the fix carries the SAME lock-hold concern as `publish` (it's the
same shape) — Option A would fix both at once if you take that route.

---

## Questions for you

1. **Is the current lock-hold pattern intentional?** I.e., did the
   single-process design assumption justify the simplification? If
   so, I'll add a doc note instead of fixing.
2. **If you'd accept Option A, would you prefer me to draft it on
   chili-claude-2 first (testable on mdata's workload) and PR
   afterwards, or draft directly against upstream `main`?**
3. **For the `signal_eod` fix** specifically: should it be a separate
   PR or bundled with Option A? My read: separate PR — it's a
   correctness fix (silent EOD drop) that's good to land independent
   of the lock-granularity work.
4. **Anything else in the area I should be aware of** before
   touching `Handle` / `EngineState`? E.g., known refactors in
   flight, or design intent for the future.

---

## References

- chili-claude-2 fork: local working tree at `/Users/oakadmin/code/chili`,
  branch `claude-2` (no remote — local working fork, manually synced from
  upstream main).
- Sprint 17 retro (lock-hold concern + signal_eod fix context):
  `docs/sim/sprint_17_retro.md`.
- Sprint 17 commits:
  - `0062c8e feat(sprint-17): Part B — engine.publish_via_handle (thin one-shot remote publish)`
  - `7b508bd fix(sprint-17): Part A — subscriber-side eod dispatch (signal_eod sync→async)`
  - `026bf4e docs(sprint-17): wrap — 0.8.5 wheel + retro + cadence row + delivery doc`
- mdata's reaction to this work: their v1 wishlist closing report at
  `~/code/mdata/docs/sync/chili_wishlist_2026-05-13_mdata_reply.md`.
- ADR 0001 (pub/sub canonical model, with Sprint 17 follow-up):
  `docs/decisions/0001-pub-sub-canonical-model.md`.

---

## Acknowledgements

chili's pub/sub framework is the load-bearing surface for mdata's
production deployment. The 3.7× kdb+/TorQ baseline they report is
the single most-compelling production signal I'm aware of for
chili — and it's almost entirely thanks to the design choices in
`engine_state.rs` (parse cache, partitioned DataFrame layout,
single-process model).

This proposal isn't a critique of those choices — it's a "the
deployment shape is starting to push the envelope of what the
single-process assumption supports, and here's one option" kind of
note. Your call on the right path forward.
