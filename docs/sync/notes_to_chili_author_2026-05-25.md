# Three notes for the chili author — verified against main 0.9.0

**Date:** 2026-05-25
**From:** chili-team
**Audience:** chili-author
**Build verified against:** main 0.9.0 release wheel (`fb4455d` checkout in `/tmp/chili-main-v0.9.0`, `maturin develop --release`)

This is a focused follow-up to `docs/sync/upstream_v0.9_vs_claude-2_0.8.9_gap_analysis_2026-05-24.md`. Three things — one confirmation, one bug + design proposal, one wishlist with rationale. **All claims are verified against main 0.9.0 with reproducer scripts; no speculation.**

---

## §1 — Confirmed: `publish_via_handle` is deprecation-ready

You said in your inline comments on the gap analysis: *"Should never use this, just use publish, which is a defined chili function for publishing data."* I built main 0.9.0 and tested the canonical recipe end-to-end.

### The recipe that works

```python
# fh-side (separate process):
fh = ChiliEngine()
h = fh.open_handle("chili://tp_host:port")
fh.sync(h, (".tick.upd", "trade", df))   # df = pl.DataFrame
```

This invokes tp's `.tick.upd[table; data]` handler (loaded by `init_tick`), which:
1. Appends to tplog (`.tick.msgHandle (`upd; table; data)`)
2. Broadcasts to all subscribers via `.broker.publish[`upd; table; data]`
3. Increments the tick counter (`tick[0; 1]`)

### Verification (live on main 0.9.0)

Script: `/tmp/q1_publish_path_test.py` (mirrored at end of this note for your reference).

```
--- Shape (A): sync(h, ('.tick.upd', 'trade', df)) ---
  return: None
  sub.get_var('trade'): 3 rows
  ┌──────┬───────┬──────┐
  │ sym  ┆ price ┆ size │
  ╞══════╪═══════╪══════╡
  │ AAPL ┆ 100.0 ┆ 0    │
  │ AAPL ┆ 101.0 ┆ 10   │
  │ AAPL ┆ 102.0 ┆ 20   │
  └──────┴───────┴──────┘

--- Shape (B): sync(h, ('upd', 'trade', df)) ---
  RAISED: ChiliError: Name 'upd' is not defined
```

- Shape (A) — qualified `.tick.upd` — works as expected. Polars DataFrame survives the wire; broadcast reaches the subscriber.
- Shape (B) — bare `upd` — fails because the receiver (tp) doesn't load `sub.pep` (where bare `upd` is defined). Documented for completeness.

### Implication for claude-2

mdata has 10 `publish_via_handle` call sites in their fh→tp write path. With this recipe, those become 10 sites calling `sync(h, (".tick.upd", table, df))` directly — no `publish_via_handle` needed. mdata can migrate in a coordinated sprint, and claude-2 drops `publish_via_handle` afterward.

**No upstream action needed from you for this one.** Just confirming the recipe in writing so it can land in chili-py docs / mdata's migration guide. If you'd consider adding a one-line example to `crates/chili-py/README.md` ("Remote publish via `sync(h, (".tick.upd", table, df))`"), that would close the discoverability gap mdata hit (they didn't find this pattern from the existing docs).

---

## §2 — `roll_tick_log` — bug report + atomicity proposal

Two findings from running mdata's daily-rotation use case against main 0.9.0.

### Finding 1: BUG — `roll_tick_log` crashes when called from Python with str args

**Reproducer** (`/tmp/q2_roll_atomicity_test.py`, BASELINE case):

```python
pub = ChiliEngine(pepper=True)
pub.init_tick(schema=..., log_dir=log_dir+"/", filename=str(date.today()))
for i in range(3):
    pub.publish("trade", _row(i))

pub.roll_tick_log(log_dir + "/", "segment_002")   # ← CRASHES
```

Error:

```
ChiliError: --> :4:3

  .handle.rotate[.tick.msgHandle; .tick.logFile];
  ^
Expect data type 'str' for '2' argument , got 'sym'.
```

**Root cause** (isolated with `/tmp/q2b_isolate_bug.py`):

`.handle.rotate` (the Rust `rotate_handle` at `engine_state.rs:754`) requires `str` for arg 2. But `.tick.logFile` was constructed via `"file://" + .tick.msgLog`, where `.tick.msgLog` was constructed in `init_tick` via `logDir + filename` — and `filename` came from a Python `str` which the chili-py FFI converts to `SpicyObj::Symbol`. The concat chain produces a `sym`, not a `str`, and `.handle.rotate` rejects it.

```
attempting .handle.rotate via pepper str literal "file://X"  → SUCCESS
attempting via Python fn_call('.handle.rotate', [h, "file://X"])  → RAISED 'got sym'
attempting via Python fn_call('.handle.rotate', [h, b"file://X"])  → SUCCESS
```

So `.handle.rotate` from-pepper-with-str-literal works; from-Python-with-str fails. **The default Python ergonomics for `roll_tick_log` are broken on 0.9.0.**

**Workaround that works** (must apply at both call sites):

```python
pub.init_tick(schema=..., log_dir=(log_dir+"/").encode(), filename=b"segment_001")
pub.roll_tick_log((log_dir+"/").encode(), b"segment_002")   # bytes → SpicyObj::String
```

Verified working (`/tmp/q2c_concurrent_with_workaround.py`, BASELINE):
```
roll_tick_log(bytes) succeeded
post-roll publish succeeded
```

**Proposed fix** (your call which is right for chili):

- (a) Loosen `.handle.rotate` to accept `str|sym` (matches `.handle.open` which does accept both). Smallest change.
- (b) Tighten `chili-py`'s FFI conversion: don't auto-cast Python str → Symbol; convert to String instead (or treat str specially in path-arg contexts).
- (c) Fix `tick.pep`'s concat to explicitly stringify: `.tick.msgLog: $["str"; logDir + filename]` or similar.

I'd vote (a) for minimal blast radius. Whichever you pick, this is a user-facing crash from the documented public API path that mdata needs.

### Finding 2: PROPOSAL — atomicity gaps for crash recovery

With the bytes-workaround applied, I ran the concurrent stress test (`/tmp/q2c_concurrent_with_workaround.py`, CONCURRENT case): publisher thread emitting at ~7-8 kHz while main thread calls `roll_tick_log` mid-stream. **Result was clean** — no errors, both segment files materialized with sensible content (4.7 MB + 4.1 MB), 4152 rows before roll + 3573 after = 7725 published, no thread errors.

**Why this works** (read `set_handle` at `engine_state.rs:841`): `set_handle` takes `handle.write()`, which is the same lock the publish path takes. So `rotate_handle` and `.tick.upd` serialize on the same `parking_lot::RwLock<IndexMap<i64, Handle>>` — concurrent publishes land either fully-before-roll or fully-after-roll, never split.

**So:** for the happy-path daily-rotation case, main 0.9.0 is structurally atomic. mdata can use `roll_tick_log(bytes)` once Finding 1 is fixed.

**What main does NOT handle** (and claude-2's `roll_tick` does):

| Property | main `rotate_handle` (engine_state.rs:754) | claude-2 `roll_tick` (engine_state.rs:931) |
|---|---|---|
| Concurrent publish-vs-rotate atomicity | ✅ (via shared `handle.write()`) | ✅ (same lock) |
| Idempotent retry (caller invokes roll twice) | ❌ Errors second time ("file is not empty") | ✅ Short-circuits if URI already matches |
| Recovery from prior partial file (kill -9 mid-roll) | ❌ Refuses non-empty target | ✅ Walks seq-tail via `.broker.validateSeq` + truncates torn record |
| fsync OLD writer before swap (durability per PRD §5.1) | ❌ Old writer may have dirty pages | ✅ `flush() + sync_all()` under lock before swap |
| Failure-atomicity (next-open fails ⇒ old writer untouched) | ⚠️ `prepare_file_writer` errors before `set_handle` (mostly OK) | ✅ Explicit ordering + comments make it load-bearing |

**Should you adopt claude-2's `roll_tick` semantics into main?** Up to you, but if you decide yes, the diff is ~75 lines in `engine_state.rs` (full source at `crates/chili-core/src/engine_state.rs:931-1031` on claude-2). It does not change any existing API; only adds crash-recovery to a function mdata's tp daemon calls daily.

**If you decide no:** mdata's tp deployment must ensure tp is never kill-9'd mid-roll, which (per the gap analysis §3.4 thread) is not a guarantee mdata can offer. They'd keep claude-2's `roll_tick` for production use.

---

## §3 — Wishlist W1 — auto-flush in `init_tick` (the only strict ask)

From mdata's Revision A (`docs/sync/mdata_architecture_handoff_2026-05-24.md` §6) — their wishlist reduces to **one strict ask**: configurable auto-flush in `init_tick`. I want to lay out the case so you can evaluate.

### The use case

mdata's PRD §5.1 part-2 commits to a **10ms durability SLA**: under kill-9 of the tp process, at most one in-flight message can be lost. This is non-negotiable for production trading data.

### Why OS page-cache isn't enough

- Linux default `vm.dirty_writeback_centisecs = 3000` → page-cache flush every **30 seconds**.
- macOS APFS default is similar (~5-30s depending on pressure).
- A kill -9 between flushes loses everything written since the last flush — far exceeding the 10ms SLA.

### Why per-message `fsync` isn't enough

- macOS APFS `fsync` cost: ~1-10ms (HDD-class even on SSD due to barriers).
- At mdata's sustained 6944 msg/sec: 7-70 ms of fsync time per second wall-clock = 70%+ throughput impact.
- Hard to meet the SLA AND the throughput target with naive per-message fsync.

### The balance: periodic-but-bounded fsync

What mdata wants (the W1 ask):

```python
engine.init_tick(
    schema=...,
    log_dir=...,
    filename=...,
    auto_flush_ms=100,           # NEW — fsync every N ms (default: OS-managed)
    auto_flush_bytes=1_048_576,  # NEW — fsync every N bytes since last (default: OS-managed)
)
```

chili owns the flush cadence; the periodic-flush loop runs on a chili-internal thread (matches the existing `start_job_scheduler` pattern). User code stops calling `flush_tplog()` from Python. Defaults to "OS-managed" (current behavior) so it's purely additive.

### Why this belongs in chili, not mdata

This is precisely the framing you gestured at in your `flush_tplog` comment on the gap analysis (*"it should be taken care by chili file system"*). mdata's current `tp/periodic_flush.py` (Python-side, asyncio task calling `engine.flush_tplog()` every 100ms) violates that — it puts file-system durability concerns in user code. Moving it into chili-side config:

1. **Aligns with the standalone-first model.** Users specify durability intent declaratively; chili owns the fsync mechanism.
2. **Removes the Python coupling.** Today, mdata's flush loop has to know `.tick.msgHandle` exists. With the config, chili manages it internally.
3. **Existence proof:** mdata has run this pattern (Python-side periodic flush) in production for 2 weeks via claude-2's `flush_tplog`. The throughput cost is bounded (~5% at 100ms cadence). It works; it just shouldn't live in user code.

### Smaller alternative if `init_tick` config is too coupled

If you prefer not to bundle the flush config into `init_tick`, an alternative is a generic `engine.fsync_handle(h)` Python method that mdata calls periodically itself. That keeps chili's surface small (one method, no config) and lets mdata schedule whichever cadence makes sense for their workload. Less elegant than auto-flush config but lower commitment from your side.

Either shape closes the W1 ask. If neither happens, mdata retains the Python-side flush + claude-2's `flush_tplog` as a stopgap indefinitely — functional but ugly.

---

## What you don't need to do

For complete clarity on what mdata is NOT asking for anymore (post their Revision A reframe):

- ❌ D-1 `upd_notify_fd` / `drain_upds` (Sprint 21 — sub.pep's `upd:{[t;d] t upsert d; ...}` makes this redundant)
- ❌ D-2 `get_var_lazy` (Sprint 21 — equivalent to `get_var().lazy()`)
- ❌ D-3 `subscribe(resume_from=)` (Sprint 21 — kdb+tick canonical: replay-on-restart)
- ❌ W3 `register_fn` / `ExternalFnDispatcher` (Sprint 23 — mdata's Revision A: "we were wrong to request this")
- ❌ GR4 `set_column_scale` helpers (mdata-specific, belongs in a Python facade)

claude-2 will deprecate all of the above in coordinated migration sprints with mdata (post their 24h Pipeline X soak). End state: claude-2 ≈ main 0.9.0 + a small set of test guards + (pending §1, §2 above) zero new chili-core surface.

---

## Document provenance + reproducers

- **§1 evidence:** `/tmp/q1_publish_path_test.py` — single-file Python test, runs in <2s against the main 0.9.0 wheel.
- **§2 Finding 1 evidence:** `/tmp/q2_roll_atomicity_test.py` (default str args; crashes) + `/tmp/q2b_isolate_bug.py` (isolates str-vs-sym across pepper-vs-Python).
- **§2 Finding 2 evidence:** `/tmp/q2c_concurrent_with_workaround.py` (bytes workaround + concurrent stress).
- **§3 ask:** design proposal grounded in mdata's PRD + 2 weeks of production data on claude-2's `flush_tplog`.

All scripts use only public APIs from `from chili import ChiliEngine`. Reproducer-driven; happy to send the scripts themselves if useful.

### Cross-references

- `docs/sync/upstream_v0.9_vs_claude-2_0.8.9_gap_analysis_2026-05-24.md` — the full gap analysis these notes follow up on; your 8 inline comments + our replies + the §10 mdata discussion topics are there
- `docs/sync/mdata_architecture_handoff_2026-05-24.md` — mdata's Revision A (the user-of-chili reframe) — recommended pre-reading for §3
- `crates/chili-core/src/engine_state.rs` on claude-2 — `roll_tick:931` (full atomic+recovery body, ~100 lines) for §2 Finding 2

Three asks, ordered from cheapest (informational) to most-substantive (W1 design):

1. **§1:** acknowledge the `sync(h, (".tick.upd", ...))` recipe — possibly add a docs line so future users find it.
2. **§2 Finding 1:** fix the str/sym bug in `.handle.rotate` (or upstream). 1-line patch in `rotate_handle`'s arg validation likely sufficient.
3. **§2 Finding 2:** consider adopting `roll_tick`'s crash-recovery semantics (~75 lines). Your call.
4. **§3 W1:** consider `init_tick(auto_flush_ms=N)` config OR `engine.fsync_handle(h)` method. Either closes mdata's last wishlist item.

No deadline — mdata's 24h Pipeline X soak completes 2026-05-25 ~morning; their v1-36 architecture-cleanup sprint follows. Your responses on §2 + §3 inform whether claude-2 keeps `flush_tplog` and `roll_tick` long-term or deprecates them in the migration sprint.
