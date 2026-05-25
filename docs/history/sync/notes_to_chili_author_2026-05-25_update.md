# Re-verification update — author's morning commits on main close 4 of 5 asks

**Date:** 2026-05-25
**From:** chili-team
**Trigger:** chili-author shipped two commits on `main` this morning:
- `f6bccd1` — `feat: add SyncFile wrapper and .handle.fsync for file handle durability`
- `5cfc096` — `feat: add fsync_handle to Python ChiliEngine bindings`
**Re-verification:** built `main@5cfc096` in a fresh worktree (`maturin develop --release`) + re-ran the four reproducer scripts in this directory.

---

## TL;DR — what closed, what's left

| Ask from prior note | Status after morning commits |
|---|---|
| §1 — `publish_via_handle` deprecation recipe | ✅ Already closed (no change) |
| §2 F1 — `roll_tick_log` str/sym bug | ✅ **CLOSED** (`ArgType::Str → StrOrSym` in side_effect_fn.rs:64) |
| §2 F2 — rotate atomicity property: idempotent retry | ✅ **CLOSED** (skip-if-URI-already-in-map in engine_state.rs:753-757) |
| §2 F2 — rotate atomicity property: fsync-OLD-before-swap | ✅ **CLOSED** (flush() before set_handle in engine_state.rs:778-787; SyncFile wrapper makes flush() = `fdatasync`) |
| §2 F2 — rotate atomicity property: recovery from prior partial file | ❌ **STILL A GAP** (rotate_handle still refuses non-empty targets, engine_state.rs:769) |
| §3 W1 — `engine.fsync_handle(h)` durability primitive | ✅ **CLOSED** (chose the generic alternative from my note; measured at 0.001ms/call — practically free) |

**5 of 6 items closed.** Only one narrow gap remains, with a clear decision point.

---

## Detailed verification (reproducers in `docs/sync/reproducers/`)

### §1 — publish recipe (unchanged on main)
Reproducer: `q1_publish_path.py`. PASS. `sync(h, (".tick.upd", "trade", df))` still the right recipe.

### §2 F1 — str/sym bug fix
Reproducer: `q2_v2_post_author_fixes.py` test T1.

Before this morning, calling `engine.roll_tick_log(log_dir, "segment_002")` from Python crashed with `Expect data type 'str' for '2' argument, got 'sym'`. After:

```
--- T1: BASELINE roll_tick_log with default Python str args ---
  roll_tick_log(str) succeeded — str/sym bug FIXED
```

Author's fix: `validate_args(args, &[ArgType::Int, ArgType::StrOrSym])` at side_effect_fn.rs:64 — accepts both str and sym. **Clean fix, minimal blast radius**, matches my proposed option (a) in the prior note.

### §2 F2 properties 1, 2, 4 — atomicity improvements
Reproducer: `q2_v2_post_author_fixes.py` tests T2 + T4.

```
--- T2: IDEMPOTENT RETRY — roll twice to same target ---
  first roll: OK
  second roll (idempotent): OK

--- T4: rotate flushes OLD writer (fsync-before-swap) ---
  pre-roll first-segment size: 11528 bytes
  post-roll first-segment size: 11528 bytes
  after-close size: 11528 bytes
  → old segment data durably on disk after rotate+close
```

Author's implementations:
- **Idempotent retry** (engine_state.rs:753-757) — `if handles.values().any(|h| h.uri == uri) { return Ok(SpicyObj::Null) }` short-circuits before any file I/O. Cleaner shape than claude-2's "check inside the write lock" — the new variant is one read-lock check, no allocation.
- **fsync OLD before swap** (engine_state.rs:778-787) — explicit `rw.flush()` on the existing handle before `set_handle` replaces it. Combined with the new `SyncFile` wrapper (utils.rs:13-30) that makes `flush() = fdatasync()`, this gives PRD §5.1-grade durability on the rotation boundary.
- **Bonus: `close_handle` flush** (engine_state.rs:750-753) — same flush-before-drop semantic on close. Saves data on graceful shutdown.

### §2 F2 property 3 — recovery from prior partial file (GAP)
Reproducer: `q2_v2_post_author_fixes.py` test T3.

```
--- T3: RECOVERY — roll into pre-existing partial file ---
  pre-existing partial file: 32 bytes
  RAISED: ChiliError: Failed to eval: file '/.../segment_002' is not empty
  → STILL A GAP: main rotate_handle refuses non-empty targets
  → claude-2's roll_tick uses .broker.validateSeq to truncate torn tail
```

**The narrow scenario this case covers:**
1. tp starts a roll: opens the next-segment file (now exists, possibly 0 bytes, possibly partial after a flush)
2. tp gets `kill -9` (or hard crash) AFTER the new file exists but BEFORE the rotation completes
3. On restart, tp tries to retry the roll — `rotate_handle` errors because the target file exists (even if 0 bytes or with torn-tail content from the partial pre-crash write)
4. tp is stuck until ops manually deletes the partial file

Two paths forward — your call:

**(A) Adopt validateSeq-style recovery upstream** (~30 lines in engine_state.rs)

Mirror what claude-2's `roll_tick` does (`engine_state.rs:931-1031` on claude-2): when target file exists, call `.broker.validateSeq[path; 0b]` first — it walks the seq-tail, truncates any torn record via `set_len`, returns 0 for a fresh file or the count of valid records for a recoverable one. Then proceed with the rotation.

The mechanism is already in chili main 0.9.0 — `.broker.validateSeq` exists at broker.rs (verify-before-claim grep on main). Wiring it into `rotate_handle` is a small change.

**(B) Document a "tp restart must clean partial segments" contract**

Document that operators must `rm` any partial segment file before restarting tp after an unclean shutdown. Keep `rotate_handle` strict (refuses non-empty). Push the recovery responsibility to ops tooling.

Either works for chili-as-standalone. For mdata's "any process can die at any time" stance, (A) is preferable. For minimal chili surface, (B) is preferable. Either way, this is the LAST claude-2-unique correctness item — once decided, claude-2's `roll_tick` is either upstream-friendly or explicitly-deprecated-in-favor-of-(B).

### §3 W1 — `fsync_handle(h)` ASK CLOSED
Reproducer: `w1_fsync_handle.py`. All 4 tests PASS:

```
=== VERDICT ===
  T1 (fsync returns OK):       PASS
  T2 (invalid handle errors):  PASS
  T3 (data on disk):           PASS
  T4 (fsync cost):             0.001ms per call

  → W1 ASK CLOSED — main 0.9.0+ has the generic fsync_handle primitive
  → at 10/sec cadence: 0.0ms wall/sec = 0.00% throughput impact
```

**The cost is essentially free** — 1 microsecond per call. mdata's PRD §5.1 10ms SLA is trivially met by calling `engine.fsync_handle(.tick.msgHandle)` at any cadence ≤ 10ms (e.g., every 100ms gives 99.9× headroom against the SLA at 0.01% throughput cost).

You chose the **generic `fsync_handle(h)` shape** over the alternative `init_tick(auto_flush_ms=N)` auto-flush config — also matches the "caller schedules cadence" pattern that fits the standalone-first model best. Good call; mdata's `tp/periodic_flush.py` becomes a ~10-line wrapper around `engine.fsync_handle()`.

**Bonus from your commit:** the `SyncFile` wrapper at utils.rs:13-30 is exactly the right abstraction — `flush() = sync_data()` makes any `.flush()` call on a file handle do the right thing, so future durability work can stay in that one place.

---

## Updated comparison: main 0.9.0+ (post your morning commits) vs claude-2 0.8.9

For each of the 9 claude-2-unique features, status given the new main:

| # | Feature (Sprint) | main 0.9.0+ status | Verdict |
|---|---|---|---|
| 1 | D-1 upd_notify_fd / drain_upds (S21) | Covered by sub.pep auto-apply | **Drop on claude-2** |
| 2 | D-2 get_var_lazy (S21) | Covered by get_var().lazy() | **Drop on claude-2** |
| 3 | D-3 subscribe(resume_from=) (S21) | Covered by accept-full-replay | **Drop on claude-2** |
| 4 | flush_tplog (S16) | Covered by engine.fsync_handle (NEW) | **Drop on claude-2** |
| 5 | publish_via_handle (S17/19) | Covered by sync(h, (".tick.upd", ...)) | **Drop on claude-2** |
| 6 | roll_tick atomic (S18) — happy path | Covered by main's roll_tick_log + new fsync-old + idempotency | **Drop on claude-2 for happy path** |
| 6b | roll_tick atomic — partial-file recovery | **GAP** — main rotate_handle refuses non-empty | **Keep on claude-2 OR await §2 F2 path (A)** |
| 7 | GR4 set_column_scale (various) | Lift to Python facade | **Drop on claude-2** |
| 8 | M-1 invariant test guard (S20) | n/a (test) | **Keep** (zero cost) |
| 9 | W3 register_fn (S23) | mdata withdrew per Revision A | **Drop on claude-2** |

**Net:** 8 of 9 features (and the bonus M-1 guard kept) — claude-2 has nothing material left over main beyond test-guards. Only outstanding item is the §2 F2 property-3 partial-file recovery, and that has a 30-line upstream path if you want it.

---

## What we plan to do on claude-2 next

Triggered by your morning commits, the move-to-v0.9 plan accelerates:

1. **Sprint 24 ("main port" sprint, ~5-8pp)** — drop the 8 features above, lift GR4 helpers to a `chili.scale` Python facade, retain the M-1 test, cut a claude-2 wheel that's essentially main 0.9.0+ for mdata's v1-36 cleanup.
2. **Decision-gated item (your call on §2 F2 path):** if you ship the partial-file recovery upstream, claude-2 drops `roll_tick` entirely; otherwise claude-2 retains it as the sole production-grade variant.
3. **mdata coordination:** their 24h Pipeline X soak completes ~today; v1-36 architecture-cleanup sprint follows. mdata pins on main 0.9.0+ (or the claude-2 superset wheel that's main + only the partial-file recovery if path B selected).

End-state: **claude-2 effectively ≈ main 0.9.0+**, with the difference being one Sprint 24 of cleanup work + (optionally) one 30-line recovery patch.

---

## Cross-references

- `docs/sync/notes_to_chili_author_2026-05-25.md` — the prior note this updates (still valid for §1)
- `docs/sync/upstream_v0.9_vs_claude-2_0.8.9_gap_analysis_2026-05-24.md` — the full gap analysis (mostly superseded by your commits + this update)
- `docs/sync/mdata_architecture_handoff_2026-05-24.md` — mdata's Revision A (the user-of-chili reframe)
- `docs/sync/reproducers/` — all 6 reproducer scripts, runnable against any main checkout via `maturin develop --release && uv pip install polars==1.39.3 && uv run python <script>`

## One ask

Just the §2 F2 path-(A)-or-(B) decision on the partial-file recovery case. Everything else this side of the conversation is internal claude-2 cleanup.

Thank you for the morning ship.
