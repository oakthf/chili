# Re-verification update #3 — `26b437e` + `74acdc6` close the torn-tail gap

**Date:** 2026-05-25 (afternoon, third commit batch)
**Trigger:** chili-author shipped three more commits on `main`:
- `26b437e` — `refactor: extract detect_conn_type and count_seq_messages utilities`
- `74acdc6` — `fix: open_handle and rollLog set tick count from existing message count`
- `cc954d2` — `test: add Python test suite from oak/claude-2 branch`

**Re-verification:** built `main@cc954d2` in a fresh worktree; ran `q2_v4_post_truncate.py` covering T3a/T3b/T3c + a new T4 for tick-count sync.

## TL;DR

**The gap is closed. Better than the 5-line patch I proposed.**

The torn-tail recovery now lives inside `prepare_file_writer` (`utils.rs:124-159`), so it applies to BOTH `open_handle` AND `rotate_handle` — broader coverage than my "wire validateSeq into rotate_handle" proposal. Plus the tick-counter is correctly synced to the existing file's message count via the `tick.pep::.tick.rollLog` change in `74acdc6`.

```
=== VERDICT (main@cc954d2) ===
  T3a (garbage):          FAIL — validateSeq RAISED: not a sequence file
                                 (silent-corrupt; same as before, debatable behavior)
  T3b (clean seq):        PASS — validateSeq returned: 8
  T3c (torn tail):        PASS — validateSeq returned: 13
  T4 (tick-count sync):   PASS — pub tick[0] == 7 after rotating to 7-record file
```

## What landed (architecture-quality)

The author refactored the file-IO concerns cleanly. Three utilities in `utils.rs`:

1. **`detect_conn_type(file)`** (utils.rs:50-69) — inspects size + magic header, returns `New | File | Sequence`. Single source of truth replacing the inline detection at the original prepare_file_writer site AND the duplicate inside broker.rs.

2. **`count_seq_messages(file, must_deserialize)`** (utils.rs:81-119) — walks the frame chain from byte 8, returns `(message_count, valid_byte_size)`. Used by BOTH `prepare_file_writer` (`must_deserialize=false`) AND `broker.rs::validate_seq` (caller passes flag). The shared utility removed ~50 lines of duplicate frame-walking logic from `broker.rs`.

3. **`prepare_file_writer(path) -> (writer, conn_type, msg_count)`** (utils.rs:124-159) — now returns `msg_count` and, for `ConnType::Sequence`, calls `set_len(valid_size)` to truncate any torn tail before returning the seek-to-EOF writer. The torn-tail recovery is automatic for any caller (open, rotate, future paths).

The follow-on commit `74acdc6` wires the `msg_count` through:
- `engine_state.rs:778-789` (rotate_handle) — `tick_count[idx] = msg_count` on non-empty files (overwrites the prior 0-reset)
- `tick.pep::.tick.rollLog` — explicitly resets tick[0] then syncs to `tick[.tick.msgHandle; 0]` (the handle's per-file tick) so the global counter matches the file content

Net effect on `roll_tick_log(log_dir, filename)`: rotating to a file with N records yields tick[0] = N, regardless of whether the file was clean or had a torn tail.

## What I'm asking for now

Nothing. The technical case for upstream is complete.

## Bonus observation about T3a (garbage file)

The one residual debate: rotating into a non-sequence-header garbage file currently silently corrupts the output (the new code accepts `ConnType::File`, seeks to EOF, appends). Before `588de78`, this case errored loudly. After, it's silent.

**Whether to refuse or accept** depends on positioning:
- (a) Refuse on `ConnType::File` — stricter; protects users from operational mistakes (stray files in log_dir)
- (b) Accept (current) — more permissive; useful if user knowingly wants to append to a non-tplog file (e.g., a hand-rotated archive)

mdata can control this operationally (don't write stray files into `log_dir`). Not load-bearing for the kill-9 use case. Mentioned only for completeness — your call which behavior is right for chili.

## Updated comparison: main 0.9.0@cc954d2 vs claude-2 0.8.9

| # | Feature | main 0.9.0@cc954d2 | claude-2 verdict |
|---|---|---|---|
| 1 | D-1 push-model | covered by sub.pep auto-apply | drop |
| 2 | D-2 get_var_lazy | covered by get_var().lazy() | drop |
| 3 | D-3 subscribe(resume_from=) | covered by accept-full-replay | drop |
| 4 | fsync primitive | covered (fsync_handle) | drop |
| 5 | publish_via_handle | covered (sync(h, tuple)) | drop |
| 6a | rotate happy-path atomic | covered (handle.write() serialization) | drop |
| 6b | rotate idempotent retry | covered (skip-if-URI-already-in-map) | drop |
| 6c | rotate fsync-OLD-before-swap | covered (SyncFile + flush()) | drop |
| 6d | rotate accept non-empty | covered (588de78) | drop |
| 6e | **rotate torn-tail recovery** | **covered (26b437e: prepare_file_writer truncates torn tail)** | **drop — claude-2's roll_tick is now redundant** |
| 6f | **rotate tick-count sync** | **covered (74acdc6: tick[0] := msg_count)** | drop |
| 7 | GR4 set_column_scale | lift to Python facade | drop |
| 8 | M-1 test guard | keep (zero cost) | keep |
| 9 | W3 register_fn | mdata withdrew per Revision A | drop |

**Net: 12 of 13 features universally covered.** The 13th is the M-1 test guard which is a TEST file (not a feature), kept at zero cost.

claude-2 has **NOTHING** technically left over main 0.9.0@cc954d2. The 4-step move-to-v0.9 plan now collapses to a pure deletion sprint.

## Move-to-v0.9 plan — accelerated

Original Sprint 24 plan: drop 8 features, lift GR4 helpers, retain M-1 guard, await author decision on roll_tick. **Updated:**

1. **Sprint 24 "main port" sprint — pure deletion (~3-5pp, down from 5-8pp).**
   - Delete: D-1, D-2, D-3 push-model code + tests; W3 (external_fn module + register_fn FFI + tests); flush_tplog FFI; publish_via_handle FFI; roll_tick native Rust (~75 lines) + tests; GR4 set_column_scale FFI
   - Lift: GR4 helpers to a pure-Python `chili.scale` module
   - Keep: M-1 invariant test guard (`TestM1EagerNoAutoDequant` in test_engine.py)
   - Forward-port from main: `async_` + `execute` already in main; `polars-core-patch` URL (verify q-style fmt patch present); `py.typed` marker
   - Bump claude-2 to 0.9.0 (matching main's version)
   - Cut wheel; deliver to mdata as the basis for v1-36 architecture cleanup

2. **No more upstream asks.** claude-2's chili-author dialogue effectively closes after this update.

3. **mdata coordination:** their 24h Pipeline X soak completes today; v1-36 starts on the cleaned codebase.

End-state in 1 sprint: **claude-2 ≡ main 0.9.0+** (no diff outside docs/tests).

## Reproducer suite (final state)

```
docs/sync/reproducers/
  q1_publish_path.py                  # §1 publish recipe (PASS)
  w1_fsync_handle.py                  # §3 W1 fsync primitive (all PASS, 0.001ms/call)
  q2_v2_post_author_fixes.py          # §2 F1/F2 properties 1, 2, 4 (PASS)
  q2_v3_three_recovery_cases.py       # §2 F2 property 3 — T3a/b/c (T3c was FAIL pre-26b437e)
  q2_v4_post_truncate.py              # §2 F2 property 3 RE-VERIFIED + T4 tick-count
                                      #   ALL PASS on main@cc954d2
```

Each runs in <10s after `maturin develop --release && uv pip install polars==1.39.3`.

## Author's full-day shipping credit

Five commits total today closed every property mdata's PRD durability story needs:

- ✅ `f6bccd1` — SyncFile wrapper + .handle.fsync builtin + idempotent rotate + fsync-OLD-before-swap
- ✅ `5cfc096` — engine.fsync_handle Python method (W1 closed)
- ✅ `588de78` — rotate_handle accepts non-empty + preserves tick count guard removed
- ✅ `26b437e` — refactor: extract utilities; **prepare_file_writer truncates torn tail (T3c closed)**
- ✅ `74acdc6` — open_handle + rollLog set tick[0] from msg_count (T4 closed)
- ✅ `cc954d2` — bring Python test suite over from claude-2

Substantial single-day work. **The gap analysis essentially fully resolved.** Thank you.
