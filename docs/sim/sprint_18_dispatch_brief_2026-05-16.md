# Sprint 18 dispatch brief — `roll_tick` atomic tplog segment-rollover primitive (mdata wishlist v2 P0)

**Kickoff:** 2026-05-16 — mdata reported the EOD-vs-`upd` silent-loss race on thread `mdata-chili-eod-upd-race-2026-05-15`; chili's source-cited analysis (outbox `chili-eod-upd-race-reply-e55aafa9…`) confirmed verdict (b) and offered option (ii); mdata chose option (ii) and sent the request (`mdata_chili_roll_tick_request_20260515T175524Z.json`, threaded on the same correlation_id).
**Owner:** coordinator-solo + mandatory 3-agent pre-execution audit (Explore + code-reviewer + planner) + code-reviewer post-impl (G2 pattern, Sprint 17 precedent)
**Type:** implementation (FFI surface + tick subsystem — dispatch brief mandatory per `.claude/rules/sprint-cadence.md`)
**Predicted pp:** 10–18 (mid 14, pre-audit; re-pin after audit appendix)
**Plan reference:** `docs/sync/ideas.md` (mdata feedback) + the cross-comms thread above
**ADR references:** ADR 0001 (pub/sub canonical — `signal_eod` is adjacent; `roll_tick` is cutover-only and does NOT touch it)

---

## Sprint objective

Ship `engine.roll_tick(...)`: a single primitive that, holding the existing `self.handle.write()` exclusive lock for the entire critical section, atomically cuts the tickerplant log over from the current segment to the next — **open next segment → in-place swap the live handle's writer → fsync+close the old segment** — so a concurrent inbound `.tick.upd` is always serviced by exactly one valid handle and lands wholly in the old segment or wholly in the new one, never `InvalidHandleErr`, never silently dropped.

**Binary success criterion (all must hold):**

1. **Teeth check:** the concurrency harness, run against the *pre-fix* close-then-reopen path, **reproduces message loss** (loss > 0). If it can't fail on buggy code it proves nothing.
2. **Zero-loss:** against `roll_tick`, with the deterministic injected-yield interleaving, **0 messages lost, 0 duplicated, single crisp boundary** across ≥ 200 randomized-timing iterations, single-writer **and** multi-writer (mdata fh fan-in) topologies.
3. Pre-commit gate green (`cargo fmt`/`clippy`/`cargo test --workspace --exclude chili-py`) + `uv run maturin develop && uv run pytest` green.
4. Failure-atomicity: a forced next-segment-open failure leaves the old segment fully writable (no half-roll).

If criterion 1 fails, the sprint **halts** — the harness is not trustworthy and nothing downstream can be ratified.

---

## Why now

- mdata's durability invariant (their PRD §5.1) is violated by construction in chili 0.8.5 at every log roll; chili's analysis on the thread established this is a chili-internals defect mdata cannot fix from its own code.
- mdata explicitly chose option (ii) and is gating their sprint v1-25.2 on the landing estimate (build the fallback Python barrier vs wait for `roll_tick`).
- Closing this lets mdata delete a Python drain-barrier they'd otherwise have to keep correct against every future chili threading change.
- **User directive (2026-05-16, load-bearing reframe):** roll is **not** daily/date-bound. It can fire at any time (size/count-triggered for UHF feeds where a daily file is too large); the segment id just monotonically increments. This generalizes mdata's proposed `next_date: date` to an opaque caller-owned segment label.

---

## Scope — Part A: the `roll_tick` core primitive

### A.1 Surface additions

**Design D — same-id in-place writer swap (chosen).** The root cause is that close (`tick.pep:9` → `close_handle` `engine_state.rs:812-816`) and reopen (`tick.pep:10` → `open_handle`) are *two separate `handle.write()` acquisitions* with the handle id `shift_remove`'d from the `IndexMap` in between; a concurrent `sync()` (`engine_state.rs:971-1123`, `handle.get_mut(h)` → `None` at `:1121`) raises `InvalidHandleErr`, and the async path logs+drops it silently (`utils.rs:371-373`). The fix moves the entire cutover **out of pepper into one Rust critical section** and **keeps the handle id constant**, so there is no removal window even to reason about.

New chili-core method (mirrors the Sprint-17 `publish_via_handle` chain — `engine_state.rs` core fn + `lib.rs` `#[pymethods]` + `engine.py` wrapper; **no new pepper statements** — doing the cutover as a `.tick.roll` pepper fn calling `.handle.close`/`.handle.open` would re-create the two-statement race one layer up):

```rust
// crates/chili-core/src/engine_state.rs (place after publish_via_handle ~1327)
/// Atomically roll the tplog to the next segment. Holds `self.handle.write()`
/// across open-next → swap-writer → fsync+close-old so a concurrent `sync()`
/// (inbound `.tick.upd`) sees exactly one valid handle for the whole cutover.
pub fn roll_tick(&self, log_dir: &str, segment_label: &str) -> SpicyResult<()>;
```

```python
# crates/chili-py/chili/engine.py (after publish_via_handle ~572)
def roll_tick(self, log_dir: str, segment_label: str) -> None: ...
```

```rust
// crates/chili-py/src/lib.rs (after publish_via_handle ~697) — py.detach GIL release
fn roll_tick(&self, py: Python<'_>, log_dir: &str, segment_label: &str) -> PyResult<()>
```

**Segment identifier is an opaque, caller-owned string** (generalizes mdata's `next_date`). chili already does plain `.tick.msgLog: logDir + date` (`tick.pep:4`) — no `_NNNN` index exists anywhere. The caller owns the monotonic increment and naming convention; a date string is one valid label. Backward-compatible: mdata passing a `date` (stringified) keeps working unchanged.

**Decided design parameters (from the prior 4-question fork; generalized):**
- *Slim signature* — `roll_tick(log_dir, segment_label)`; reuse the live `.tick.schema` engine var (set by the prior `init_tick`, `engine.py:461`; `createLog` does not consume schema). Not re-passed.
- *Cutover-only* — does **not** fire `signal_eod`. Caller still calls `engine.eod(d)` first if/when it wants the EOD broadcast. (For UHF intra-day rolls there is typically no `eod` at all — another reason not to subsume it.)
- *fsync inside the lock* — step 3 below `flush()`+`sync_all()`s the old segment before the writer is dropped (satisfies mdata PRD §5.1; cost is one fsync of the old tail under the lock, paid once per roll).
- *Contract* — raises on failure; **idempotent no-op** if the live handle's `uri` already equals `log_dir+segment_label` (safe under EodScheduler / size-trigger retry).

### A.2 Implementation hints

Critical-section ordering (single `self.handle.write()` acquisition):

1. Read `.tick.msgHandle` (i64) from the `vars` lock; copy; drop vars lock. Compute `next_path = format!("{log_dir}{segment_label}")` and `next_uri = format!("file://{next_path}")` (mirrors `tick.pep:4-5`).
2. `let mut handle = self.handle.write();` — single acquisition. `let entry = handle.get_mut(&h).ok_or(SpicyError::InvalidHandleErr(h))?;`
3. **Idempotent guard:** if `entry.uri == next_uri` → drop lock, return `Ok(())`.
4. **Open-next-before-touching-old (failure-atomicity invariant):** open the next file with the *same* `OpenOptions` `open_handle` uses for `file://` (`engine_state.rs:733-738`: `read(true).write(true).create(true).truncate(false)`). If it errors → return `Err`, lock dropped, **old segment untouched and still writable**.
5. **Durability:** `entry.rw`’s `flush()` then `sync_all()` (the `ReadWrite::sync_all` from Sprint 16, `engine_state.rs:75/81-82/93`) — old tail fsync'd while still the live writer.
6. **Swap:** if the freshly-opened file is empty, write the 8-byte sequence header `[255,0,0,0,0,0,0,0]` and set `entry.conn_type = ConnType::Sequence` (matches the `New`→`Sequence` transition `engine_state.rs:1044-1066`); set `entry.rw = Some(Box::new(next_file))`; `entry.uri = next_uri`; `entry.socket`/path updated; `entry.bytes_since_flush.store(0, Relaxed)`. The old `Box<dyn ReadWrite>` drops here (already fsync'd in step 5).
7. **Seq reset (replicates `createLog:7`):** still relevant — `createLog` does `tick[0; .broker.validateSeq[.tick.msgLog; 0b]]`. For a fresh next segment `validate_seq` returns 0 immediately (`broker.rs:65-72`, size==0). Set the per-handle `tick_count` slot (`engine_state.rs:131`, `tick()` `:1901-1910`, `MAX_HANDLE_NUM=1024` `:122`) to `validate_seq(next_path)`. **Lock-ordering hazard — audit focus:** `roll_tick` would acquire `handle.write()` then `tick_count.write()`. Verify no existing path takes `tick_count` then `handle` (deadlock edge). `sync()` takes only `handle`; `tick()` takes only `tick_count` — independent today; this introduces the first nested edge. The implementer's RULE-7 grep must confirm this before writing.
8. Drop locks. Update `.tick.msgLog` var to `next_path` so `.tick.subscribe` (`tick.pep:26`) reports the new path.

GIL: `lib.rs` wrapper uses `py.detach(move || …)` exactly like `publish_via_handle` (`lib.rs:686-697`) + `self.check_fork()?`.

### A.3 Storage / schema

No on-disk format change. tplog frame format unchanged (`[len:u64 LE | ts:u64 LE | serde9 payload]`, 8-byte `[255,0,0,0,…]` header). Int64-quantized price-column convention untouched. The segment **boundary** is the new artifact: last frame of segment N is complete, first frame of segment N+1 is complete, zero overlap (the anti-Q2 property — chili's tplog has no per-message seq id to dedup on; sequence is positional per file, `broker.rs:53-122`).

### A.4 Tests

See Part B — the harness is the deliverable spine, not an afterthought.

---

## Scope — Part B: the verification harness (binary success criterion lives here)

Four non-negotiable design principles (a suite violating any gives false confidence on a race):

1. **Teeth check.** Same harness run against the pre-fix close-then-reopen path MUST show loss > 0. Implement as a feature-gated/`#[cfg(test)]` "legacy roll" that does `close_handle` then `open_handle` (two locks) and assert the property test FAILS on it.
2. **Real concurrent path.** Rust integ contends the real `self.handle.write()` from real `thread::spawn`ed writers calling the real `sync()`. chili-py pytest drives a **real TCP async publisher** (mirror `tests/test_publish_via_handle.py`) — the bug only exists on the per-connection-thread async path (`engine_state.rs:2215-2225` → `handle_chili_conn` → `eval` → `sync`); an in-process single-thread test cannot reproduce it.
3. **Deterministic interleaving.** A `#[cfg(test)]` injected yield/sleep point inside the cutover so the contended interleaving fires every run (turns a flaky 2-syscall race into a deterministic pass/fail). Old path → deterministic loss; new path → deterministic zero-loss because the lock is held across the yield.
4. **Independent oracle.** A standalone tplog reader (raw `[255,0,0,0,…]` + frame walk + serde9 payload extract) implemented separately from chili's writer — in the Rust test as a helper, in pytest as a pure-Python parser. Never verify with the writer's own code.

Test matrix:

| Tier | Where | Cases |
|---|---|---|
| 1 (pre-commit) | chili-core Rust integ `roll_tick_test.rs` | single-writer crisp-boundary (set-union = full range, set-intersection = ∅, each file sorted, `max(D)+1==min(E)`, conservation); multi-writer per-publisher-monotone (mdata SEQ-MONO shape); injected-yield determinism; **teeth check vs legacy path**; failure-atomicity (next-open EACCES → old still writable); bounded iteration count for CI speed |
| 2 (pre-commit) | chili-py `tests/test_roll_tick.py` | real-TCP end-to-end roll under async publish; idempotent-retry no-op; restart with pre-existing non-empty next segment (seq = `validate_seq(existing)`, content preserved, `truncate(false)`); non-file handle → documented `Err`; unset `.tick.msgHandle` → documented error no panic; `.tick.schema` byte-identical post-roll; `.tick.msgLog` updated; `bytes_since_flush`/`flush_tplog()` == 0 post-roll; **UHF: 50 rapid successive rolls in a tight loop with continuous publish — every message in exactly one segment, segments strictly ordered, no loss across all 50 boundaries**; EOD-ordering contract lock-in (`eod(d)` then `roll_tick`: subscriber gets `(eod;d)` broadcast, and the documented `eod→roll` gap behavior is asserted so a future change can't silently alter cutover-only) |
| 3 (nightly/soak — runtime estimated before run per `runtime-estimation.md`) | harness | 200+ randomized-timing iterations; kill-9 durability harness (child: init_tick→write M→roll_tick→write more; parent SIGKILLs at random point; if `roll_tick` returned, old segment contains all M — the only true PRD §5.1 proof) |

---

## Out of scope (defer)

- **Cross-segment global seq carry-over.** `roll_tick` replicates `createLog:7` per-segment reset (fresh segment ⇒ seq 0). Whether mdata wants a monotone global seq across segments (composite `(segment_label, in_segment_seq)` vs flat) is a **deferred mdata decision** — surfaced in Part C delivery doc, defaulted to existing createLog semantics (least surprise, backward-compatible). Do NOT silently pick carry-over.
- A `.tick.roll` pepper-level convenience wrapper (would reintroduce multi-statement non-atomicity; explicitly rejected).
- Auto-increment of the segment label inside chili (caller owns it per user directive).
- Changing `init_tick`/`createLog` (untouched; `roll_tick` is additive).
- Upstreaming to `main` (claude-2 only; surface to user if it should reach `main`).

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | `crates/chili-core/src/engine_state.rs` — `EngineState::roll_tick` + `#[cfg(test)]` legacy-roll + yield hook | edit |
| 2 | `crates/chili-py/src/lib.rs` — `roll_tick` `#[pymethods]` (py.detach) | edit |
| 3 | `crates/chili-py/chili/engine.py` — `roll_tick` wrapper + docstring | edit |
| 4 | `crates/chili-core/tests/roll_tick_test.rs` — Tier-1 suite incl. teeth check | new |
| 5 | `crates/chili-py/tests/test_roll_tick.py` — Tier-2 suite incl. UHF rapid-roll | new |
| 6 | `dist/chili_sauce-0.8.6-cp310-abi3-macosx_11_0_arm64.whl` + sha256 | new |
| 7 | `docs/sync/mdata_chili_2026-05-16_0.8.6_delivery.md` — answers mdata's 6 open Qs (code-cited) + segment generalization + the deferred seq decision + landing-estimate-now-actual | new |
| 8 | cross-comms reply on `mdata-chili-eod-upd-race-2026-05-15` (draft → user-confirmed before send) | new |
| 9 | `docs/decisions/0001-*` cross-ref note (roll_tick is cutover-only, does not touch signal_eod) | edit |
| 10 | `docs/sim/sprint_18_retro.md` + `cadence_metrics.md` row + `sprints_index.md` row | new (post-sprint) |

---

## Lead allocation

Coordinator-solo for implementation. **Mandatory pre-execution 3-agent parallel audit** (Explore + code-reviewer + planner) per `~/.claude/rules/self-audit-on-plans.md` (this brief touches the chili-py FFI surface, names ≥ 3 code artifacts, and is ≥ 5pp — all triggers fire) — dispatched immediately after this brief is written, appendix folded before any code. Code-reviewer dispatched again post-Part-A per the Sprint-15/16/17 G2 pattern (lesson 7, reviewer-before-retro: budget ~1pp absorption). No worktree (single linear sprint, no parallel sprint contention).

---

## Mid-checkpoint plan

At ~50% predicted-pp (≈ 7pp): post status answering —

- Teeth check: does the harness reproduce loss on the legacy path? (If no → halt, trigger 2.)
- Is the `handle.write()` → `tick_count.write()` lock-order edge confirmed safe (no inverse path)?
- ETA to wrap.

Halt-and-escalate criteria (template standard):

1. Scope-blowing bug — actual-pp would exceed 150% of predicted.
2. Plan-pivot — premise contradicted (e.g., `sync()` does NOT actually serialize all tplog writes, invalidating design D).
3. User-decision needed — a reversible architectural choice not surfaced here.
4. Watchdog — 5h ≥ 80% AND remaining > 15pp.

---

## Wrap (per ceremony)

- Pre-commit gate green: `cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace --exclude chili-py`.
- Python wrap: `cd crates/chili-py && uv run maturin develop && uv run pytest`.
- No bench-gate-touching change expected (parse-cache hot path untouched); confirm parse_cache criterion still ≤ 400 ns if any chili-core recompile risk (golden rule 6).
- Test-count delta documented.
- 0.8.6 wheel cut + sha256 + byte-equivalence note vs 0.8.5 for unaffected surface.
- Retro at `docs/sim/sprint_18_retro.md`; row appended to `cadence_metrics.md`; `sprints_index.md` updated; brief `git mv`'d to `docs/history/sprints/` post-ratification.
- HALT until user ratifies.

---

## Pp accounting reference

Closest comparables from `cadence_metrics.md`:

- **Sprint 17** (publish_via_handle thin wrapper + signal_eod rewrite) — predicted 11–25 (mid 18 post-audit), actual ~12. Comparable: same FFI chain to mirror; `roll_tick` core is similarly small. **Difference: Sprint 18's test harness is materially heavier** (concurrency stress + teeth check + independent oracle + UHF) than Sprint 17's 4 pytest.
- **Sprint 16** (wishlist bundle, 3 features) — predicted 10.7–18.2 (mid 14), actual ~14. Comparable scope envelope.
- **Sprint 14** (small surgical FFI, GIL release) — predicted 5–9, actual ~5. Lower bound: that's the *primitive-only* cost; Sprint 18 ≠ that because the harness dominates.

Sprint 18 expected at the **mid band (~14)**, the harness being the long pole, not the ~50-LOC primitive. Upper edge (18) reserved for: lock-order edge requiring redesign, or teeth-check forcing harness rework. Pattern-6 (autonomous-run sub-50% ceiling) does **not** apply — this is implementation, not perf-pass/research; Sprints 3/4/5/7/14/15/16/17 implementation sprints calibrate at band.

---

## Cross-references

- Cross-comms thread: `mdata-chili-eod-upd-race-2026-05-15` (mdata request `mdata_chili_roll_tick_request_20260515T175524Z.json`; chili analysis `chili-eod-upd-race-reply-e55aafa9…`)
- ADR 0001 (pub/sub canonical; `signal_eod` Sprint-17 Async — `roll_tick` deliberately does NOT touch it)
- Sprint 16 retro (`flush_tplog`/`ReadWrite::sync_all`/`bytes_since_flush` — reused by step 5/6)
- Sprint 17 retro (`publish_via_handle` chain — the template)
- `docs/sync/mdata_chili_2026-05-14_0.8.5_delivery.md` (prior delivery; 0.8.6 supersedes for mdata)
- Golden rules 5 (GIL release) + 6 (parse-cache hot path — untouched, confirm)

---

## Appendix — Independent audit (2026-05-16)

3-agent parallel audit (Explore + code-reviewer + planner) per `~/.claude/rules/self-audit-on-plans.md`. Original brief above preserved as audit trail. Contested CRITICALs independently re-verified by the coordinator (RULE-7 second-order); citations replayed below.

### Material corrections

**CRITICAL-1 — design D must not hand-roll `open_handle`'s file-prep (data loss on pre-existing/retry segment).** Verified `Read engine_state.rs:725-770`: `open_handle`'s `file://` path does open (`:733-738`, **no `.append(true)`**) → conn_type detect (`:743-756`: `New` if len 0, `File` if <8, else 4-byte header → `Sequence` if `[255,0,0,0]` else `File`) → **`file.seek(SeekFrom::End(0))` at `:758`** → `set_handle` (`:761-769`, allocates a new id). Original A.2 steps 4/6 open the file directly and omit the EOF seek + full conn_type detection ⇒ on a pre-existing/non-empty next segment the writer sits at offset 0 and clobbers the header/frames (CRITICAL data loss on idempotent-retry / restart). **Resolution (supersedes A.2 steps 4 & 6):** extract a private helper `prepare_file_writer(path) -> SpicyResult<(Box<dyn ReadWrite>, ConnType)>` covering exactly `:733-760` (open + conn_type detect + seek-to-EOF). Refactor `open_handle` to call it (then `set_handle`). `roll_tick` calls the same helper, then swaps the returned writer into the **existing** entry under the held `handle.write()` (NOT via `set_handle` — same-id swap preserved). This is drift-proof: it subsumes the cursor fix *and* the conn_type fix and makes "design D forgot one of open_handle's side-effects" unrepresentable. Open-next (via helper) still happens before touching old → failure-atomicity invariant intact.

**CRITICAL-2 — `sync()` `:1121` failure surface is broader than "missing key".** Verified `Read engine_state.rs:973-981,1121`: the `Some` arm matches `Handle { rw: Some(rw), .. }`, so a present handle with `rw: None` also falls to `_ => Err(InvalidHandleErr(*h))`. Corrections: (a) reword the design-D motivation/failure-mode text — concurrent `sync()` hits the wildcard arm on *either* a missing key *or* `rw: None`; (b) add an explicit invariant to A.2: **roll_tick never publishes an observable `rw: None`** — the swap is a single `entry.rw = Some(Box::new(next))` assignment under the exclusive lock with the old `Box` dropped *after* assignment; `rw` is `Some` at every lock-release point. The idempotent guard `get_mut(&h).ok_or(InvalidHandleErr)?` therefore only errors on a genuinely absent/torn handle, which is the correct contract.

**MAJOR-1 — "silently dropped" is inaccurate.** Verified earlier this session `utils.rs:351,353-373`: `:351` `state.eval(...)`; Sync path returns the error to the caller (`:353-369`); Async path `:371-372` `else if let Err(e)=res { error!("{}", e); }` — the server **logs** via `error!`, the *publisher* gets no error back, loop continues. Correction: replace "logged and dropped silently" → "dropped with no error returned to the publisher (server-side `error!` log only)". **Teeth-check consequence:** the harness failure signal MUST be reader-side message loss via the independent oracle (count/coverage) — never absence-of-panic, never log-scrape.

**MAJOR-2 — `tick_count` index bound is a latent error path.** Verified `engine_state.rs:122` `const MAX_HANDLE_NUM: usize = 1024;` + comment `:120-121` "Handle numbers must be in the range 0..MAX_HANDLE_NUM" + `tick()` `:1901-1910`. A.2 step 7 must bounds-check `(h as usize) < MAX_HANDLE_NUM` before the `tick_count.write()` and return a documented `Err` (not panic) on violation. Add a Tier-1 test for the out-of-range handle.

**MAJOR-3 — teeth-check must be red-first (sequencing inversion).** Original order impls Part A before the Part B harness; the binary success criterion 1 is then unfalsifiable. **Revised sequencing (see below):** the legacy-roll teeth-check is built and asserted *failing* (loss > 0) BEFORE `roll_tick` exists.

**MAJOR-4 — lock-order grep is a pre-step-0, not A.2 step 7.** If an inverse `tick_count → handle` path existed it would invalidate design D before any code. Coordinator pre-verified (Explore): `grep` shows the only `tick_count.write()` is `tick()` `:1908` and the only `tick_count.read()` is `:551`; neither nests a `handle` acquisition; `sync()` takes only `handle.write()`. **No inverse edge exists today** — design D's `handle.write()` → `tick_count.write()` is the first nested edge and is deadlock-safe. The implementer still re-confirms via RULE-7 grep (third independent check) as the first action of Part A; the expected result is documented here so a *change* in answer is the escalation signal.

**MAJOR-5 — concurrent double-`roll_tick` not in the matrix.** EodScheduler double-fire / two-thread call. Design serializes on `handle.write()`; 2nd call sees the idempotent guard. Add Tier-1 cases: (a) two concurrent `roll_tick` same label → exactly one cutover, 2nd is Ok no-op, zero loss; (b) two concurrent `roll_tick` *different* labels → documented last-writer-wins, no corruption/loss (contract must be stated, not emergent).

**MAJOR-6 — `segment_label` contract undefined.** `format!("{log_dir}{segment_label}")` with `""` or `"../.."` silently misroutes. Resolution: match chili's existing opaque-label model (createLog does not sanitize `logDir+date` either) — caller owns sanitization — but add one minimal guard: empty `segment_label` → `Err`. Over-validating would diverge from the established model. Tier-2 test: empty → `Err`; normal label routes correctly.

### Additional opportunities surfaced

**OPP-1 — `validate_seq` cost/semantics under the lock.** Verified `broker.rs:53-122`: size==0 → returns 0 at `:71` with no `set_len` (the normal fresh-segment roll → ~0 cost). Non-empty → frame walk + `file.set_len(valid_size)` `:120-121` truncating a torn tail. Surface in A.2: normal roll is the fast path; the rare pre-existing-non-empty segment holds `handle.write()` for a bounded frame walk and the trailing-corrupt-frame truncation is **intentional recovery, identical to `createLog:7` today** — acceptable, not a correctness bug. No redesign.

**Confirmed correct (no change):** idempotent guard `entry.uri == next_uri` — `set_handle` stores `uri` as the full `file://…` string (`:764`), comparison consistent. `.tick.msgHandle` is `SpicyObj::I64` (`open_handle`→`set_handle` returns `I64` `:770`). `signal_eod` filters `ConnType::Publishing` and skips `Sequence` → cutover-only is correct, roll_tick must not touch it. thread-per-conn `:2215-2225`. Golden rules: edition 2024, polars 0.53 pinned, no `#[global_allocator]` in libs — all met. Citation drift (Box `ReadWrite` impl `:92-95` not `:93`; `validate_seq` fast-path ends `:71` not `:72`) — navigable; no brief-body change.

### Cross-cutting gates

- Teeth-check failure signal = independent-oracle reader-side loss only (per MAJOR-1).
- Implementer's first action = RULE-7 lock-order re-grep (per MAJOR-4), expected "no inverse edge".
- `prepare_file_writer` extraction touches `open_handle` (a load-bearing handle-subsystem fn) — code-reviewer post-Part-A MUST diff `open_handle` behavior for regression (it currently backs `init_tick`/`createLog`).
- mdata reply stays draft → user-confirmed before send (cross-comms producer contract).

### Revised sequencing

0. **Part A pre-0:** RULE-7 lock-order re-grep (expect: no inverse `tick_count→handle`).
1. **Part B.0 (red-first):** build the independent oracle + the `#[cfg(test)]` legacy-roll (close-then-open, two locks) + the property harness; assert it **fails (loss > 0)** on the legacy path. HARD HALT if it cannot reproduce loss.
2. **Part A:** extract `prepare_file_writer`, refactor `open_handle` to use it, implement `roll_tick` (same-id swap, fsync-in-lock, bounds-checked seq reset, idempotent + empty-label guards).
3. **Part B.1:** same harness → assert zero-loss/no-dup/crisp-boundary on `roll_tick`; full Tier-1/Tier-2 matrix incl. the 3 audit-added categories (out-of-range handle, double-roll, label contract).
4. Code-reviewer post-Part-A (G2; **must include `open_handle` regression diff**).
5. **Part C:** 0.8.6 wheel + byte-equiv vs 0.8.5 + delivery doc (state cross-segment-seq default as a *documented contract*, not just a flag) + mdata cross-comms reply (draft→confirm).
6. Wrap: **every-5-sprint deep housekeeping is MANDATORY** (overdue) — see Sprint sizing.

### Sprint sizing

Row count in `cadence_metrics.md` since the Sprint 11 sweep = **7** (12, 13, 13.5, 14, 15, 16, 17); ≥5 even excluding the half-sprint (12,13,14,15,16 = 5 by Sprint 16). The every-5-sprint trigger fired at Sprint 15/16 wrap and was **not executed** → the deep housekeeping sweep (docs-lifecycle + memory + CLAUDE.md state refresh) + post-sweep `/compact` recommendation is **mandatory at Sprint 18 wrap** per `.claude/rules/sprint-cadence.md`. Added to deliverables (new #11) + wrap ceremony.

Predicted pp **re-pinned 11–20 (mid 15)** (was 10–18 mid 14): +1–2 for the `prepare_file_writer` extraction + `open_handle` refactor/regression (load-bearing fn, not pure-additive as originally scoped) + 3 audit-added test categories. Upper edge 20 reserved for: `open_handle` regression forcing rework, or teeth-check forcing harness redesign. The mandatory housekeeping sweep is tracked as a *separate* wrap-phase cost (historically ~1.5–3pp per `cadence_metrics` Sprints 6/11), NOT inside the 11–20 implementation band — surface it to the user at wrap as an explicit add-on, do not silently fold.

### Deliverables addendum

| # | Artifact | Type |
|---|---|---|
| 11 | Every-5-sprint deep housekeeping sweep (docs-lifecycle + memory + CLAUDE.md state) + `/compact` recommendation | wrap-phase (mandatory; separate pp) |
