# Sprint 16 dispatch brief — mdata wishlist v1 bundle (P0 + P3 + P2)

**Kickoff:** TBD — pending user ratification of brief + audit appendix
**Owner:** coordinator-solo / coordinator + tester subagent
**Type:** implementation (small surfaces, three independent items)
**Predicted pp:** 9–14 (pre-audit estimate)
**Plan reference:** `~/code/mdata/docs/sync/chili_wishlist_2026-05-13.md`
**ADR references:** none (no architectural decisions; pure API additions)

---

## Sprint objective

Close the three smallest items in mdata's 2026-05-13 wishlist:

- **P0** — `engine.flush_tplog()` Python-callable fsync hook on the tplog handle (mdata blocker for PRD §5.1 part-2 `kill -9` durability).
- **P3** — `engine.add_at_time(fn_name, start_time, description)` Python binding wrapping the existing Rust `.job.addAtTime` (mdata PRD §3.2 Option A EOD timer path).
- **P2** — pepper grammar accepts `;` as a top-level statement separator (mdata daemon-boot ergonomics).

Binary success criterion: all three deliverables have passing chili-py pytest coverage AND a corresponding mdata-side acceptance test is named in mdata's wishlist for them to flip xfail→strict-pass on receipt.

Out of scope: the two P1 items (`engine.publish_remote` and subscriber-side `eod` dispatch). Both need additional investigation (live xfail reproduction; protocol-ownership scoping conversation with mdata) before they're brief-able — deferred to Sprint 17.

---

## Why now

- **mdata wishlist is the highest-value input we have.** Their v1 arc shipped 7 process daemons against chili 0.8.3 hitting 3.7× the kdb+/TorQ baseline; the wishlist is the surfaced friction from real production use. Sprint 16 closes the three smallest items as a high-signal acknowledgement.
- **Each of P0 / P3 / P2 is independent.** Different files, different test surfaces, no inter-dependency. Parallel-friendly if we want to fan out, but small enough that coordinator-solo is also fine.
- **Sprint 17 needs Sprint 16's clarifications before it can start.** P1 publish_remote depends on whether mdata or chili owns the TCP client (Open Q #3 in their wishlist) — best answered AFTER they've used our new APIs from Sprint 16 and we've talked. Sprint 16 buys that conversation window.
- **No load-bearing architectural changes.** None of the three touches the parse-cache hot path, the on-disk schema, or GIL-release semantics. Risk profile is small.

---

## Scope — Part A: P0 — `engine.flush_tplog()`

### A.1 Surface additions

**chili-core:**

```rust
// crates/chili-core/src/engine_state.rs

// EXTEND the ReadWrite trait with a default-no-op sync_all:
pub trait ReadWrite: Read + Write + Send + Sync {
    fn sync_all(&self) -> io::Result<()> { Ok(()) }
}

// EXISTING blanket impl stays but loses the default; add a specific override:
impl ReadWrite for fs::File {
    fn sync_all(&self) -> io::Result<()> { fs::File::sync_all(self) }
}
// Generic blanket impl for TCP / other Read+Write+Send+Sync wrappers
// keeps the no-op default (TCP sync_all has no meaningful semantic).
```

**EngineState method:**

```rust
// crates/chili-core/src/engine_state.rs

pub fn flush_handle(&self, h: &i64) -> SpicyResult<i64> {
    // Returns bytes-since-last-flush as i64 (matches mdata's preference
    // for a real number replacing their stat().st_size proxy).
    // For file:// handles, calls rw.flush() then rw.sync_all().
    // For non-file handles, returns Err (sync semantically meaningless).
}
```

**chili-py:**

```python
# crates/chili-py/chili/engine.py

def flush_tplog(self) -> int:
    """Flush + fsync the active tplog handle. Returns bytes-since-last-flush.

    Looks up .tick.msgHandle (set by init_tick / .tick.createLog) and
    calls EngineState::flush_handle on it. Raises if init_tick hasn't
    been called or the handle is non-file.

    PRD §5.1 part-2: after this returns, a hard kill (SIGKILL) does
    not lose any bytes written prior to the flush_tplog call.
    """
    h = self.get_var(".tick.msgHandle")
    if h is None:
        raise RuntimeError("flush_tplog called before init_tick — no tplog handle")
    return self.engine.flush_handle(h)
```

### A.2 Implementation hints

- **Trait extension is the cleanest approach** — keeps the blanket `impl<T: Read+Write+Send+Sync> ReadWrite for T` but adds a default no-op `sync_all` so existing TCP-backed Handles compile unchanged.
- **The override happens via a specific impl block** — `impl ReadWrite for fs::File { fn sync_all() { fs::File::sync_all(self) } }`. This shadows the blanket impl for `fs::File` specifically. Verify it compiles cleanly with the existing blanket impl (Rust's coherence rules will allow this since `fs::File` is in std and we own ReadWrite).
- **Tplog handle source of truth** — `.tick.msgHandle` global pepper var. Set by `.tick.createLog` (`crates/chili-py/chili/src/tick.pep:10`) via `.handle.open .tick.logFile` which routes to the file:// branch at `engine_state.rs:696`. The handle is opened with `OpenOptions{read, write, create, truncate=false}`. No O_SYNC anywhere — verified zero hits across `crates/`.
- **bytes-since-last-flush** — need to track a counter on Handle. Either a `bytes_written_since_flush: AtomicU64` field, or `Handle::file_position_before_flush` and subtract. AtomicU64 is simpler. Reset on flush, increment on each successful write. **This is the largest design choice in Part A** — verify with mdata that "bytes-since-last-flush" semantics match what their monitor probe expects (vs. cumulative bytes-fsynced-total).
- **Error behavior** — calling `flush_tplog()` before `init_tick` or on a non-file handle is a real-mode error in mdata's flow, not a graceful no-op. Raise `RuntimeError` / `SpicyError::Err`.

### A.3 Tests

- **Rust unit (chili-core):** open a file:// handle, write 100 bytes, call `flush_handle`, assert it returns 100; write 50 more, flush, assert returns 50.
- **Python pytest (chili-py):** `tests/test_tick_tplog_flush.py` — boot tick, publish 50 rows, call `engine.flush_tplog()`, assert returns the expected byte count.
- **Manual smoke (no automated test — mdata will run this as their acceptance test):**
  ```python
  tp.boot(today); tp.publish("trades", df_50)
  engine.flush_tplog()
  os.kill(os.getpid(), SIGKILL)
  # ... new process reads tplog, asserts 50 rows recoverable
  ```
  mdata commits to running this as `tests/tp/test_tp_durability.py::test_kill_9_durability` and flipping xfail → strict-pass.

### A.4 Sizing

**Predicted: 4–6 pp.** Trait extension (1pp) + flush_handle method + bytes-since-last-flush tracking (2pp) + chili-py wrapper (1pp) + tests (1-2pp).

---

## Scope — Part B: P3 — `engine.add_at_time()` Python binding

### B.1 Surface additions

**chili-py FFI:**

```rust
// crates/chili-py/src/lib.rs

#[pymethods]
impl EngineStatePy {
    /// Schedule fn_name to fire at start_time inside the chili event loop.
    /// Wraps the existing Rust .job.addAtTime registered fn.
    /// Returns the job ID for cancellation.
    fn add_at_time(
        &self,
        fn_name: &str,
        start_time: &PyDateTime,
        description: Option<&str>,
    ) -> PyResult<i64> { ... }
}
```

**chili-py wrapper:**

```python
# crates/chili-py/chili/engine.py

def add_at_time(self, fn_name: str, start_time: datetime, description: str = "") -> int:
    """Schedule fn_name to fire at start_time on the chili scheduler.

    fn_name must be a pepper-defined function in the engine's global namespace.
    Returns the job ID. Use cancel_job(job_id) to revoke.
    """
    return self.engine.add_at_time(fn_name, start_time, description)
```

### B.2 Implementation hints

- **Rust fn already exists** at `crates/chili-core/src/job.rs:96` (`pub fn add_at_time`). Signature: `(state, stack, args)` taking `[StrOrSym, Timestamp, StrOrSym]`, returns `SpicyObj::I64(job_id)`.
- **Two implementation approaches:**
  - **(a)** PyO3 wrapper calls `state.fn_call(".job.addAtTime", &[name_sym, ts_obj, desc_sym])` — mirrors how publish/eod/etc. work in chili-py. Smaller diff, matches the existing convention.
  - **(b)** PyO3 wrapper calls the Rust `job::add_at_time` function directly. Skips fn_call dispatch overhead. Slightly more boilerplate (build args vector, pass empty stack).
- **Prefer (a)** — matches existing convention (`def publish(...): self.fn_call(".tick.upd", ...)`), no perf-critical reason to bypass fn_call.
- **Timestamp marshalling** — Python `datetime.datetime` → chili Timestamp i64. Mirror the existing pyo3-chrono conversion path (chili-py already depends on pyo3's chrono feature per `Cargo.toml`).
- **Scheduler must be running** — caller's responsibility to have called `arc_state.start_job_scheduler()` (already done in `engine.py` init flow). Document in docstring.

### B.3 Tests

- **Python pytest:** `tests/test_job_scheduler.py::test_add_at_time` — define a pepper fn that mutates a global; schedule it 100ms in future; sleep 200ms; assert the global was set.
- **mdata acceptance:** their wishlist's example test in `test_chili_side_eod_timer` — they own it; we don't need to write it.

### B.4 Sizing

**Predicted: 2–3 pp.** PyO3 wrapper + Python wrapper + 1-2 tests.

---

## Scope — Part C: P2 — Pepper `;` top-level statement separator

### C.1 Surface change

Allow `;` between top-level statements in `parser_pepper()` so that

```python
engine.eval("a: 1; b: 2; c: a + b")
```

succeeds (currently raises `Punc';' syntax error`).

### C.2 Implementation hints

- **No REPL vs script split in the parser.** `crates/chili-core/src/parser.rs:680` dispatches by `.chi` extension → `parser_chili()` else → `parser_pepper()`. Same parser is used for both `engine.eval(source)` and file-loaded scripts. **mdata's framing of "REPL mode vs script mode" is incorrect** — the actual gap is in the pepper grammar's top-level production.
- **Where the grammar likely needs to change** — `parser_pepper()` at `crates/chili-parser/src/expr.rs:559`. The top-level statement-list production needs to accept `;` as an alternative separator alongside whatever it accepts today (newline / EOF). Investigate the exact production before editing.
- **`;` already accepted in some contexts** — `sub.pep` uses `;` between statements WITHIN function bodies (`{[table; data] table upsert data; tick[this.h; 1]; }`). So the lexer-level token is fine; only the top-level-statement production needs the alternative.
- **Risk: dict / list / arg-separator `;` already meaningful.** Pepper uses `;` to separate args in function calls and elements in lists. The grammar change must not break those existing uses. **Verify by running the full chili-py pytest suite after the change.**

### C.3 Tests

- **Rust unit (chili-parser):** add `tests/test_pepper_top_level_semicolon.rs` with three shapes:
  1. `a: 1; b: 2` parses (no trailing `;`)
  2. `a: 1; b: 2;` parses (trailing `;`)
  3. `a: 1;; b: 2` — should it parse (empty statement between) or error? **Decide before implementing.** mdata's example has no double `;` so either is defensible; safer to error.
- **chili-py pytest:** mdata's acceptance test:
  ```python
  engine.eval("a: 1; b: 2; c: a + b")
  assert engine.get_var("c") == 3
  ```

### C.4 Sizing

**Predicted: 3–5 pp.** Parser production change + 2-4 Rust unit tests + 1 Python pytest + regression scan of existing pepper usage. Risk-driven mid-range estimate; could be 2pp if the grammar change is one line.

---

## Out of scope (deferred to Sprint 17)

| Item | Why deferred |
|---|---|
| **P0 par_df Parquet write fsync** | mdata wishlist explicitly asks for tplog fsync only. If they also want par_df fsync for full §5.1 part-2 durability, surface that question to them as part of Sprint 16 wrap. Not committing without confirmation. |
| **P1 subscriber-side `eod` dispatch** | Verification surfaced that `handle_chili_conn` ALREADY calls `state.eval(stack, msg, src_path)` on incoming messages and `engine_state.eval` ALREADY dispatches MixedList → `eval_op`. So mdata's diagnosis (the handler doesn't dispatch) may be incomplete — there could be a different bug (eval_op behavior on symbol-headed list, namespace isolation, stack handle wiring). **Need a live xfail reproduction in chili-py tests before drafting the fix.** Sprint 17 Part A. |
| **P1 `engine.publish_remote()` / `RemoteTpClient`** | Wire protocol marshalling already exists in `sync(h, msg)` at `engine_state.rs:932` — a remote publish is essentially `open_handle("chili://...") + sync(h, (`upd; table; df))`. But mdata's Open Q #3 asks whether chili or mdata should own the TCP client. **Need scope conversation with mdata before drafting the API.** Sprint 17 Part B (or split sprint if chili-owned client is large). |

---

## Deliverables table

| # | Surface | Crate(s) | Surface size | Tests | Owner |
|---|---|---|---|---|---|
| A.1 | `ReadWrite::sync_all()` trait method + `fs::File` override | chili-core | ~20 LOC | rust unit | coordinator |
| A.2 | `EngineState::flush_handle()` | chili-core | ~30 LOC | rust unit | coordinator |
| A.3 | bytes-since-last-flush tracking on Handle | chili-core | ~10 LOC | covered by A.2 test | coordinator |
| A.4 | `engine.flush_tplog()` Python | chili-py | ~15 LOC | pytest | coordinator |
| B.1 | `EngineStatePy::add_at_time` PyO3 | chili-py | ~25 LOC | pytest | coordinator |
| B.2 | `engine.add_at_time()` Python wrapper | chili-py | ~10 LOC | pytest | coordinator |
| C.1 | `parser_pepper` top-level `;` production | chili-parser | ~5-15 LOC | rust unit + pytest | tester subagent (parser changes warrant a fresh-context reviewer) |
| Wrap | Sprint 16 retro + cadence-metrics row | docs/sim/ | — | — | coordinator |

---

## Lead allocation

- **Coordinator-solo** for Part A + Part B. Both are well-scoped chili-core+chili-py additions following existing conventions. No subagent fan-out needed.
- **`tester` subagent** for Part C — parser changes have subtle risk (the dict/list/arg-separator `;` is already meaningful), and a fresh-context reviewer is more likely to catch grammar regressions than coordinator-solo. Spawn after Part A + B land; serial dependency.
- **`docs` subagent (optional)** if Sprint 16 wrap requires updating `docs/sync/chili_wishlist_2026-05-13_response.md` cross-references to mdata's wishlist. Light if needed.

No worktree fanout this sprint — three sequential bundles, all in-tree.

---

## Mid-checkpoint plan

After Part A lands (~4-6pp in):

- **Gate 1** — `cargo test --workspace --exclude chili-py` green AND `uv run pytest` green for chili-py.
- **Gate 2** — manually verify the SIGKILL durability path: publish 10 rows, call `flush_tplog`, hard-kill the Python interpreter, restart, read tplog from disk, assert 10 rows recoverable. (Mdata will own the formal test; we just smoke-check.)
- **Halt-and-escalate triggers:**
  1. The trait extension breaks the existing TCP / non-File ReadWrite impls (compile failure or semantic shift). → halt; re-scope.
  2. Bytes-since-last-flush semantics don't match what mdata expected. → escalate to user; surface for mdata clarification.
  3. SIGKILL durability test FAILS even with flush_tplog called. → halt; investigate (potential page-cache layer issue beyond fs::File::sync_all).
  4. Pp burn at Part A end > 7pp. → halt; defer Part B + C to Sprint 17 to avoid run-overs.

---

## Wrap ceremony

Per `.claude/rules/sprint-cadence.md`:

1. Sprint retro at `docs/sim/sprint_16_retro.md` (template at `docs/sim/_retro_template.md`).
2. Append row to `docs/sim/cadence_metrics.md` (10 fields).
3. Move this brief to `docs/history/sprints/sprint_16_dispatch_brief_2026-05-13.md`.
4. Update `docs/sim/sprints_index.md` with the new sprint row.
5. Update `CLAUDE.md` project-state row if test counts change.
6. Surface to user: any unresolved mid-checkpoint escalations.
7. **If Sprint 17 is to start in the same session**, escalate the deferred questions to mdata (Open Q #3 for publish_remote scope; tplog-vs-par_df scope for full durability) — these answers gate Sprint 17.

---

## Pp accounting reference

| Part | Predicted pp |
|---|---|
| A (P0 flush_tplog) | 4–6 |
| B (P3 add_at_time) | 2–3 |
| C (P2 REPL `;`) | 3–5 |
| Wrap + retro | 0.5–1 |
| **Total** | **9.5–15** |

Aligns with the Sprint 14 / 15 range (8–12pp implementation sprints). Compare to `docs/sim/cadence_metrics.md` after wrap.

---

## Cross-references

- mdata wishlist (source of all three asks): `~/code/mdata/docs/sync/chili_wishlist_2026-05-13.md`
- Sprint 14 + 15 dispatch briefs (most recent precedent for shape): `docs/history/sprints/sprint_15_dispatch_brief_2026-05-09.md`
- Tplog handle definition: `crates/chili-py/chili/src/tick.pep:1-11`
- ReadWrite trait + Handle struct: `crates/chili-core/src/engine_state.rs:62-74`
- job::add_at_time implementation: `crates/chili-core/src/job.rs:96-122`
- Pepper parser entry: `crates/chili-parser/src/expr.rs:559`
- Pepper parser dispatch (by file extension): `crates/chili-core/src/parser.rs:680-689`

---

## Appendix — Independent audit (2026-05-13)

Per `~/.claude/rules/self-audit-on-plans.md` — 3-agent parallel audit on the
above draft before kickoff. Agents: Explore (codebase scan), code-reviewer
(adversarial verification), planner (sequencing + sizing). Two CRITICAL,
four MAJOR, four MINOR, one NIT.

### Material corrections (sprint shape changes)

#### CRITICAL-1 — Part A trait-extension approach will not compile

**Convergent finding (code-reviewer C1, planner finding #2).** The brief
proposes:

```rust
pub trait ReadWrite: Read + Write + Send + Sync { fn sync_all(&self) -> io::Result<()> { Ok(()) } }
impl<T: Read + Write + Send + Sync> ReadWrite for T {}   // existing blanket
impl ReadWrite for fs::File { fn sync_all(&self) -> io::Result<()> { fs::File::sync_all(self) } }   // new override
```

Rust's coherence rules reject the third line with E0119: a specific impl
cannot override a blanket impl when both are in the same crate (this is not
an edition / 2024 subtlety — it fails on any Rust version). Verified by
reading `crates/chili-core/src/engine_state.rs:62-64`:

```rust
pub trait ReadWrite: Read + Write + Send + Sync {}
impl<T: Read + Write + Send + Sync> ReadWrite for T {}
```

**Revised approach (pick one before Part A kickoff):**

- **Option α (recommended)** — *Don't extend the trait.* Add a separate
  `EngineState::flush_file_handle(h: &i64) -> SpicyResult<i64>` that
  pattern-matches on `Handle.conn_type ∈ {New, File, Sequence}` (the
  file:// branch types) and **downcasts** the `&dyn ReadWrite` to `&fs::File`
  via `Any` trickery, OR more cleanly, change `Handle` to store an enum
  `HandleRw::File(fs::File) | HandleRw::Net(Box<dyn ReadWrite>)` so the
  file:// path retains a typed `fs::File` for `sync_all` calls.
- **Option β** — Remove the blanket impl, enumerate per-type:
  `impl ReadWrite for fs::File`, `impl ReadWrite for TcpStream`,
  `impl<R: Read+Write+Send+Sync> ReadWrite for BufWriter<R>` etc. More
  invasive; requires updating every Handle construction site. Audit found
  three construction sites: `engine_state.rs:845` (`set_handle`), `:895`
  (subscriber redirect), `:766` (Outgoing conn_type setter).
- **Option γ (sealed helper trait)** — Add a separate `SyncAll` trait
  with conditional impls. More boilerplate for the same outcome.

**Recommendation:** Option α (enum on Handle). Smallest blast radius. The
file:// branch already has its own ConnType variants; an enum split is
natural.

**Cost impact:** Part A revised pp 5–8 (was 4–6); +1–2 pp for the design
rework.

#### CRITICAL-2 — Part C may have no parser change to make

**Convergent finding (all three agents — Explore CRITICAL, code-reviewer M1,
planner finding #1).** Verified by reading `crates/chili-parser/src/expr.rs:1025-1038`:

```rust
let terminated_statement = statement.clone().then_ignore(just(Token::Punc(';')));
terminated_statement.clone().repeated().collect::<Vec<_>>()
    .then(statement.clone().or_not())
    .map_with(|(mut v, last), e| { ... Expr::Block(...) })
```

The pepper top-level production **already** accepts `;` as a statement
separator: zero-or-more `;`-terminated statements followed by an optional
unterminated final statement. So `a: 1; b: 2; c: a + b` *should* already
parse. mdata's claim "pepper REPL mode doesn't accept `;` as a top-level
statement separator" may be incorrect, or the failure they hit is in a
different layer (e.g., `engine.eval()` Python wrapper marshalling, or the
eval dispatch returning only the last value).

**Revised Part C scope:**

1. **Reproduce mdata's failure first** — write a Rust unit test calling
   `parser_pepper().parse("a: 1; b: 2; c: a + b")` AND a chili-py pytest
   calling `engine.eval("a: 1; b: 2; c: a + b")`. Compare outputs.
2. **Three possible outcomes:**
   - Both succeed: mdata's wishlist claim is wrong. Surface to them. Part C
     ships as a single docs commit clarifying the grammar already supports
     this. **~1 pp.**
   - Rust parses but `engine.eval` Python wrapper rejects: the bug is in
     chili-py's `engine.py` (string handling) or in chili-core's `eval()`
     path. Investigate before any parser edit. **~3–5 pp.**
   - Rust parser actually rejects (unlikely given the grammar above):
     identify the precise rejecting production and fix. **~3–5 pp.**
3. **Sprint 16 ships whichever outcome applies** — do NOT pre-commit to a
   grammar change. The brief's claim "fix site is `expr.rs:559`" is wrong
   regardless — that's just the `parser_pepper` function signature; the
   real top-level production is at line ~1025.

**Cost impact:** Part C revised pp 1–6 (was 3–5); wider band reflecting
the reproduction-first uncertainty.

### Additional MAJOR findings

- **`get_var(".tick.msgHandle")` raises NameError, doesn't return None** (code-reviewer M2).
  The Python wrapper's `if h is None: raise RuntimeError(...)` is dead code —
  `get_var` returns `Err(SpicyError::NameErr)` from `engine_state.rs:253-258`
  which PyO3 propagates as `NameError`, not as `None`. Fix: either
  `try: ... except NameError:` or check `engine.has_var(".tick.msgHandle")`
  first. **No pp impact** (logic fix, same surface size).
- **Handle struct + AtomicU64 racing with concurrent writes** (Explore MAJOR).
  Adding `bytes_since_flush: AtomicU64` to Handle requires updating every
  `rw.write_all` call site (Explore identified `engine_state.rs:948, 970`)
  to increment the counter. Easy to miss in `handle_chili_conn` /
  `broker.rs`. Recommend a `Handle::write_tracked()` helper. **+0.5 pp.**
- **No 0.8.4 wheel-cut deliverable** (planner finding #4). mdata can't
  flip their xfail tests until they receive a new wheel. Add to deliverables
  table + wrap ceremony. **+0.5–1 pp.**
- **Sprint 16 wrap must trigger every-5-sprint housekeeping** (planner #3).
  Last sweep was Sprint 11. Per `.claude/rules/sprint-cadence.md`, Sprint 16
  wrap is rule-mandated to trigger a deep housekeeping sweep. Brief omits
  this entirely. **+1–2 pp** (housekeeping cost; could spill into a
  Sprint 16.5 if time-constrained, matching Sprint 13.5 pattern).

### Additional MINOR findings

- **`signal_eod` line cite off by 2** (code-reviewer m1) — brief says
  `engine_state.rs:1146`; actual line is 1144. Update brief cross-ref
  section.
- **`&PyDateTime` deprecated in pyo3 0.27** (code-reviewer m3) — Part B's
  PyO3 signature should use `Bound<'_, PyDateTime>` per pyo3 0.27 API.
  Verify against existing `EngineStatePy` patterns in `chili-py/src/lib.rs`.
- **Trigger 4 threshold 7pp too tight** (planner #5) — Part A predicted
  4–6pp; halt threshold should be >8pp (33% slack vs 17%). Change.
- **`/tmp/polars-py-1.39.3` missing from kickoff gates** (planner #6) —
  CLAUDE.md P0 backlog. Add as kickoff gate: `ls /tmp/polars-py-1.39.3/crates/polars/Cargo.toml` must succeed.

### NIT findings

- **`tester` is wrong subagent for Part C investigation** (planner #7) —
  before reproduction confirms the layer, the work is investigation, not
  test-writing. Change Part C lead to **coordinator-solo** (investigate,
  reproduce, then escalate to a subagent only if a Rust parser change
  turns out to be needed).

### Revised sequencing

| # | Part | Predicted pp (revised) | Predicted pp (original) | Note |
|---|---|---|---|---|
| Pre-kickoff gates | — | 0.5 | 0 | Add: `/tmp/polars-py-1.39.3` check + reproduce P2 failure |
| A (P0 flush_tplog) | 5–8 | 4–6 | +1–2pp for trait coherence rework |
| B (P3 add_at_time) | 2–3 | 2–3 | Unchanged (pyo3 0.27 API check is in scope) |
| C (P2 REPL `;`) | 1–6 | 3–5 | Wider band; reproduce-first |
| Wheel cut + handoff doc | 0.5–1 | 0 | NEW — required for mdata acceptance test flip |
| Wrap + retro | 0.5–1 | 0.5–1 | Unchanged |
| Deep housekeeping (sprint 11+5 rule) | 1–2 | 0 | NEW — rule-mandated; could spill to Sprint 16.5 |
| **Total** | **10.5–21.5** | **9.5–15** | Wider band; honest mid-range ≈ 14pp |

### Cross-cutting gates (add to brief)

1. **Kickoff gate K1:** `/tmp/polars-py-1.39.3/crates/polars/Cargo.toml` exists. If missing, halt and restore via `vendor/polars-core/README.md` recovery protocol.
2. **Kickoff gate K2:** rustc ≥ 1.95 (`rustc --version | grep -E '1\.(9[5-9]|[0-9]{3,})'`).
3. **Pre-Part-A gate:** resolve trait-coherence design (Option α / β / γ above). Don't write code until the resolution strategy is settled.
4. **Pre-Part-C gate:** reproduce mdata's `engine.eval("a: 1; b: 2; c: a + b")` failure in a chili-py pytest before any parser change.
5. **Pre-commit gate:** unchanged (fmt + clippy + test, including the chili-py gate; rustc 1.95-aware now).

### Sprint sizing realism

Comparison to last 3 ratified sprints (`docs/sim/cadence_metrics.md`):
- Sprint 14: predicted 5–9 pp, actual 5 pp (low-edge finish).
- Sprint 15: predicted 8–10 pp, actual ≈9 pp (midpoint).
- Sprint 13: predicted 6–10 pp, actual 3 pp (REVERTED at midpoint).

Sprint 16's revised 10.5–21.5 pp band is wider than recent sprints. The width
reflects two real unknowns: Part A trait coherence design choice + Part C
reproduction-first uncertainty. If both resolve favorably (Option α straightforward, P2 already-works outcome), sprint lands ≈10pp. If both go hard, ≈18–20pp — at which point Sprint 16.5 housekeeping spillover is likely.

**Recommendation:** Brief stays as-is but adopt the appendix's cross-cutting gates + revised sizing. Kickoff with K1 + K2 verified; resolve Part A design before code; reproduce Part C failure before code. If pp budget at 14pp threatens overrun, defer Part C to Sprint 17 (it's the most uncertain).

---

## Appendix 2 — mdata reply lock-in (2026-05-13)

Source: `~/code/mdata/docs/sync/chili_wishlist_2026-05-13_mdata_reply.md`.
All four clarification questions answered. Wheel sha256 cross-check passed
(0.8.3 wheel byte-identical between chili build output and mdata's pinned
hash — no drift trip-wire).

### Q1 — fsync scope: LOCKED to (a) tplog only

par_df Parquet writes are wdb's responsibility per mdata PRD §3.3: wdb runs
`os.fsync(fd)` itself on the file it wrote via `chili.write_partitioned_df`.
chili doesn't need to fsync par_df — redundant + cross-cuts wdb's
`latest_durable_seq` accounting.

**Part A scope unchanged.** `engine.flush_tplog() -> int` returns
bytes-since-last-flush. Predicted pp band stays 5–8.

### Q2 — `;` separator: LOCKED, scope tightens significantly

mdata confirmed both data points. Reproduced locally on chili-sauce 0.8.3:

| Case | Result |
|---|---|
| `engine.eval("a: 1; b: 2; c: a + b")` | **ACCEPTS** → c = 3. Audit's reading of `expr.rs:1025-1038` was right. |
| `engine.eval(".sub.eod.fired: ::; eod: {...}")` | **REJECTS** at col 19 with `found 'Punc';' expected arguments` while parsing binary. |
| `engine.eval("x: ::; y: 1")` (predicted minimum) | **REJECTS** at col 6 with same "expected arguments" parsing-binary error. |

**Root cause** (verified): `::` is being parsed as a *binary operator* expecting RHS arguments, then encounters `;` and the binary-arg production rejects. The bug is specifically the `::` (null-literal) vs `:: <expr>` (binary continuation) ambiguity.

**Revised Part C scope (narrow):** Disambiguate the `::` null-literal token from the binary-operator continuation so the parser knows where the `::` arg list ends before `;` is consumed. This is **not** a top-level statement-production change (the audit was right — that production already accepts `;`); it's a token-level disambiguation in the `::` production.

**Acceptance test:** mdata is authoring `test_pepper_syntax.py::test_null_literal_semicolon_disambiguation` against the actual repro shape — narrower than the wishlist's original "accept `;` everywhere" test.

**Part C predicted pp band tightens to 2–4** (was 1–6). The narrower-scope estimate makes Sprint 16's risk band shrink:
- Original audit total: 10.5–21.5 pp
- **Post-lock-in total: 8.5–18 pp** (honest mid-range ~11–13 pp)

**Lead change:** keep `coordinator-solo` (per audit NIT) — the work is now a clear token-disambiguation fix, not an investigation. Reproduce + minimal grammar tweak.

### Q3 — `publish_remote` API: LOCKED to (b) thin marshalling, Sprint 17 saves ~10 pp

mdata reverses their wishlist preference: chili ships `engine.publish_via_handle(h, table, df)` as a thin one-shot publish primitive. mdata writes the `RemoteTpClient` connection-manager class on their side (~50–80 LOC, mdata-internal `src/mdata/feed/remote_tp_client.py`).

**Sprint 17 P1-publish scope drops to ~8 pp** (was 15–25). Surface is just:

```rust
pub fn publish_via_handle(&self, h: &i64, table: &str, df: &SpicyObj) -> SpicyResult<()> {
    // Marshals MixedList[`upd, table, df] via existing sync(h, msg)
    self.sync(h, &SpicyObj::MixedList(vec![
        SpicyObj::Symbol("upd".into()),
        SpicyObj::Symbol(table.into()),
        df.clone(),
    ])).map(|_| ())
}
```

Plus chili-py wrapper. Sprint 17 Part B will draft against this scope.

### Q4 — Subscriber `eod` dispatch: copy-pasteable repro provided

mdata provided full failing test source (`test_subscriber_eod_shim_triggered_by_publisher_eod`), shim source (`eod: {[msg] .sub.eod.fired: msg}`), and boot order (eval-defines-eod BEFORE engine.subscribe). Key facts:

1. Shim is registered via `engine.eval` from main Python thread BEFORE subscribe opens the IPC handle — so the global var is set before any IPC traffic.
2. Subscriber thread has `Stack::new(None, 0, handle, user)` (fresh stack) but shares `Arc<EngineState>` — global vars from main thread should be visible.
3. `_check_eod` polls `get_var(".sub.eod.fired")` — always raises `NameError`, never returns a value. Meaning `.sub.eod.fired` is never written by the eod shim. Meaning `eod` was never invoked despite `(eod; date)` arriving at the subscriber engine.

**This isolates the bug to hypothesis (1) from our audit:** `eval_op(MixedList[Symbol("eod"), date])` does NOT dispatch as function-call. Sprint 17 P1-eod-dispatch becomes a clear scope: special-case the chili IPC handler `handle_chili_conn` (or `engine_state.eval_op`) to recognize symbol-headed MixedList on incoming Subscribing-conn messages and invoke as `eod[date]`.

mdata committed to copy the test into chili's tree if helpful. Sprint 17 will likely just port the test into chili-py's pytest suite as the load-bearing acceptance test.

### Bonus signals from mdata's reply

- **0.8.3 wheel sha256 confirmed identical** between chili build (`6345fcac...`) and mdata's pinned hash. No drift; chili-side bug repro will match what mdata observes in prod daemons. Future wheels should preserve this property.
- **mdata's cadence_metrics:** 7-sprint v1-arc (v1-14 → v1-20) landed ~28 pp actual vs 29–43 pp predicted (−18% to −38% under). Their coordinator-solo `first_of_kind` model consistently under-runs. Their suggestion: Sprint 16 might land 6–8 pp on chili side. Our cadence model differs (we don't have a `first_of_kind` axis); we keep our 8.5–18 pp band but note theirs as a data point.
- **chili-side `first_of_kind` data point comparison:** Sprint 14 was a chili-side first-of-kind (GIL release on direct-FFI) and landed 5 pp against a 5–9 pp prediction — also low-edge, matching mdata's pattern. Suggests chili-side `first_of_kind` sprints similarly under-run.
- **Capability inventory pointer:** `~/code/mdata/docs/standards/chili_capability_inventory.md` § 1-5 useful for Sprint 18+ scoping. §3 notes IPC message size limit ~10MB-ish per `sync` call before stalls — worth knowing for `publish_via_handle` if mdata sends large DataFrames.

### Locked Sprint 16 scope (final)

| # | Surface | Predicted pp (locked) |
|---|---|---|
| Pre-kickoff gates K1 + K2 | 0.2 | 0.2 |
| Part A — `engine.flush_tplog()` + trait coherence design (Option α/β/γ) | 5–8 | 5–8 |
| Part B — `engine.add_at_time()` PyO3 binding | 2–3 | 2–3 |
| **Part C — `::` null-literal/binary-arg disambiguation** | **2–4** | (was 1–6) |
| 0.8.4 wheel cut + handoff doc | 0.5–1 | 0.5–1 |
| Wrap + retro + every-5-sprint housekeeping trigger | 1–2 | 1–2 |
| **Total** | **10.7–18.2 pp** | (was 10.5–21.5) |

**Kickoff status: READY.** All audit findings resolved; all four mdata answers locked.
