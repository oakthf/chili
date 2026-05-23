# Sprint 22 dispatch brief — mdata wishlist 2026-05-23 (W1 + W2; W3 deferred)

**Kickoff:** TBD — user go-ahead post audit-appendix review
**Owner:** coordinator-solo (no subagent fanout; surface is small)
**Type:** implementation (2 small additive features + 1 wheel cut + delivery)
**Predicted pp:** 7–13 (mid 10, post-audit)
**Plan reference:** `docs/sync/mdata_wishlist_2026-05-23_remote-eval-surface.md` (authoritative chili-side copy; mdata-side `~/code/mdata/docs/sync/chili_wishlist_2026-05-23_remote-eval-surface.md`). Wishlist also arrived as bus event 17614 (correlation `chili-wishlist-2026-05-23-remote-eval-surface`).
**ADR references:** none new (W1+W2 are non-architectural; W3 deferral noted in "Out of scope")

---

## Sprint objective

Close two of mdata's three v1-36-cutover gaps in a single 0.8.8 wheel:

- **W1** add an `eval_str` pepper builtin so mdata can `engine.sync(h, (Symbol("eval_str"), "<pepper source>"))` and get arbitrary remote eval — unblocks the chili-IPC `qcon` REPL across all 7 mdata daemons.
- **W2** make `start_tcp_listener`'s accept loop tolerant of bare TCP connect-and-close (no chili handshake bytes) — unblocks mdata's `dis` liveness probe.

**Binary success criterion:** the 0.8.8 wheel is built + sha-recorded + delivered via `docs/sync/mdata_chili_2026-05-23_0.8.8_delivery.md`; mdata can (1) call `sync(h, (Symbol("eval_str"), "1+1"))` and receive `SpicyObj::I64(2)` via the existing IPC roundtrip, and (2) `socket.connect((host, port)); socket.close()` against the chili port without crashing the listener thread; the listener continues accepting subsequent legitimate handshakes.

---

## Why now

- mdata sent the wishlist 2026-05-23 (bus event 17614). W1+W2 specifically named as the "1 mdata sprint = full attach-socket retirement" bundle. mdata's mock-prod (Mon 2026-05-26) is NOT blocked by this; v1-36+ cutover is.
- chili-side surface verified small: W1 ≈ 10 LOC + 1 registration + 1 pytest; W2 ≈ 11 `.unwrap()` + 1 `panic!` to convert in a single accept loop + 1 pytest. Pre-conditions for both are already in tree.
- The fn_call I64 dispatch arm (the chain `engine.sync` → `fn_call` → `eval_call` → `state.sync`) is guard-tested by `crates/chili-core/tests/fn_call_i64_test.rs:28-67` — adding `eval_str` does NOT need new dispatch wiring; it just becomes a callable target.
- W3 (Python-callable-as-pepper-fn) is structurally hostile to golden rule 5 (GIL released around `Engine::eval`) — re-introducing Python callbacks on the pepper hot path would re-introduce contention that Sprint 7's 6.10× concurrent win specifically eliminated. mdata explicitly flagged poll-on-variable as their workaround.

---

## Scope — Part A: W1 `eval_str` pepper builtin

### A.1 Surface additions

One new pepper builtin, registered in `crates/chili-core/src/side_effect_fn.rs`'s `SIDE_EFFECT_FN` table. No chili-py FFI addition needed — `engine.sync(h, msg)` (`crates/chili-py/chili/engine.py:650`) already routes via `fn_call`, and `fn_call`'s I64 arm (`engine_state.rs:1942-1951`) dispatches arbitrary `Func`s to `eval_call → state.sync`.

```rust
// crates/chili-core/src/eval.rs — new fn, ~10 LOC
pub fn eval_str(
    state: &EngineState,
    _stack: &mut Stack,
    args: &[&SpicyObj],
) -> SpicyResult<SpicyObj> {
    validate_args(args, &[ArgType::Str])?;
    let src = args[0].str()?;
    let ast = state
        .parse("", src)
        .map_err(|e| SpicyError::EvalErr(e.to_string()))?;
    state.eval_ast(ast, "", src)   // returns raw SpicyObj, no .to_string()
}

// crates/chili-core/src/side_effect_fn.rs — new entry in SIDE_EFFECT_FN
(
    "eval_str".to_owned(),
    Func::new_side_effect_built_in_fn(
        Some(Box::new(eval_str)),
        1,
        "eval_str",
        &["string"],
    ),
),
```

### A.2 Implementation hints

- The behavioral delta vs `evalc` (`eval.rs:466-478`) is exactly the trailing `Ok(SpicyObj::String(obj.to_string()))` → `Ok(obj)`. evalc was designed for console pretty-printing; eval_str returns the raw object so mdata can apply Polars / pepper operations on the result of a remote query.
- Naming: `eval_str` matches mdata's request verbatim. Avoid `eval2` / `reval` / `r_eval` — mdata's wishlist cites the exact symbol.
- Do NOT use `evali` even though it exists — `evali` row-limits DataFrames and is designed for an IDE preview pane. mdata wants the full result (or whatever the pepper expression evaluates to, including non-tabular results).
- The `state.parse("", src)` 1st-arg empty string is the file path — `evalc` / `evali` both use it. Source-line attribution will show "" if `eval_str` errors; acceptable for v1 (mdata can wrap their own error context).

### A.3 Storage / schema — N/A

Builtin-addition only. Zero impact on golden rule 4 (Int64-quantized price columns).

### A.4 Tests

- **Rust unit test (`crates/chili-core/tests/eval_str_test.rs` — NEW):** ~3 tests, ~50 LOC.
  - `eval_str_evaluates_pepper_source_and_returns_raw_object`: `state.fn_call("eval_str", &[&SpicyObj::String("1+1".to_owned())])` returns `SpicyObj::I64(2)`, not `SpicyObj::String("2")`. (verify-before-claim guard: this is the contract delta vs evalc.)
  - `eval_str_errors_on_non_string_arg`: passing `SpicyObj::I64(0)` returns `EvalErr`, not panic.
  - `eval_str_errors_on_parse_failure`: passing `"invalid )(*&"` returns `EvalErr` with the parser's message; does not panic.
- **Python pytest (`crates/chili-py/tests/test_eval_str.py` — NEW):** ~2 tests, ~60 LOC.
  - `test_sync_eval_str_simple`: open a `file://` handle + `engine.sync(h, (Symbol("eval_str"), "1+1"))` returns `2`. Mirrors `fn_call_i64_test.rs` Python-side.
  - `test_sync_eval_str_select_round_trip`: define a small table on the remote side (via `eval_str` itself), then query it via a second `eval_str` call. Validates result-as-DataFrame round-trip.

---

## Scope — Part B: W2 graceful TCP-connect on `start_tcp_listener`

### B.1 Surface additions

No new public surface. Behavior change inside `EngineState::start_tcp_listener` (`engine_state.rs:2581-2660`). The 11 `.unwrap()` + 1 `panic!` in the accept loop convert to `match` / `if let Err(e)` + `info!(...)` + `continue` so a single bad connection (closed mid-handshake, wrong version byte, etc.) cannot kill the listener thread.

### B.2 Implementation hints

Exhaustive inventory of panic sites in the accept loop (`engine_state.rs:2598-2659`) — each is a separate fix:

| Line | Construct | Fix |
|---|---|---|
| 2600 | `let mut stream = stream.unwrap();` | `let mut stream = match stream { Ok(s) => s, Err(e) => { info!("accept failed: {e}"); continue; } };` |
| 2606 | `stream.peer_addr().unwrap()` (in auth-fail log) | Use `.ok().map(\|a\| a.to_string()).unwrap_or_else(\|\| "<unknown>".into())` to log a placeholder if the socket already dropped. |
| 2608 | `stream.shutdown(...).unwrap()` | `let _ = stream.shutdown(...);` (best-effort; the socket may already be closed by the peer). |
| 2614 | `stream.peer_addr().unwrap()` (in success log) | same as 2606. |
| 2617/2619 | `stream.write_all(&[6\|9]).unwrap()` | `if let Err(e) = stream.write_all(...) { info!("version-byte write failed: {e}"); continue; }` |
| 2622 | `stream.set_nodelay(true).unwrap()` | `if let Err(e) = stream.set_nodelay(true) { info!("set_nodelay failed: {e}; continuing"); }` (nodelay failure is non-fatal). |
| 2623 | `let peer_addr = stream.peer_addr().unwrap().to_string();` | hoist into a `match`; on Err, `continue;` (we need peer_addr for set_handle URL). |
| 2625 | `.unwrap_or_else(\|\| panic!("unsupported ipc version: ..."))` | log + continue. The version byte was already validated by `validate_auth_token`; this panic is defensive but mis-shaped (should not abort the listener). |
| 2628 | `stream.try_clone().unwrap()` (inside `Box::new`) | `match stream.try_clone()` → `Some(Box::new(s))` / `None + continue`. |
| 2636 | `set_handle(...).unwrap()` | `if let Err(e) = ... { info!("set_handle failed: {e}"); continue; }` |
| 2643 / 2653 | `h.to_i64().unwrap()` (inside `thread::spawn`) | These are in spawned threads — a panic isolates to that thread. Convert anyway so the spawned handler logs cleanly and exits without `RUST_BACKTRACE` noise; replace with `.expect("handle slot returned non-i64; logic bug")` or `match` + early return. **Lower priority than the accept-loop unwraps**, since they don't kill the listener thread itself. |

Adjacent surface to audit but **NOT scope** for Sprint 22:
- `validate_auth_token` (referenced at line 2601) — if it `.unwrap()`s internally on socket reads, the same bare-TCP-connect path may panic there before reaching the accept-loop fixes. The implementer MUST read `validate_auth_token` end-to-end during Part B kickoff and either confirm it's already tolerant or flag as a halt-and-escalate (criterion #1, scope-blowing bug).
- `utils::handle_chili_conn` / `utils::handle_q_conn` (the spawned-thread bodies) — out of scope; a per-connection thread crash is acceptable.

### B.3 Storage / schema — N/A

Network-layer behavior change. Zero impact on storage schema or FFI surface.

### B.4 Tests

- **Rust integration test (`crates/chili-core/tests/tcp_listener_graceful_test.rs` — NEW):** ~2 tests, ~80 LOC.
  - `bare_tcp_connect_close_does_not_kill_listener`: spawn a chili `EngineState`, `start_tcp_listener` on a free port (use `std::net::TcpListener::bind("127.0.0.1:0")` then pass the port), then in a loop: open a `TcpStream::connect(addr)`, drop it immediately. Repeat 10×. After, open a legitimate connection (manual handshake bytes via `open_handle` from a second `EngineState`) — assert it succeeds. The listener thread must still be alive.
  - `bad_version_byte_does_not_kill_listener`: connect, send 1 byte = 0xFF (unsupported), drop. Confirm listener still accepts a subsequent good connection.
- **Python pytest:** none required. The Rust integration test exercises the network path end-to-end with no chili-py dependency.

---

## Out of scope (defer)

- **W3 (P2) Python-callable as pepper fn** — `engine.set_var(name, python_callable)` / `register_fn`. Structurally hostile to golden rule 5 (GIL released around `Engine::eval`); pepper hot path would need to re-acquire the GIL on every invocation, re-introducing the Sprint-7 contention. mdata flagged poll-on-variable as workaround. **Decision:** ack receipt to mdata in the delivery doc; do NOT open an ADR placeholder yet — if/when the attach-socket cutover specifically blocks on it, a dedicated design sprint with an ADR is warranted, not a tactical addition.
- **Adjacent unwraps in `validate_auth_token`, `handle_chili_conn`, `handle_q_conn`** — flagged in B.2 as halt-and-escalate trigger if `validate_auth_token` is not already tolerant; otherwise treated as separate future work.
- **`evalc` / `evali` deprecation** — neither is removed. Both have specific use cases (console pretty-print, IDE preview). `eval_str` is purely additive.
- **Source-line attribution for `eval_str` errors** — `state.parse("", src)` passes an empty file path; errors will show "" as the file. Acceptable for v1; mdata can wrap with their own context if they need richer messages.

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | `crates/chili-core/src/eval.rs` (+`pub fn eval_str`) | edit |
| 2 | `crates/chili-core/src/side_effect_fn.rs` (+`SIDE_EFFECT_FN` entry) | edit |
| 3 | `crates/chili-core/tests/eval_str_test.rs` | new |
| 4 | `crates/chili-py/tests/test_eval_str.py` | new |
| 5 | `crates/chili-core/src/engine_state.rs` (`start_tcp_listener` panic-site fixes, ~11 unwraps + 1 panic) | edit |
| 6 | `crates/chili-core/tests/tcp_listener_graceful_test.rs` | new |
| 7 | `Cargo.toml` `[workspace.package] version = "0.8.8"` | edit |
| 8 | `crates/chili-py/Cargo.toml` `version = "0.8.8"` | edit |
| 9 | `crates/chili-py/pyproject.toml` `[project] version = "0.8.8"` | edit (maturin reads wheel version from HERE, not Cargo.toml — lesson 14) |
| 10 | `dist/chili_sauce-0.8.8-cp310-abi3-macosx_11_0_arm64.whl` | new |
| 11 | `docs/sync/mdata_chili_2026-05-23_0.8.8_delivery.md` | new |
| 12 | Cross-comms reply to mdata via direct-WS bypass (bus bridge bug is open; vantage-cto event 17615) | publish |
| 13 | `docs/sim/sprint_22_retro.md` | new (post-sprint) |
| 14 | `docs/sim/cadence_metrics.md` row | append (post-sprint) |
| 15 | Move this brief to `docs/history/sprints/sprint_22_dispatch_brief_2026-05-23.md` post-ratification | mv |

---

## Lead allocation

**Coordinator-solo.** The surface is small enough (≈ 100-150 LOC across 4 files + tests + wheel-cut chore) that subagent fanout would cost more than it saves. Per `cadence_metrics.md` Pattern 6, autonomous-run sprints of this shape underrun their predicted band; budget the upper edge for the wheel-cut + delivery doc.

Code-reviewer subagent dispatched once, post-impl, pre-wheel-cut (lesson 7 + Sprint-15/16/17/18/21 G2 spirit). No worktree; commits land on `claude-2`.

---

## Mid-checkpoint plan

At ~50% predicted pp (~5pp) consumed, post a short status:

- W1 builtin + tests landing on the gate? (`cargo test -p chili-core` + `uv run pytest -k eval_str`)
- W2 accept loop fixes — did `validate_auth_token` turn out to need its own pass? (halt-and-escalate trigger 1 if yes)
- ETA to wrap + wheel + delivery doc.

**Halt-and-escalate criteria:**

1. **Scope-blowing bug** — `validate_auth_token` (or another deeper layer) panics on bare TCP connect-close and the fix cascade pushes actual pp > 15. Surface to user; user may decide to land W1 only this sprint.
2. **Plan-pivot finding** — chili-side `sync(h, ...)` does NOT dispatch the new `eval_str` builtin remotely (i.e., the assumed end-to-end path `engine.sync` → `fn_call` → `eval_call` → remote `state.sync` → remote dispatch into `SIDE_EFFECT_FN["eval_str"]` doesn't fire as expected). Pretty unlikely given `fn_call_i64_test.rs` proves the dispatch chain, but verify-before-claim insists on the end-to-end pytest before declaring W1 done.
3. **User-decision needed** — only architectural choice that could surface: whether `eval_str` should be allowed to mutate state (it can today — `state.eval_ast` is fully privileged). If mdata-side use case implies read-only-eval, an ADR is warranted; not assumed currently.
4. **Watchdog approaching** — 5h ≥ 80% AND remaining work > 15pp.

---

## Wrap (per ceremony)

- Pre-commit gate green: `cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace --exclude chili-py`.
- Python-bindings wrap: `cd crates/chili-py && uv run maturin develop && uv run pytest`.
- No bench delta expected (W1+W2 don't touch scan / eval / load_par_df / write_partition / parse-cache hot path). Confirm `cargo bench --bench parse_cache -p chili-core` parse_cache hit still ≤ 400 ns (golden rule 6) as a precautionary spot-check.
- Test-count delta documented: predicted +3 Rust (eval_str_test) + 2 Rust integration (tcp_listener_graceful_test) + 2 chili-py pytest (test_eval_str). Expected end state: **204 Rust + 99 pytest** (from current 201 + 97).
- Build the 0.8.8 wheel; **verify the wheel METADATA Version line is 0.8.8** (lesson 14 — pyproject.toml must be bumped, not just Cargo.toml).
- Record sha256 of the wheel.
- Author retro at `docs/sim/sprint_22_retro.md`.
- Append row to `docs/sim/cadence_metrics.md`.
- Draft `docs/sync/mdata_chili_2026-05-23_0.8.8_delivery.md` with W1 contract + W2 contract + W3 explicit deferral note + sha256.
- Draft + send cross-comms reply to mdata via direct-WS bypass (vantage bridge bug still open; using mdata's existing wishlist correlation_id `chili-wishlist-2026-05-23-remote-eval-surface`).
- Move this brief to `docs/history/sprints/`.
- HALT until user ratifies.

---

## Pp accounting reference

Closest historical comparables from `docs/sim/cadence_metrics.md`:

- **Sprint 16 (mdata wishlist v1 P0 — tplog flush + add_at_time + pepper `::`)** — predicted 10.7–18.2, actual ~14, mid-band. Comparable in shape (mdata wishlist multi-feature bundle + wheel cut + delivery doc + 3-agent audit) but Sprint 22 is materially smaller: no parser surface (W1 is one builtin; Sprint 16 had pepper `::` disambiguation in two parsers), no FFI dtype work (Sprint 16 had pyo3-chrono tz handling). Expected at the **lower edge** vs Sprint 16.
- **Sprint 17 (publish_via_handle + signal_eod Async dispatch)** — predicted 11–25, actual ~12, low-edge. Comparable in pp shape (2 features + wheel + delivery + audit). Sprint 22's W2 is less risky than Sprint 17's signal_eod (no IPC fire-and-forget retrofit; just defensive `.unwrap()` replacement).
- **Sprint 18 (roll_tick)** — predicted 11–20, actual ~16, upper-mid. Larger than Sprint 22 (atomic locking semantics + audit-found `set_handle:874` second failure mode); Sprint 22's W1+W2 are individually smaller and structurally independent.

**Sprint 22 expected at the low end of 7–13 (≈ 7–9pp actual)**, capped above by: (a) `validate_auth_token` needing its own pass (halt-1), (b) wheel-cut friction (lesson 14 maturin version-source discipline), (c) audit appendix folding 1-2 additional concerns. Pattern 6 (autonomous-run underrun) favors low-end.

---

## Cross-references

- mdata wishlist text: `docs/sync/mdata_wishlist_2026-05-23_remote-eval-surface.md` (durable chili-side copy) + bus event 17614 transient: `.cross_comms/inbox/.sent/0000017614.json` + `~/code/mdata/docs/sync/chili_wishlist_2026-05-23_remote-eval-surface.md` (mdata-side authoritative)
- Wishlist correlation_id: `chili-wishlist-2026-05-23-remote-eval-surface`
- Existing eval-string builtins (`evalc`, `evali`): `crates/chili-core/src/eval.rs:466-505`, registered `crates/chili-core/src/side_effect_fn.rs:519-540`
- Remote-dispatch guard test: `crates/chili-core/tests/fn_call_i64_test.rs:28-67` (proves `engine.sync` → `fn_call` I64 arm → `eval_call` → `state.sync`)
- Existing chili-py `sync` adapter: `crates/chili-py/chili/engine.py:650-658` (`fn_call("set", ["pyHandle", h]) → fn_call("pyHandle", [query])`)
- W2 panic-site inventory: `crates/chili-core/src/engine_state.rs:2598-2659`
- Adjacent vantage-bus bridge bug (cross-comms reply path): vantage-cto event 17615 (`vantage-bridge-optimistic-sent-bug-2026-05-23`); workaround = direct-WS publish until vantage patches
- Sprint cadence rule: `.claude/rules/sprint-cadence.md`
- Self-audit rule: `~/.claude/rules/self-audit-on-plans.md`
- Verify-before-claim rule: `~/.claude/rules/verify-before-claim.md`
- Lesson 14 (maturin wheel-version source-of-truth = pyproject.toml): `docs/standards/iteration_lessons.md`

---

## Appendix — Independent audit (2026-05-23)

3-agent parallel audit per `~/.claude/rules/self-audit-on-plans.md` (Explore + code-reviewer + planner). Findings folded below; original draft preserved as audit trail.

### Material corrections (fold into impl plan before kickoff)

**MC-1 — `validate_auth_token:2540` mis-scoped as "adjacent" — it is in-scope for W2 (CRITICAL × 2 agents).**

Both Explore and code-reviewer independently caught this. The brief's B.2 panic-site table covers `engine_state.rs:2598-2659` (the accept loop body) and notes `validate_auth_token` as "adjacent surface to audit but NOT scope." Re-reading `engine_state.rs:2490-2541` confirms:

- Line 2540: `stream.shutdown(std::net::Shutdown::Both).unwrap()` — reached when `stream.read(&mut buffer)` returns `Err` (e.g., `ConnectionReset` on peer RST). This `.unwrap()` runs **on the accept-loop thread** (validate_auth_token is called from `start_tcp_listener:2601` BEFORE the per-connection thread spawns), so it kills the listener. The exact panic mdata cited as `engine_state.rs:2608` is most plausibly this site (line-number drift between mdata's checkout and ours).
- Lines 2506, 2514: `stream.peer_addr().unwrap()` — protected by the `n < 2` short-circuit at 2497-2500 on bare connect-close, but still panickable on mid-handshake close where `peer_addr()` errors. Fix for hygiene.

**Folded fix:** add 3 rows to the W2 fix list (executed inline below B.2's table):

| Line | Construct | Fix |
|---|---|---|
| 2506 | `stream.peer_addr().unwrap()` (in version-too-old log) | `.ok().map(\|a\| a.to_string()).unwrap_or_else(\|\| "<unknown>".into())` (same pattern as 2606/2614). |
| 2514 | `stream.peer_addr().unwrap()` (in successful-version log) | same. |
| 2540 | `stream.shutdown(Both).unwrap()` (in read-Err arm) | `let _ = stream.shutdown(std::net::Shutdown::Both);` — best-effort; peer may already be closed. This is the **most important fold** — it is reached by the exact scenario W2 targets. |

Total panic-sites for W2: **14 unwraps + 1 panic** (was 11 + 1).

**MC-2 — Dispatch claim over-stated; the Python pytest is the ONLY true end-to-end guard (MAJOR — code-reviewer M1).**

The brief says "`fn_call_i64_test.rs` proves the end-to-end dispatch chain works for any registered side-effect fn." Verified the test only proves the `fn_call` I64-arm dispatch to `eval_call → state.sync` for a FILE handle (sequence-file path). It does NOT prove the remote-side `handle_chili_conn → eval_op → state.get_var("eval_str") → SIDE_EFFECT_FN["eval_str"]` lookup happens. That chain is structurally sound (`SIDE_EFFECT_FN` entries load into `state.vars` at `EngineState::initialize()`), but it has never been tested for `Symbol`-headed MixedList dispatch on a registered side-effect fn over IPC.

**Folded:** the Python pytest `test_sync_eval_str_simple` is upgraded from "validation" to a **mandatory W1 closure gate**. Reword halt-criterion 2 (see MC-4 below).

**MC-3 — TCP integration-test scaffolding budget undershot ~50% (MAJOR — code-reviewer M3).**

Zero existing tests in `crates/chili-core/tests/` use `start_tcp_listener` / `TcpListener` / `TcpStream`. The scaffolding cost is first-time, not mirror-from-existing. Realistic estimate: ~150 LOC (free-port discovery via `TcpListener::bind("127.0.0.1:0")` + drop-before-spawn + background-thread `EngineState` + raw `TcpStream` connect + simulated legit handshake for the alive-assertion + teardown).

**Folded:** test estimate revised 80 → 150 LOC. Pp band unchanged (still inside 7–13) but actual likely 8–10pp not 7–9pp.

**MC-4 — Reword halt-criterion 2 as a mandatory closure gate, not a contingency (MINOR — planner).**

Original: "verify-before-claim insists on the end-to-end pytest before declaring W1 done." Implementer might read this as discretionary.

**Folded reword:** "**W1 is not closed until `test_sync_eval_str_simple` passes end-to-end.** If the dispatch chain does not fire (the Python sync round-trip returns `EvalErr` or panics rather than the expected value), halt and escalate — this means MC-2's structural assumption broke and the brief's premise is wrong."

**MC-5 — Add explicit W1-only fallback under halt-criterion 1 (MINOR — planner).**

If `validate_auth_token` scope-blows (criterion 1), the brief doesn't say what happens to the 0.8.8 wheel.

**Folded:** "If criterion 1 fires, cut **0.8.8 with W1 only**; W2 becomes Sprint 23 P0 (rename Sprint 23 brief to reflect)." Clean split, no half-shipped state.

**MC-6 — L20 cross-read step missing for W2 panic-site table (MAJOR — planner).**

The W2 table is exactly the shape that L20 binds (≥ 2 normative line-number citations to one quantity in a single doc). Lesson 18 ("the implementer's first read is the last defense") applies.

**Folded as kickoff step:** "Before touching `engine_state.rs`, re-read lines 2490-2660 end-to-end and verify each panic-site table row's line number against the current source. Cite the verification grep output in the impl commit message." (Cheap; closes the lesson-18-19-20 corridor.)

**MC-7 — Direct-WS bypass path needs explicit tool reference (MINOR — planner).**

Brief says "via direct-WS bypass" but doesn't say HOW. The bypass used earlier this session was an inline `uv run python` snippet — there is no checked-in `tools/direct_ws_publish.py` yet.

**Folded:** "For the cross-comms delivery reply, use the same direct-WS publish pattern as this session's earlier vantage-bug send: `uv run python` snippet authenticating with `.cross_comms/.chili.token`, publishing `design_question` with `recipients=['mdata']` and the wishlist `correlation_id`. Do NOT write to `.cross_comms/outbox/` (bridge is still silent-dropping until vantage-cto's fix lands)." *[2026-05-23 update during audit: vantage-cto event 17667 reports fix shipped (commit 1800e72); confirm wheel-cut sprint can use standard outbox path after re-verification.]*

**MC-8 — `eval_str` state-mutation = fully privileged; add to out-of-scope explicitly (MINOR — planner).**

Originally only in halt-criterion 3 as a contingency. Should be in out-of-scope as the current default.

**Folded out-of-scope addition:** "`eval_str` read-only enforcement / sandboxing — out of scope for v1; fully privileged eval is the current behavior (matches `evalc`/`evali`). ADR warranted only if mdata explicitly requires sandboxing."

### Additional opportunities surfaced (not folded; flagged for retro consideration)

**O1 (Explore-OPP)** — DRY refactor: `eval_str` and `eval_for_console` differ only in the trailing stringification. A future internal `eval_raw(state, _stack, args) -> Result<SpicyObj>` + two thin wrappers could DRY. Not blocking; keep duplication for v1 to minimize blast radius.

**O2 (Explore-OPP)** — `cargo bench --bench parse_cache` warmup could include an `eval_str` call to confirm no parse_cache regression. Brief already includes the GR6 spot-check; this just makes it specifically eval_str-aware. Defer to Sprint 22 retro if any anomaly surfaces.

**O3 (Explore-OPP)** — `test_eval_string` exists in `crates/chili-py/tests/test_engine.py:73` (tests `engine.eval('"hello"')` — string literal in pepper). New file `test_eval_str.py` won't collide, but the implementer's test docstring should disambiguate.

### Cross-cutting gates

**Housekeeping check** — planner suggested housekeeping was overdue. **REFUTED:** the post-Sprint-21 deep sweep landed `604dbcb` (2026-05-19) earlier this session and covered docs/sync→history + CLAUDE.md compaction + state-line fixes. Counting Sprints 12-21 since Sprint 11's housekeeping (`b5...` Sprint 11), the deep sweep at Sprint 21 wrap satisfies the every-5-sprints rule. No housekeeping required this sprint.

**Vantage-bus bridge bug status** — at audit-draft time, the bug was open (vantage-cto event 17615, my report). **UPDATE 2026-05-23 mid-audit:** vantage-cto event 17667 reports the fix is shipped (`bus/listener_bridge.py:332` `_ship_one` now awaits the daemon publish-ack; commit 1800e72). Before the Sprint 22 cross-comms delivery, re-verify the bridge accepts a probe send and persists to events.db; if so, switch the delivery reply from direct-WS bypass back to the standard `.cross_comms/outbox/` atomic-write path.

### Revised sequencing

No revision — W1 → W2 stands. Added one-sentence rationale: **"W1 first: validates the end-to-end `fn_call` → `eval_call` → `state.sync` → remote `SIDE_EFFECT_FN` lookup chain (MC-2) before committing to W2's accept-loop surgery."**

### Revised sprint sizing

- Predicted band unchanged: 7–13 (mid 10).
- Expected actual REVISED: **8–11pp** (was 7–9pp), driven by:
  - +1pp from the 3 added panic sites (MC-1).
  - +1pp from TCP-integration-test first-time scaffolding (MC-3).
  - **Lesson-11 wheel-rebuild floor explicit:** ~3pp of the 8–11 sits on the wheel-cut step (`uv sync` triggers a release-profile rebuild after `pyproject.toml` version bump). This is the single largest pp item in the sprint and is non-compressible.
  - Pattern 6 (autonomous-run underrun) still pulls down somewhat, but Sprints 16/17/18 (closest structural comparables) all ran 12-16pp actual — Sprint 22 is smaller than those but not dramatically so.
- Calibration: planner's call of **8–11pp** vs Sprint 17's actual 12pp (low-edge of 11–25): Sprint 22 is comparable in shape but materially smaller (1 trivial + 1 surgical vs Sprint 17's signal_eod Async retrofit).

### Audit cost

3 parallel agents, ~3 min wall total. Pp cost ~2pp (per `self-audit-on-plans.md` empirical measurements). Folded findings prevent ~3-5pp of downstream rework (most notably the validate_auth_token mis-scoping that would have hit halt-1 and caused mid-sprint scope renegotiation). Asymmetry holds.

### Material corrections from durable-wishlist read (turn-7 revision, found post-3-agent-audit)

The bus event 17614 body and the durable wishlist doc (`docs/sync/mdata_wishlist_2026-05-23_remote-eval-surface.md`) DIVERGE — mdata revised the doc post-send. Per L20 (cross-read normative claims for contradiction) the durable doc is canonical. Read end-to-end and folded:

**MC-9 — W2 is **P0 (highest)**, not P1; W3 is P1, not P2 (priority inversion).**

The bus message body listed `W1(P0), W2(P1), W3(P2)`. The revised wishlist `## Priority summary (revised turn 7)` table at line 82-92 elevates **W2 to P0-highest** because it is the *only* ask with NO user-space workaround (W1 has tuple-form sync for programmatic use; W3 has poll-on-variable). The brief's initial triage carried the bus-message priorities forward. No scope change (we're shipping W1+W2 either way) but the **delivery doc + cross-comms reply must use the revised priority labels** so mdata's tracking reconciles.

**MC-10 — W3 cannot be framed as "deferred" without an explicit re-evaluation gate. mdata: "none of the three is acceptable to drop."**

The revised wishlist line 84 says verbatim "mdata wants chili to deliver **all three** capabilities. Ranking is for chili-team scheduling guidance only — none of the three is acceptable to drop." The brief's W3 out-of-scope language ("Decision: ack receipt to mdata in the delivery doc; do NOT open an ADR placeholder yet") is too thin given this stance.

**Folded delivery-doc framing for W3:** "**W3 deferred to a future sprint** (not dropped). Reasoning: a Python-callable-on-pepper-hot-path is non-tactical — golden-rule-5 (GIL released around `Engine::eval`, the load-bearing invariant behind Sprint 7's 6.10× concurrent win) requires the design to be done in an ADR, not a feature ticket. Re-evaluation gate: chili-team commits to opening a W3 design sprint when (a) mdata's v1-36 attach-socket cutover specifically blocks on it AND poll-on-variable proves insufficient, OR (b) chili-team has dedicated bandwidth for an ADR + design sprint — whichever comes first. Acknowledged mdata's 'none acceptable to drop' stance; this is a sequencing decision, not a rejection."

**MC-11 — W1 naming: `eval_str` vs `.eval_str` — chili-convention vs mdata-spec divergence.**

mdata's wishlist consistently writes `.eval_str` (leading dot, dot-prefixed convention used for `.tick.createLog`, `.job.addAtTime`, `.mdata.eod.fire`). Chili's existing eval-family builtins (`eval`, `evalc`, `evali`) are bare (no leading dot). The wishlist itself defers: "Proposed API (shape — chili owns the final form)" (line 106).

**Folded decision:** ship as bare `eval_str` (matches chili convention `eval`/`evalc`/`evali`). The Symbol-keyed dispatch path (`SIDE_EFFECT_FN["eval_str"]`) is name-driven — mdata's tuple call shape `sync(h, (Symbol("eval_str"), "..."))` works either way. Document the chosen name in the delivery doc; mdata can alias on their side with a 1-line wrapper if they want the dotted form. Avoid surfacing this as a halt-3 user-decision — it is a chili-convention call.

**MC-12 — W1 lazy-mode contract is unstated; default is "follow engine state" (matches evalc/evali).**

Wishlist constraint line 124: "Lazy/eager mode: `.eval_str` follows the engine's `lazy` mode (or accepts an explicit `lazy: bool` second arg)." Brief silent on this.

**Folded decision:** `eval_str` follows engine state (delegated through `state.eval_ast`), matching evalc/evali behavior. Do NOT add a second `lazy: bool` arg for v1 (KISS; mdata can always wrap with an `evalc`/`evali`-style explicit pre-set if they need it). Document in delivery doc + Rust test asserts that toggling engine lazy mode is reflected in eval_str output.

**MC-13 — W2 latency target <1ms server-side overhead, not in brief.**

Wishlist constraint line 157: "Latency on bare connect-close: target <1ms server-side overhead." Brief silent.

**Folded:** add a Rust microbench (or assertion inside the integration test): time 100 bare-TCP connect-close iterations, average server-side overhead must be < 1ms each. Trivial to bolt onto `bare_tcp_connect_close_does_not_kill_listener`. ~10 LOC extra in the test. Adds ~0.5pp to W2 budget — still inside the 8-11pp expected range.

### Audit-appendix recursive check (Lesson 20 application)

Per L20 (cross-read normative lines for contradiction): the appendix above contains its own normative claims (panic line numbers, pp targets, MC counts). Cross-checked:

- Panic-site line numbers (2506, 2514, 2540) verified against `engine_state.rs:2490-2541` re-read after MC-1 folding. Match.
- Total panic-sites "14 unwraps + 1 panic" — re-counted: original 11 + 3 added (2506, 2514, 2540) = 14. Match.
- Expected actual band "8–11pp" — appendix consistent with planner's calibration call. Match.
- MC-1 through MC-13 numbering — contiguous, no skips, no duplicates. Match.

No internal contradictions detected.
