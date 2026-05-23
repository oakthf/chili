# mdata → chili wishlist — 2026-05-23 (REVISED turn 7)

**From:** mdata project (post-v1-32; A-033 + A-035 closed; 6h Pipeline X crypto soak 12/12 rc=0; gate 879 pass)
**To:** chili-team
**Re:** Remote-eval surface — unlock chili-IPC qcon experience + clean attach-socket retirement path
**Status:** draft for chili-team review + evaluation (revised 2026-05-23 turn 7 after `remote=True` re-investigation)
**chili version pinned by mdata:** 0.8.7 (path wheel)
**Track A status:** filed for chili-team; mdata Track B (gw multi-instance via attach-socket transport) SHIPPED in v1-33.

---

## Context

chili 0.8.7 push model (D-1…D-3) has been excellent — mdata's rdb/wdb subscribers are event-driven via `upd_notify_fd()` + `drain_upds()`; A-033 closure in v1-32 was 100% mdata-side. Thank you. This wishlist is **one coherent capability set**: enabling mdata to retire its bespoke attach-socket pepper-eval protocol and offer operators a chili-native "qcon" experience against any of the 7 mdata daemons.

This was surfaced by a 3-spike pre-sprint investigation (mdata `docs/sync/v1_33_pre_spike_findings_2026-05-23.md`) that revealed three concrete chili-side constraints. Each constraint is a clean gap rather than a deep design hazard.

**mdata position (turn 7):** Pure-pepper workarounds exist for W1 and W3, but **we want chili to solve the surface properly**. The workarounds are sufficient for the gw-query-path that v1-33 already shipped (via attach-socket transport); they are NOT a substitute for first-class chili support if we want a clean operator qcon REPL + a unified Python-callable bridge + safe TCP probe semantics. v1-33 shipped without these via Track B (attach-socket transport); v1-36+ chili-IPC cutover wants W1+W2+W3 to land cleanly.

### Spike status snapshot (after turn 7 re-investigation)

| Spike | Original turn-4 verdict | Revised turn-7 verdict | Workaround? | chili-side fix needed |
|---|---|---|---|---|
| **S1** Python-callable as pepper fn | FAIL — `set_var` rejects callables | FAIL — confirmed | (a) pure pepper, (b) poll-on-variable | **W3** (Python-callable bridge) |
| **S2** Bare-TCP probe to listener | FAIL — Rust panic; **also fails with `remote=True`** | FAIL — confirmed across both bind modes | **None** — Rust panic is unrecoverable | **W2** (graceful bare-TCP) |
| **S3** Remote arbitrary pepper eval | FAIL — "sync is named-function-invoke only" | **PARTIALLY REVISED** — `sync(h, tuple)` IS the cross-process function-invoke form (works), but `sync(h, "1+1")` (string → eval) does NOT work. Arbitrary pepper-eval over the wire still requires `.eval_str`. | tuple-form sync covers programmatic use cases (gw query path with per-table pepper fns); does NOT cover operator qcon REPL (arbitrary expressions typed by humans) | **W1** (`.eval_str` pepper builtin) |

**Bottom line:** S2 is the only spike with **no workaround at all**. S1 and S3 have workarounds for some use cases (S1: pure-pepper or poll-on-variable; S3: tuple-form sync for programmatic), but not for the full operator surface (qcon arbitrary-pepper REPL + Python-callback control verbs across the bus). mdata wants chili to solve all three.

---

## Root cause (3 gaps, mdata-verified against installed 0.8.7 wheel)

### Gap 1 — `sync(h, …)` STRING form is variable-lookup, not pepper-eval (turn-7 corrected)

`engine.sync(h, arg)` semantics by arg type (re-verified turn 7):

| `arg` form | Behavior | Example | Result |
|---|---|---|---|
| **String** that names a remote variable | Variable LOOKUP | `sync(h, "myvar")` | returns `42` |
| **String** that doesn't name a variable | Error `Name 'X' is not defined` | `sync(h, "1+1")` | error — `"1+1"` is not a variable name |
| **Tuple** `(fn_name, arg1, ...)` | Remote function INVOCATION | `sync(h, (".myfn", 5))` | returns `15` ✓ |
| **List** `[fn_name, arg1, ...]` | Same as tuple | `sync(h, [".myfn", 5])` | returns `15` ✓ |

So chili 0.8.7 IPC IS sufficient for **programmatic** cross-process query (define per-table named pepper functions, invoke via tuple-form sync). What it does NOT support is **arbitrary pepper-string evaluation over the wire** — there is no `.eval_str` pepper builtin and no remote-eval mechanism (verified via grep across `chili-core/src/`).

**Pepper has no string-eval primitive either** (verified turn 4: `eval`/`value`/`parse`/`do` all fail with parser errors). So we can't WORKAROUND with a user-space `.eval_str: {[code] eval code}` — chili-side support is required.

**mdata-side workaround** (used today): per-table named pepper functions invoked via tuple-form sync. Sufficient for programmatic gw → rdb/hdb queries; **insufficient for operator qcon REPL** where the operator types arbitrary pepper expressions.

### Gap 2 — Bare TCP connect to `start_tcp_listener` port causes a Rust panic (no workaround exists)

```python
socket.socket().connect_ex(('localhost', chili_port))
# Result: errno 61 + Rust panic in chili-core/src/engine_state.rs:2608
#   Result::unwrap() on Err: "Socket is not connected"
```

A bare TCP probe (connect + immediate close, no chili handshake) is not gracefully rejected by chili's listener. The Rust panic propagates as an OS error to the client and corrupts the listener's state on the chili-engine thread. **Re-verified turn 7 with `remote=True`** — same panic; not a bind-interface artifact.

This is the single biggest blocker for mdata's dis (discovery) probe migration — dis pings every daemon every N seconds; a TCP-connect-and-close pattern was mdata's proposed F7-fast-path-preserving probe.

**There is no mdata-side workaround.** Any TCP probe of the listener triggers the panic and corrupts listener state. dis must therefore keep using attach-socket `__ping__` until chili can absorb bare TCP cleanly OR until dis migrates to a different liveness mechanism (e.g., chili-IPC ping via `sync(h, (".ping",))` — but that contradicts F7's no-chili-lock-on-probe contract, which was the load-bearing fix for A-033).

**Of the three gaps, this is the only one with no user-space workaround. It is the highest-priority ask from mdata's perspective.**

### Gap 3 — `set_var(name, value)` rejects Python callables

```python
engine.set_var(".mdata.test.fn", my_python_callable)
# ChiliError: Unsupported Python type for chili conversion: function
```

Pepper functions are first-class in chili-core (`Func` type in `func.rs`), but the Python wrapper exposes only `set_var(id, value)` which accepts polars DataFrames, scalars, lists, etc. — not Python callables. There is no `register_fn(name, callable)` Python API.

Implication: mdata's control verbs (eod-fire, wdb-finalize, hdb-reload) that need to trigger Python-side daemon logic cannot be re-implemented as pepper functions invokable via chili IPC.

**mdata-side workaround** (possible but ugly): poll-on-variable pattern. Pepper IPC handler sets a variable `.mdata.eod.fire.request`; Python main loop polls every N ms; on change, Python dispatches to the daemon handler and writes back `.mdata.eod.fire.ack`. This works but adds latency (poll interval), requires shared-state coordination, and ties every Python-callback control verb to a polling cadence that competes with other event loops. **W3 is the clean path.**

---

## Priority summary (revised turn 7)

mdata wants chili to deliver **all three** capabilities. Ranking is for chili-team scheduling guidance only — none of the three is acceptable to drop:

| Ask | Revised priority | Workaround available? | Why this priority |
|---|---|---|---|
| **W2** graceful bare-TCP-connect | **P0 (highest)** | **No** | Only ask with no user-space workaround. Blocks dis probe migration (F7 fast-path preservation). Also blocks any future port-liveness check. |
| **W1** `.eval_str` pepper builtin | **P0** | None for arbitrary expressions (tuple-form sync covers programmatic only) | Required for operator qcon REPL. Without it, qcon must use a fixed verb vocabulary — operator UX loss. |
| **W3** Python-callable bridge | **P1** | poll-on-variable (works but high-latency + complex shared state) | Required for clean Python-callback control verb dispatch. Workaround exists but is structural complexity tax across every Python-bound control verb. |

**Original turn-4 ranking** (W1 → P0, W2 → P1, W3 → P2) was based on impact-on-mdata. The turn-7 revision elevates W2 because S2's "no workaround" status is now confirmed across both `remote=False` and `remote=True` bind modes, while S1/S3 have partial workarounds.

---

## W1: `.eval_str` pepper builtin (string → pepper eval, remotely invokable) — P0

**Goal:** allow `sync(h, (Symbol(".eval_str"); "select last 5 from trades"))` to invoke pepper-eval of the string on the remote engine and return the result.

### Motivation

mdata's operator experience today: `mdata qcon rdb.crypto_perp` opens an interactive prompt; operator types arbitrary pepper expressions (`select count(*) from trades`, `last 5 ohlcv`, `.broker.publish 0; 'eod; ...`); the attach-socket protocol forwards each line to the daemon's chili engine via Python-side `engine.eval(line)`, returns serialized result.

Migrating this to chili IPC needs a chili-native way to evaluate a STRING (arbitrary pepper) on the remote engine. A pre-defined named-function vocabulary works for programmatic clients (gw query path) but breaks the interactive REPL experience.

### Proposed API (shape — chili owns the final form)

```pepper
// chili-core registered builtin:
.eval_str: {[code_str] /* parse code_str as pepper AST + eval against current engine + return result */ };
```

So that:

```python
# mdata-side caller:
result = client.sync(handle, (Symbol(".eval_str"), "select last 5 from trades"))
# returns: pl.DataFrame
```

### Constraints / acceptance

- Must use the SAME parser + eval path as `engine.eval()` (so behavior is byte-identical to local eval).
- Lazy/eager mode: `.eval_str` follows the engine's `lazy` mode (or accepts an explicit `lazy: bool` second arg).
- Errors propagate as `ChiliError`/structured response (not Rust panic).
- Performance: target sub-1ms for simple expressions (`.ping[]` equivalent).

### mdata-side use

- `mdata qcon <daemon>` uses `.eval_str` instead of attach-socket text protocol.
- mdata's chili-IPC qcon CLI implementation: ~50 LoC client wrapper.
- Enables full attach-socket-deletion in a later mdata sprint.

---

## W2: Graceful bare-TCP-connect handling on chili listener — P0 (highest)

**Goal:** `start_tcp_listener` port accepts bare TCP connect + immediate close without panic. Listener state stays clean for legitimate chili handshakes.

### Motivation

mdata's dis (discovery service) is the canonical liveness oracle. Today dis probes every daemon every N seconds via attach-socket UNIX ping. F7 in mdata v1-32 made that ping fast-path (no chili eval) specifically to avoid chili-lock contention (A-033 root cause). When mdata retires the attach socket, dis needs a chili-port-compatible probe.

The lightest-weight probe is bare TCP connect-and-close — proves the listener is alive, costs ~0.01ms, doesn't enter chili eval at all. Today this causes a Rust panic.

### Proposed behavior

When chili's listener receives a TCP connect that drops without a chili handshake:
- Log at INFO level (optional, for debug)
- Discard the partial state cleanly
- Listener accepts the next legitimate handshake

### Constraints / acceptance

- No Rust panic.
- No corruption of listener state for concurrent legitimate handshakes.
- Latency on bare connect-close: target <1ms server-side overhead.

### mdata-side use

- dis probe replaces UNIX-socket `__ping__` with TCP-connect-and-close to each daemon's chili port.
- Preserves F7 fast-path (no chili-lock contention).
- Enables retiring the attach-socket-side `__ping__` handler in a later mdata sprint.

---

## W3: Python-callable registration as pepper function — P1

**Goal:** allow mdata daemons to register Python callables as pepper functions, so chili IPC `sync(h, (Symbol(".mdata.eod.fire"); date))` can invoke a Python-side handler.

### Motivation

mdata has 3 control verbs that need to trigger Python-side daemon logic:
- `.mdata.eod.fire[date]` (tp) — broadcasts EOD message + bookkeeping
- `.mdata.wdb.finalize[date]` (wdb) — drains write buffer + finalizes idb partition
- `.mdata.hdb.reload[]` (hdb) — reloads partition cache after EOD

Today these dispatch via attach-socket text protocol → Python handler. The Python handler calls into chili (`engine.fn_call(".tick.eod", [date])`) and does its own bookkeeping. Pure-pepper would lose the bookkeeping; pepper has no pepper→Python callback.

### Proposed API (two shapes — chili picks)

**Option A — `engine.register_fn(name, callable)`:**

```python
engine.register_fn(".mdata.eod.fire", my_eod_fire_handler)
# my_eod_fire_handler is invokable from pepper as `.mdata.eod.fire[date]`
```

**Option B — `set_var` extended to accept callables:**

```python
engine.set_var(".mdata.eod.fire", my_eod_fire_handler)
# set_var dispatches to chili-core's Python-callable variant if callable
```

### Constraints / acceptance

- Python callable's args + return are converted via existing chili type-bridge (DataFrames, scalars, lists, dicts).
- Exceptions in Python propagate as `ChiliError` on the chili side (not Rust panic).
- Thread-safety: callable can be invoked from the chili IPC thread; mdata-side responsible for thread-safety of the callable.
- GIL: chili acquires the GIL before invoking the callable, releases after.

### mdata-side use

- mdata daemons register control verbs at chili-engine init: `engine.register_fn(".mdata.eod.fire", self._eod_fire_control)`
- Caller (CLI or other daemon): `sync(h, (Symbol(".mdata.eod.fire"); date))` → invokes Python handler → returns ack
- Enables retiring the attach-socket control-verb handlers in a later mdata sprint.

---

## Priority + dependency notes (revised turn 7)

**mdata wants all three.** Workarounds exist for W1 and W3, but we want chili to solve the surface properly — the workarounds tax every future chili-IPC integration with structural complexity. The turn-7 priority revision reflects "which gap is most blocking right now," not "which gap mdata is willing to drop":

- **W2 (graceful bare-TCP) — P0 highest, NO WORKAROUND.** dis probe migration is structurally blocked. Any future port-liveness checking (mon dashboards, ops scripts, integration tests) hits the same Rust-panic wall. This is the gap mdata cannot route around.
- **W1 (`.eval_str`) — P0.** Without it, operator qcon REPL must use a fixed verb vocabulary (Alt-3 from spike findings). Tuple-form `sync(h, (fn, args))` covers programmatic use cases (mdata's v1-33 gw query path uses this approach via attach-socket transport today, and will adopt chili IPC tuple-form once dis can probe safely), but does not cover human operators typing arbitrary pepper at a prompt.
- **W3 (Python-callable bridge) — P1.** Poll-on-variable workaround exists but adds latency + shared-state complexity to every Python-bound control verb. mdata has 3 such verbs today (eod.fire / wdb.finalize / hdb.reload) and will add more as Pipeline P scope opens; the workaround tax compounds.

**Capability set is coherent.** All three deliver "chili-IPC is the only IPC mdata needs." Picking any subset leaves mdata maintaining the attach-socket transport indefinitely for the un-covered case (W2 absent → dis probe; W1 absent → qcon REPL; W3 absent → control verbs).

**If chili-team must triage:** W2 first (only one without workaround). W1 + W3 in either order after.

---

## Out of scope

- mdata's own implementation (mdata-side work after chili delivery is a separate ~6-10pp sprint).
- chili source code changes (mdata cannot do those; chili-team owns).
- Performance regressions in existing chili APIs (no changes to non-listed APIs).

---

## mdata-side commitments

1. Acceptance test: mdata will ship an integration test (`tests/spikes/test_chili_remote_eval_surface.py` or similar) that exercises W1/W2/W3 against the delivered chili wheel; chili-team can re-run.
2. Documentation: mdata will update `docs/standards/chili_capability_inventory.md` to record the post-delivery surface.
3. Retirement: mdata commits to retiring the attach-socket protocol in a v1-36+ chili-IPC cutover sprint once W1 + W2 + W3 land.

---

## Cross-references

- mdata pre-spike findings: `docs/sync/v1_33_pre_spike_findings_2026-05-23.md` (S3 revised turn 7)
- mdata execution roadmap: `docs/plans/post_v1_32_execution_roadmap_2026-05-23.md`
- mdata v1-32 retro (A-033 closure): `docs/sim/sprint_v1_32_a033_fix_retro.md`
- mdata v1-33 retro (Track B shipped via attach-socket transport): `docs/sim/sprint_v1_33_gw_multi_instance_retro.md`
- Prior chili wishlists adopted live in mdata:
  - `docs/sync/chili_wishlist_2026-05-17_push-model.md` (D-1…D-3 — shipped in 0.8.7, adopted v1-26)
  - `docs/sync/chili_wishlist_2026-05-22_async-surface.md` (W1-W5 async + RwLock fairness — future, not blocking)
