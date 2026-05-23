# mdata → chili wishlist — 2026-05-23 (REVISED turn 9 — W1 RESOLVED self-discovered)

**From:** mdata project (post-v1-32; A-033 + A-035 closed; 6h Pipeline X crypto soak 12/12 rc=0; gate 912 pass)
**To:** chili-team
**Re:** Remote-eval surface — W1 RESOLVED (bytes-form sync works; just undocumented); W2 still blocking; W3 has workaround
**Status:** REVISED turn 9 after user-prompted `sync(h, b"...")` test surfaced bytes-form arbitrary pepper-eval. W1 no longer asked.
**chili version pinned by mdata:** 0.8.7 (path wheel)
**Track A status:** trimmed to W2 only (no workaround); W1 self-discovered shipped; W3 demoted (poll-on-variable workaround viable).

---

## Turn-9 finding — W1 IS ALREADY SHIPPED (please document)

User asked "did you try `sync(h, b\"1+1\")`?" — empirically verified that **bytes-form sync IS arbitrary pepper-eval over the wire**. Both `remote=False` and `remote=True` listeners accept it. So `sync(h, X)` is three-way polymorphic by type:

| `X` type | Behavior | Example | Result |
|---|---|---|---|
| `str` | Variable-name LOOKUP | `sync(h, "myvar")` | `42` |
| `bytes` (or `str.encode()`) | **ARBITRARY PEPPER EVAL** | `sync(h, b"1+1")` | `2` ✓ |
| `tuple`/`list` | Named-function INVOCATION | `sync(h, (".myfn", 5))` | `15` ✓ |
| `bytearray` | NOT supported | `sync(h, bytearray(b"1+1"))` | ChiliError |

Multi-statement, compound expressions, remote variable definition, error propagation — all work. Confirmed under `remote=True` (full audit script at mdata `/tmp/test_sync_bytes_deeper.py`).

**Ask for chili-team:** please document this in chili's docstring + reference docs. The string-form-fail / bytes-form-succeed pattern is non-obvious; the user-facing docstring for `sync(handle, query)` currently shows `query: Any` with no type-polymorphism note. A future mdata-style audit would still miss this without the empirical bytes test.

---

## Context

chili 0.8.7 push model (D-1…D-3) has been excellent — mdata's rdb/wdb subscribers are event-driven via `upd_notify_fd()` + `drain_upds()`; A-033 closure in v1-32 was 100% mdata-side. Thank you. This wishlist is **one coherent capability set**: enabling mdata to retire its bespoke attach-socket pepper-eval protocol and offer operators a chili-native "qcon" experience against any of the 7 mdata daemons.

This was surfaced by a 3-spike pre-sprint investigation (mdata `docs/sync/v1_33_pre_spike_findings_2026-05-23.md`) that revealed three concrete chili-side constraints. Each constraint is a clean gap rather than a deep design hazard.

**mdata position (turn 9):** Two of three blockers self-resolved upon further investigation:
- **W1 (arbitrary pepper-eval)** ships today via bytes-form `sync(h, b"...")` — purely a docs gap.
- **W2 (graceful bare-TCP)** is the ONLY remaining chili-side blocker — Rust panic on bare-connect; no user-space workaround.
- **W3 (Python-callable bridge)** has a clean workaround now that bytes-form sync exists: caller writes `sync(h, b".eod.fire.request:date")`; Python side polls `.eod.fire.request` and dispatches. Latency suboptimal but functional.

### Spike status snapshot (after turn-9 bytes-form discovery)

| Spike | Final verdict | Workaround? | chili-side fix needed |
|---|---|---|---|
| **S1** Python-callable as pepper fn | FAIL — `set_var` rejects callables | Poll-on-variable (now drivable end-to-end via bytes-form sync) | W3 — DEMOTED (workaround viable) |
| **S2** Bare-TCP probe to listener | FAIL — Rust panic both `remote=False` and `remote=True` | **None** — Rust panic is unrecoverable | **W2 — STILL BLOCKING.** Single remaining chili dependency. |
| **S3** Remote arbitrary pepper eval | **RESOLVED (turn 9)** — bytes-form `sync(h, b"...")` is arbitrary pepper eval. String-form is variable-lookup; tuple-form is function-invoke. | n/a — chili already supports it | W1 — RESOLVED (just docs ask: please document bytes-form polymorphism) |

**Bottom line:** mdata wishlist trimmed from W1+W2+W3 to **W2-only** as the hard chili-side blocker. W1 is a documentation ask (no chili code change). W3 is now a "nice-to-have" with a viable workaround.

---

## Root cause (3 gaps, mdata-verified against installed 0.8.7 wheel)

### Gap 1 — RESOLVED (turn 9). `sync(h, b"...")` IS arbitrary pepper-eval.

**Self-discovered turn 9** — `engine.sync(h, arg)` is three-way polymorphic by Python type:

| `arg` type | Behavior | Example | Result |
|---|---|---|---|
| `str` matching a remote variable | Variable LOOKUP | `sync(h, "myvar")` | `42` |
| `str` not matching any variable | `Name 'X' is not defined` | `sync(h, "1+1")` | error |
| **`bytes`** | **ARBITRARY PEPPER EVAL** | `sync(h, b"1+1")` | `2` ✓ |
| `str.encode()` (same as bytes) | Arbitrary pepper eval | `sync(h, "1+1".encode())` | `2` ✓ |
| `tuple`/`list` of `(fn, args...)` | Named-function INVOCATION | `sync(h, (".myfn", 5))` | `15` ✓ |
| `bytearray` | NOT supported | `sync(h, bytearray(b"1+1"))` | ChiliError |

**Empirical proof** (script: mdata `/tmp/test_sync_bytes_deeper.py`; tested 2026-05-23 against chili-sauce 0.8.7):

- Arithmetic: `sync(h, b"myvar*10")` → `420` (compound w/ remote var)
- Multi-statement: `sync(h, b"a:10; a+5")` → `15`
- Define + invoke: `sync(h, b"defined:99")` → `99`; then `sync(h, "defined")` → `99`
- Error path: `sync(h, b"undefined_var")` → `ChiliError: Name 'undefined_var' is not defined`
- `remote=True` listener: identical behavior (no panic; eval works)

**No chili-side change required.** The capability exists. The single chili-team ask for this gap is **documentation**: please add a docstring note on `sync(handle, query)` explaining the str/bytes/tuple polymorphism. The mdata 2026-05-23 turn-4 spike missed bytes-form for ~5 days because it's not documented.

mdata will:
- Self-discover-credit: `sync(h, b"...")` lands in mdata's chili capability inventory turn-9
- Migrate v1-36a `RemoteRdbClient` / `RemoteHdbClient` query path to bytes-form sync
- Build `mdata qcon` REPL on bytes-form sync (no fixed verb vocabulary needed)

### Gap 2 — Bare TCP connect to `start_tcp_listener` port PANICS the listener thread (turn-9 reproduction)

Full reproduction (mdata `/tmp/test_s2_bare_tcp.py`):

```python
import chili, socket
eng = chili.ChiliEngine()
eng.eval("myvar:42")
eng.start_tcp_listener(port, remote=True)   # remote=False also fails

# (1) Proper chili handshake works first:
client = chili.ChiliEngine()
h = client.open_handle(f"chili://127.0.0.1:{port}")
client.sync(h, "myvar")   # → 42 ✓

# (2) Bare TCP connect + close → PANIC + listener dies:
socket.create_connection(('127.0.0.1', port), timeout=2.0).close()
```

**Exact panic** on chili stderr at the moment the bare connect lands:

```
thread '<unnamed>' (54883597) panicked at
  /Users/oakadmin/code/chili/crates/chili-core/src/engine_state.rs:2608:59:
called `Result::unwrap()` on an `Err` value:
  Os { code: 57, kind: NotConnected, message: "Socket is not connected" }
```

After the panic — listener is permanently dead:

```python
# (3) Second probe attempt:
socket.socket().connect_ex(('127.0.0.1', port))   # → 61 (ECONNREFUSED)

# (4) Proper chili handshake AFTER → also fails:
chili.ChiliEngine().open_handle(f"chili://127.0.0.1:{port}")
# → ChiliError: Connection refused (os error 61)
```

**Failure shape:** not "bare connect rejected" — it's **"bare connect crashes the listener thread; subsequent legitimate handshakes all ECONNREFUSED"**. The listener stays dead until the chili engine reboots. Re-verified under both `remote=True` and `remote=False` listeners; identical behavior.

**Suspected root cause** (not confirmed against chili source): `engine_state.rs:2608:59` does `.unwrap()` on a socket-read that expects the peer to send a chili handshake byte stream. When the peer drops the connection without sending anything, the read returns `Err(NotConnected)` and `.unwrap()` panics, crashing the listener task. A defensive `match` or `if let Ok(...) = ... else { log + drop }` at that site would let the listener log the half-open accept + discard the connection gracefully, ready for the next handshake.

**Why dis probe can't route around this.** mdata's `dis` daemon pings every other daemon every N seconds for liveness. The lightweight pattern is a TCP-connect-and-close (no payload, no chili eval, no chili lock — preserves the F7 fast-path that was the load-bearing fix for A-033 in v1-32). Any bare-TCP probe to a chili listener crashes that listener; so dis stays on attach-socket UNIX-ping until W2 lands. Migrating to chili-IPC ping via `sync(h, b".ping[]")` is functionally possible but re-introduces chili-lock contention on the ping path — A-033-class regression risk.

**Of the three gaps in this wishlist, W2 is the only one with no user-space workaround.** It is also the smallest possible chili-side change (single `.unwrap()` → defensive match).

### Gap 3 — `set_var` rejects ONLY Python callables (turn-9 detailed table)

Comprehensive turn-9 test of `set_var(name, value)` across Python types (script `/tmp/test_s1_set_var.py`):

| Input type | Result |
|---|---|
| `int`, `float`, `str`, `bytes`, `bool`, `None` | ✓ OK |
| `datetime.date` object | ✓ OK |
| `datetime.datetime` (tzinfo=None) | ✗ `TypeError: expected a datetime with non-None tzinfo` (polars/pyarrow constraint, not chili) |
| ISO-string date `"2026-05-23"` | ✓ OK |
| `list`, `tuple`, `dict` | ✓ OK |
| `polars.DataFrame`, `polars.Series` | ✓ OK |
| **`lambda x: x*2`** | ✗ `ChiliError: Unsupported Python type for chili conversion: function` |
| **`def my_fn(x): ...`** | ✗ same — `function` |
| **builtin `len`** | ✗ `ChiliError: Unsupported Python type for chili conversion: builtin_function_or_method` |
| **bound method `obj.m`** | ✗ `ChiliError: Unsupported Python type for chili conversion: method` |

`set_var` is permissive for almost everything — it accepts dates, primitives, collections, DataFrames, Series. **It ONLY rejects Python callables.** The chili-core Python wrapper has converters for all the working types but no `PyAny → Func` converter for arbitrary callables.

**Implication for mdata.** Pepper functions are first-class in chili-core (`Func` type in `func.rs`), but the Python-side has no way to register a Python callable as a pepper-invokable function. mdata's control verbs (`eod_fire`, `wdb_finalize`, `hdb_reload`) need to trigger Python-side daemon bookkeeping (not just pepper) — for example, `wdb_finalize` drains a Polars-managed in-memory buffer + writes to disk. Pure-pepper rewrite would require porting that bookkeeping into pepper.

**mdata-side workaround (turn-9 — now viable):** poll-on-variable pattern, drivable end-to-end via bytes-form sync.

```python
# Caller (e.g. gw):
client.sync(h, b'.mdata.eod.fire.request: "2026-05-23"')   # set the request var on remote

# Python main loop on the daemon side:
last_seen = ""
while True:
    req = engine.fn_call("get", [".mdata.eod.fire.request"])
    if req != last_seen:
        result = my_eod_fire_handler(req)   # Python handler
        engine.set_var(".mdata.eod.fire.ack", result)
        last_seen = req
    time.sleep(0.1)

# Caller polls for completion:
ack = client.sync(h, ".mdata.eod.fire.ack")
```

Adds ~100-200ms latency per control verb (the poll interval); tolerable for low-frequency control-plane verbs (eod/finalize/reload happen at most daily per asset class).

Before turn-9, this workaround was theoretical because there was no chili-IPC way to set the variable from a remote client. With bytes-form `sync(h, b"...")` confirmed, it's a real path. **W3 is the cleaner path (no poll loop, no latency cost), but no longer blocking.** Downgraded from P1 to P2.

---

## Priority summary (revised turn 9 — W1 self-resolved)

Wishlist trimmed from W1+W2+W3 to **W2-only as the hard ask**, plus a documentation request for what's already shipped.

| Ask | Revised priority | Workaround available? | Why this priority |
|---|---|---|---|
| **W2** graceful bare-TCP-connect | **P0 (only hard ask)** | **No** | Rust panic on bare TCP; no user-space recovery. Blocks dis probe migration (F7 fast-path preservation). |
| **W1** docstring/docs for bytes-form `sync` | **DOC ONLY** (P3) | n/a — capability already shipped | Capability exists in 0.8.7. Please document so the next mdata-style audit doesn't miss it. |
| **W3** Python-callable bridge | **P2** (downgraded) | Poll-on-variable, now drivable via bytes-form sync | Workaround functional; W3-direct still cleaner if cheap; not blocking. |

**Turn-history:**
- **Turn 4** (original draft): W1+W2+W3 all P0/P1/P2 based on "no workaround" beliefs.
- **Turn 7** (post `remote=True` re-investigation): tuple-form sync discovered; W1 priority dropped for programmatic use but kept for qcon REPL.
- **Turn 9** (post user `b"..."` test): bytes-form sync discovered → W1 IS the capability we asked for. Wishlist trimmed.

---

## W1: docstring + reference docs for bytes-form `sync` polymorphism — DOC ONLY

**Goal:** update chili's `sync(handle, query)` docstring + reference docs to explain the bytes/str/tuple polymorphism. No code change.

### Why this matters

The capability mdata originally asked for (arbitrary remote pepper-eval) IS already shipped in 0.8.7. The mdata turn-4 spike concluded W1 was needed because `sync(h, "1+1")` failed with "Name '1+1' is not defined" — and the docs/docstring give no hint that `b"1+1"` would dispatch differently. Two subsequent rounds of audit (turn-7 + the 3-agent committee on the v1-34 brief) ALSO missed bytes-form. Only direct user nudge ("did you try `sync(h, b\"1+1\")`?") surfaced it.

### Proposed docstring update

Current chili `engine.py`:
```python
def sync(self, handle_num: int, query: Any) -> Any:
    self.fn_call("set", ["pyHandle", handle_num])
    return self.fn_call("pyHandle", [query])
```

Suggested addition:
```python
def sync(self, handle_num: int, query: Any) -> Any:
    """Invoke a remote operation via an open handle.

    Three-way polymorphic on the type of ``query``:
      - str   → looks up a variable named ``query`` on the remote engine
      - bytes → evaluates ``query`` as a pepper-source expression remotely
      - tuple/list of (fn_name, *args) → invokes a named pepper function

    Examples:
        sync(h, "myvar")             # variable lookup → 42
        sync(h, b"1+1")              # arbitrary eval → 2
        sync(h, (".myfn", 5))        # function call → fn-result

    Note: bytearray is NOT supported (raises ChiliError).
    """
```

### mdata-side downstream

- mdata v1-36a will migrate `RemoteRdbClient` / `RemoteHdbClient` from attach-socket transport to bytes-form chili-IPC sync.
- mdata `qcon` CLI implementation (~50 LoC) ships when v1-36a lands.
- mdata updates `docs/standards/chili_capability_inventory.md` to record bytes-form polymorphism (will reference this wishlist as the discovery moment).

---

## W2: Graceful bare-TCP-connect handling on chili listener — P0 (only hard ask)

**Goal:** `start_tcp_listener` port accepts bare TCP connect + immediate close without panic. Listener state stays clean for legitimate chili handshakes.

**Failure site identified:** `chili-core/src/engine_state.rs:2608:59` — `Result::unwrap()` on a socket-read that returns `Err(NotConnected)` when the peer drops without sending a chili handshake. See Gap 2 above for the full reproduction + exact panic message + listener-corruption-after pattern.

### Motivation

mdata's dis (discovery service) is the canonical liveness oracle. Today dis probes every daemon every N seconds via attach-socket UNIX ping. F7 in mdata v1-32 made that ping fast-path (no chili eval) specifically to avoid chili-lock contention (A-033 root cause). When mdata retires the attach socket, dis needs a chili-port-compatible probe.

The lightest-weight probe is bare TCP connect-and-close — proves the listener is alive, costs ~0.01ms, doesn't enter chili eval at all. Today this causes a Rust panic AND permanently kills the listener.

### Proposed behavior

When chili's listener receives a TCP connect that drops without a chili handshake:
- Log at INFO/DEBUG level (optional, for debug)
- Discard the partial state cleanly
- Listener accepts the next legitimate handshake

### Suggested fix shape

At `chili-core/src/engine_state.rs:2608:59`, replace the `.unwrap()` with defensive error handling — e.g.:

```rust
// (illustrative; chili-team knows the right Rust idiom for their codebase)
match result {
    Ok(v) => { /* normal path */ }
    Err(e) if e.kind() == ErrorKind::NotConnected => {
        // Peer dropped before chili handshake. Liveness probe pattern; ignore.
        log::debug!("listener: peer dropped before handshake: {}", e);
        continue;   // or: return early from this connection's task
    }
    Err(e) => panic!("unexpected listener error: {}", e),   // keep panic for genuine bugs
}
```

The intent is to filter the specific "client dropped before chili handshake" case (the legitimate-but-discardable case for a TCP probe) WITHOUT swallowing other classes of socket errors (which might mask genuine bugs).

### Constraints / acceptance

- No Rust panic.
- No corruption of listener state for concurrent legitimate handshakes.
- Latency on bare connect-close: target <1ms server-side overhead.
- mdata will ship `tests/spikes/test_chili_listener_bare_tcp_graceful.py` as the acceptance gate; reproduces the four-step pattern from Gap 2 + asserts that step (4) — proper chili handshake AFTER a bare connect — succeeds.

### mdata-side use

- dis probe replaces UNIX-socket `__ping__` with TCP-connect-and-close to each daemon's chili port.
- Preserves F7 fast-path (no chili-lock contention).
- Enables retiring the attach-socket-side `__ping__` handler in v1-36b sprint.

---

## W3: Python-callable registration as pepper function — P2 (downgraded; workaround viable)

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

## Priority + dependency notes (revised turn 9)

**Single hard ask: W2.** W1 is a docs request for what's already shipped. W3 is downgraded — the bytes-form discovery makes the poll-on-variable workaround functionally viable.

- **W2 (graceful bare-TCP) — P0, NO WORKAROUND.** dis probe migration is structurally blocked. Any future port-liveness checking (mon dashboards, ops scripts, integration tests) hits the same Rust-panic wall. This is the gap mdata cannot route around. Without W2, dis probe stays on attach-socket indefinitely.

- **W1 (bytes-form docs) — DOC ONLY.** Capability exists in 0.8.7 as `sync(h, b"...")`. Three-tier audit chain (turn-4 spike + turn-7 re-investigation + 3-agent committee on v1-34 brief) all missed bytes-form because docs don't mention it. Please add a docstring example.

- **W3 (Python-callable bridge) — P2.** Cleaner than poll-on-variable but no longer blocking. Workaround latency ~100-200ms per control verb is acceptable for low-frequency control-plane verbs (eod.fire, wdb.finalize, hdb.reload).

**Capability set is achievable with W2 alone.** With W2: mdata can fully retire the attach-socket pepper-eval protocol in v1-36a/b. dis probe migrates. qcon REPL ships on bytes-form. Control verbs run on poll-on-variable.

**Workaround for W2** (just to spell it out — there isn't one): bare-TCP causes Rust panic + corrupts listener state; chili IPC ping (`sync(h, b".ping[]")`) bypasses the panic but re-introduces chili-lock contention (A-033 regression risk per v1-32 retro lesson L5b — F7 deliberately made `__ping__` no-chili-lock). So mdata is stuck on the attach-socket UNIX-ping for liveness until W2 lands.

---

## Out of scope

- mdata's own implementation (mdata-side work after chili delivery is a separate ~6-10pp sprint).
- chili source code changes (mdata cannot do those; chili-team owns).
- Performance regressions in existing chili APIs (no changes to non-listed APIs).

---

## mdata-side commitments

1. **W1 self-discovery acknowledged.** mdata documents bytes-form polymorphism in `docs/standards/chili_capability_inventory.md` (v1-34 wrap or v1-36a kickoff, whichever lands first).
2. **W2 acceptance test ready.** mdata will ship `tests/spikes/test_chili_listener_bare_tcp_graceful.py` against the next chili wheel that addresses W2 — bare TCP connect+close to listener does not panic + listener state stays usable.
3. **Retirement plan.** v1-36a (mdata-side, no chili wishlist gate): switch `RemoteRdbClient` / `RemoteHdbClient` query path from attach-socket to chili-IPC bytes-form sync. Ship `mdata qcon` REPL. v1-36b (gated on W2): migrate dis probe from attach-socket `__ping__` to chili listener bare-TCP-connect-and-close. Delete attach-socket transport entirely.

---

## Cross-references

- mdata pre-spike findings: `docs/sync/v1_33_pre_spike_findings_2026-05-23.md` (S3 revised turn 7)
- mdata execution roadmap: `docs/plans/post_v1_32_execution_roadmap_2026-05-23.md`
- mdata v1-32 retro (A-033 closure): `docs/sim/sprint_v1_32_a033_fix_retro.md`
- mdata v1-33 retro (Track B shipped via attach-socket transport): `docs/sim/sprint_v1_33_gw_multi_instance_retro.md`
- Prior chili wishlists adopted live in mdata:
  - `docs/sync/chili_wishlist_2026-05-17_push-model.md` (D-1…D-3 — shipped in 0.8.7, adopted v1-26)
  - `docs/sync/chili_wishlist_2026-05-22_async-surface.md` (W1-W5 async + RwLock fairness — future, not blocking)
