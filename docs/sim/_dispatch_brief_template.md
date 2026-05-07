# Sprint N dispatch brief — <theme> (<priority labels>)

**Kickoff:** TBD — <gate condition>
**Owner:** coordinator-solo / coordinator + <subagent>
**Type:** implementation / design / scaffold / hotfix
**Predicted pp:** N–M
**Plan reference:** `docs/sync/<roadmap>.md` §<priority>
**ADR references (if any):** `docs/decisions/<adr>.md`

---

## Sprint objective

One paragraph stating the sprint's primary goal. Plain language;
"close the X gap" / "wire Y to Z" / "design ADR for W". State the
binary success criterion.

---

## Why now

Bullets answering "why this sprint, why this order, why now?" — usually
2–4 short reasons. E.g., gate-condition cleared; cross-project unblocker;
post-bench data demands it.

---

## Scope — Part A: <sub-priority>

### A.1 Surface additions

Code surface this sprint adds (new methods / modules / crates). One
paragraph or bullet list per item. Include type signatures for new
public APIs (Rust trait impls, PyO3 method signatures, parser surface).

### A.2 Implementation hints

Non-obvious hints the implementer would otherwise have to discover.
Cite existing patterns in the codebase to mirror — e.g., parse-cache
hot-path conventions in `crates/chili-core`, GIL-release pattern in
`crates/chili-py`.

### A.3 Storage / schema (if applicable)

New tables / new on-disk format / changes to existing dtypes. The
Int64-quantized price-column convention is load-bearing (CLAUDE.md
golden rule 4) — coordinate any change with mdata's storage layer.

### A.4 Tests

Rust unit / Rust integration / Python pytest / benchmarks — what's
expected to land.

---

## Scope — Part B: <sub-priority>

(same shape as Part A)

---

## Out of scope (defer)

Bullet list of items *deliberately* not covered by this sprint, with
brief rationale. Flag where they're tracked (next-sprint brief,
follow-up ADR, future ADR territory).

---

## Deliverables

| # | Artifact | Type |
|---|---|---|
| 1 | `<path>` | new / edit |
| 2 | … | … |
| N | `docs/sim/sprint_N_retro.md` | new (post-sprint) |

---

## Lead allocation

Coordinator-solo / coordinator + spawn pattern. State which subagent
(if any), why, and the budget delegation in pp.

If running in worktree (parallel sprints): state worktree path,
branch name, coordination boundaries (DO NOT touch / DO touch /
ESCALATE-IF), and merge strategy.

---

## Mid-checkpoint plan

At ~50% predicted-pp consumed, post a short status:

- <checkpoint question 1>
- <checkpoint question 2>
- ETA to wrap.

Halt-and-escalate criteria:

1. **Scope-blowing bug** — discovered issue would push actual-pp >150% of predicted.
2. **Plan-pivot finding** — sprint premise contradicted by mid-sprint discovery (e.g., the bottleneck is a different component than predicted).
3. **User-decision needed** — reversible architectural choice not previously surfaced.
4. **Watchdog approaching** — 5h ≥ 80% AND remaining work > 15pp. (The 90% `SHUTDOWN_SIGNAL` write is the hard backstop; don't reach it.)

---

## Wrap (per ceremony)

- Pre-commit gate green: `cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test`.
- Python-bindings wrap (if `crates/chili-py` touched): `cd crates/chili-py && uv run maturin develop && uv run pytest`.
- Bench delta documented (if touching scan / eval / load_par_df / write_partition / parse-cache hot path).
- Test-count delta documented.
- Author retro at `docs/sim/sprint_N_retro.md`.
- Append row to `docs/sim/cadence_metrics.md`.
- HALT until user ratifies.

---

## Pp accounting reference

Closest historical comparable sprints from `docs/sim/cadence_metrics.md`:

- **Sprint X (<theme>)** — predicted N–M, actual K. Comparable because <reason>.
- **Sprint Y** — …

Sprint N expected at the <low/mid/high> end of the predicted band, capped
above by <potential-overrun-driver>.

---

## Cross-references

- Roadmap: `docs/sync/<file>.md`
- Related ADRs: …
- Cross-project (if any): …
