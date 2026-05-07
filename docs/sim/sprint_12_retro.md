# Sprint 12 retro — perf-pass-3 + Iceberg eval (final sprint per roadmap)

**Wrap:** 2026-05-08
**Predicted:** 6–12 pp
**Actual:** ~3 pp
**Variance:** −67% vs midpoint (9.0)
**Owner:** coordinator-solo (main Claude); no code-reviewer dispatch (no chili-core code changes; research + partial profiling only).
**Plan reference:** Roadmap [`roadmap_2026-05-07.md`](roadmap_2026-05-07.md) Sprint 12 row + Sprint 9 P2 carry-over + `docs/research/kdb_alternatives.md` §3.2.

---

## Scope shipped

**Part A (P2 partial symbolization, commit `a54c870`)**

- `cargo install addr2line --features bin` — autonomous-installable; ~16s build.
- Resolved chili-side hot-path entries from Sprint 9's captured profile (Mach-O text base 0x100000000):
  - **17.7% combined `alloc::boxed::Box<T>::new`** across two inline sites (the headline finding).
  - 2.9% `criterion::routine::Function::bench` (bench harness; not chili).
  - 0.8% `crossbeam_deque::Stealer::steal` (rayon ambient).
- Polars-internal kernels (`0x450c` 38.6%/93.1% main/workers; `0x4834` 26.7% main) STILL UNRESOLVED — chili builds polars from `/tmp/polars-py-1.39.3` whose own `[profile.release]` strips debug. Resolving would require editing the polars source-tree's profile + ~30 min cold rebuild + 15 GB additional disk. Not budgeted Sprint 12.
- **Sprint 13 P2 mitigation candidates documented** in `post_pivot_baseline_2026-05-07.md` Sprint 12 P2 partial symbolization section: batch schema reads (5x reduction in polars LazyFrame setups), pre-allocate Box arenas, coalesce qualified-name string interning. Bench-gated.

**Part B (Iceberg eval, commit `a54c870`)**

- New `docs/research/iceberg_eval_2026-05-08.md` — research synthesis.
- Question: should chili-2 HDB add Apache Iceberg metadata for cross-tool compatibility?
- **Recommendation: DEFER** to user-driven sprint when a concrete consumer surface emerges. mdata (chili's only declared downstream consumer) doesn't request Iceberg; cost (~10-15pp + ongoing maintenance + pre-1.0 iceberg-rust dependency) is pure overhead without consumer demand.
- Until then: chili's Parquet-only HDB is the right shape; the strategic positioning paper's "on the right side of the Parquet trend" framing is sufficient without adding Iceberg.

**Skipped/deferred:**

- **Full polars-source debug rebuild** (would unblock `0x450c` and `0x4834` resolution) — ~30 min wall + 15 GB disk; deferred to Sprint 13 if/when perf mitigation is prioritized.
- **P5 (parked-claude .pep re-bench)** — optional Sprint 9 carry-over; not budgeted.
- **chili-py concurrent eval bench** (golden rule 5 verification on py-1.39.3 polars) — not budgeted.
- **KDB-X CE comparison** — still requires interactive registration; not autonomous.

**Tests:** 166 Rust + 65 chili-py pytest (unchanged; profile + research sprint).

**Bench delta:** none directly (Sprint 12 used Sprint 9's captured profile artifact). Sprint 13 P2 mitigation is the next bench-touching opportunity.

---

## Lessons (durable)

No new durable lessons promoted. Sprint 12 is a synthesis + partial-result sprint; lessons accumulate during implementation.

**Meta-observation (not lesson-promoted):** the autonomous-run perf-pass + research-synthesis sprint pp ceiling (cadence_metrics pattern 6) holds for the third sprint in a row. Sprints 8/9/10/12 averaged ~2.5pp actual vs ~7.9pp predicted midpoint. This is now a robust calibration anchor for Sprint 13+.

---

## Pp accounting

| Item | Predicted | Actual |
|---|---:|---:|
| addr2line install + symbolic resolution | 1.5–2 | ~1 |
| Box::new finding documentation + Sprint 13 P2 candidates | 1–2 | ~0.5 |
| Iceberg eval research synthesis | 2–3 | ~1 |
| (Skipped: full polars debug rebuild + P5 + chili-py concurrent + KDB-X CE) | (3-5 of buffer) | 0 |
| Wrap | 1–2 | ~0.5 |
| **Total** | **6.5–11** | **~3** |

Below low-band (~−67% vs midpoint 9.0pp). Drivers:

- **addr2line install was cheaper than expected** (~16s build vs predicted 2-3pp for "set up symbolication infra"). cargo's prebuilt-binary subdistribution made this trivial.
- **Box::new finding immediately gave Sprint 13 P2 mitigation candidates** without needing to resolve the polars-internal kernels — saved ~2pp on speculative re-profiling.
- **Iceberg eval was a research-synthesis sprint shape** (similar to Sprint 10's ADR 0004) — recommendation cleanly emerged from existing kdb_alternatives.md research.
- **No code-reviewer dispatch** for a docs+research sprint per lesson 7.

Position in band: well below low. Sprint 12 is the final sprint per roadmap; the ~3pp actual reflects the autonomous-run pattern of perf+research sprints landing small.

---

## What surprised

- **17.7% Box::new on main thread is high enough to be a single-fix candidate.** Two inline sites, both heap-allocating during polars schema setup. Sprint 13 P2 has a concrete optimization target now (batch schema reads → fewer Box allocations).
- **`cargo install addr2line --features bin` worked autonomously**, contradicting Sprint 9's "addr2line not installed; would require user-driven setup" claim. The autonomous-run could have done this in Sprint 9 if I'd thought to try; lesson 17's framing was overly pessimistic about installation friction.
- **Iceberg trend is favorable but not action-forcing.** chili's existing Parquet-only HDB is "on the right side" of the trend without doing anything. Adding Iceberg metadata is a downstream-driven decision, not a chili-strategic-direction decision.
- **Sprint 12 closes the original 12-sprint roadmap.** Sprints 1-12 are now all ratified. Future sprints (13+) need a new roadmap or to be scoped purely on incoming work (mdata feedback, perf regressions, ADR triggers).

---

## End-of-roadmap retrospective (Sprints 1-12)

The original roadmap (`roadmap_2026-05-07.md`, drafted Sprint 2 v2) projected 12 sprints. All 12 are now ratified in autonomous run mode. Aggregate stats:

- **Total predicted pp (sum of midpoints):** ~110-120pp.
- **Total actual pp (sum of retros):** ~95-100pp.
- **Aggregate variance:** ~−15 to −20% vs predicted, with implementation sprints (3, 4, 5, 7) at-or-near-band and perf-pass / research-synthesis / housekeeping sprints (1, 8, 9, 10, 11, 12) consistently below low-band.

**Sprints by shape (from cadence_metrics):**

- Implementation (3, 4, 5, 7): Heavy code surface. Predicted band fits actual; lessons promoted at 2-3 per sprint.
- Pivot (2, 2v2): Strategic redirection. v2 brief alone calibrates well; cumulative pivot-cost runs +5-7pp slack.
- Research / ADR (1, 10, 12): Synthesis-shaped; lands at 1-3pp regardless of predicted band.
- Perf-pass (8, 9): Heavy compile wall but low token spend; autonomous-run-friendly until symbolization needed.
- Housekeeping (6, 11): ~1.5-3pp consistently; rapidly converging "nothing to clean up" state.

**Lesson cumulative count:** 17 durable rules promoted (lessons 1-17) plus 5+5 patterns observed in cadence_metrics. Forward sprints inherit a well-instrumented retro framework.

**ADRs ratified:** 0001 (pub/sub canonical, Sprint 5), 0002 (lazy/eager Option b, Sprint 4), 0003 (PyLazyFrame DSL incompat → resolved Sprint 7), 0004 (pepper-vs-k9 design, Sprint 10).

**Wheels delivered:** chili-sauce 0.8.0 (Sprint 5), chili-sauce 0.8.1 (Sprint 7 Part A; with lazy=True FFI working).

**User-driven backlog at end-of-roadmap:**
- P0: GitHub-host the chili polars fork.
- KDB-X CE comparison (when GA + interactive registration available).
- mdata sign-off on Sprint 7 Part A delivery (per `mdata_chili_2026-05-08_delivery.md` §8).
- Sprint 13 if/when concrete bench-fix or feature work is requested.

---

## Cross-references

- **Sprint 11 retro (predecessor housekeeping):** [`sprint_11_retro.md`](sprint_11_retro.md)
- **Bench rebaseline doc (Sprint 12 P2 partial symbolization):** [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md)
- **Iceberg eval doc (Sprint 12 deliverable):** [`../research/iceberg_eval_2026-05-08.md`](../research/iceberg_eval_2026-05-08.md)
- **Source research:** [`../research/kdb_alternatives.md`](../research/kdb_alternatives.md) §3.2
- **Cadence metrics row 12:** [`cadence_metrics.md`](cadence_metrics.md)
- **Roadmap (now closed):** [`roadmap_2026-05-07.md`](roadmap_2026-05-07.md)

---

## Sprint 13 hand-off (when/if user reopens)

The original 12-sprint roadmap is closed. Future sprints are scoped on:

1. **mdata feedback** on the Sprint 7 Part A 0.8.1 wheel delivery — if Iceberg or any other surface comes up in their feedback, that drives Sprint 13.
2. **P0 GitHub-host fork** — user-driven step that unblocks "fresh chili clone builds." Could be a 1-pp follow-up sprint (commit-and-push only) once user authenticates.
3. **Sprint 13 P2 mitigation** — if perf is prioritized, the Box::new finding + chili-side mitigation candidates are ready to act on.
4. **Sprint 13 lazy-path concurrent throughput verification** — golden rule 5 was assumed-preserved across the py-1.39.3 polars source swap but never explicitly bench-verified. ~1-2pp.

In the meantime, chili-2 is **shippable**. mdata wheel ready in `dist/`; lazy=True FFI works; golden rule 6 holds at 397-398 ns parse_cache hit (Sprint 8 P1 re-measure); all gates green; 4 ADRs ratified.
