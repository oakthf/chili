# Sprint 8 dispatch brief — perf-pass-1 (Sprint 7 R1/R2/R3 fixes + A/B fill)

**Kickoff:** 2026-05-08 (autonomous run, user pre-ratification).
**Owner:** coordinator-solo (main Claude); `code-reviewer` subagent at sprint wrap.
**Type:** perf optimization + bench finishing.
**Predicted pp:** 6–12. Plan reference: Sprint 7 retro [`sprint_7_retro.md`](sprint_7_retro.md) "Sprint 8 backlog" + bench rebaseline doc R1/R2/R3.

---

## Sprint objective

Address the three regressions surfaced in Sprint 7 Part B's bench A/B sweep, populate the eval/projection A/B rows, and reinstate golden rule 6 (≤400 ns parse_cache hit) end-to-end. **P0 (GitHub-host the chili polars fork) requires user GitHub auth; flagged for follow-up, not part of this sprint's autonomous scope.**

**Binary success criterion:**

1. **P1 — parse_cache hit ≤400 ns** confirmed via re-measurement, OR ADR 0004 amends golden rule 6 with the py-1.39.3 baseline.
2. **P2 — load_multitable_5x200p +22.8% characterized** with flamegraph + identified driver. Mitigation if cheap; deferred to Sprint 9 if not.
3. **P3 — eval bench parser regression resolved** (bench file `src_path` fix `.chi` → `.pep`).
4. **P4 — eval/projection A/B rows populated.**
5. **P5 (optional) — chili-py concurrent eval bench** validates golden rule 5 (6.10× concurrent throughput) on py-1.39.3 polars.
6. Sprint 8 retro + cadence_metrics row 8 + sprints_index update.

---

## Out of scope

- **P0 — GitHub-host fork.** User-driven; sprint provides migration steps doc as Sprint 8 wrap deliverable.
- Phase17 / STAC-M3 / KDB-X — Sprint 9+.
- chili-syntax permissivity ADR — only if P3 reveals a real semantic question. Default = bench file fix.

---

## Scope summary

**Part A (P1 — parse_cache).** First move: re-measure. Apple Silicon thermal/memory variance is 20-40 ns per run (reviewer C1). If 3 runs are under 400 ns, P1 resolved (no code change). If confirmed >400 ns, samply profile + reclaim chili-side; ADR 0004 is the escape hatch.

**Part B (P3 + P4 — eval bench).** Reviewer S1 fix: change `crates/chili-op/benches/eval.rs` `src_path = "bench.chi"` → `"bench.pep"`. Bench engine already calls `state.enable_pepper()`. Re-run claude-2's eval + projection benches; populate the A/B Δ% rows. ~1.5–2pp.

**Part C (P2 — load_multitable).** samply flamegraph workflow per bench rebaseline doc R2. Compare against single-table load (only +2.2% — within noise) to identify per-table linear cost driver. Mitigation if chili-side; document for upstream report if deep in py-1.39.3 polars internals. ~2–4pp.

**Part D (wrap).** code-reviewer dispatch (Part D.1 absorption); standard retro + cadence + index. Conditional 0.8.2 wheel re-cut if P1+P2 both resolved with material wins. ~1.5–2.5pp.

**P5 (optional).** chili-py concurrent eval bench at `crates/chili-py/tests/bench_concurrent.py` to confirm golden rule 5 didn't regress on py-1.39.3 polars. ~0.5pp if green.

---

## Halt-and-escalate criteria

1. P2 profiling reveals deep polars-internals issue chili can't fix in <5pp → defer to Sprint 9.
2. P1 confirmed >400 ns and chili-side mitigation isn't surgical → ADR 0004 amends golden rule 6.
3. Compile breakage during bench iteration → triage, fix, resume.
4. Watchdog at 5h ≥ 80% AND remaining work > 5pp → halt + cron resume per shutdown protocol. Headroom at kickoff: ~98% (fresh window).
5. `df -h /` < 15 GB free → halt + clean target/.

---

## Cross-references

- Sprint 7 retro: [`sprint_7_retro.md`](sprint_7_retro.md)
- Bench rebaseline doc (R1/R2/R3 source): [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md)
- ADR 0003 (resolved Sprint 7 Part A): [`../decisions/0003-pylazyframe-dsl-incompat.md`](../decisions/0003-pylazyframe-dsl-incompat.md)
- Iteration lessons: [`../standards/iteration_lessons.md`](../standards/iteration_lessons.md)
- mdata 0.8.1 delivery: [`../sync/mdata_chili_2026-05-08_delivery.md`](../sync/mdata_chili_2026-05-08_delivery.md)
