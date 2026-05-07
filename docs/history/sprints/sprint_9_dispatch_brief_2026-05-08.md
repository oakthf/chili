# Sprint 9 dispatch brief — perf-pass-2 + load_multitable profile (Sprint 8 P2 carry-over)

**Kickoff:** 2026-05-08 (autonomous run, user pre-ratification).
**Owner:** coordinator-solo (main Claude); `code-reviewer` subagent at sprint wrap if any chili-core code changes (lesson 7).
**Type:** perf optimization — symbolized profiling + targeted fix for the +22.8% load_multitable_5x200p regression carried over from Sprint 7 R2.
**Predicted pp:** 5–10. Plan reference: Sprint 8 retro [`sprint_8_retro.md`](sprint_8_retro.md) "Sprint 9 hand-off".

---

## Sprint objective

Identify and (if cheap) fix the +22.8% load_multitable_5x200p regression vs parked-claude. Workspace `[profile.bench]` symbol-retention override (Sprint 9 P7, landed at kickoff per Sprint 8 lesson 16) makes symbolized profiling possible. KDB-X CE comparison **conditional on GA status**; if unavailable, skip P6.

**Binary success criterion:**

1. **P2 — load_multitable cost driver IDENTIFIED** (named function or known polars internal). Fix lands if chili-side; documented as upstream report if polars-internal.
2. **P6 — KDB-X CE comparison done** if KDB-X CE is GA AND chili can install + test. Skip if not.
3. **P5 (optional) — re-bench parked-claude with .pep src_path** to remove the apples-to-oranges caveat from Sprint 8 P3+P4. Only if Sprint 9 budget remains.
4. Sprint 9 retro + cadence_metrics row 9 + sprints_index update.

---

## Out of scope

- **P0 — GitHub-host fork.** User-driven; sprint flags status only.
- chili-syntax permissivity ADR — only if Sprint 9 P5 surfaces a real semantic question.
- Phase17 / STAC-M3 / Iceberg — Sprint 12.

---

## Scope

**Part A (P7) — bench profile symbol-retention override (~0.5pp)** — landed at kickoff:

```toml
[profile.bench]
strip = false
debug = true
```

Cold rebuild bench binaries: ~10-15 min wall.

**Part B (P2) — symbolized samply profile + analysis (~2-4pp)**

```bash
samply record --save-only --output /tmp/load_multi_symbolized.json \
    target/release/deps/load_par_df-* \
    --bench load_multitable_5x200p --profile-time 10
```

Re-run Sprint 8 Python analysis script. Identify per-table linear cost driver. Likely: polars-plan LazyFrame setup / polars-io schema inference / mimalloc chunked Series allocation.

**Part C (P2 mitigation if cheap; ~0-3pp)** — chili-side fix bench-gated; defer if polars-internal.

**Part D (P6) — KDB-X CE comparison (~1-3pp if available; 0pp if not)** — install check + if installable, run representative query alongside chili. Compare wall-clock + memory.

**Part E (P5 optional) — parked-claude .pep re-bench (~2-3pp)** — remove apples-to-oranges caveat. Only if budget permits.

**Part F — wrap (~1.5-2pp)** — code-reviewer if Part C touches chili-core; retro; cadence row 9; sprints_index; brief move; CLAUDE.md.

---

## Halt-and-escalate criteria

1. P2 reveals deep polars-internals issue chili can't fix <5pp → defer to Sprint 12.
2. KDB-X CE not available → skip P6 silently.
3. P5 re-bench reveals plans truly different between .chi and .pep → ADR 0004 territory.
4. Watchdog 5h ≥ 80% AND remaining work > 5pp.
5. `df -h /` < 15 GB → halt + clean target/.

---

## Cross-references

- Sprint 8 retro: [`sprint_8_retro.md`](sprint_8_retro.md)
- Sprint 7 R2 source: [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md) Sprint 7 Part B section
- Roadmap Sprint 9 row: [`roadmap_2026-05-07.md`](roadmap_2026-05-07.md)
- Iteration lessons: [`../standards/iteration_lessons.md`](../standards/iteration_lessons.md)
