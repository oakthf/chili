# Chili — Competitive Position and Strategic Roadmap, 2026-05-06

**Audience:** the user. This is the read-this-first synthesis doc for Sprint 1.
**Scope:** answer two questions: (a) where does chili stand vs kdb+ / Shakti / OSS alternatives today; (b) what is the 6–12 sprint sequence to close the gap.
**Companion docs (deep dives):** `q_kdb_landscape.md` (kdb+ itself, 500 lines), `kdb_alternatives.md` (competitors, 666 lines), `shakti_analysis.md` (Shakti / k9, 403 lines), `main_vs_claude_inventory_2026-05-06.md` (commit-by-commit), `../history/sim/roadmap_2026-05-06.md` (sprint sequence).

---

## TL;DR (for the user)

1. **Chili is a credible kdb+ replacement candidate, but not yet a kdb+ replacement.** The architectural foundation is sound (Polars + Arrow + Parquet + Rust + Python binding + q-like language layer); no other open-source project has all five together. The gap to kdb+ is **performance + benchmark verification + pub/sub canonical contract**, not architecture.
2. **The realistic 12-month target is "match-or-beat kdb+ 4.1 on STAC-M3-shape Antuco operations on commodity hardware."** Beating Kanaga (the harder STAC suite) or Whitney's 2022 Shakti DDN result requires capital and hardware partnerships chili doesn't have today; aspirational, not Q1 target.
3. **The biggest near-term blocker is the mdata pub/sub wishlist** — and it's blocked on a real architectural decision (3 competing pub/sub models in claude's tree) that needs an ADR, not a cherry-pick. Sprint 2 starts the foundation; Sprint 4 makes the architectural call.
4. **The vision (full main→claude reconciliation; outperform kdb+ benchmarks) is reachable in 6-10 sprints,** with measurable inflection points at Sprints 4 (pub/sub canonicalized), 6-7 (chili baseline against kdb+ benchmarks), and 8 (main-merge milestone).

---

## 1. Where chili stands today — by axis

### 1.1 Architecture (where chili already wins or ties)

| Axis | kdb+ / Shakti | Modern OSS alternatives (DuckDB, ClickHouse, etc.) | Chili |
|---|---|---|---|
| Open-source license | ❌ Closed source (Kx) / closed (Shakti) | ✅ Apache 2 / GPL | ✅ Open source |
| Memory safety | ❌ C, manual memory management | Mixed (C++ in CH, C in DuckDB) | ✅ **Rust** — only Rust-based competitor with full kdb+ topology |
| Native columnar engine | ✅ Bespoke | ✅ DuckDB, ClickHouse, etc. | ✅ via Polars + Arrow |
| Native Python binding | ❌ kdb+ has PyKX as an add-on; Shakti has no public binding | Polars (yes); DuckDB (yes); ClickHouse (yes) | ✅ chili-py with FFI rewrite (no IPC bytes; `pyo3_polars::PyDataFrame` direct) |
| q-like language layer | ✅ q (kdb+); k9 (Shakti) | ❌ — DuckDB / ClickHouse expose SQL only; no q-like alternative in mainstream OSS | ✅ **Pepper** (q-like) + **Chili** (JS-like). **Unique combination in OSS.** |
| Parquet on-disk | ❌ Native splay/parted (proprietary) | ✅ DuckDB, ClickHouse | ✅ |
| Arrow in-memory | ❌ kdb+ exposes via converters; not native | ✅ DuckDB, Polars, DataFusion native | ✅ via Polars |

**The architectural axis where chili is uniquely positioned:** OSS + Rust + Polars + chili-py + Pepper. No other project ticks all five.

### 1.2 Performance (where chili is unverified / behind)

| Operation | kdb+ baseline (best public) | Chili measured | Gap |
|---|---|---|---|
| STAC-M3 Antuco mean response | kdb+ 4.0 / 4.1, see `q_kdb_landscape.md` §3.1 | **Not yet benchmarked in STAC-M3 shape** | unknown — Sprint 6 baseline establishes |
| Shakti's published 3.7× / 3.3× over kdb+ | 2022 STAC SHK211203, Antuco | n/a | upper-bound aspirational target |
| Parse cache hit | n/a (kdb+ has no equivalent) | **~385 ns** (CLAUDE.md golden rule 6) | chili leads here |
| `load_par_df` wall-clock | n/a | bench data in `docs/bench/phase{1..7,9}.md` | needs apples-to-apples vs kdb+ |
| Concurrent throughput (chili-py) | n/a | **6.10× single-thread baseline** (CLAUDE.md golden rule 5; GIL-released eval) | chili-specific win; not a kdb+ comparable |

**The performance axis is the biggest open question.** Sprint 6 stands up the benchmark suite; Sprints 7 / 10 / 12 close the gap. Until then, performance claims are theoretical.

### 1.3 Ecosystem (where chili is small)

| Axis | kdb+ | DuckDB | Chili |
|---|---|---|---|
| Years of production hardening | 25+ | 6 (since 2019) | ~1 |
| Public users | Hundreds of HFT shops | Tens of thousands of dev / research users | mdata, nxcar (1-2 prod consumers; this user) |
| Community size | Niche; q meetup ~hundreds | Thousands; rapid corporate adoption | Tiny — early-stage |
| Tooling (IDE, debugger, observability) | Extensive (qStudio, KX Dashboards) | DuckDB CLI; growing IDE support | chili CLI; no dashboards yet |
| Docs / education | Extensive (q books, KX Academy, Stack Exchange q tag) | Extensive (DuckDB docs, blog, conferences) | CLAUDE.md + READMEs + research docs |

**The ecosystem gap is the slowest to close** and arguably the least urgent. Performance-led displacement (kdb+ replaced because chili is faster on the same hardware) is more leveraged than community-led displacement.

### 1.4 The honest summary

Chili **today** is a small, technically-sound, single-user-validated columnar timeseries engine with a unique-in-OSS combination (Rust + Polars + Python binding + q-like + JS-like). It is **not** today's open-source kdb+ replacement (DuckDB, ClickHouse, KDB-X CE compete more credibly on most workloads). It **could be** the open-source kdb+ replacement if it (a) verifies kdb+-class performance on a benchmark suite, (b) ships a canonical kdb+-tickerplant-equivalent surface, and (c) attracts a second non-trivial production user. The roadmap addresses (a) and (b); (c) is downstream of (a) and (b).

---

## 2. Strategic posture — three load-bearing decisions

The user faces three decisions in the next 6 weeks. Each is documented in detail in companion docs but called out here so the strategic frame is clear.

### 2.1 Sprint 2 shape — cherry-pick or full merge?

**Per `main_vs_claude_inventory_2026-05-06.md` §3:** the wishlist's 5 commits cherry-pick approach is 3-of-5 clean and 2-of-5 architectural-conflict. The conservative path is Sprint 2 = clean cherry-picks + Sprint 4 = ADR + reconciliation. The aggressive path is Sprint 2 = full main → claude merge (the Sprint 8 milestone moved earlier).

**Recommendation: conservative.** mdata is unblocked sufficiently by Sprint 2 cherry-picks (TCP listener + serde9 fix + recursive load_par_df + stats). The pub/sub framework they ultimately want is a Sprint 4 deliverable regardless of cherry-pick or merge approach.

### 2.2 Sprint 4 ADR — pub/sub canonical model?

**Per `inventory_2026-05-06.md` §2.6:** three options weighed; user direction 2026-05-07 selected **Option (c) with measured retirement**.

- (a) **Adopt upstream's tick/sub framework** as canonical; retire claude's two existing models.
- (b) **Keep claude's two models;** port only tplog durability as additive.
- (c) **Hybrid.** Upstream's `tick.pep` / `sub.pep` as Pepper-canonical; claude's `publish(ipc_bytes)` retained as low-level escape hatch — but **as a transitional state, not a permanent fixture.** Sprint 4 implements the hybrid + A/B benchmark scaffolding; Sprint 4.5 runs the A/B comparison on perf / compactness / efficiency + collects mdata production-audience feedback; the retirement ADR (`docs/decisions/0002-…`) decides whether claude's models retire ahead of Sprint 8's full merge or persist durably.

**Decision rationale:** preserves nxcar/mdata's existing Python callers while gaining the canonical kdb+ tickerplant surface; gates the eventual retirement on data not opinion; treats mdata as the production-audience feedback source they actually are.

### 2.3 Sprint 11 — Pepper conformance to k9 design?

**Per `shakti_analysis.md` §4.3:** Whitney has converged on a smaller primitive set in k9 than in q. Pepper today is q-shaped, not k9-shaped. For chili to be a credible kdb+ *successor* (not just replacement), Pepper should evaluate k9 design choices.

**Recommendation: defer to Sprint 11.** Performance-parity (Sprints 6-10) needs to land first; language-layer changes are user-visible and should be made when the engine is mature, not while it's evolving.

---

## 3. The roadmap (per `../history/sim/roadmap_2026-05-06.md`)

| Sprint | Theme | Predicted pp | Why |
|---|---|---:|---|
| 2 | mdata-foundation (TCP listener + serde9 fix + stats) | 5-8 | unblock mdata fast |
| 3 | recursive load_par_df + multi-subscriber | 4-7 | mdata Wave-3 HDB |
| 4 | pub/sub ADR + Option (c-measured) + A/B scaffolding | 10-16 | architectural inflection |
| 4.5 | pub/sub A/B measurement + mdata feedback + retirement-call ADR | 4-7 | data-gated retirement |
| 5 | deep housekeeping #1 | 3-5 | 5-sprint cadence |
| 6 | bench-suite-v0 (STAC-M3 shape) | 5-8 | establish baselines |
| 7 | perf-pass-1 (primitives + storage codecs) | 6-10 | Shakti's ~70-80% edge |
| 8 | full main → claude merge milestone | 8-14 | strategic vision |
| 9 | deep housekeeping #2 | 3-5 | 5-sprint cadence |
| 10 | perf-pass-2 + KDB-X CE comparison | 6-10 | apples-to-apples |
| 11 | Pepper conformance to k9 | 5-10 | language-layer ADR |
| 12 | perf-pass-3 + Iceberg eval | 6-12 | future-proof storage |

Total predicted: **~60-105pp over 6 months.**

---

## 4. Open questions and risks

### 4.1 Open questions feeding next research wave (consolidated)

From the four deep-dive docs:

- **Q1 (q_kdb landscape):** STAC-M3 absolute ms timings — fetched from Supermicro audited PDF in Sprint 6 prep.
- **Q2 (kdb_alternatives):** KDB-X CE GA timing and resource caps — verify before Sprint 10.
- **Q3 (kdb_alternatives):** OneTick / KX merger productization timeline — relevant to Sprint 11+ language strategy.
- **Q4 (shakti):** any second Shakti STAC-M3 result published since 2022? — re-check before Sprint 7.
- **Q5 (shakti):** ngn/k vs growler/k as the active fork — confirm before any k9-design borrowing.
- **Q6 (inventory):** CLAUDE.md vs pyproject.toml chili-pie name discrepancy — Sprint 2 prep.
- **Q7 (inventory):** PyLazyFrame need on claude — Sprint 2 prep.
- **Q8 (inventory):** parking_lot vs std::sync benchmark on parse_cache hot path — Sprint 7 prep.

### 4.2 Risks to the roadmap

1. **Sprint 4 takes longer than 8-14pp.** Three-model reconciliation is structurally complex; if it slips, Sprints 5-12 shift right.
2. **mdata can't wait 4 sprints for full pub/sub.** If Sprint 2's partial pickup isn't enough, Sprint 4 may need to come first; this would inflate Sprint 2's scope.
3. **Sprint 6 benchmark baseline is worse than expected.** If chili is materially behind kdb+ on STAC-M3-shape ops, the optimization sprints (7, 10, 12) may not close the gap in 12 months. Need to consider whether to lower the kdb+-replacement aspiration to "kdb+ subset" (timeseries analytics for non-HFT shops where kdb+ is overkill).
4. **Upstream main keeps moving.** If the chili author ships another wave of refactors during Sprints 2-4, the inventory expands. Mitigation: the cherry-pick cycle continues until Sprint 8's full-merge.
5. **License / IP concerns.** Pepper's q-like syntax is close enough to q that Kx might object. Lower-risk than copying q's runtime, but worth a one-time legal check before public marketing of "kdb+ replacement" framing. Sprint 11 ADR could include this.

---

## 5. What this Sprint 1 produced (artifact register)

For Sprint 1's binary success criterion: this doc + four deep dives + a roadmap.

| File | Lines | Role |
|---|---|---|
| `docs/research/q_kdb_landscape.md` | 500 | kdb+ deep dive — history, benchmarks, strengths/weaknesses |
| `docs/research/kdb_alternatives.md` | 666 | competitor catalog (5 groups, ~20 projects), taxonomy, chili-fit analysis |
| `docs/research/shakti_analysis.md` | 403 | Shakti / k9 deep-dive, design lineage assessment, decomposition of Shakti's edge |
| `docs/research/main_vs_claude_inventory_2026-05-06.md` | ~285 | every claude..main commit classified, conflict surface predicted, pickup verdicts |
| `docs/history/sim/roadmap_2026-05-06.md` (superseded by `roadmap_2026-05-07.md` post-pivot) | ~225 | Sprints 2-12 sequence with pp bands and gating |
| `docs/research/competitive_position_2026-05-06.md` (this doc) | ~200 | the read-this-first synthesis |

Total Sprint 1 deliverable: ~2280 lines of strategic research + planning.

---

## 6. Next action — Sprint 1 retro and Sprint 2 kickoff

After this synthesis lands and the user reviews:

1. Author `docs/sim/sprint_1_retro.md` per `_retro_template.md`. Land cadence_metrics row 1. Update sprints_index.md to "Wrapped (awaiting ratification)."
2. **HALT** for user ratification of Sprint 1 retro per cadence rule.
3. On ratification, draft Sprint 2 dispatch brief per `_dispatch_brief_template.md` covering the mdata-foundation cherry-picks. Halt for ratification before Sprint 2 starts.

The cadence machinery now has its first real load test. If Sprint 1 ratifies cleanly, the convention is proven. If the retro surfaces process issues, those become iteration_lessons.md entries — exactly the calibration loop the convention exists for.
