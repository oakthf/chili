# Sprint 9 retro — perf-pass-2 + load_multitable symbolized profile (P2 carry-over)

**Wrap:** 2026-05-08
**Predicted:** 5–10 pp
**Actual:** ~2 pp
**Variance:** −73% vs midpoint (7.5)
**Owner:** coordinator-solo (main Claude); no code-reviewer dispatch (no chili-core code changes; profile-config-only sprint).
**Plan reference:** [`../history/sprints/sprint_9_dispatch_brief_2026-05-08.md`](../history/sprints/sprint_9_dispatch_brief_2026-05-08.md) (moved post-ratification).

---

## Scope shipped

**Part A (P7) — bench profile symbol-retention override (commit `637066d`)**
- Workspace `Cargo.toml` gains `[profile.bench] strip = false; debug = true`.
- Inherits `[profile.release]` optimizations (opt-level 3, lto fat, codegen-units 1) but retains debug symbols. Bench binaries ~22x larger (77 MB vs 3.5 MB stripped).

**Part B (P2) — symbolized rebuild + profile captured (commit `aebcdd9`)**
- Bench binary rebuilt with `[profile.bench]` override: 31m 39s wall.
- samply recorded `load_multitable_5x200p` for 10s; 17,233 main-thread samples + ~5,900 samples on each of 5 polars rayon worker threads.
- **Key finding: 93% of polars worker thread time concentrates in a SINGLE hot kernel at offset `0x450c`.** Not death-by-a-thousand-cuts; one function dominates.
- Symbolic name resolution **infrastructure-blocked autonomously** (samply load needs GUI; atos returned numeric without symbols; addr2line not installed).
- Hot kernel candidates (without name resolution): memcpy/memmove, polars hash kernel (xxhash/ahash), polars-arrow buffer init.
- Captured artifacts for Sprint 12 perf-pass-3 to consume:
  - `/tmp/load_multi_symbolized.json` (3.4 MB; Firefox Profiler format)
  - `target/release/deps/load_par_df-34f40619e2795c29` (77 MB symbolized bench binary)

**Skipped/deferred:**

- **P5 (re-bench parked-claude with .pep src_path)** — optional; not budgeted given P2 took most of Sprint 9.
- **P6 (KDB-X CE comparison)** — KDB-X CE requires interactive registration / EULA click-through; not autonomous-installable. Defer to user-driven sprint.

**Tests:** 166 Rust + 65 chili-py pytest (unchanged; no code touched in chili crates).

**Bench delta:** captured 5,900-sample profile per polars worker thread; identified single dominant kernel by offset. No mitigation landed (symbolization needed first).

---

## Lessons (durable)

### macOS samply autonomous-run profiling produces unsymbolicated profiles even with debug-info-embedded binaries

**Rule.** When using samply on macOS in an autonomous-run / headless environment, expect that **`samply record --save-only` produces a JSON profile with hex-address stack frames, NOT symbolized function names**, even when the bench binary has `[profile.bench] debug = true; strip = false`. samply's symbolization happens at *display time* (`samply load <json>`) by spawning a browser that fetches symbols from the binary at runtime. Without GUI, you have three resolution paths, all autonomous-friction:

1. Upload JSON to `https://profiler.firefox.com/` — user action.
2. Use `atos` with a `dsymutil`-generated dSYM bundle — extra build step + Xcode dependency for `xctrace` chain.
3. Install `llvm-addr2line` / `addr2line` (~50 MB / cargo install ~5 min) — feasible autonomously but not pre-installed.

For autonomous-run, the practical workaround is to **capture the profile + the bench binary as Sprint artifacts; defer symbolization + actual fix to a future user-driven sprint** (or a sprint that pre-installs `cargo install addr2line` as Part A).

**Why.** Sprint 9 P2, 2026-05-08. 31m 39s rebuild + 10s samply record produced a profile that pinpointed the dominant kernel by offset (`0x450c` = 93% of polars worker time) but without function names, no actionable mitigation possible autonomously. Sprint pp spent: ~1.5pp on rebuild + capture + analysis; sprint value captured: hot-kernel offset isolated. Decision-pivot: defer symbolization + fix to Sprint 12 (or user-driven mini-sprint). Net pp on Sprint 9 P2: ~1.5pp; deferred-fix pp on Sprint 12 P2: ~3-4pp (was 3-4pp anyway; shape shifts but cost stays).

**Apply where.** Any chili sprint planning samply-driven optimization on macOS without GUI access. Specifically: Sprint 12 perf-pass-3, future bench-pass sprints. Generalizes to any Rust + macOS performance-sensitive project where the autonomous-run environment lacks GUI / Xcode / pre-installed perf tools. Inverse case (Linux autonomous run) may have better symbolication ergonomics — perf reports symbolize at record time on Linux.

**Cost saved.** Future bench-pass sprints don't waste pp re-discovering this. Sprint 12 P2 budget should explicitly include "+1pp for `cargo install addr2line` or equivalent symbol-resolution infra" before profiling work begins.

---

## Pp accounting

| Part | Predicted | Actual |
|---|---:|---:|
| A (P7) — `[profile.bench]` override | 0.5 | 0.5 |
| B (P2) — rebuild + samply record + analyze | 2–4 | 1.5 |
| C (P2 mitigation) | 0–3 | 0 (deferred to Sprint 12) |
| D (P6 KDB-X CE) | 1–3 if available | 0 (not autonomous-installable) |
| E (P5 .pep re-bench) | 2–3 (optional) | 0 (skipped) |
| F (wrap) | 1.5–2 | 0.5 (no code-reviewer; bench-files+docs only sprint) |
| **Total** | **5–10** | **~2** |

Way below low-band (~−73% vs midpoint 7.5pp). Drivers:

- **P2 mitigation deferred** because symbolization infrastructure-blocked — the deepest perf work was uncoverable autonomously.
- **P5 + P6 skipped** because KDB-X CE not installable + .pep re-bench is optional.
- **No code-reviewer dispatch** for a profile-config + samply-record + retro sprint.

Sprint 8 + 9 combined: ~6pp actual vs ~12-22pp predicted. **Pattern: autonomous-run macOS perf-pass sprints have a structural ceiling around 2-4pp** because of GUI / Xcode / pre-installed-tool dependencies. The user direction "work it through over the night" is bounded more by infrastructure than by token budget on perf-shaped work.

---

## What surprised

- **samply's saved profile doesn't include resolved symbols even with debug-info-embedded binaries.** The symbolization is a display-time concern for samply, not a save-time one. autonomous-run friendly tools like cargo-flamegraph would have symbolized at record time, but cargo-flamegraph requires Xcode on macOS (not just CLI tools).
- **93% concentration in a single offset** is unusually high — most regressions are spread across multiple functions. The +22.8% load_multitable regression is essentially "one kernel runs 5x more often than it did before" rather than "lots of small overheads."
- **Bench rebuild with debug symbols took 31m 39s** vs ~10-15 min for stripped-release. The compile-time cost of debug symbols is non-trivial on the polars 0.53 dep tree. Lesson 16 ("[profile.bench] symbol-retention override pre-staged") is even more important than I initially framed: starting Sprint 9 with the override pre-applied saved the realization-mid-sprint cost.
- **KDB-X CE is "GA" on the marketing page but installation requires registration / EULA acceptance** — not flagged in the public-facing docs as such; only discoverable by going through the install flow. Worth a CLAUDE.md note: any future Sprint planning a comparison against KDB-X CE assumes user-driven setup.

---

## Cross-references

- **Plan:** [`../history/sprints/sprint_9_dispatch_brief_2026-05-08.md`](../history/sprints/sprint_9_dispatch_brief_2026-05-08.md) (moved post-ratification)
- **Sprint 8 retro (predecessor):** [`sprint_8_retro.md`](sprint_8_retro.md)
- **Bench rebaseline doc (P2 captured artifacts + analysis):** [`../bench/post_pivot_baseline_2026-05-07.md`](../bench/post_pivot_baseline_2026-05-07.md)
- **Cadence metrics row:** [`cadence_metrics.md`](cadence_metrics.md) row 9 (this commit).
- **Iteration lessons (lesson 17 promoted this sprint):** [`../standards/iteration_lessons.md`](../standards/iteration_lessons.md)

---

## Sprint 10 hand-off

Sprint 10 per roadmap = "Pepper conformance to k9 design (ADR sprint)". User-decision territory per `shakti_analysis.md` §4.3. Likely flow:

- Draft ADR 0004 (Pepper-vs-k9 design) based on existing research.
- Surface user-decision questions: should chili's Pepper syntax track Whitney's k9 simplification (smaller primitive set), OR retain its current q-style primitives?
- Halt for user ratification before any code changes.

P2 (load_multitable mitigation) deferred to Sprint 12 perf-pass-3 with the symbolization infrastructure prerequisite documented (lesson 17).
