# Iteration Lessons — chili durable rules

Central append-only file for lessons promoted from sprint retros. A lesson promotes here
after being observed in **2+ independent sprints** OR after a single high-cost incident
where the cost is unambiguous. If you can't estimate "Cost saved," leave the lesson in
the sprint retro (`docs/sim/sprint_N_retro.md`); don't promote yet — the discipline is
the filter.

Forward-only: this file begins empty under the new cadence (seeded 2026-05-06). Past
chili lessons — the GIL-release win (CLAUDE.md golden rule 5), the parse-cache hot-path
constraint (golden rule 6), the Int64-quantized storage convention (golden rule 4), the
no-remote / `claude`-only branch policy — are already encoded in `CLAUDE.md` and stay
there; they are not retro-promoted into this file.

## Entry format

```markdown
### <Title — the rule itself>
**Rule.** What to do or avoid. One paragraph.
**Why.** Concrete sprint reference + evidence. One paragraph.
**Apply where.** Which future contexts this binds.
**Cost saved.** Estimate of pp / wall-clock / risk avoided. One line.
```

Never rewrite or remove existing entries. Always append below the last entry.

## Promotion criterion

Promote a lesson here only when **both** are true:

1. The rule has been validated by ≥2 independent sprints OR a single incident with
   clearly attributable cost.
2. "Cost saved" can be estimated concretely (pp / wall-clock / risk avoided). If you
   can't estimate, the lesson isn't durable yet — leave it in the sprint retro.

Leads (and main Claude) read this file before writing code. Treat it as a project-
specific extension of the global rules in `~/.claude/rules/`.

---

### Hard rollback beats manual revert when undoing a committed multi-file change

**Rule.** When a committed change needs partial undoing (some files revert, others
stay), prefer `git reset --hard HEAD~N` (or `git checkout <sha>~ -- path`) followed
by re-applying the keeper edits over manually re-typing the originals via Edit / Write.
The git object store has the exact pre-commit bytes; manual re-typing introduces typo /
off-by-one / missing-precision risk that is silent and not caught by tests when the
typo happens to be bit-equivalent (e.g. f32 literals where the rounded form parses to
the same bit pattern).

**Why.** 2026-05-06 onboarding session, single-incident promotion. Commit `2128c11`
truncated f32 literals in `crates/chili-op/tests/arithmetic_test.rs` from full
bit-precision form (e.g. `5.420000076293945_f32`) to clippy's "minimal-precision"
form (`5.42_f32`). Bit-equivalent — tests passed — but the corner-case signal that
these values were the result of imprecise f32 arithmetic was lost. When directed to
revert, the first attempt was Edit-based manual re-typing of 8 multi-decimal literals
from clippy's earlier suggestion output. User flagged this required triple-checking
every digit; instead `git reset --hard HEAD~1` restored exact pre-commit bytes
and the keeper edits (`#![allow(clippy::excessive_precision)]` + `useless_vec` fix
+ unrelated chili-py fixes) were re-applied on top as commit `a8d4014`. Zero
typo risk.

**Apply where.** Any partial undo of a committed multi-file change: clippy fix-ups,
formatter sweeps, refactors that touched too many files at once. Especially
load-bearing in test files where bit-exact assertion values are silent-failure
surfaces, in HDB partition layout files where dtype/precision is load-bearing
(CLAUDE.md golden rule 4), and in parse-cache hot-path code (golden rule 6). Inverse
case (single-file targeted edit revert with no committed checkpoint to roll back to)
doesn't need this — `git checkout HEAD -- file` is fine. Branch is local-only by
chili's branch policy (no remote), so `--hard` rollback is invisible to the outside.

**Cost saved.** ~1–3pp typo-debugging round avoided in this session; non-trivial risk
reduction on shipping subtly-wrong test assertions downstream (mdata and other
consumers import the chili-py wheel directly — silent test corruption could land in
their CI as a hard-to-trace upstream regression).

### API divergence silently invalidates cherry-pick plans

**Rule.** Before drafting a cherry-pick / merge plan against an upstream branch, run
a per-surface diff of `claude` vs the relevant upstream commits — especially for
surfaces where claude has its OWN feature implementation. If claude already has a
divergent shape (different signature, different lock model, different return type),
cherry-picking will produce real line-level conflicts, NOT trivial whitespace.
Surface "we have surface X, upstream has surface X under different shape" as a
HEAVY conflict prediction in any inventory doc, with named source paths on both
sides.

**Why.** Sprint 1 Part D, 2026-05-07. The mdata wishlist
(`~/code/mdata/docs/sync/chili_upstream_wishlist_2026-05-06.md`) requested
cherry-picking 5 commits from main into claude. A surface-level read suggested
clean cherry-picks. A diff-by-diff inventory revealed THREE competing pub/sub
models on claude (in-process Python `publish(ipc_bytes)` at `chili-py/src/lib.rs:594`,
cross-process TCP `publish(handle, bytes)` at `chili-core/src/engine_state.rs:1103`,
AND now upstream's `publish(table, df)` from `7948744`). Claude also has a separate
`overwrite_partition()` function whereas upstream's `3aeee62` folded overwrite
into `write_partition(overwrite=…)`. The wishlist's "cherry-pick clean" premise
was wrong; the right path is an ADR + multi-sprint reconciliation. Without the
inventory pass, ~10–20pp of Sprint 2 would have been spent thrashing on merge
conflicts and possibly shipping a broken pub/sub state.

**Apply where.** Any future sprint that proposes cherry-picking commits or merging
upstream branches into a long-lived feature branch with its own divergent work.
Especially load-bearing for the Sprint 8 "main → claude full merge" milestone in
`docs/history/sim/roadmap_2026-05-06.md` — that sprint's brief MUST include a per-surface
API-divergence audit before scoping the merge resolution work. Also generalizes
to cross-project adoption (mdata ← chili, chili ← upstream, nxcar ← chili): any
time we adopt a feature from another project, check whether the destination
already has a divergent implementation of the same surface area before scoping.

**Cost saved.** ~10–20pp of Sprint 2 thrashing avoided + risk of shipping a broken
pub/sub state to mdata's production-adjacent tp/rdb refactor. Recurring cost as
the cherry-pick cycle continues until Sprint 8's full merge.

### After API errors during subagent dispatch, check disk before retrying

**Rule.** When a subagent returns an API 500 / timeout / network error after
long-running work, before treating the result as lost and retrying, verify the
actual filesystem state. Subagents often write to disk before returning their
final response packet; the error may be at the response stage, not the work stage.
Specifically: `ls -la <expected-output-path>` and `wc -l <expected-output-path>`
BEFORE redispatching with the same prompt. If the file is there with reasonable
content, you've saved a full subagent re-dispatch.

**Why.** Sprint 1 Part B, 2026-05-06. The first general-purpose subagent for the
kdb+ alternatives catalog returned API 500 after ~12 minutes (76 tool uses, 4pp
burned). Retrying the exact same prompt cost another ~7pp on a second subagent;
the second subagent immediately found `docs/research/kdb_alternatives.md` already
on disk with 666 lines of well-cited content — written by the FIRST subagent
before its API error. The first subagent's work was complete; only the response
packet died. Recognizing the file was there earlier could have saved most of the
second dispatch (only the consistency review was strictly needed). Generalizes
to: trust filesystem state over tool-result success/failure indicators when the
tool is filesystem-mutating.

**Apply where.** Any subagent dispatch where the agent has filesystem-write
authority and can write deliverables directly. Especially load-bearing for
research subagents producing single-file outputs that may run for 5+ minutes.
Inverse case (subagent dispatched to a non-writing operation, e.g. a query)
doesn't apply.

**Cost saved.** ~5–7pp per occurrence (the cost of a duplicate research-subagent
run). Will recur every time a long subagent errors on the response packet.

### Cherry-pick conflict accumulation — invert the merge direction

**Rule.** When ≥2 planned cherry-picks all hit the same divergence surface (the same
file or conceptual layer that has been substantially rewritten between fork point and
current upstream), or when a single cherry-pick produces conflict regions > 30 lines
per file, pause the cherry-pick path and consider inverting the merge direction. The
cumulative cost of N cherry-pick conflict resolutions on a divergent surface can
exceed the cost of one re-baseline (fork fresh from upstream tip) plus N feature
ports onto the new base. Inversion also collapses the eventual "full merge"
milestone into the present, eliminating monotonic divergence accumulation. The
*previous* "API divergence silently invalidates cherry-pick plans" lesson handles
discovery; this one handles the response when discovery returns a high count.

**Why.** Sprint 2 Part A, 2026-05-07. The original Sprint 2 brief planned three
cherry-picks (`b20177c` clean, surgical extraction from `7948744`, partial `3aeee62`)
based on inventory §2.4 / §2.6 / §2.3 predictions. The first cherry-pick alone produced
12 conflict regions in `crates/chili-py/src/lib.rs`, multiple > 30 lines, largest 101
lines. Investigation revealed all three wishlist commits were authored against the
*pre-FFI-rewrite* chili-py shape; claude's chili-py was substantially rewritten by
`08fe588` (the 2026-04-26 FFI merge) post-fork. So all three cherry-picks would have
hit the same divergence cost — a recurring tax, not a one-off. User-directed pivot
(2026-05-07): park `claude`, restart `claude-2` from main tip, port claude-only
features. The inversion collapsed the originally-Sprint-8 "full merge" milestone into
the present, eliminating recurring cherry-pick conflict cost from the entire 12-sprint
roadmap forward.

**Apply where.** Any cherry-pick / merge plan against an upstream branch where
(a) ≥2 planned commits share a divergence surface, OR (b) a single commit's conflict
surface > 30 lines per file, OR (c) the long-lived branch's unique work overlaps with
heavy upstream churn near that divergence surface. Doesn't apply when upstream is a
stable contract that rarely changes near your divergence surface — then cherry-picks
stay viable. Generalizes to all `git rebase upstream` decisions and to cross-project
adoption (mdata ← chili, nxcar ← chili) where the receiving project has substantially
reshaped the surface area being adopted. Specifically deprecates Sprint 8's
"main → claude full merge" milestone in the original `docs/history/sim/roadmap_2026-05-06.md` — that
work is now structurally accomplished by Sprint 2's pivot.

**Cost saved.** ~10-30pp of cherry-pick conflict thrashing avoided across the
original Sprints 2-4 plan + permanent elimination of the cherry-pick conflict-resolution
cost that would have recurred every upstream sync. New plan's pivot sprint is ~10pp;
ports for claude-only features are ~10-15pp combined. Net: cheaper *and* the end state
is "claude-2 ≈ main + delta," which is the strategic vision target end-state per
`project_chili_vision`.

### Verify framework-level GIL-release behavior before scoping FFI design around GIL

**Rule.** Before claiming a Python ↔ Rust FFI design constraint is "load-bearing
because of the GIL" (e.g., "we can't return type X because the user would call
`.method()` with GIL held"), verify what the underlying framework wrapper
actually does. Many pyo3 wrappers (pyo3-polars, pyo3-numpy, etc.) already call
`py.allow_threads()` internally on their heavy methods — the GIL is released
during the actual computation, NOT held as the naive "Python is calling a
method on a Python object" mental model suggests. Verify by reading the
wrapper's source (e.g., pyo3-polars' `LazyFrame.collect` impl) or with a
concurrent-throughput micro-bench. The symmetric trap also applies: don't claim
"GIL is released" without checking the wrapper actually does that.

**Why.** Sprint 2 v2 wrap conversation, 2026-05-07. While elaborating
PyLazyFrame for the user, I framed the lazy-eval design choice as constrained
by golden rule 5 (GIL released around `Engine::eval` for the 6.10× concurrent
throughput win), claiming `engine.eval(lazy=True)` returning a `PyLazyFrame`
would force `.collect()` to run with the GIL held. The user asked "is GIL
release and LazyFrame mutually exclusive?" — prompting me to actually check
pyo3-polars' source. `pyo3-polars` 0.26's `LazyFrame.collect()` impl calls
`py.allow_threads(|| lf.collect())` internally — the GIL release is preserved.
My initial framing led to a too-conservative recommendation (options a/b
only, with c framed as "high refactor cost solely on grounds we now know
are wrong") when in fact option (c) was viable on GIL grounds (still high
mdata-refactor cost, but for a different reason — breaking change for every
caller, not GIL constraints). Caught at ~0pp this turn because the user's
single question surfaced it in conversation; future-self scoping a sprint
around the false constraint could have burned 5-15pp on avoidable
architecture work + shipped a needlessly degraded API surface.

**Apply where.** Any Python ↔ Rust FFI design discussion where someone (incl.
me) claims "X is constrained because of the GIL." Especially: pyo3-polars
(LazyFrame / DataFrame / Series collect / write methods), pyo3-numpy,
pyo3-asyncio, ndarray FFI. Generalizes to ANY framework-level "free behavior"
claim in design discussions: verify the framework actually behaves the claimed
way before scoping decisions around it. The symmetric trap (claiming GIL
release when none is happening) is just as dangerous; both directions warrant
verification.

**Cost saved.** ~0pp this occurrence (conversation catch — user's question
forced verification). Future-occurrence estimate: 5-15pp per avoided sprint
that would have been scoped around a false GIL constraint, plus risk reduction
on shipping API surfaces unnecessarily limited by misunderstood framework
behavior.

### Re-verify inventory claims against destination code before locking sprint scope

**Rule.** When a sprint will consume a feature inventory, audit doc, or
cross-branch comparison authored more than ~24 hours before sprint kickoff,
re-grep the destination codebase for each claimed-missing surface BEFORE
accepting the inventory's classification. Inventories drift the moment the
destination branch moves; rebases especially absorb upstream-shipped features
that look "claude-only" relative to a stale fork point. A 5-minute grep pass
at sprint kickoff regularly avoids ~1pp of doomed implementation work and
the harder-to-debug case of duplicated definitions that collide at link time.

**Why.** Sprint 3 Part B kickoff, 2026-05-07. The brief sourced its 4
SMALL/TRIVIAL features from `claude_only_features_inventory_2026-05-07.md`
Class 3, authored that same morning. By Sprint 3 kickoff that afternoon, the
inventory had already drifted on B.1 — `ChiliError` + 6 subclasses +
`spicy_error_to_pyerr` were already on main (chili-py/src/lib.rs:42-70)
inherited verbatim by claude-2's tip via the FFI rewrite. Inventory also
listed B.3 lifecycle as 5 methods; parked-claude only ever had 2. Pre-kickoff
greps for each named identifier (`ChiliError`, `set_column_scale`, `LOG_FN`,
`MiMalloc`, `is_loaded`, `table_count`, `unload`, `reload`, `close`)
classified each surface as already-present / partially-present / missing in
~5 minutes, and revealed two over-spec'd inventory items before any
implementation work started. Pattern: rebases compress inventory-drift
latency from days to hours; the audit ages hourly.

**Apply where.** Every sprint that consumes an inventory, audit doc, or
cross-branch comparison, especially across rebases or main-side merges.
Specifically: every chili port-wave sprint (Sprints 3, 4, 5 currently;
future port sprints if they emerge). Generalizes to nxcar / mdata /
cross-project audits where the destination project has uncommitted upstream
churn between audit-write time and port-execution time. Doesn't apply to
inventories that are explicitly marked "frozen at <ref>" and consumed as
historical record (e.g., `docs/history/` provenance docs).

**Cost saved.** ~1-2pp per sprint that would have implemented a feature
already on the destination + risk avoidance on duplicate-definition link
errors. Sprint 3 alone: ~1pp on B.1 (skipped exception hierarchy port) +
~0.5pp on B.3 (3 fewer lifecycle methods than spec'd). Recurs every port
sprint until the inventory drift discipline is internalized.

### Run code-reviewer subagent before retro commit, fix in-sprint as Part E.1

**Rule.** Schedule the `code-reviewer` subagent dispatch as the LAST work
item of sprint Part E (immediately before retro authoring), not after.
Reviewer findings often surface fixable correctness issues — substring-match
fragility, single-table loop limits, docstring over-promises, off-by-one
edge cases — that should land as a focused E.1 commit on the same sprint.
Pushing them to the next sprint either bleeds them into unrelated work or
causes a "we already retro'd" hesitation. The cost of in-sprint absorption
is sub-1pp; the cost of cross-sprint deferral compounds every retro that
references "fix this next sprint" without strict tracking.

**Why.** Sprint 3 Part E wrap, 2026-05-07. The code-reviewer dispatch (49k
tokens, ~3 minutes wall) flagged 3 must-fix items in the column-scale port:
C1 substring-fragile match (`f"from {table}" not in query` would false-match
`from trades` against `from all_trades`, risking golden rule 4 violations
on multi-table queries against mdata's schema); W3 silent single-table loop
break (multi-table joins only dequantized the first registered table); W2
`query_plan` docstring over-promised chili-syntax support (the internal
eval is pepper-only). All three were sub-1pp fixes. Landing them as Sprint 3
Part E.1 (commit `b269ec0`) preserves the audit trail: "Sprint 3 shipped
this and code-reviewer agreed it was correct after these specific fixes."
Pushing to Sprint 4 would have been remembered only via the retro's
"future-sprint" notes — which historically slip when the next sprint has
its own scope pressure.

**Apply where.** Every sprint that ports non-trivial logic across branches,
adds new public API surface, or touches load-bearing invariants (golden
rules 4-6). Build the cadence as: implement → reviewer → fix → retro,
in that order, in one continuous session. Reviewer at wrap is mandatory
for any sprint that touches: chili-py FFI surface, parse-cache code,
pub/sub code, partition I/O, anything in `crates/chili-core/src/engine_state.rs`.
Doesn't apply to pure-docs sprints, retro-only sprints, or chore-only
clippy/fmt fix sprints (those don't need a separate review pass).

**Cost saved.** ~1pp deferred-fix overhead per sprint where the reviewer's
finding actually lands in the next sprint vs in-sprint absorption. Plus
the latent-bug-in-production risk reduction on findings that could have
shipped to mdata: Sprint 3's C1 alone could have caused silent
miss-rescaling on production queries. Reviewer-at-wrap converts those from
"shipped bug, debug under pressure later" to "blocked at sprint wrap, fix
in 0.6pp." Recurs every implementation sprint.

### Predict bench-related sprint cost in *full release-profile compile* time, not bench-runtime

**Rule.** When scoping a sprint that runs `cargo bench` (or `cargo bench
--no-run`), budget the *release-profile compile* cost separately from the
*bench runtime* cost. On chili's polars 0.53 dependency tree, the
release-profile compile (`-C opt-level=3 -C linker-plugin-lto -C
codegen-units=1`) is the dominant cost: 5-10 min cold, 1-2 min warm per
crate × 4 polars crates. Bench *runtime* alone is 10-30s per bench
function. The release artifact cache is NOT shared with `cargo build` /
`cargo test` / `cargo check` — those use dev profile by default. So a
sprint that bench-touches a polars dep for the first time on claude-2
since session start pays the full release compile cost on its bench step.

**Why.** Sprint 4 Part C, 2026-05-07. Scoped at 2-3pp assuming "bench run
is fast (~30s) per file." Actual time-to-first-bench-result was ~10 min
compile + ~1-2 min bench runtime per file. After 5+ minutes of compile
with no visible criterion output (cargo's stdout buffered until exit),
the strategy shifted from "wait for measurement" to "downgrade to harness
validation, defer measurement to Sprint 5 A/B sweep." That call kept
Sprint 4 within band (9pp actual vs 9-14pp predicted) but cost ~1.5pp on
the wasted compile cycles + the strategy-shift overhead. A pre-sprint
build of the release artifacts would have surfaced this cost in advance.
Generalized: any cargo-profile transition (dev → release for benches; or
the new test profile if added) triggers a fresh artifact cache invalidation
on heavy dep trees like polars 0.53.

**Apply where.** Any future sprint that includes `cargo bench` on chili-op
or chili-core. Especially: Sprint 5's bench A/B sweep (parked-claude
binary + claude-2 binary + 4-5 bench files = up to 2× the release
compile cost of a single binary). Generalizes to any project where bench
artifacts use a different cargo profile than the rest of the build.

**Cost saved.** ~1.5pp on Sprint 4 (downgrade saved completing the bench
runs; the realized cost was the tokens spent waiting for compile).
Sprint 5 budget: 3-5pp for compile alone + 2-3pp for runtime + 2-3pp for
parked-claude binary build. Recurring whenever the polars dep tree
changes (Cargo.toml workspace edit invalidates the cache).

### `pytest.mark.xfail(strict=False)` for known external-version blockers

**Rule.** When a pytest case fails due to an EXTERNAL version skew (not
a chili bug — e.g., polars Python/Rust DSL schema mismatch, transient
upstream API change), mark with `@pytest.mark.xfail(strict=False,
raises=ExpectedExceptionClass)`. `strict=False` makes the test silently
pass (XPASS) once the external dependency resolves, without breaking the
suite. `raises=` narrows the marker to the specific exception class so
an unrelated regression isn't masked. Use `strict=True` ONLY for actual
chili bugs the team has decided to ship around, where unintentional
"fixing" should break the suite to force re-evaluation.

**Why.** Sprint 4 Part B, 2026-05-07. The 4 lazy-return tests fail due
to polars 1.39.3 Python ↔ 0.53.0 Rust DSL hash skew (NOT a chili bug —
it's a polars cross-version FFI constraint). Marked xfail with
`strict=False, raises=Exception`:
- Today: tests fail → XFAIL (suite green).
- Post-Sprint-5 polars version pin: tests pass → XPASS (suite still
  green; no re-marking needed).
- Future regression to chili's lazy path: still fails for a different
  reason → XFAIL silently (acceptable; the broader `raises=Exception`
  means we'd miss only changes-in-error-type, not changes-in-failure).
The cost of `strict=True` would be a hard suite break the moment the
polars pin lands — exactly the wrong signal for "we fixed the upstream."
Without xfail at all, the suite would be RED until the pin, blocking
all chili-py pytest verification of unrelated work.

**Apply where.** Any chili-py test that depends on a Python ↔ Rust
polars boundary feature where the version compat is brittle (LazyFrame
DSL, Series ChunkedArray ABI, future polars features). Generalizes to
nxcar / mdata cross-project tests where one side's version pin lags the
other's. Inverse case (an actual chili bug being shipped around): use
`strict=True` so the suite breaks on accidental fix + unintentional
regression.

**Cost saved.** ~0.5pp avoided per sprint that would otherwise see
XPASS break the suite + 1pp avoided on the "did this regress?"
investigation cost when an unrelated chili change surfaces a different
failure mode. Recurs every sprint that touches polars FFI surfaces.

### ADR a structural blocker the moment cost-of-resolution exceeds cost-of-deferral

**Rule.** When a Sprint discovers a structural blocker — a constraint
baked into the dependency graph or external API surface, NOT just a
bug — and the fix path costs >5pp while the workaround / deferral path
costs <1pp, draft an ADR documenting the blocker + the explicit defer
decision + all viable resolution paths ranked by cost. The ADR is the
discovery's durable home; without it the structural finding rots in
retro notes and gets re-discovered in a future sprint at full cost.
Don't rely on retro mentions — those don't bind future sprints. An ADR
with `Status: Accepted (defer-resolution)` does.

**Why.** Sprint 5 Part A, 2026-05-07. Tested 4 Python polars versions
(1.20, 1.30, 1.39.0, 1.39.3). All failed to match Rust polars 0.53.0's
`DSL_SCHEMA_HASH`. Investigation revealed the root cause is chili's
`hinmeru/polars-core-patch.git#v0.53.0` fork — its `polars-plan`
source hashes differently from any stock Python polars wheel. NO
version pin on the Python side resolves this; the only fixes are
(a) pyo3-polars upstream releases a DSL-decoupled transfer
(~0.5pp when it ships); (b) chili replaces the patch fork with stock
polars (5-15pp); (c) chili custom-builds a Python polars to match
(5-10pp + ongoing publish maintenance). Without ADR 0003, a future
sprint would re-test versions, re-investigate, and re-conclude — the
same sunk discovery cost. With ADR 0003 documenting the structural
finding + ranked resolution paths, any future sprint can immediately
decide which path to pursue (or wait for option a to ship). The ADR
also surfaces the decision to `Status: Accepted (defer-resolution)`,
making the choice non-silent for future-self review.

**Apply where.** Any Sprint that surfaces "X is not version-fixable
because Y" or "this works around Z by Y but the real fix is W which
costs >5pp." Especially load-bearing for: pyo3-polars version
transitions (lesson 5 + ADR 0003 territory); polars-core-patch fork
maintenance; FFI ABI changes; cross-project version-skew constraints
between chili and mdata or nxcar. Inverse case (a clean fix exists at
≤1pp) doesn't warrant an ADR — just fix it.

**Cost saved.** ~3-5pp per future sprint that would otherwise
re-investigate the same structural blocker + risk reduction on a future
engineer attempting a fix that ranks lower in the ADR's cost-ranked
options without realizing the higher-ranked option exists. Recurs
every sprint that touches the affected surface.

### `uv sync` triggered by pyproject `dependencies` change rebuilds maturin wheel at release profile

**Rule.** When editing `pyproject.toml` `[project] dependencies = [...]`
or `[dependency-groups] dev = [...]` (or any field that changes the
project state hash), `uv sync` will rebuild any maturin-managed
editable install at the **release profile**. On chili-py with the
polars 0.53 dep tree, that's a 5+ min wall cost on the first `uv sync`
after each pyproject change. Subsequent `uv run pytest` invocations
reuse the cached release artifacts. Plan accordingly: pyproject edits
in a sprint should be batched and committed BEFORE running tests, and
the test gate budget should include that one-time rebuild cost
(~3-5pp on chili).

**Why.** Sprint 5 Part A, 2026-05-07. Added `polars==1.39.3` to
pyproject for the mdata wheel pin. uv sync triggered a release-profile
rebuild that took ~5-7 min wall and burned ~3pp before pytest could
even run. Predicted Part A cost (1.5-2.5pp) was based on "edit
pyproject + run existing pytest" — actual was 4-5pp because of the
rebuild. The coupling exists because pyproject is the source of truth
for both Python deps AND maturin's Rust build state; uv treats any
field change as project-state invalidation. Generalizes to any
maturin-managed editable Python project.

**Apply where.** Any sprint that edits `crates/chili-py/pyproject.toml`.
Especially: dep-bump sprints (pyo3, pyo3-polars, polars); ADR resolution
sprints touching Python deps; wheel-rev sprints. Specifically: Sprint 6
deep housekeeping should NOT edit pyproject without budgeting the
rebuild cost; future ADR 0003 resolution sprints (option a:
pyo3-polars upgrade) MUST budget ~5pp for this rebuild on top of the
upstream version bump. Inverse case (pure-Python project, no maturin)
doesn't apply.

**Cost saved.** ~3-5pp per sprint that edits pyproject + runs pytest
in the same wrap. Recurs on every dep-bump sprint, ADR resolution
sprint, and wheel-rev sprint. Saves the calibration drift that comes
from "I predicted this at 2pp but it took 5pp because of the rebuild."

### Empirical bisection beats version-guess speculation when external-version-skew is suspected

**Rule.** When ADR or sprint analysis hypothesizes "the right version
of upstream X exists and we just need to find it," budget a focused
empirical bisection BEFORE building any fix infrastructure (vendoring,
forking, custom-publishing, custom-building). A linear scan of 5-10
published versions with one-line install commands and a single test
invocation each costs ~5 minutes of wall time and produces hard data:
either it confirms the hypothesis (find the matching version, done) OR
it eliminates the hypothesis (no version matches, redirect resolution
path to the next-cheapest option). Skipping the bisection and proceeding
directly to the heavyweight resolution wastes effort on a path that may
have a cheaper alternative the bisection would have surfaced.

**Why.** Sprint 7 Part A, 2026-05-08. The original ADR 0003 (Sprint 5)
implicitly assumed "find a Python polars version that matches Rust
polars 0.53.0's DSL hash." The correct empirical move was to test
Python polars 1.20 / 1.30 / 1.31-1.34 / 1.37 / 1.39 / 1.39.3 against
chili's compiled-in hash. ~5 min wall, ~1pp tokens. Outcome: NO PyPI
Python polars matches the hash chili emits (all 1.31-1.39 emit the same
`124a6...` hash; chili emits `17d5d...`). Option-1 (version pin) is
dead; option 3 (git-pin Rust to py-1.39.3) becomes the cheapest viable
resolution. Without the bisection, Sprint 7 might have spent multi-pp
searching for a non-existent matching version, OR jumped straight to
option 3 without confirming option 1 was eliminated (wasting investment
in option 3 if option 1 had actually worked).

**Apply where.** Any ADR resolution sprint where "find the matching
version" or "wait for upstream X to release Y" appears as a candidate
path. Especially: pyo3-polars / polars-rs / pyo3 / maturin version
transitions; Python deps where chili's wheel pins downstream consumers;
"upstream is archived; what now?" scenarios. Generalizes to nxcar /
mdata cross-project tests where one side's pin lags the other's.
Doesn't apply when the bisection space is unbounded (e.g., "find the
magic compiler flag combination") or when the hypothesis has already
been ruled out by static analysis. Always pair the bisection with
documenting the negative result in the ADR — even a "no version
matched" outcome is durable evidence that future sprints reuse.

**Cost saved.** ~3-5pp per ADR resolution sprint where bisection rules
out a wrong-direction path before the heavyweight investment. Plus
risk reduction on multi-week vendor/fork investment that becomes
unnecessary if a simple version pin would have worked. Recurs whenever
an external dep ABI changes near chili's pinned version.

### Worktree-based A/B benchmark methodology

**Rule.** When benchmarking two versions of a Rust binary that must
compile from the same workspace tree, use `git worktree add
/tmp/<branch>-bench <ref>` to create a separate working copy with its
OWN `target/` directory. Run benches in each worktree **sequentially**,
NEVER in parallel — release-profile compile saturates CPU and double-
time = 2x serial wall, NOT half. Both bench results land in their
respective `target/criterion/` trees and can be diffed/compared offline.
For chili specifically: workspace target/ + chili-py/target/ are TWO
separate trees per worktree (chili-py is `exclude`d from workspace);
account for ~25 GB peak disk per worktree pair.

**Why.** Sprint 7 Part B, 2026-05-08. Bench A/B sweep needed claude-2
(current tip with py-1.39.3 polars source) AND parked-claude
(`claude-baseline-2026-05-07` tag with hinmeru fork polars-core).
Worktree at `/tmp/chili-parked-bench` produced parked-claude's bench
numbers without touching `/Users/oakadmin/code/chili/target`. Subsequent
claude-2 bench at the workspace produced its numbers in workspace's
`target/release/`. Total wall: ~60 min for both; total disk peak: ~25 GB;
results: 13 bench number pairs collected with zero cross-contamination
of build artifacts. The alternative (manually checking out branches
back-and-forth in the same workspace) corrupts incremental compile
cache between A and B and produces non-comparable numbers.

**Apply where.** Every future bench A/B sprint: Sprint 8 perf-pass-1
(needs to compare pre-fix vs post-fix on each P1/P2 task), Sprint 9
perf-pass-2, Sprint 12 perf-pass-3, any future "did we regress vs the
last release" comparison. Generalizes to A/B comparison of any two
Rust binaries from the same repo at different commits. Inverse case
(single-binary benching) doesn't need the worktree — just bench the
current tree. Pre-flight: check `df -h /` before adding a worktree —
peak disk during a worktree-based A/B bench is 2x a single-binary
bench's footprint.

**Cost saved.** ~1pp per bench-pass sprint vs the alternative (manual
checkout + rebuild + non-comparable numbers + "did A's compile
contaminate B" doubt). Plus eliminated cross-bench cache pollution; each
`target/criterion/` is independently reproducible.

### Wheel-only install protocol for downstream consumers (NEVER `pip install -e`)

**Rule.** When chili (or any compiled-binding Python project) ships to
a downstream consumer, the install protocol MUST be wheel-based, NOT
editable. Editable installs (`pip install -e <path>` /
`uv pip install --editable`) link the consumer's runtime to chili's
mid-build state, causing the consumer to break when chili's compile
cycles invalidate intermediate artifacts. Document the wheel-only
protocol with explicit uninstall + install + verification commands;
provide a verification step that catches `.pth`-file editable-install
ghosts (which survive `uv pip uninstall`).

**Why.** Sprint 7 Part B context, 2026-05-08 (carried over from Sprint
4-7 of the autonomous run). mdata installed chili 0.8.0 wheel as
`pip install -e /Users/oakadmin/code/chili/crates/chili-py`. During
chili's mid-Sprint compile work — Sprint 4 Part B's pyproject change
triggered uv-sync rebuild; Sprint 5 Part A's polars pin triggered
another; Sprint 7 Part A's polars source swap triggered a third —
mdata's runtime broke because Python imports resolved to chili's
mid-rebuild state. Cumulative cost across both projects: ~3-5pp on
mdata-side downtime debugging + ~1pp on chili-side coordination per
incident. Wheel-based install with `chili.__file__` resolution check
(in `mdata_chili_2026-05-08_delivery.md` §4.3) guarantees the
consumer's site-packages doesn't accidentally ghost-link the source
repo. The two-check verification (`uv pip show` Location + `chili.__file__`
import-time path) catches both the "Cargo metadata says editable" case
AND the "`.pth` file linked the source dir despite uninstall" case.

**Apply where.** Every chili-sauce wheel cut going forward (Sprint 12+
assumed; any future delivery sprint that produces a wheel for an
external consumer). Generalizes to any chili-built Python package +
downstream consumer (mdata + future projects that depend on chili's
Python bindings). Per-delivery instances tracked in
`docs/sync/mdata_chili_<date>_delivery.md`. Inverse case (chili-internal
pytest using `maturin develop`) is fine — that's chili's own dev loop,
no external consumer; editable is acceptable there.

**Cost saved.** ~3-5pp per delivery cycle that would otherwise see
editable-install-induced consumer outages + ~1pp per outage on debugging
which version mdata was actually running. Recurs every wheel cut +
downstream-install pair. Saves consumer-trust cost too (mdata stops
fearing chili's compile cycles).

### Re-measure bench numbers within ±10% of a hard target before triggering mitigation work

**Rule.** When a bench result lands within ±10% of a golden-rule target
(or any known threshold that triggers escalation), the FIRST move is
re-measurement, NOT investigation / profiling / mitigation. Apple
Silicon thermal/memory variance can account for 20-40 ns on
sub-microsecond benches and similar relative variance on µs/ms benches.
Two or three additional runs cost ~30s wall each and produce a
confidence interval. If all extras land safely in-target, the original
was an outlier and no work is needed. If extras confirm out-of-target,
escalate to investigation. Skipping the re-measure and going directly
to investigation wastes pp on diagnosing noise.

**Why.** Sprint 7 Part B measured parse_cache hit at 410.58 ns; golden
rule 6 target is ≤400 ns. The +2.6% over target prompted "Sprint 8 P1 =
profile + reclaim ≤400 ns or ADR amend" in the Sprint 7 retro. Sprint 8
P1's first move was the re-measurement: 397.47 / 379.08 / 398.33 ns
across 3 runs — comfortably under target. The 410.58 ns was thermal
noise. A naive Sprint 8 that skipped re-measure and jumped straight to
"profile parse_cache hot path with samply" would have spent ~2-3pp
investigating a non-issue. Re-measure cost: ~0.5pp. Saving: 1.5-2.5pp +
the cognitive overhead of an unnecessary "is golden rule 6 broken?"
investigation.

**Apply where.** Any bench-pass sprint that surfaces a marginal
regression (within ±10% of a target). Specifically: parse_cache hit
(golden rule 6 ≤400 ns); future Python concurrent eval (golden rule 5
6.10× throughput); any new golden rule that pegs to a numerical
threshold. Generalizes to any benchmark-driven decision point —
re-measure before treating a result as load-bearing.

**Cost saved.** ~1.5-2.5pp per sprint that would otherwise pursue
noise. Recurs every bench-pass sprint where natural variance straddles
a target threshold.

### macOS bench profiling needs `[profile.bench]` symbol-retention override pre-staged

**Rule.** When a chili sprint plans to run `samply record` /
`cargo flamegraph` / any sampling profiler on a bench binary, FIRST
verify (and if needed, pre-add) a workspace `[profile.bench]` override
that retains debug symbols. Workspace currently has
`[profile.release] strip = true` for production binary leanness; this
strips bench binary symbols too (cargo `bench` profile inherits from
`release`). Profiling without symbols produces unsymbolized
hex-address stacks — the profile data is captured but functions can't
be named, so root-cause analysis is impossible. Pre-staging the
override at sprint kickoff costs ~10-15 min wall on the initial cold
release-with-debug build (lesson 11 rebuild territory); discovering
mid-sprint and adding the override costs the same plus pp on the
failed profile attempt + decision-pivot.

**Why.** Sprint 8 Part C, 2026-05-08. samply captured 17,216
main-thread samples on `load_multitable_5x200p` cleanly. All stack
frames resolved to bare hex addresses (e.g., `0x450c 42.5%`). Without
function names, no way to identify the per-table linear cost driver
that Sprint 7 R2 wanted profiled. P2 deferred to Sprint 9 with the
override + symbolized re-profile as Sprint 9 P2's entry plan. Cost on
Sprint 8: ~0.5pp on the failed profiling attempt. Cost saved on
Sprint 9 by capturing the lesson: ~0.5pp on the same mid-sprint
discovery if it recurred. `cargo flamegraph` on macOS additionally
requires Xcode (only CLI tools installed on autonomous-run machines)
for `xctrace`; samply is the autonomous-run-compatible alternative
IF the binary has symbols.

**Apply where.** Sprint 9 P2 (deferred from Sprint 8). Future
perf-pass sprints (Sprint 12 perf-pass-3). Generalizes to any project
where production-build size optimizations strip symbols. Inverse case
(profiling-free sprint, e.g., docs-only or feature-add-only) doesn't
need the override.

**Cost saved.** ~0.5pp per perf-pass sprint that would otherwise
mid-sprint discover the symbol issue. Plus ~10-15 min wall on the
unnecessary profile run that produces hex-only data.

### macOS `samply` autonomous-run profiling produces unsymbolicated profiles even with debug-info-embedded binaries

**Rule.** When using `samply record --save-only` on macOS in an
autonomous-run / headless environment, expect the saved JSON to contain
hex-address stack frames, NOT symbolized function names, even when the
bench binary has `[profile.bench] debug = true; strip = false`. samply's
symbolization happens at *display time* (`samply load <json>` opens a
browser that fetches symbols from the binary at runtime). Without GUI,
three resolution paths exist, all with autonomous-run friction:

1. Upload JSON to `https://profiler.firefox.com/` — user action.
2. Use `atos` with a `dsymutil`-generated dSYM bundle — extra build
   step; on macOS often needs full Xcode (not just CLI tools).
3. Install `llvm-addr2line` / `addr2line` (~50 MB) and write a custom
   resolver script — feasible autonomously, ~2pp setup + use.

**For autonomous-run, the practical pattern is: capture the profile +
the symbolized bench binary as sprint artifacts; defer symbolization +
mitigation to a future sprint that has Path 3 pre-installed (or a
user-driven mini-sprint with GUI).**

**Why.** Sprint 9 P2, 2026-05-08. 31m 39s rebuild + 10s samply record
produced a profile that pinpointed the dominant kernel by offset
(`0x450c` = 93% of polars worker time on `load_multitable_5x200p`) but
without function names, no actionable mitigation possible autonomously.
Sprint pp spent on P2: ~1.5pp; sprint value captured: hot-kernel offset
isolated + bench binary preserved with debug info. Decision-pivot:
defer symbolization + fix to Sprint 12 perf-pass-3. Net pp on Sprint 9
P2: ~1.5pp; deferred-fix pp on Sprint 12 P2: ~3-4pp (cost-stable
shift; the symbolization step adds ~2pp infrastructure setup but saves
the rebuild cost since the bench binary is preserved).

**Apply where.** Any chili sprint planning samply-driven optimization
on macOS without GUI access. Specifically: Sprint 12 perf-pass-3,
future bench-pass sprints, any future "we need to know what function
is hot" investigation. Generalizes to any Rust + macOS
performance-sensitive project where the autonomous-run environment
lacks GUI / Xcode / pre-installed perf tools. Inverse case (Linux
autonomous run) likely has better ergonomics — `perf` symbolizes at
record time on Linux. Pre-flight: any sprint that lists "samply
profile" as a deliverable should include "+1-2pp for addr2line install
or dSYM generation" in the prediction band.

**Cost saved.** ~0.5-1pp per future bench-pass sprint that would
otherwise re-discover this; ~2pp on the symbolization-infra setup
spread across multiple sprints if the install is shared. Plus
risk-reduction on the "I have a profile but can't act on it" anti-
pattern in autonomous run.

### verify-before-claim binds a brief/ADR's *mechanism* claims, not only current-state facts — and the implementer is the last line

**Rule.** A brief/ADR/dispatch claim of the form "component X currently
*behaves* like Y" (not just "X exists" / "X is at version V") is a
load-bearing claim. RULE-7 it — open and read the function — before
writing it into the artifact. A 3-agent audit will NOT catch a wrong
mechanism claim: audit agents inherit the brief's framing (documented
2026-05-09 chili Sprint-15 inherited-wrong-premise incident). The
implementer's first read of the cited code is the last line of
defense; structure the impl so that read happens before the claim is
relied on.

**Why.** Sprint 18: the dispatch brief + its 3-agent audit appendix
both asserted (a) the tplog logical seq "resets per segment ⇒ composite
global seq" and (b) a "per-handle `tick_count` slot indexed by `h`".
Reading `EngineState::tick` at implementation showed `tick_count[index]
+= inc` (cumulative carry-over, NOT a reset) and that the tplog slot is
the literal `0` (`tick.pep:6`), not the handle id. Both claims were
speculation; all 3 audit agents inherited and propagated the framing
(audit MAJOR-2 flagged a `tick_count` 1024-bound panic that was moot
once the mechanism was actually read). This is a confirmed recurrence
of the chronic speculation-where-verification-is-cheap pattern
(`feedback_speculation_pattern` memory; 2026-05-09 Sprint-15;
cross-project mdata incidents) — generalizing
`~/.claude/rules/verify-before-claim.md`'s "current state" bullet from
*existence/version* facts to *behavioral/mechanism* claims.

**Apply where.** Every dispatch brief, ADR, audit appendix, or
cross-project reply that says "X currently does Y" about an existing
function/lock/counter/format. Especially load-bearing when a
"deferred decision" or a downstream consumer's design hinges on the
claimed mechanism (here: mdata's SEQ-MONO partition invariant).

**Cost saved.** Prevented shipping a wrong "deferred cross-segment-seq
decision" + a wrong seq mental model into the mdata 0.8.6 delivery doc
and cross-comms reply — i.e. a bad cross-project round-trip plus
downstream mdata mis-design built on a false premise. Conservatively
~2–4pp (one mdata clarification cycle) + the inherited-premise risk to
any v1-25.2 work that consumed the wrong seq model.
