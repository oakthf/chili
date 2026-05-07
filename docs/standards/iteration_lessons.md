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
