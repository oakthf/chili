# Sprint N retro — <theme> (<priority labels>)

**Wrap:** YYYY-MM-DD
**Predicted:** N–M pp
**Actual:** ~K pp
**Variance:** <%> (<position-in-band>)
**Owner:** coordinator-solo / coordinator + tester / etc.
**Plan reference:** `docs/history/sprints/sprint_N_dispatch_brief_<date>.md`

---

## Scope shipped

Bullet list of what was delivered, with the originating commit SHA in
parentheses. Group by sub-priority (P1, P3, etc.) when the sprint
covers multiple priorities. Match `dispatch_brief.md` deliverables —
flag any cut/added scope.

Tests: +N (M Rust unit, K Rust integration, L Python pytest).

Bench delta (if touching hot paths): scan / eval / load_par_df /
write_partition / parse-cache numbers vs prior baseline.

---

## Lessons (durable)

Durable lessons (observed 2+ sprints, or single high-cost incident) promote to
`docs/standards/iteration_lessons.md`. Non-durable surprises go to "What surprised" below.
Use the 4-field format; if "Cost saved" is unknown, leave the lesson here and don't promote.

### 1. <lesson title>

**Rule.** What to do or avoid. One paragraph.
**Why.** Concrete sprint reference + evidence. One paragraph.
**Apply where.** Future contexts this binds.
**Cost saved.** Estimate of pp / wall-clock / risk avoided. (Mandatory for promotion.)

### 2. <lesson title>

…

---

## Pp accounting

| Item | Predicted | Actual |
|---|---|---|
| <sub-item 1> | … | ~… |
| <sub-item 2> | … | ~… |
| **Total** | **N–M** | **~K** |

<one-paragraph commentary>: position-in-band, what drove variance.

---

## What surprised

Bullet list of small or non-durable surprises that don't warrant a
durable rule but are worth recording for future-self.

---

## Cross-references

- Plan: `docs/history/sprints/sprint_N_dispatch_brief_<date>.md`
- Cadence metrics row: `docs/sim/cadence_metrics.md`
- Related retros: paths to companion retros
- Cross-project (if any): `docs/proposals/...` or downstream-project paths
