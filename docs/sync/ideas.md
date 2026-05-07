# Ideas — backlog of unscoped items

Capture-as-you-think file for ideas not yet tied to a sprint or ADR. No particular
priority. Cull when items get scoped (move to a sprint dispatch brief) or rejected
(move to history with a rejection note).

Adapted from mdata's `docs/sync/ideas.md` shape.

---

## Format

`- [tag] **Title** — short hook describing the idea + (optionally) why it's
interesting + (optionally) cross-reference.`

One bullet per idea; max 3 lines. **Bracketed tag is mandatory** on every new entry —
untagged entries are a sweep target during housekeeping. Canonical tag set:

- `[architecture]` — design / API / module structure
- `[ops]` — tooling / CI / cron / process
- `[incident]` — bug or anomaly worth recording beyond the commit message
- `[observation]` — cross-project / external finding worth capturing
- `[validation]` — test / bench / soak / verification idea

If an entry grows beyond 3 lines, promote to a real plan, ADR, or `docs/proposals/`
proposal.

Reading is async — user reviews and replies inline with blockquotes. Never branch
implementation off an idea entry without first promoting it to a sprint brief or ADR.

---

<!-- First idea goes here. -->
