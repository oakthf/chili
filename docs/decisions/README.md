# Architectural Decision Records (ADRs)

Reversible architectural decisions for chili live here as numbered, frozen records.
Adapted from mdata's `docs/decisions/` pattern.

## Filename convention

`<NNNN>-<slug>.md` — zero-padded 4-digit sequential number, kebab-case slug.

Examples (conventional, not yet present): `0001-storage-format.md`,
`0002-parse-cache-shape.md`.

## Lifecycle

- **Draft** — proposed; under review. Don't gate other work on a Draft ADR unless you
  must.
- **Accepted** — user-ratified. The decision binds future work.
- **Superseded** — replaced by a later ADR. Keep the file; add a top-of-file pointer
  to the superseding ADR.

## Required sections

- Title (`# ADR-NNNN — Title`)
- **Date** + **Status** + **Cutover commits** (if the ADR landed via specific commits)
- **Context** — why the decision was needed
- **Decision** — what was chosen
- **Consequences** — what this binds, what it excludes
- **Alternatives considered** (optional but encouraged)

## When to write an ADR vs a row in `decisions-needed.md`

- **Reversible** decisions → ADR here.
- **Irreversible** decisions (data-loss risk, on-disk format change, cross-project
  contract) → row in `docs/sync/decisions-needed.md` and halt for user direction.

See onboarding §10.4 (`~/team/onboarding.md`) for the full convention.
