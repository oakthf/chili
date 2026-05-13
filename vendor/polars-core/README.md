# vendor/polars-core/

Reference material for reconstructing chili's q-style polars-core fmt patch
when `/tmp/polars-py-1.39.3` is lost (macOS daily /tmp cleanup, fresh clone,
CI runner, etc.). Lives in-tree because the original lived only in the local
clone's `.git/` and we've now had it disappear once.

## Why this directory exists

Per workspace `Cargo.toml`'s `[patch.crates-io]` block (and ADR 0003), all
`polars-*` crates are pinned to a local clone of `pola-rs/polars` at the
`py-1.39.3` tag, with a chili-side q-style fmt patch applied on top
(historically commit `8d56f02` in the local clone). The chili-side test
`crates/chili-core/tests/fmt_test.rs` asserts the q-style duration string
`0D00:00:00.000001000`, which only matches if the patch is applied.

When `/tmp/polars-py-1.39.3` was wiped by macOS's daily /tmp cleanup
(2026-05-13), the `.git/` was gutted and the 30-line chili-side port commit
was lost. We re-ported the q-style hunks by adapting `hinmeru/polars-core-patch`
(the original source the chili port was derived from). These reference
patches preserve the hinmeru material in-tree so we don't need to re-fetch
it from GitHub if hinmeru ever archives or deletes the repo.

## Files

| File | Source | What it is |
|---|---|---|
| `hinmeru-reference/01-a600521-cosmetic-fmt.patch` | hinmeru/polars-core-patch commit `a600521 patch fmt` | Cosmetic only — re-adds trailing commas to match arms. **Not load-bearing.** Kept for completeness. |
| `hinmeru-reference/02-6c64273-fmt-qstyle-only.patch` | hinmeru/polars-core-patch commits `a600521..6c64273` (fmt.rs slice only) | The actual q-style Datetime/Duration display logic (`fmt_datetime`, `fmt_duration_string` bodies). **This is what chili depends on semantically.** |
| `hinmeru-reference/03-init-to-tip-full-fmt.patch` | hinmeru/polars-core-patch full fmt.rs delta `c14b1be..6c64273` | The full hinmeru fmt.rs diff from init to v0.53.0 tip. Superset of (01) + (02). |

## Reconstruction protocol — when /tmp/polars-py-1.39.3 is missing

```bash
# 1. Re-clone the base
rm -rf /tmp/polars-py-1.39.3
git clone --branch py-1.39.3 --depth 1 https://github.com/pola-rs/polars /tmp/polars-py-1.39.3

# 2. Apply the q-style hunks. Hinmeru's patches are against v0.53.0,
#    not py-1.39.3 — they will NOT apply with `git apply` cleanly.
#    Instead, port two function bodies manually:
#      - `fn fmt_datetime` (around line 987 in py-1.39.3 polars-core/src/fmt.rs):
#        replace its `match tz { ... }` body with the q-style match
#        (see 02-6c64273-fmt-qstyle-only.patch, the fmt_datetime hunk —
#        emits "%Y.%m.%dD%H:%M:%S%.f" for ns/us, "%Y.%m.%dT%H:%M:%S%.f" for ms).
#      - `pub fn fmt_duration_string` (around line 1021 in py-1.39.3):
#        replace the multi-part "3d 22m 55s 1ms" body with the q-style
#        "{sign}{days}D{HH}:{MM}:{SS}.{frac}" body (precision depends on TimeUnit).
#
# 3. Verify by running fmt_test:
cargo test --workspace --exclude chili-py -- fmt_test
#    Expected: `test fmt_duration_series ... ok`
```

## Why we don't just check the polars clone into vendor/ as a submodule

Two reasons:
1. The polars source is ~140K LOC; vendoring it would balloon the chili repo
   meaningfully and slow `git clone` for every contributor.
2. The chili-side patch is small and stable. The right long-term fix
   (CLAUDE.md P0 backlog) is to host the polars fork on GitHub and replace
   `path = "/tmp/polars-py-1.39.3"` with `git = "..." + tag = "..."` in the
   workspace `[patch.crates-io]` block.

This `vendor/polars-core/` directory is the interim safety net — a 30 KB
reference material set that survives in our git history.

## Provenance

- `hinmeru/polars-core-patch` repo: `https://github.com/hinmeru/polars-core-patch.git`
  at tag `v0.53.0`. Commits referenced:
  - `c14b1be init commit` — vendored polars-core v0.53.0 base.
  - `a600521 patch fmt` — cosmetic style cleanup (trailing commas).
  - `6c64273 patch 0.53.0` — broader 140-file polars-core ergonomic patches;
    the fmt.rs slice contains the q-style display logic chili ported.

The hinmeru repo is **upstream of chili**; the chili port (`8d56f02` in the
local clone, now lost) was a curated subset applied on top of py-1.39.3
polars-core rather than v0.53.0. Reconstructing the chili port from these
patches requires manual adaptation — they are reference material, not
drop-in patches.

## See also

- `docs/decisions/0003-pylazyframe-dsl-incompat.md` — the ADR that introduced
  the polars-py-1.39.3 patch block and the chili-side q-style fmt port.
- Workspace `Cargo.toml` lines 58-99 — the `[patch.crates-io]` block + a
  block comment that explains the load-bearing dependency on /tmp/polars-py-1.39.3.
- `crates/chili-core/tests/fmt_test.rs` — the one test that hard-asserts on
  the q-style duration string `0D00:00:00.000001000`.
