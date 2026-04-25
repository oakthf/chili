# mdata → chili test corpus

These test files are copied verbatim from
`~/code/mdata/tests/server/` at commit `64603d2` on the
`claude` branch (2026-04-11).

License: MIT — see `~/code/mdata/LICENSE`.

## Files

| File | Source | Purpose |
|---|---|---|
| `test_chili_pepper_patterns.py` | `tests/server/test_chili_pepper_patterns.py` | Pins the exact pepper query strings mdata's `ChiliGateway` uses in production. If chili changes pepper parsing, these tests surface it. |
| `test_chili_gateway_quantized.py` | `tests/server/test_chili_gateway_quantized.py` | Pins the Int64-quantized OHLCV round-trip path (Sprint I / L). |

## Usage

These files serve as the acceptance contract between chili and mdata:

1. **Python parallels** — run from `crates/chili-py/tests/` as verification at the
   PyO3 boundary (pepper parsing + eval + dtype mapping). Both files are included
   in the chili-py test suite as-is.
2. **Rust integration tests** — the pepper patterns from `test_chili_pepper_patterns.py`
   are also ported to `crates/chili-op/tests/mdata_patterns_test.rs` so they run in
   `cargo test` without a Python dependency.
3. **Report divergences** — any pattern that fails should be
   reported back to mdata so we can reconcile. Chili's pepper surface
   is the contract mdata relies on.

## Dependencies

```python
import chili          # Engine, pepper flag
import polars as pl   # DataFrame, dtypes
import pyarrow        # IPC
from mdata.server.chili_gateway import ChiliGateway    # wrapper under test
from mdata.vendor.schemas import quantize_ohlcv        # Int64 helper
```

The `ChiliGateway` import is the mdata-side wrapper — chili's own
tests should exercise `chili.Engine` directly and use these files as
a reference for which pepper patterns must work.

## Related deliverables

- **Fixtures**: `../fixtures/` (20 ohlcv_1d partitions + 5 ohlcv_1m)
- **Schema doc**: `../artifacts/quantized_schema.md`
- **Broker parity test**: `../artifacts/broker_parity_test.py`
- **Status tracker**: `../STATUS.md`

## When to refresh

Re-copy from mdata whenever:

1. mdata's chili-backed code changes (new pepper pattern, new column
   scale, new schema field) — detected by a diff in
   `~/code/mdata/src/mdata/server/chili_gateway.py` or
   `~/code/mdata/src/mdata/vendor/schemas.py`.
2. A chili phase lands that changes query semantics (Phase 9 column
   pruning, Phase 10 symbol pushdown, Phase 13 errors, Phase 15
   quantized) — re-run mdata's tests locally first, then re-copy to
   make sure the pinned patterns still reflect production usage.

## License

MIT. Same as mdata.
