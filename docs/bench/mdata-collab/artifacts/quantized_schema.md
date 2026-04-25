# mdata Int64 quantization reference

This document describes the exact storage layout mdata uses for
market-data price columns on disk. Chili Phase 15 (`set_column_scale` /
pepper `dequantize` helper) must round-trip these values losslessly
against mdata's reference implementation.

Drop source: `mdata/src/mdata/vendor/schemas.py`
(commit `64603d2` on the `claude` branch, 2026-04-11).

## The scale factor

```python
PRICE_SCALE: int = 1_000_000
```

Every quantized price column is stored as

    stored_int = round(logical_float * 1_000_000)

and recovered via

    logical_float = stored_int / 1_000_000

which gives **6 decimal places** of precision ($0.000001 = 0.01 bps).
Empirically verified lossless against the live HDB on
`ohlcv_1m/2026.04.08_0000` — lower scales (`1e4`, `1e5`) introduced
1-ULP rounding errors on 31–53% of rows AND zero out two real symbols
(FOXO, ICCT) whose minimum price is `$0.000001`. `1e6` is the smallest
scale that round-trips losslessly across the current equity universe.

If chili wishes to reproduce the test, the corresponding mdata
property tests live at:

- `tests/vendor/test_schemas_quantize.py`
- `tests/vendor/test_schemas_quantize_trade.py`
- `tests/vendor/test_schemas_quantize_quote.py`

## Future scale factor

If the vendor universe expands to include crypto micro-tokens that
quote below the current floor, bump the constant to `100_000_000`
(8 decimal places, Bitcoin satoshi convention) and re-run the HDB
quantization migration. `Int64` still fits any plausible price
(max safe price drops from $9.2 quadrillion to $92 billion, still
fine for any real asset).

Chili's `set_column_scale` implementation must accept any power-of-ten
scale factor in `{1, 10, 100, 1_000, ..., 10**18}` and not hard-code
`1_000_000` — upstream flexibility for future mdata needs.

## Per-table column map

| Table | Column | Storage dtype | Scale factor | Logical dtype |
|---|---|---|---:|---|
| `ohlcv_1d` | `open`   | `Int64` | 1_000_000 | `Float64` |
| `ohlcv_1d` | `high`   | `Int64` | 1_000_000 | `Float64` |
| `ohlcv_1d` | `low`    | `Int64` | 1_000_000 | `Float64` |
| `ohlcv_1d` | `close`  | `Int64` | 1_000_000 | `Float64` |
| `ohlcv_1d` | `vwap`   | `Int64` | 1_000_000 | `Float64` |
| `ohlcv_1d` | `volume` | `UInt64` | **1 (not scaled)** | `Float64` |
| `ohlcv_1d` | `trades` | `UInt32` | **1 (not scaled)** | `UInt32` |
| `ohlcv_1m` | *(same as ohlcv_1d)* | | | |
| `trades`  | `price`  | `Int64` | 1_000_000 | `Float64` |
| `trades`  | `size`   | `Int64` | 1_000_000 | `Float64` |
| `quotes`  | `bid_price` | `Int64` | 1_000_000 | `Float64` |
| `quotes`  | `ask_price` | `Int64` | 1_000_000 | `Float64` |
| `quotes`  | `bid_size`  | `UInt32` | **1 (not scaled)** | `UInt32` |
| `quotes`  | `ask_size`  | `UInt32` | **1 (not scaled)** | `UInt32` |

## Important distinctions

### Volume is NOT scaled

OHLCV `volume` is a `UInt64` share count on disk and `Float64` at the
logical boundary — but the dequantize step is a pure **dtype cast**,
not a divide. Applying `set_column_scale("ohlcv_1d", "volume", 1_000_000)`
would corrupt the data by 6 orders of magnitude. Chili should either:

1. Skip `volume` columns entirely in `set_column_scale` (recommended), or
2. Accept scale `1` as a no-op divide + dtype cast.

### Trade `size` IS scaled

Unlike OHLCV volume, tick-level `trades.size` is quantized to
`Int64 × 1_000_000` because some vendors (crypto, fractional-share
equity) send fractional sizes. Preserving up to 6 decimal places is
sufficient for any plausible fractional share.

### Quote sizes are NOT scaled

`bid_size` and `ask_size` in the quotes table are `UInt32` on both
sides — always integer share counts at NBBO time, so no quantization
needed.

### Timestamps are NOT scaled

All `timestamp` columns are `Polars Datetime("ns", "UTC")` on both
sides. No quantization, no scale, no cast.

### `trades` column (OHLCV)

The OHLCV `trades` column (trade count per bar) is `UInt32` on both
sides. Not scaled.

## Reference dequantization implementations

All three live in `mdata/src/mdata/vendor/schemas.py`:

```python
def dequantize_ohlcv_lazy(lf: pl.LazyFrame) -> pl.LazyFrame: ...
def dequantize_trade_lazy(lf: pl.LazyFrame) -> pl.LazyFrame: ...
def dequantize_quote_lazy(lf: pl.LazyFrame) -> pl.LazyFrame: ...
```

Each one:

1. Reads the LazyFrame schema via `lf.collect_schema()` (no data
   materialization).
2. For each price column that is currently `Int64` → emits
   `(pl.col(col).cast(pl.Float64) / PRICE_SCALE).alias(col)`.
3. Columns that are already `Float64` are **skipped** — this is the
   legacy-Float64 backward-compat shim, which lets mdata's query path
   run the same code against pre-quantized and post-quantized files.

Chili's `set_column_scale("ohlcv_1d", "close", 1_000_000)` must
produce **bit-identical** output to `dequantize_ohlcv_lazy` when the
input is an Int64 partition at scale 1_000_000. Chili's round-trip
test should:

1. Load one of mdata's fixture partitions (e.g.
   `~/code/chili/docs/bench/mdata-collab/fixtures/ohlcv_1d_2024.07.15_0000`).
2. **ohlcv_1d fixtures are Int64 as of 2026-04-11** — mdata's
   `scripts/quantize_hdb.py --execute` ran against the live `ohlcv_1d`
   HDB on 2026-04-11 and this directory's fixtures were refreshed
   afterwards. The ohlcv_1m fixtures are still Float64 — the
   `ohlcv_1m` rewrite has not been run yet.
3. Compare chili output vs. `dequantize_ohlcv_lazy` output using
   `polars.testing.assert_frame_equal` (exact, not approximate).

## Round-trip contract

For any logical Float64 value `x` in the recoverable range
`(-9.2e12, 9.2e12)` (Int64 bounds divided by scale):

    abs(dequantize(quantize(x)) - x) <= 0.5 / PRICE_SCALE = 5e-7

mdata's property tests at `tests/vendor/test_schemas_quantize*.py`
enforce this bound on 5,000 random draws per test. Chili's
`set_column_scale` implementation should satisfy the same bound.

## Integration with mdata's query path today

Quantization happens at the **storage write boundary** in
`StorageEngine.write_partition` (src/mdata/db/storage.py) and
`ChiliGateway.write_partition`
(src/mdata/server/chili_gateway.py) — not at the vendor normalizer
boundary. This means:

- `MassiveNormalizer` still produces Float64 frames.
- `RdbBuffer` / `RdbSubscriberCache` still hold Float64 rows in memory.
- Only the on-disk representation is Int64.
- `QueryEngine.ohlcv` / `ChiliGateway.ohlcv` call
  `dequantize_ohlcv_lazy` before returning to the caller unless the
  caller passes `raw=True`.

When chili ships `set_column_scale`, mdata will:

1. Register the scale at engine-load time in `ChiliGateway.load`:
   ```python
   for col in ("open", "high", "low", "close", "vwap"):
       engine.set_column_scale("ohlcv_1d", col, PRICE_SCALE)
       engine.set_column_scale("ohlcv_1m", col, PRICE_SCALE)
   ```
2. Drop the `dequantize_ohlcv_lazy` call from the chili-backed query
   path (it becomes a no-op because chili already returned Float64).
3. Leave the Polars/DuckDB paths unchanged — they continue to call
   `dequantize_ohlcv_lazy` because they bypass chili.

See `project_chili_wishlist_pickup.md` in mdata's agent memory for
the full Phase 15 action plan.
