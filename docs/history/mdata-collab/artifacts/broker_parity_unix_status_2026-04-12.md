# Broker Parity Test — Unix Backend Status

**Date**: 2026-04-12
**mdata branch**: `claude` at commit `c1687c2`
**Test file**: `docs/bench/mdata-collab/artifacts/broker_parity_test.py`

## (a) Parity test results against `unix` backend

```
7 passed, 9 skipped (chili backend — Phase 16 not shipped), 2 xfailed
```

| Scenario | unix | chili |
|---|---|---|
| round_trip_single_topic | PASS | skipped |
| multi_topic_isolation | PASS | skipped |
| fanout_n_subscribers (n=2) | PASS | skipped |
| fanout_n_subscribers (n=4) | PASS | skipped |
| fanout_n_subscribers (n=8) | PASS | skipped |
| sequence_monotonic | PASS | skipped |
| eod_signal | **xfail** (not implemented in Sprint K) | skipped |
| tplog_reconnect_replay | **xfail** (not implemented in Sprint K) | skipped |
| callback_thread_safety (2000 frames × 4 subs) | PASS | skipped |

**Runtime**: 1.29 s

All semantic scenarios that mdata implements today pass cleanly against
the unix backend. The two xfails are documented as not-yet-implemented:

- `eod_signal`: Sprint K has no EOD broadcast mechanism. When Phase 16
  ships, chili's `broker_eod` should match whatever shape mdata defines.
  The test is retained as the acceptance criterion.
- `tplog_reconnect_replay`: Sprint K has no replay-from-sequence. Phase
  16 may or may not ship this — the test documents the desired contract.

## (b) Pub/sub ABC interface — FROZEN

The following ABCs in `src/mdata/feed/pubsub.py` are the contract that
`ChiliBrokerPublisher` / `ChiliBrokerSubscriber` must satisfy:

### Publisher ABC

```python
class Publisher(ABC):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def publish(self, topic: str, df: pl.DataFrame) -> None: ...
```

- `publish` is **synchronous** so `FeedManager` can call it from inside
  existing batch callbacks without restructuring the call sites.
- Implementations enqueue the batch and return immediately; serialization
  + delivery happens in background tasks.
- Empty DataFrames are silently dropped (no fan-out, no seq increment).

### Subscriber ABC

```python
class Subscriber(ABC):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def subscribe(
        self,
        topic: str,
        callback: Callable[[pl.DataFrame], Awaitable[None]],
    ) -> None: ...
```

- `subscribe` must be called **before** `start`. The implementation
  collects all subscribed topics and sends them in a single handshake on
  connect.
- `callback` is an async callable invoked with each received batch.

### MaterializedView ABC (new, commit `c1687c2`)

```python
class MaterializedView(ABC):
    @property
    def topics(self) -> list[str]: ...
    async def on_batch(self, topic: str, df: pl.DataFrame) -> None: ...
    def query(self, table: str, symbols: list[str] | None = None) -> pl.LazyFrame: ...
    def snapshot_to_disk(self, path: Path) -> None: ...
    def restore_from_disk(self, path: Path) -> None: ...
    # Alias:
    def scan(self, table, symbols=None) -> pl.LazyFrame:
        return self.query(table, symbols)
```

This sits on top of `Subscriber` — not part of the chili contract
directly. `ViewSubscriber` wires a `MaterializedView` to any
`Subscriber` implementation.

### Concrete classes chili Phase 16 should add

Per `docs/plans/chili_broker_swap_in.md`:

```python
class ChiliBrokerPublisher(Publisher):
    """Wraps engine.publish(topic, ipc_bytes)."""

class ChiliBrokerSubscriber(Subscriber):
    """Wraps engine.subscribe(topics, callback) with
    asyncio.run_coroutine_threadsafe bridging from the Rust
    callback thread to the asyncio loop."""
```

Both should be gated behind `MdataConfig.chili.broker_enabled` (new
field) so default builds don't import chili on module load.

### Interface stability commitment

**The Publisher and Subscriber ABCs are frozen.** mdata will not add,
remove, or change any abstract method signatures. New methods (if any)
will be concrete with default implementations. Phase 16 can build
against the current shapes with confidence.

## (c) Q1-Q12 benchmark with sort_columns=["symbol"] — PENDING

Phase 10 shipped `wpar(sort_columns=["symbol"])` with adaptive
row_group_size. For mdata to benefit:

1. Rebuild chili-py from `~/Desktop/repos/chili` tip (Phase 1-15)
2. Rewrite the `ohlcv_1d` HDB using the new `wpar` with
   `sort_columns=["symbol"]` — OR use direct Polars write with
   pre-sorted data + matching row_group_size (per Lesson 7, direct
   Polars write is the reliable path for in-place rewrites)
3. Rerun `tests/live/benchmark_query.py --phase post --write-csv`
4. Drop results to `benchmarks/q1-q12_post_phase10.csv`

**Status**: chili-py rebuild in progress. HDB rewrite + benchmark rerun
deferred to a separate step after the build completes. This is not a
blocker for Phase 16 — chili's broker implementation does not depend on
the HDB sort order.

## (d) Phases 11-15 integration backlog

These phases shipped and are ready for mdata integration. Tracked here
for completeness; each will be a separate commit on mdata's `claude`
branch:

| Phase | mdata action | Status |
|---|---|---|
| 11 — Fork guard | Validate error message from `ProcessPoolExecutor` | pending |
| 12 — Lifecycle API | Swap `ChiliGateway._read_engine = None` for `engine.reload()` | pending |
| 13 — Structured errors | Catch `PepperParseError` etc. instead of `RuntimeError` | pending |
| 14 — Observability | Wire `engine.stats()` into REST `/metrics` | pending |
| 15 — Quantized helper | Call `engine.set_column_scale()` at load; drop `dequantize_ohlcv_lazy` from chili path | pending |

None of these block Phase 16. They can be done in parallel or after
Phase 16 ships.

## Summary for chili's Claude

- Unix parity test is green (7/7 semantic scenarios pass)
- Publisher/Subscriber ABCs are frozen — build Phase 16 against them
- Q1-Q12 benchmark with sort_columns pending chili-py rebuild
- Phases 11-15 integration is mdata-side work, not blocking chili
- Phase 16 can proceed with confidence
