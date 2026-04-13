# mdata + chili vs. kdb+/q — Deep Comparative Analysis

*Prepared 2026-04-13. Reference document for planning future enhancements.*

---

## TL;DR

| Dimension | mdata + chili | kdb+/q |
|---|---|---|
| **Workload sweet-spot** | Daily bars + intraday minute, partition-pruning wide scans | Tick-scale asof joins, sub-ms single-symbol queries on hot mmap |
| **Surface** | Python (Polars/DuckDB/FastAPI/Arrow Flight) + restricted "pepper" dialect | q language + PyKX (mature in 2026) |
| **Cost** | ~$0 (all OSS + optional chili binary) | Free Personal / KDB-X Community; ~$15k/core/yr commercial |
| **Storage** | Plain Parquet (portable, any Arrow consumer) | Splayed+partitioned memory-mapped columns (proprietary) |
| **Runtime binary** | Python + multiple libs (~1 GB venv) | Single ~1 MB `q` binary + license file |
| **Proven at scale** | Single-node, 818 tests passing, 10K US symbols | STAC-M3 record holder 15/17 years; 1.6 PiB tested Oct 2025 |

---

## 1. Feature comparison

| Feature | mdata + chili | kdb+/q |
|---|---|---|
| Columnar storage | Parquet (`data/hdb/{table}/{date}_0000`) | Splayed+partitioned mmap columns (`par.txt` multi-disk striping) |
| Price quantization | Int64 @ 1e6 scale, auto at write (`schemas.py:100-117`) | Typed vectors (`int`, `long`, `real`, `float`), user-chosen |
| Column attributes / indexes | Parquet row groups only | `` `p`` (parted), `` `s`` (sorted), `` `g`` (grouped), `` `u`` (unique) — sub-ms lookups |
| Compression codecs | Parquet defaults (snappy) | gzip / snappy / lz4hc / zstd with transparent decompress |
| Tickerplant | `RdbBuffer` + `UnixSocketPublisher` / `ChiliBrokerPublisher` (`feed/pubsub.py`) | Canonical `tick.q` + `r.q` + `u.q` blueprint |
| Crash recovery / replay | WAL via `TransactionLog` + chili broker sequence replay (Phase 16) | TP log replay from byte offset, at-least-once + FIFO guarantees |
| RDB → HDB live merge | Three modes: in-proc `RdbBuffer`, pull `RdbProxy`, push `RdbSubscriberCache` (`db/query.py:68-90`) | RDB rejoins TP feed after `.Q.hdpf` EOD flush |
| Asof join | Polars `join_asof` (app-layer) | `aj`/`wj`/`pj` — tuned binary-search primitives, industry-leading |
| Corporate actions / adjustments | `AdjustmentEngine` + `CorporateActionsStore` (splits, dividends) | Roll your own (no built-in) |
| Aggregation pushdown | Q11 @ 181 ms (chili Phase 17 closed 2026-04-12 — target met via Phases 10+15) | `update ... by` native, vectorized |
| SQL surface | DuckDB (full ANSI) | `s.k` limited SQL; KDB-X adds SQL 2016 |
| Ad-hoc analytics (wide/sparse) | Polars/DuckDB eat it | Splayed model fights you past ~50 sparse cols |
| ML/AI interop | Zero-copy Arrow → torch/sklearn | Boundary tax via PyKX or C |
| Vector/embeddings | — | KDB-X ships vector libs + MCP server for LLMs |
| Auth / RBAC | REST `X-API-Key` in prod; Flight=loopback/no-auth; pub/sub no auth | kdb Insights Enterprise (paid); OSS q is roll-your-own |
| Clustering / sharding | Single-node only (`scalability_architecture.md` SUPERSEDED) | `par.txt` segmentation; Insights Enterprise adds k8s |
| Observability | JSONL `RunLog`, `mdata status/doctor/logs` CLI | `.z.ts` timers; enterprise tooling paid |

**Feature-parity verdict**: mdata has built a remarkably complete kdb+-equivalent feature set for the 80/20 case. Where it's **behind**: asof joins as a first-class primitive, column attributes for sub-ms point lookups, and sharding. Where it's **ahead**: portable Parquet (any Arrow consumer can read), ML interop, ad-hoc SQL, gentle auth story via FastAPI.

---

## 2. Syntax & API ergonomics

Same task — "last close for AAPL, last 30 trading days" — in each stack:

**mdata — Polars QueryEngine** (tests/server/test_rest.py:42+)
```python
lf = engine.ohlcv(Timeframe.DAY_1, symbols=["AAPL"],
                  start=date(2026, 3, 14), end=date(2026, 4, 13))
df = lf.collect()
```

**mdata — DuckDB** (`db/duckdb_query.py`)
```python
engine.query("SELECT timestamp, close FROM ohlcv_1d "
             "WHERE symbol='AAPL' AND timestamp >= '2026-03-14'")
```

**mdata — Chili pepper dialect** (`chili_gateway.py:447-454`)
```
select from ohlcv_1d where date>=2026.03.14, date<=2026.04.13, symbol=`AAPL
```

**kdb+ — q native**
```q
select time, close from ohlcv_1d where date within 2026.03.14 2026.04.13, sym=`AAPL
```

**kdb+ — asof join (no mdata equivalent primitive)**
```q
aj[`sym`time; trade; quote]   / attach prevailing quote to each trade, <1ms/day/symbol
```

**Learning curve**: mdata hands a Polars/SQL user productivity on day one. q takes 2–4 weeks to basic productivity, 6–12 months to fluency — cryptic errors, no real debugger, 16-local-var ceiling. q is write-only for non-experts. Chili's pepper is a restricted q-like dialect (partition filters + symbol `in` only), so mdata users never have to learn full q. **mdata wins on ergonomics by a wide margin**; kdb+ wins on expressive density for those already fluent.

---

## 3. Performance

### mdata benchmarks (from `tests/live/benchmark_results/20260413_005339_post.md`)

| Query | DuckDB | Chili | Polars | Target | Winner |
|---|---|---|---|---|---|
| Q1: single date, all syms, ohlcv_1d | 10.1 ms | **4.1 ms** | — | 50 ms | Chili 2.5× |
| Q2: single sym, 1yr 1d | **53.2 ms** | 184.5 ms | — | 50 ms | DuckDB 3.5× |
| Q3: 5 syms, 1yr 1d | **88.0 ms** | 209.6 ms | — | 100 ms | DuckDB 2.4× |
| Q7: 100 syms, 1yr 1d | **66.2 ms** | 197.2 ms | — | 200 ms | DuckDB 3× |
| Q8: all syms, 1 wk | 15.0 ms | **12.6 ms** | — | 250 ms | Chili 1.2× |
| Q9: SPY 1yr, 1m bars | — | **1876 ms** | 2294 ms | 500 ms | Both FAIL |
| Q11: pepper `last close by symbol` | — | **181 ms** | — | 200 ms | Chili PASS (Phase 17 closed 2026-04-12) |

**Pattern**: chili wins on wide partition-prune scans, loses on narrow per-symbol multi-year queries (partition-load overhead not amortized). DuckDB is the surprisingly consistent all-rounder. Polars is the baseline.

### kdb+ reputation (external, vendor-neutral)

- **STAC-M3** is the only vendor-independent tick benchmark. kdb+ holds ~15 of 17 global records. Oct 2025: kdb+ 4.1 on Supermicro/Micron/Intel — 100-user Antuco 36% faster with 62% fewer cores than prior record; 1.6 PiB tested capacity (6× next-highest).
- Single-symbol-day point queries on a parted `sym` column: sub-ms to low-ms on warm mmap.
- Full-day VWAP across 500M trades: hundreds of ms to low seconds with secondaries.
- No public head-to-head vs. DuckDB/Polars on identical hardware.

### Head-to-head read

For the workloads mdata actually runs (daily bars + intraday minute, US equity universe ~10K symbols), **DuckDB is ahead of chili on narrow queries** and **chili is ahead on wide scans**. kdb+ would almost certainly beat both on (a) single-symbol asof joins at tick cardinality and (b) the specific suite STAC-M3 measures — but mdata isn't doing (a) today. For daily bars, the gap is "kdb+ ~2–5× faster on hot cache" territory — not the 50–100× sometimes quoted.

---

## 4. Resource / dependency footprint

| | mdata + chili | kdb+/q |
|---|---|---|
| **Core binary** | Python 3.12+ venv: polars, pyarrow, duckdb, fastapi, websockets, chili-pie (+Rust binary) — ~800MB–1.2GB installed | Single ~1 MB `q` binary + `k4.lic` / `kc.lic` — no other runtime deps |
| **chili source** | Not vendored — `CHILI_PY_PATH` env var, sys.path shim (`chili_gateway.py:56-103`) | N/A |
| **Platforms** | macOS + Linux (AF_UNIX socket 104-char limit on Mac handled via `short_tmpdir` fixture) | Linux x86-64, macOS (Intel+ARM64), Windows x86-64, Linux-ARM64 |
| **Disk format** | Plain Parquet — portable to any Arrow/Spark/R/Python consumer | Proprietary splayed columns — readable only by kdb+ / PyKX / `k.h` |
| **IPC / network** | UNIX sockets, TCP (REST 8000, Flight 8815 loopback), optional chili-broker | TCP `port` integer; async/sync single-line q primitives |
| **License** | MIT-style (project OSS); chili-pie unknown but local path | Free Personal: 24 cores, 2 machines, non-commercial, no cloud, always-on internet. KDB-X Community (Nov 2025): free for commercial use. Commercial: ~$15k/core/yr |
| **HA / scaling** | Single-node, no sharding | `par.txt` multi-disk segmentation; Insights Enterprise adds k8s/RBAC |
| **Ops surface** | CLI (`refresh/status/logs/doctor/run_feed`), EOD cron `30 8 * * 2-6`, 818 tests | `tick.q/r.q/u.q` trio, log replay, `hopen` handle lifecycle |

**KDB-X Community (Nov 2025) is the material shift.** Historically kdb+'s free tier was crippled (non-commercial, no cloud, always-on internet). KDB-X Community removed the commercial-use block, bundled Python+SQL+q surfaces, KX Dashboards, and an MCP server for LLMs. For a single-machine workload it's now a genuine free alternative.

---

## 5. Pros and cons

### mdata + chili — pros
- Zero licensing friction, entire stack OSS-ish and Python-native.
- Portable on-disk format (plain Parquet) — not vendor-locked.
- Polars / DuckDB / SQL / REST / Arrow Flight — four mature query surfaces, one storage layer.
- Excellent ML interop (zero-copy Arrow → torch/sklearn).
- CLAUDE.md + 818 tests + CI gates = reproducible build.
- Quantized Int64 storage is working and idempotent; re-ingest is safe (`storage.py:48`).
- ChiliBrokerPublisher gives tickerplant-class guarantees without kdb+ dependency (Phase 16 shipped 2026-04-12).

### mdata + chili — cons
- Single-node only; no sharding story (SUPERSEDED plan only).
- No asof-join primitive as fast as kdb+'s `aj` — Polars `join_asof` is the app-layer substitute.
- Chili loses on narrow multi-year single-symbol queries (Q2/Q3 benchmarks 2–3× slower than DuckDB).
- Auth posture weak: Flight is loopback-only with no auth; pub/sub socket has none.
- Chili itself is a moving target — not in `pyproject.toml`, path shim + external build.
- Large runtime footprint (Python venv + multiple Rust extensions) vs. kdb+'s 1 MB binary.
- Massive WS constraint: one concurrent WS per API key (verified 2026-04-13) — multi-stream feed requires multiplexing refactor or plan upgrade.

### kdb+/q — pros
- Sub-ms single-symbol point queries on hot mmap — unmatched at tick cardinality.
- `aj`/`wj`/`pj` asof/window joins are first-class primitives, tuned for decades.
- On-disk columns are already the in-memory vector — zero-decode reads.
- STAC-M3 dominant since 2014; proven at 1.6 PiB in Oct 2025.
- Tiny operational surface — one binary, one license file, no runtime deps.
- TP→RDB→HDB blueprint is the industry reference; every quant desk knows it.
- PyKX 3.1.9 (Apr 2026) is production-mature — Python teams can use kdb+ without daily q.
- KDB-X Community (Nov 2025) finally allows free commercial use for single-node deployments.

### kdb+/q — cons
- Commercial license ~$15k/core/yr for large deployments.
- q is write-only for non-experts; hiring and onboarding are structural headwinds.
- Cryptic error messages (`'type`, `'length`), no mainstream debugger, 16-local-var ceiling.
- Weak ML ecosystem without PyKX boundary tax.
- Not ideal for wide sparse / JSON / geospatial / text — DuckDB wins.
- Community and third-party tooling ~1% the size of Polars/DuckDB/Arrow.
- Free Personal Edition's "no-cloud + always-on internet + non-commercial" clauses are hostile to modern dev flow (KDB-X Community fixes most of this).
- Proprietary storage format — lock-in is real.

---

## 6. Recommendation framing for future enhancements

**Stay on mdata+chili if**: your workloads are daily bars + minute bars on a universe of ~10K US symbols, you want portable Parquet, you value Python/ML interop, and you can tolerate ~200ms single-symbol 1yr queries.

**Next-enhancement priorities (in order)**:
1. **Investigate Q2/Q3 single-symbol regression** — why chili is 3× slower than DuckDB on narrow multi-year queries (partition prefetch? page cache warmup? read-ahead?).
2. **Evaluate asof-join primitive** — either push upstream to chili, or optimize the Polars `join_asof` path in `QueryEngine` for tick-scale trade/quote analytics.
3. **Auth hardening** — Flight loopback → mTLS or token auth; pub/sub socket → SO_PEERCRED + UID allowlist.
4. **Massive WS multiplexing** — refactor `MassiveClient` to share one socket across T/Q/AM channels, removing the "one stream at a time" constraint.
5. **Sharding story** — if data scales past single-node, revisit `scalability_architecture.md` (currently SUPERSEDED).

**Adopt kdb+ (specifically KDB-X Community) if**: you move to tick-scale trade/quote analytics, need `aj` at 100M+ trades/day, or your counterparties demand it.

**Hybrid worth considering**: keep mdata for daily/minute + Parquet portability + Python quant workflows; stand up a kdb+ HDB alongside specifically for tick-scale asof joins. PyKX bridges the two cleanly, and mdata's Arrow Flight surface makes interop trivial.

---

## Sources

**Internal**: `CLAUDE.md`, `src/mdata/server/chili_gateway.py`, `src/mdata/feed/pubsub.py`, `src/mdata/db/query.py`, `tests/live/benchmark_results/20260413_005339_post.md`, `docs/plans/tick_snapshot_plan.md`.

**External**:
- [STAC-M3 benchmark hub](https://docs.stacresearch.com/m3)
- [KDB-X Community GA blog](https://kx.com/blog/kdb-x-now-generally-available-the-next-era-of-kdb-for-ai-driven-markets/)
- [PyKX on PyPI](https://pypi.org/project/pykx/)
- [code.kx.com tick architecture](https://code.kx.com/q/architecture/tickq/)
- [Supermicro/STAC Oct 2025 record](https://ir.supermicro.com/news/news-details/2025/Supermicro-Intel-and-Micron-Collaborate-on-Record-Breaking-Results-for-the-STAC-M3-Quantitative-Trading-Benchmark/default.aspx)
- [kdb+ Personal Edition license](https://kx.com/kdb-free-personal-edition-license-agreement/)
- [kdb+ file compression KB](https://code.kx.com/q/kb/file-compression/)
- [kdb+ splayed tables KB](https://code.kx.com/q/kb/splayed-tables/)
- [kdb+ partitioned tables KB](https://code.kx.com/q/kb/partition/)
