# The kdb+ Alternatives Landscape: Competitive Catalog

**Author:** Research subagent (one of five strategic-positioning research deliverables)
**Date compiled:** 2026-05-06
**Companion doc:** [`q_kdb_landscape.md`](./q_kdb_landscape.md) — kdb+ itself, in 500 lines. This document does **not** rewrite that.
**Out of scope here:** Shakti deep-dive (separate sibling subagent); chili's own pitch (a separate "positioning" doc consumes this).

**Editorial conventions:**
- Every benchmark figure carries `[Source: <url>, retrieved 2026-05-06]`.
- Vendor-published numbers are tagged `[VENDOR]`. Independent or third-party numbers are tagged `[INDEPENDENT]`. Press-release-adjacent numbers are tagged `[VENDOR-ADJACENT]`.
- All retrieval dates are 2026-05-06 unless otherwise noted.
- When data is thin, the doc says so explicitly rather than papering over.
- **Skepticism > confidence. Citations > claims. Tables > prose.**

---

## 1. Methodology and scope

### 1.1 What "alternative" means here

A project is in scope iff **both**:

(a) It markets or is marketed as addressing **timeseries + analytics + columnar** workloads (any two-of-three is a near-miss; observability metrics-only systems are sub-segment alternatives, not direct replacements).

(b) Either users or its own marketing position it as a **substitute for kdb+** (or address the same audience: tick-data analytics, financial timeseries, low-latency streaming + historical queries).

Out of scope:
- **Generic OLAP DBs unrelated to timeseries** (BigQuery, Redshift unless someone's running a kdb-replacement POC there — covered in §2.E briefly).
- **ETL frameworks** (Airbyte, Fivetran, dbt) — not engines.
- **Plain Postgres with no timeseries extension** — TimescaleDB and pg_partman are in; vanilla Postgres is not.
- **Generic message queues** (Kafka, NATS, Redpanda) — they ship data into kdb+/alternatives, they don't replace them.

### 1.2 Borderline calls (recorded for transparency)

- **InfluxDB** — included. Marketed as "the open-source kdb alternative for observability+IoT" historically, and v3 (FDAP stack: Flight + DataFusion + Arrow + Parquet) is architecturally close to chili's lineage.
- **Prometheus** — included briefly. Observability-first, *not* analytics; marked so.
- **Vaex** — included. Out-of-core columnar dataframe; positioned as analytics-on-laptop. Ambiguous timeseries angle but routinely surfaces in "kdb-on-a-budget" threads. Activity recently slowed (see entry).
- **Dask** — included briefly. Distributed pandas; not really a kdb+ replacement in spirit, but noted because some shops migrated kdb+→Dask+Parquet.
- **Snowflake / Databricks** — covered in §2.E only. They're not "kdb+ alternatives" in the technical sense, but at least one global fixed-income firm reportedly replicated kdb+ analytics on Snowflake [BigDATAwire 2025-09-15, see §3.2]. Including for market-pressure context, not as a feature-for-feature comparator.
- **TDengine** — included. Open-source TSDB with kdb-style ambitions in IoT, dual-licensed under AGPL-3.0 + commercial.
- **DolphinDB** — included. Closed-source proprietary product with explicit kdb-style multi-paradigm DSL; community edition free-tier is ≤2 nodes / 2 cores / 8 GB.

### 1.3 Comparison axes (all entries fill these)

| Axis | What goes here |
|---|---|
| License | SPDX where possible (Apache 2.0 / MIT / AGPL / commercial / source-available). Tag any non-OSI license. |
| Language layer | Query DSL, host language, embedded language. |
| Storage format | On-disk shape (custom columnar / Parquet / row pages / hypertables). |
| In-memory shape | Column / hybrid / row. |
| Primary marketed use case | What the homepage actually says. |
| Latest release | Version + date. |
| Public benchmarks vs kdb+ | Table; flag vendor vs independent. |
| Strengths vs kdb+ | 1-3 bullets. |
| Weaknesses vs kdb+ | 1-3 bullets. |
| Strategic relevance to chili | One paragraph. |

### 1.4 Skepticism note

**kdb+'s license historically prohibits publishing benchmarks without KX approval** — a "DeWitt clause" [Source: https://news.ycombinator.com/item?id=20762564 ; https://dwheeler.com/essays/dewitt-clause.html, retrieved 2026-05-06]. That means almost every "X vs kdb+" number in this doc is either (a) published by an alternative-vendor (QuestDB, KX itself for KDB-X CE) or (b) a small-N independent post (Medium, blog) that may have skipped kdb+'s terms. **Treat all such numbers as directional**, not benchmarks. The most credible cross-system numbers come from STAC-M3 — but those are expensive, vendor-funded, and use "fastest available hardware humanly possible" tunings [Source: https://www.timestored.com/data/time-series-database-benchmarks, retrieved 2026-05-06].

---

## 2. The catalog

### 2.A — Column-store analytical DBs (the strongest 2025-2026 alternatives)

These are the pragmatic kdb+ displacers in shops where (a) cost matters more than the last 10× of latency, (b) workloads are mostly batch-historical not microsecond-streaming, or (c) language friction with q is the real blocker.

#### 2.A.1 DuckDB

| Axis | Value |
|---|---|
| License | MIT [Source: https://duckdb.org/why_duckdb] |
| Language | SQL (PostgreSQL-flavored), embedded API for Python / R / Java / C++ / Node / WASM |
| Storage | Single-file `.duckdb` columnar format; reads/writes Parquet, CSV, JSON natively; Iceberg via extension (read; v3 writes still limited as of 1.5.x [Source: https://duckdb.org/docs/current/core_extensions/iceberg/overview.html]) |
| In-memory | Vectorized columnar; MonetDB/X100 + Vectorwise lineage |
| Marketed use case | "In-process analytical database" — laptop-to-medium-server OLAP |
| Latest | v1.5.2 (2026-04-13) [Source: https://github.com/duckdb/duckdb/releases]; v1.0 was 2024-06-03 [Source: https://duckdb.org/2024/06/03/announcing-duckdb-100.html] |
| Stewardship | DuckDB Labs (~20 core contributors) + DuckDB Foundation (non-profit, holds IP); funded by consulting/support (no VC) |

**Public benchmarks vs kdb+:** None published with full methodology at the time of writing. ClickBench (where DuckDB is a regular top-3) does not include kdb+ [Source: https://benchmark.clickhouse.com/, retrieved 2026-05-06]. KX's TSBS-based KDB-X CE comparison does not include DuckDB [Source: https://medium.com/kx-systems/benchmarking-kdb-x-vs-questdb-clickhouse-timescaledb-and-influxdb-with-tsbs-2090f4533be0, retrieved 2026-05-06]. **No credible head-to-head DuckDB-vs-kdb+ public benchmark exists** — the absence is itself a finding.

**Strengths vs kdb+:**
- Free, MIT, no DeWitt clause. Can be embedded in any process; no server. Backwards-compatible storage from v1.0 forward (commitment 2024-06).
- Massively wider language reach: every Python/R/Node/Rust shop can use it without learning q.
- Native Parquet/Iceberg/Arrow integration — fits cloud-native + data-lake architecture out of the box.

**Weaknesses vs kdb+:**
- Single-process by design (cannot natively scale-out to a kdb+ HDB-cluster shape; multi-node is via MotherDuck or partitioned-Parquet patterns).
- Streaming/append story is weaker than kdb+'s tickerplant pattern.
- No first-class q-style language; SQL-only. Hard to express asof-join + windowed-vector arithmetic as concisely as q.

**Strategic relevance to chili:** **Closest design cousin in the OSS world.** DuckDB and chili are both: in-process, column-store, vectorized, single-binary, MIT/Apache-permissive. The strategic divergence is that DuckDB doubles down on SQL while chili keeps pepper (q-like) + chili (JS-like) DSLs. Chili should consider DuckDB the **primary benchmark target** for single-node OLAP. If chili can't keep within ~2× of DuckDB on equivalent batch queries, the q-syntax pitch alone won't carry it. DuckDB is also a *reference* for what a sustainable OSS analytical-DB foundation looks like (DuckDB Labs + Foundation model).

#### 2.A.2 ClickHouse

| Axis | Value |
|---|---|
| License | Apache 2.0 [Source: https://github.com/ClickHouse/ClickHouse, license metadata] |
| Language | SQL (with array-join, window funcs, CH-specific dialect) |
| Storage | MergeTree family (LSM-style on-disk); Parquet via table function; Iceberg + Delta read; S3-backed cold tier |
| In-memory | Columnar, vectorized, JIT compiled |
| Marketed use case | "high-performance, column-oriented SQL DBMS for OLAP" — real-time analytics at billions-of-rows/sec [Source: https://clickhouse.com/docs/en/intro] |
| Latest | v26.4.1.1141-stable (2026-05-05) [Source: https://github.com/ClickHouse/ClickHouse/releases/latest]; release cadence is monthly |
| Stars | ~47.2k |

**Public benchmarks vs kdb+:**
- KX-published TSBS comparison (Nov 2025): ClickHouse "exhibited a slowdown of up to 1,100x for certain queries" vs KDB-X CE — but KDB-X was capped at 4 threads / 16 GB while ClickHouse used full hardware, **so the 1,100× figure does not mean what it appears to mean** [VENDOR; Source: https://medium.com/kx-systems/benchmarking-kdb-x-vs-questdb-clickhouse-timescaledb-and-influxdb-with-tsbs-2090f4533be0, retrieved 2026-05-06]. The benchmark also used the DevOps TSBS workload, not financial tick patterns.
- Kozloski 2024 (Medium, 7-system FX-data benchmark, 1M rows): "ClickHouse emerged as the winner" on ingest+compression (123 MB CSV → 20 MB ClickHouse). kdb+ was excluded due to "$100,000" annual licensing cost, not technical performance [INDEPENDENT; small-N; Source: https://medium.com/@ev_kozloski/timeseries-databases-performance-testing-7-alternatives-56a3415e6e9e, retrieved 2026-05-06].
- Deutsche Bank internal benchmark referenced by TimeStored: "ClickHouse fastest" (no methodology disclosed in the public listing) [VENDOR-ADJACENT; Source: https://www.timestored.com/data/time-series-database-benchmarks, retrieved 2026-05-06].

**Strengths vs kdb+:**
- Cluster-native by default (Distributed engine, ZooKeeper/Keeper-coordinated replicas).
- ClickHouse Cloud + ClickPipes give a managed cloud SKU; kdb+ Insights is the equivalent but is opaquely-priced enterprise-only.
- SQL skill base is enormous.
- Apache 2.0; no DeWitt clause; you can publish benchmarks freely.

**Weaknesses vs kdb+:**
- Asof-join and time-bucketed vector math feel grafted-on vs first-class in q.
- Mutation/update semantics are LSM-merge-eventual; kdb+'s in-memory tables update in microseconds.
- Real-time streaming ingest is good but historically less microsecond-tight than kdb+ tickerplant.

**Strategic relevance to chili:** ClickHouse is the **cost-driven displacement** option for shops where the kdb+ sticker price is the blocker. Architecturally chili and ClickHouse diverge on scale-out (ClickHouse is built for it; chili is single-node-with-partitioned-HDB like kdb+). Chili should not try to compete with ClickHouse on cluster-scale OLAP; it competes with ClickHouse on **embedded / single-node / Python-binding** ergonomics, where ClickHouse is awkward (chDB exists but is a side project). The strategic angle is "ClickHouse for the data warehouse, chili for the desk".

#### 2.A.3 QuestDB

| Axis | Value |
|---|---|
| License | Apache 2.0 (OSS core); commercial Enterprise tier [Source: https://questdb.com/] |
| Language | SQL with timeseries extensions (`SAMPLE BY`, `LATEST ON`, `ASOF JOIN`, `HORIZON JOIN`, `WINDOW JOIN`, `PIVOT`); Postgres wire protocol |
| Storage | Custom columnar partitions + WAL; Apache Parquet for cold tier; S3/Azure/NFS-backed Tier 3 |
| In-memory | Columnar, SIMD-accelerated, JIT on x86_64 + (since 9.3.3) ARM64 |
| Marketed use case | Capital-markets timeseries, observability — "trading floors to mission control" [Source: https://questdb.com/, retrieved 2026-05-06] |
| Latest | 9.3.5 (2025-04-13) [Source: https://github.com/questdb/questdb/releases/latest, GitHub release `9.3.5` at 2026-04-13T17:21:09Z — note: page text suggests 2025 calendar references; release timestamp is normative] |
| Notable users | B3 (Brazil B exchange), Airbus, Mizuho, OKX, Ripple, BTG Pactual [self-reported, https://questdb.com/] |

**Public benchmarks vs kdb+:**
- Self-published OHLCV bar benchmark (5-min bars on tick data): QuestDB ~25 ms, kdb+ ~109 ms, ClickHouse ~547 ms — "QuestDB ~4.4× faster than kdb+ on this specific OHLCV query" [VENDOR; small N=1 query class; Source: synthesized from https://www.timestored.com/data/time-series-database-benchmarks search-cited 2024 result, retrieved 2026-05-06]. Hardware not fully disclosed.
- QuestDB-published vs InfluxDB 3 Core: "12-36× faster for ingestion, 43-418× faster for complex analytical queries" [VENDOR; methodology not in the blog post itself; Source: https://questdb.com/blog/, retrieved 2026-05-06].
- KX-published TSBS (Nov 2025): QuestDB was the closest competitor to KDB-X CE — average slowdown 3.36× — but KDB-X was capped at 4 threads/16 GB [VENDOR; Source: https://medium.com/kx-systems/benchmarking-kdb-x-vs-questdb-clickhouse-timescaledb-and-influxdb-with-tsbs-2090f4533be0, retrieved 2026-05-06]. **The cap-vs-uncap asymmetry is what makes this number noteworthy: even uncapped, QuestDB only beat KDB-X on 6 of 64 scenarios.** That suggests KDB-X is the closer-to-kdb+ proxy and QuestDB is the closest open-source kdb+-like.

**Strengths vs kdb+:**
- Capital-markets tick-data positioning is **direct head-to-head with kdb+'s home turf** — and they have real exchange + bank customers.
- Three-tier storage (WAL → columnar partitions → Parquet on S3) addresses kdb+'s "partition-by-day, manage-by-hand" pattern with cloud-native semantics.
- SQL + Postgres wire = 100× wider tooling ecosystem than q.
- Capital-markets-specific SQL extensions (`ASOF JOIN`, `HORIZON JOIN`, `twap()`, markout analysis) close the q-vs-SQL expressiveness gap for finance workloads specifically.

**Weaknesses vs kdb+:**
- Single-node ingestion is high (8 M rows/s claimed [VENDOR]) but distributed-shard story is enterprise-only.
- No q-equivalent terseness for ad-hoc desk research.
- TSBS benchmarks suggest a 3-4× single-thread gap to kdb+ remains on aggregate query workloads.

**Strategic relevance to chili:** **QuestDB is chili's direct positioning competitor in capital markets.** Chili's pepper-language pitch is "kdb+ ergonomics on open-source" — QuestDB's pitch is "tick-data analytics on SQL". They split the audience: QuestDB wins shops where SQL + Postgres-tooling are decisive; chili wins shops where q-syntax-fluency or in-process Python embedding are decisive. Watch QuestDB closely: their roadmap (HORIZON JOIN, markout, twap, JIT-on-ARM64) is *exactly* the feature set capital-markets shops will demand from chili too.

#### 2.A.4 Apache Druid

| Axis | Value |
|---|---|
| License | Apache 2.0 [Source: https://github.com/apache/druid] |
| Language | Druid SQL (Calcite-based); Native JSON query API |
| Storage | Segment files (compressed columnar with bitmap indexes); deep storage = HDFS/S3/Azure |
| In-memory | Columnar; per-segment dictionary + bitmap indexes for high-cardinality dims |
| Marketed use case | "real-time analytics database that delivers sub-second queries on streaming and batch data at scale and under load" [Source: https://druid.apache.org/, retrieved 2026-05-06] |
| Latest | druid-36.0.0 (2026-02-09) [Source: https://github.com/apache/druid/releases/latest] |
| Stars | ~14.0k |

**Public benchmarks vs kdb+:** None I could find. Druid's benchmarks are typically vs ClickHouse, BigQuery, Pinot, and Snowflake. The kdb+ / Druid comparison is ~absent from public material — these systems serve different audiences.

**Strengths vs kdb+:**
- Streaming + batch ingest unified out of the box (Kafka + S3 backfills).
- Loose-coupling: brokers / historicals / coordinators / overlords scale independently.
- Sub-second queries on high-cardinality user-event analytics.

**Weaknesses vs kdb+:**
- Operationally heavy: requires ZooKeeper, deep storage, at least 4 separate process types.
- Segment-based storage isn't optimized for the "give me all ticks for symbol X between 09:30 and 10:00" pattern at HFT latencies.
- Historically targeted at clickstream/logs/observability analytics, not financial tick.

**Strategic relevance to chili:** Largely **orthogonal**. Druid is for "many-tenant user-event analytics" — Imply's bread and butter — not for kdb+'s tight microsecond-latency tick pattern. Chili can mostly ignore Druid as a head-to-head competitor; it's worth knowing only because its segment + bitmap-index pattern is interesting prior art for cardinality-heavy chili workloads (which chili currently doesn't target).

---

### 2.B — Timeseries-first DBs

#### 2.B.1 InfluxDB (1.x / 2.x / 3.x — three different products)

| Axis | Value |
|---|---|
| License | InfluxDB 3 Core: MIT + Apache 2.0 dual-licensed [Source: https://www.influxdata.com/products/influxdb/, retrieved 2026-05-06]; Enterprise is commercial |
| Language | InfluxQL (legacy 1.x); Flux (functional, 2.x); SQL + InfluxQL on 3.x via DataFusion |
| Storage | TSM tree (1.x/2.x); v3 = Parquet on object storage with FDAP stack (Apache Flight + DataFusion + Arrow + Parquet) |
| In-memory | v3: Arrow columnar; vectorized via DataFusion |
| Marketed use case | "Open Source Engine for Real-Time Data" — observability, IoT, real-time monitoring (financial tick is *not* primary) |
| Latest releases | v3.9.0 (2026-04-03), v2.9.0 (2026-05-01), v1.12.4 (2026-04-13) — **all three lines actively maintained as of May 2026** [Source: https://github.com/influxdata/influxdb/releases] |

**Architectural shift to flag:** InfluxDB 3.x ("IOx") was a **complete rewrite in Rust** on the FDAP stack (Flight + DataFusion + Arrow + Parquet). This puts v3 architecturally in the same family as chili — Rust + Arrow + Parquet + columnar — but with a metrics/observability lens rather than tick analytics.

**Public benchmarks vs kdb+:** No direct public benchmark vs kdb+ that I could find with full methodology. InfluxDB is most commonly benchmarked vs TimescaleDB and QuestDB.

**Strengths vs kdb+:**
- Massive observability-ecosystem integration (Telegraf, Grafana, Prometheus remote write, OpenTelemetry).
- Cloud-managed product (InfluxDB Cloud) with three-tier serverless economics.
- Open core; v3 storage on object storage is genuinely cloud-native.

**Weaknesses vs kdb+:**
- Three-line maintenance burden (1.x, 2.x, 3.x are different products). Migration paths are non-trivial; 3.x doesn't fully cover Flux feature surface.
- Observability-first design choices (downsampling, retention policies, tag/field model) don't map cleanly to financial-tick semantics.
- v3 still less mature than v1 / v2 in some operational corners.

**Strategic relevance to chili:** **Architecturally the closest to chili's lineage** — both are Rust + Arrow + Parquet + DataFusion-adjacent stacks (chili sits on Polars rather than DataFusion, but the family resemblance is strong). InfluxData's commercial trajectory (open core + managed cloud) is a possible model. The use-case divergence (observability vs capital markets) means chili and InfluxDB 3 don't directly compete *today*, but the FDAP architecture validates chili's bet on Arrow+Parquet as the right substrate. **Worth tracking InfluxDB 3's DataFusion contributions** — those upstream improvements benefit chili indirectly via the broader Arrow ecosystem.

#### 2.B.2 TimescaleDB / Tiger Data

| Axis | Value |
|---|---|
| License | Dual: Apache 2.0 (community) + Timescale License (TSL) for advanced features [Source: https://github.com/timescale/timescaledb, license metadata = `NOASSERTION` because dual; project has a TSL that is *not* OSI-approved] |
| Language | SQL (as a Postgres extension); pgvector compatibility |
| Storage | Hypertables (auto-partitioned chunks); columnstore compression; tiered storage (SSD → object storage) |
| In-memory | Postgres row pages + columnar compression chunks; hybrid |
| Marketed use case | "PostgreSQL platform trusted by enterprises processing trillions of metrics daily" — observability, IoT, financial RAG, AI-native [Source: https://www.tigerdata.com/, retrieved 2026-05-06] |
| Latest | 2.26.4 (2026-04-28) [Source: https://github.com/timescale/timescaledb/releases/latest] |
| Rebrand | Timescale → **Tiger Data** announced 2025-06-17; majority of cloud workloads are no longer time-series; "best PostgreSQL" repositioning [Source: https://www.tigerdata.com/blog/timescale-becomes-tigerdata, retrieved 2026-05-06] |

**Public benchmarks vs kdb+:** None I could find directly. Most Timescale benchmarks compare against InfluxDB and QuestDB. The KX-published TSBS run includes TimescaleDB and reports it crashed on `groupby-orderby-limit` [VENDOR; Source: https://medium.com/kx-systems/..., retrieved 2026-05-06] — take with TSBS-config-asymmetry caveats.

**Strengths vs kdb+:**
- Postgres ecosystem: every ORM, every BI tool, every JDBC/ODBC driver works.
- Operational footprint = Postgres operational footprint (well understood by every DBA).
- Hypertable + continuous aggregate + retention policy abstractions match observability/IoT patterns natively.

**Weaknesses vs kdb+:**
- Postgres row-engine roots show up in tick-data workloads — even with columnstore, you're not in the same ballpark as a native columnar engine on `select * from trades where sym=X and time within Y` patterns.
- TSL features (continuous aggregates, compression policies, multi-node) are not OSI open-source.
- The 2025 rebrand is a real signal: the company itself is moving away from "time-series first."

**Strategic relevance to chili:** Tiger's rebrand-away-from-timeseries is **strategically significant** — it suggests the pure-play TSDB market is hard to make a billion-dollar business in. Tiger pivoted to "Postgres for AI / RAG / agents." Chili's stay-narrow bet (capital-markets columnar tick) deliberately rejects this generalization, which means chili needs to be confident that a focused-but-smaller market is enough. From a competition standpoint: Tiger is **largely out-of-scope** as a head-to-head — they no longer fight for the same workloads.

#### 2.B.3 Prometheus (brief — sub-segment, not direct alternative)

| Axis | Value |
|---|---|
| License | Apache 2.0 |
| Language | PromQL |
| Storage | TSDB (custom, append-only log + chunk files); local storage by design |
| In-memory | Per-block chunks; not classical columnar |
| Marketed use case | Cloud-native monitoring + alerting (Kubernetes-native) |
| Latest | v3.11.3 (2026-04-27) [Source: https://github.com/prometheus/prometheus/releases/latest] |

**Strategic relevance to chili:** Prometheus is **observability-first, not analytics**. Operationally it solves a problem orthogonal to kdb+'s. It belongs in this catalog only because (a) observability TSDBs are sometimes mis-cited as "kdb alternatives" and (b) Prometheus's remote-write protocol is the ingest standard the entire CNCF ecosystem uses, which is a path chili could opt into if it ever wanted to pick up observability use cases. **Chili should ignore Prometheus as a competitor** but consider Prometheus remote-write as a potential ingestion bridge.

#### 2.B.4 VictoriaMetrics

| Axis | Value |
|---|---|
| License | Apache 2.0 (community); Enterprise tier commercial |
| Language | MetricsQL (PromQL-compatible superset) |
| Storage | Custom columnar TSDB with very high compression |
| In-memory | Columnar block-compressed |
| Marketed use case | Drop-in Prometheus replacement at scale; observability + logs + traces |
| Latest | v1.142.0 (2026-04-28) [Source: https://github.com/VictoriaMetrics/VictoriaMetrics/releases/latest] |
| Stars | ~17k |

**Public benchmarks vs kdb+:** None I could find. VictoriaMetrics benchmarks are vs Prometheus, M3DB, InfluxDB.

**Strategic relevance to chili:** Same as Prometheus — observability sub-segment, **not a kdb+ replacement**. Notable only because VictoriaMetrics's compression engine (per their published numbers, often >10× metric-point compression) is interesting prior art if chili ever extended into observability. Otherwise: ignore as competition.

---

### 2.C — Array-language successors / variants of K

These are the niche "Whitney-loyalist" or array-language-purist communities. None is a kdb+ replacement at scale; they exist for love of the language model. Their *cumulative* relevance is that they're chili's natural surface-language community — anyone who likes pepper would already be in this ecosystem.

#### 2.C.1 ngn/k

| Axis | Value |
|---|---|
| License | AGPLv3 (v3 only) |
| Language | K (closely-K6-compatible) |
| Status | **DORMANT** — `readme` explicitly says "this k implementation is no longer supported"; latest commit 2025-11-17 (issue #112). Author has redirected users to `growler/k` fork [Source: https://codeberg.org/ngn/k, retrieved 2026-05-06] |
| Stars | 59 (Codeberg) |

**Strategic relevance:** Project is dormant; pepper-language community knows ngn/k as the modern-K reference for years. Chili can mention pepper as "ngn/k-influenced" only with the awareness that ngn/k itself is no longer the active fork.

#### 2.C.2 growler/k (active fork of ngn/k)

| Axis | Value |
|---|---|
| License | AGPLv3 (v3 only) |
| Language | K |
| Status | Active. 4,603 commits on master; last activity 2026-05-06 [Source: https://codeberg.org/growler/k, retrieved 2026-05-06] |
| Stars | 16 (Codeberg) |

**Strategic relevance:** Tiny but active. Useful as a reference implementation for any pepper-language semantics question. Not competitive with chili at any scale; conceivably a partnership opportunity (cross-pollinate test corpora, q-edge-case behavior).

#### 2.C.3 Klong (Nils M. Holm)

| Axis | Value |
|---|---|
| License | Project page does not state OSI license explicitly; Holm typically releases under simple permissive terms [Source: https://t3x.org/klong/, retrieved 2026-05-06] |
| Language | Klong — "an array language, like K, but without the ambiguity" |
| Storage | N/A (interpreter, not a DB) |
| Latest | klong20221212 (2022-12-12); previous 2019, 2017 |
| Status | **Effectively maintenance-only** since 2022; not strictly dormant but very low cadence |

**Strategic relevance:** Klong is a teaching/personal-research K dialect, not a production engine. Cite only as an example of the broader array-language community. **Ignore as competition.**

#### 2.C.4 Shakti / k9 (Whitney's closed-source successor)

**Deferred to sibling subagent's deep-dive doc.** What this catalog records:

- Closed-source, Whitney + Lustgarten, founded 2018, public material from 2019 onward [Source: q_kdb_landscape.md §1, citing https://en.wikipedia.org/wiki/Arthur_Whitney_(computer_scientist)].
- Marketed as a unified database/language/streaming/security platform, claimed substantially faster than kdb+ on same hardware.
- Domain redirects: `shakti.com` → `k.nyc`; `k.nyc` returns minimal content (just the letter "k" served on HTTP) when fetched programmatically [retrieved 2026-05-06]. Public web presence is intentionally cryptic.
- DDN press releases position Shakti+DDN appliances as performance-differentiated [VENDOR-ADJACENT].

**Strategic relevance to chili (deferred for depth):** Shakti is the "Whitney-loyalist" successor and the legitimate-heir-to-kdb+ pitch. Chili and Shakti are ideologically *opposite* — Shakti doubles down on closed-source-single-genius engineering; chili is open-source community work. **The Shakti deep-dive doc should treat Shakti as the maximalist counter-philosophy chili is implicitly arguing against.**

#### 2.C.5 KDB-X Community Edition (KX's 2025 OSS-adjacent move)

| Axis | Value |
|---|---|
| License | Custom KX Community License (`kc.lic`) — **not OSI open-source** but free-for-commercial-use with embedded resource caps [Source: https://kx.com/blog/kdb-x-ga-built-for-developers/, retrieved 2026-05-06] |
| Language | q + Python + SQL (kxsql); MCP server for AI integration |
| Storage | Native kdb+ on-disk + Parquet (via core module) + S3/Azure/GCS direct query |
| In-memory | kdb+ engine, unchanged at the core |
| Marketed use case | "World's fastest time-series and analytics engine" with developer-first OSS-adjacent positioning |
| Latest GA | 2025-11-17 (KX blog announcement); KDB-X release 2025-10-22 (Public Preview ended) [Source: https://docs.kx.com/public-preview/kdb-x/Releases/release-notes-latest.htm, retrieved 2026-05-06] |
| Resource caps | `.Q.lim[]` returns: cores=24, threads=4, mem=16 GB, conns=8 (per process) [Source: https://www.defconq.tech/blog/From%20Elite%20to%20Everyone..., retrieved 2026-05-06; corroborated by community reports surfaced via KX docs]. **No expiry**; allowed for **commercial and offline** use. |

**KX-published benchmark (TSBS DevOps workload):** KDB-X CE fastest in 58 of 64 scenarios vs QuestDB (avg slowdown 3.36×), ClickHouse (slowdown up to 1,100× on outlier queries), TimescaleDB (crashed on `groupby-orderby-limit`), InfluxDB. Hardware: AMD EPYC 9755 (256 cores, 2.2 TB RAM). KDB-X used "1.5% of available CPU threads and 8% of system memory" while competitors used full hardware [VENDOR; Source: https://medium.com/kx-systems/..., retrieved 2026-05-06].

**Skepticism note on the 1,100× claim:** TSBS is a DevOps/IoT workload, not financial tick; ClickHouse's 1,100× outlier is on query types where kdb+ has a kernel-tight advantage. The methodology disclosure is unusually transparent for a vendor benchmark (KX repo: https://github.com/KxSystems/tsbs), but the resource-asymmetry (capped vs uncapped) is the kind of framing every vendor benchmark uses to flatter the home team.

**Strategic relevance to chili:** **This is the single most important 2025 development in the kdb+ landscape.** KX, under TA Associates PE ownership, opened up a free-for-commercial-use kdb+ with caps that are generous enough for many production small-shop workloads (24 cores / 16 GB is enough for a small market-data desk's HDB) but clipped just below the threshold where you'd need it for a real bank. This means chili's "we are the open-source kdb+" pitch now has to compete with **kdb+ itself, which is now free up to 16 GB**. The strategic implications:

1. **The free-tier ceiling matters.** KDB-X CE caps at 16 GB / 24 cores. Chili has no such cap. Workloads above that ceiling become chili's natural target — they're priced out of kdb+ Insights Enterprise and squeezed out of KDB-X CE.
2. **Source availability still matters.** KDB-X CE is binary-only, no source. Shops that need source-modify access (debug, security audit, custom kernels) cannot use KDB-X CE. Chili can.
3. **Language parity has been unbundled from the engine.** With KDB-X CE free at small scale, the q-language pitch alone no longer differentiates anyone from kdb+. Chili's pepper has to be paired with the OSS / Python-first / Polars-foundation story.

---

### 2.C.6 oK (JohnEarnest)

| Axis | Value |
|---|---|
| License | MIT |
| Language | K5 dialect; JavaScript/HTML implementation |
| Status | Active community interest (646 stars), "buggy, incomplete and occasionally flat-out wrong, but slowly improving over time" — last commit 2026-03-02 [Source: https://github.com/JohnEarnest/ok, retrieved 2026-05-06] |
| Latest release | python-0.4.0 listed in tags (mostly historical; release cadence is irregular) |

**Strategic relevance:** Toy/learning interpreter. Useful as MIT-licensed reference implementation for K5 semantics; useful chili-pepper test corpus source. **Not a competitor at any scale.**

---

### 2.D — Dataframe / lazy-eval engines (chili's architectural cousins)

#### 2.D.1 Polars

| Axis | Value |
|---|---|
| License | MIT |
| Language | Rust + Python + Node + R bindings; expression DSL; SQL frontend |
| Storage | Reads/writes Parquet, CSV, JSON, IPC, Arrow, Delta; not a DB — a query engine + dataframe |
| In-memory | Arrow-native columnar; vectorized + multi-threaded; streaming + lazy execution |
| Marketed use case | "Blazingly fast DataFrames in Rust, Python, Node, R" |
| Latest | py-1.40.1 (2026-04-22); Rust polars 0.53.0 (2026-02-09) [Source: https://github.com/pola-rs/polars/releases] |

**Public benchmarks vs kdb+:** None directly — Polars positions vs pandas, dplyr, DuckDB.

**Strategic relevance to chili:** **Polars is chili's foundation, not its competitor.** Chili is built on Polars 0.53.0 (workspace pin). The relationship is asymmetric: Polars upstream is enormous, well-funded, MIT-licensed, and accelerating. Chili is a vertical specialization on top: DSL surfaces (chili / pepper), persistent partitioned HDB, q-interop via kola, GIL-released `eval`, quantization. **Chili lives or dies by Polars's continuing trajectory.** Strategic implications:

1. **Stay close to Polars upstream.** The pinning policy in CLAUDE.md (golden rule #2) is correctly load-bearing. Polars version drift = chili breakage.
2. **Don't reimplement what Polars does.** When in doubt, file a Polars issue, not a chili workaround.
3. **Recognize that Polars itself could absorb half of chili's roadmap.** Streaming, asof-join, group-by-dynamic, rolling windows are all moving into Polars core. Chili's defensible value is the q-like surface + partitioned-HDB + tick-pattern operations that don't fit in a general-purpose dataframe.

#### 2.D.2 Apache DataFusion

| Axis | Value |
|---|---|
| License | Apache 2.0 |
| Language | SQL + DataFrame API in Rust; embeddable as a query engine |
| Storage | Reads CSV/Parquet/JSON/Avro; storage-agnostic |
| In-memory | Apache Arrow columnar; vectorized; multi-threaded streaming execution |
| Marketed use case | "extensible query engine ... for developers building database and analytic systems" — i.e., a building block, not a product |
| Latest | v54.0.0 + (more recent tags exist; release-page schema differs) [Source: https://github.com/apache/datafusion, retrieved 2026-05-06] |
| Stars | 8.7k |

**Strategic relevance to chili:** Polars and DataFusion are the **two major Rust-Arrow query engines**. They have somewhat different design philosophies (Polars: opinionated, tight Rust ergonomics, Python-first; DataFusion: explicit query-planner, extensible, embedded-in-other-systems). **InfluxDB 3 is built on DataFusion, not Polars.** That divergence is the closest thing to a long-term strategic risk for chili: if DataFusion's planner ecosystem (extensions, custom optimizers, Spark-via-Comet) outpaces Polars on tick-pattern queries, chili is on the wrong substrate. **Currently no public evidence this has happened**, but worth periodically re-evaluating. Today: not a competitor; a parallel-path to evaluate.

#### 2.D.3 Vaex

| Axis | Value |
|---|---|
| License | MIT |
| Language | Python; expression-based |
| Storage | HDF5 / Arrow / Parquet; memory-mapped on-disk |
| In-memory | Out-of-core columnar via mmap; lazy expressions |
| Latest commits | 2026-02-05 (release config fixes); previous substantive commit 2025-10-02 [Source: https://api.github.com/repos/vaexio/vaex/commits, retrieved 2026-05-06] |
| Status | **Slowing** — not formally dormant (8.5k stars, repo not archived) but commit cadence has dropped sharply since 2024. Polars has visibly absorbed Vaex's mindshare. |

**Strategic relevance to chili:** Vaex's "out-of-core dataframes via mmap" was a precursor design pattern; Polars's lazy-engine + streaming has superseded it in practice. **Effectively obsolete as a competitor.** Mention only for completeness.

#### 2.D.4 Dask (brief)

| Axis | Value |
|---|---|
| License | BSD-3-Clause |
| Language | Python |
| Marketed use case | "Parallel computing with Python" — distributed pandas / numpy / scikit-learn |
| Latest | 2026.3.0 (2026-03-19) [Source: https://github.com/dask/dask/releases/latest] |

**Strategic relevance to chili:** Dask competes for the "pandas-but-bigger" mindshare, not for kdb+'s. Some shops do migrate kdb+ workloads to "Dask + Parquet + Spark", but it's a different shape entirely (distributed vs single-node). **Not a direct chili competitor.** Note only as ecosystem context.

---

### 2.E — Other notable mentions

#### 2.E.1 KDB.AI (Kx's vector-search add-on)

| Axis | Value |
|---|---|
| License | Commercial (90-day free trial, then paid) [Source: https://kdb.ai/, retrieved 2026-05-06] |
| Use case | Vector + temporal hybrid search; "the scalable vector database for AI" |
| Indexing methods | HNSW, IVFPQ, Flat, TSC, TSS, qFlat, qHNSW |
| Notable | NVIDIA cuVS GPU acceleration; multimodal AI workloads |

**Strategic relevance to chili:** KDB.AI is **not a kdb+ replacement** — it's KX's defensive market-expansion play to ride the LLM/RAG boom. Strategically what it tells us: KX is following the same strategic recipe as Tiger Data (pivot toward AI/RAG), suggesting that even the kdb+ incumbent feels the pure-tick-analytics market alone isn't sufficient to grow into a TA-Associates-acquisition-justified outcome. Chili can ignore KDB.AI as a head-to-head; it's worth tracking only because it tells us where the incumbent is investing R&D dollars (not in core kdb+ improvements; in adjacent vector-search products).

#### 2.E.2 KX Insights / Insights Enterprise / Insights SDK

| Axis | Value |
|---|---|
| License | Commercial (RAM-capacity-based; opaque pricing — see q_kdb_landscape.md §2.2) |
| Language | q + Python + Java + C# + C++ + Rust + R |
| Storage | kdb+ engine + cloud-native deployment |
| Marketed | "cloud-native, high-performance, scalable analytics platform for real-time analysis" [Source: https://kx.com/products/kdb-insights/, retrieved 2026-05-06] |

**Strategic relevance to chili:** Insights is KX's enterprise k8s pivot. It addresses kdb+'s historical operational weakness (single-node, manual sharding, hand-rolled HA) by wrapping kdb+ in cloud-native plumbing. **Where chili competes against Insights:** at the bottom-of-the-stack engine, on cost. Where chili does *not* compete: enterprise k8s management, multi-tenant SLAs, ISV-vendor procurement. A capital-markets shop comparing chili-vs-Insights is comparing apples to oranges; the procurement decision is dominated by org-level ops capability.

#### 2.E.3 OneTick / OneMarketData (post-Sept-2025 = part of KX)

KX merged with OneMarketData (owner of OneTick) on **2025-09-15** under TA Associates ownership [Source: https://www.businesswire.com/news/home/20250915723209/en/KX-and-OneTick-Merge..., retrieved 2026-05-06]. OneTick is a closed-source competitor that historically targeted the same capital-markets niche as kdb+. **The merger eliminates a major direct competitor and strengthens KX's grip on the financial-tick analytics segment.**

**Strategic relevance to chili:** The KX+OneTick merger is the **second most important 2025 event** in this landscape (KDB-X CE being the first). It signals consolidation: TA Associates is rolling up the closed-source capital-markets tick-analytics market. The space chili plays in is **getting more concentrated at the top**, which (a) increases the attractiveness of an open-source alternative for shops who don't want vendor-lock-in to a TA-rolled-up entity and (b) raises the bar for the level of feature parity chili needs to be a credible alternative.

#### 2.E.4 Snowflake / Databricks (cloud warehouses crossing into timeseries)

Not direct competitors but worth one paragraph: a global fixed-income firm reportedly replicated kdb+ time-series analytics on Snowflake in a 2-month POC, achieving ~60% storage cost reduction [VENDOR-ADJACENT; Source: https://hakkoda.io/resources/snowflake-time-series-data-functions/, retrieved 2026-05-06]. Snowflake added native time-series functions (`ASOF JOIN`, time-bucket aggregations) in 2024. Databricks added similar Delta-Lake-on-Iceberg patterns. **What this tells us:** the cloud warehouse vendors are slowly crossing into kdb+'s territory from above, just as DuckDB / QuestDB / chili approach from below. The middle-of-the-market squeezes kdb+; chili is one player in that squeeze. **Direct competition with Snowflake/Databricks is not realistic for chili** — their TCO-at-petabyte-scale story is entirely different.

#### 2.E.5 DolphinDB (closed-source proprietary, China-origin)

| Axis | Value |
|---|---|
| License | Proprietary; community edition free with caps [Source: https://www.dolphindb.com/downloads/dolphindb_lic.zip, retrieved 2026-05-06] |
| Caps (community) | 2 nodes, 2 cores, 8 GB RAM per node, 20-year license validity |
| Language | DolphinScript — multi-paradigm (imperative + vectorized + functional + SQL) |
| In-memory | Distributed columnar TSDB with stream-processing engine |
| Marketed | "real-time platform for analytics and stream processing" — financial services + IoT in China |
| Public source | No; only API client repos are on GitHub (api-csharp, api-go, api-javascript, all Apache 2.0) |

**Strategic relevance to chili:** DolphinDB is the **closest "kdb+ done differently" closed-source competitor outside the Whitney lineage**. Strong adoption in Chinese financial markets (GF Securities, BYD). Like kdb+ Insights Enterprise, the procurement reality is that this is an enterprise-vendor decision, not a chili-comparable. **Chili's relevance vector:** open-source advantage is decisive for any shop wary of vendor lock-in to a Chinese-origin closed-source platform (geopolitical procurement reality).

#### 2.E.6 TDengine

| Axis | Value |
|---|---|
| License | AGPL-3.0 + commercial dual [Source: https://github.com/taosdata/TDengine, license metadata] |
| Language | SQL-like (TDengine SQL) |
| Storage | Custom timeseries (vnode/block); Parquet ingest; Kafka/Spark connectors |
| Marketed | Industrial IoT timeseries — "10:1 data compression"; edge-to-cloud |
| Latest | v3.4.1.6 (2026-04-30) [Source: https://github.com/taosdata/TDengine/releases/latest] |
| Stars | ~24.8k |

**Strategic relevance to chili:** AGPL-3.0 is hostile to closed-source-product redistribution, which drastically narrows TDengine's enterprise reach in finance. Strong in industrial IoT (China-origin, similar pattern to DolphinDB), not in capital markets. **Largely orthogonal to chili.** Notable as a counter-example to chili's permissive-license bet — TDengine demonstrates that AGPL-3.0 + commercial dual licensing is a viable OSS model for TSDBs but at the cost of finance-sector adoption.

---

## 3. Cross-cutting analysis

### 3.1 The taxonomy of "kdb+ alternative"

Different competitors compete on **different axes**. Understanding the axis is more useful than ranking the projects.

| Axis | What's at stake | Strongest examples | Weakest fit |
|---|---|---|---|
| **Cost-driven displacement** | The kdb+ Insights Enterprise list price is the blocker; the workload doesn't truly need kdb+'s last 10× of latency. | DuckDB, ClickHouse, QuestDB OSS | Shakti (closed-source, similar price model) |
| **Cloud-native displacement** | kdb+ assumes single-node + manual sharding; cloud-native = S3-backed, Parquet, Iceberg, k8s-orchestrated. | ClickHouse Cloud, Snowflake, InfluxDB 3 + IOx, QuestDB Tier 3 | ngn/k variants (single-binary mindset) |
| **Language-driven displacement** | q's right-to-left, glyph-dense syntax is a hiring bottleneck; SQL-first or Python-first are easier to staff. | DuckDB (SQL), Polars (Python), QuestDB (SQL+Postgres wire) | Shakti, k9 — they double down on terse-K |
| **Use-case sub-segmentation** | Don't replace kdb+ on its turf; carve out adjacent timeseries territory (observability, IoT, monitoring). | Prometheus, VictoriaMetrics, InfluxDB 1.x/2.x, TimescaleDB (post-rebrand) | DolphinDB (still tries to be kdb+-shaped) |
| **Whitney-loyalist successors** | Argue from "K is the right model; kdb+ was the imperfect Kx execution; we're building Whitney's next chapter." | Shakti / k9, ngn/k, growler/k | Polars, ClickHouse (rejected the K model entirely) |

**Where does chili fit in this taxonomy?**

Chili sits at an unusual three-axis intersection:

1. **Cost-driven displacement** — chili is MIT/Apache-permissive and has no price tag.
2. **Language-driven displacement (inverted)** — chili offers *both* SQL-adjacent (chili = JS-like) *and* q-like (pepper) surfaces, refusing to fully commit to either side.
3. **Embedded/in-process** — chili is built as a Polars-foundation library with Python bindings (chili-pie) and a CLI binary, not a server. This makes it sister to DuckDB more than to kdb+.

The combination is **uncommon but not unique**. The most honest claim: chili is the only project that **embeds in Python like DuckDB, has a q-like syntax for capital-markets desks (like Shakti / k9 but open-source), and uses Polars + Arrow + Parquet (like InfluxDB 3 / DataFusion) as its substrate**. Each of those three dimensions has stronger single-axis competitors; the bet is that the *intersection* is a credible niche.

**Is the niche real?** Honestly: small. Capital-markets shops who want q-syntax usually already have kdb+. The opening for chili is shops who:
- Are budget-constrained out of kdb+ Insights Enterprise *and* above the 16 GB / 24 core cap of KDB-X CE, **and**
- Want to stay in the Polars + Python + Arrow Rust ecosystem for the rest of their stack, **and**
- Have at least one quant who would prefer pepper over SQL.

That's a thin slice, but a real one — particularly in mid-size quant shops, prop firms, and hedge-fund tech-heavy teams.

### 3.2 The competitive-pressure curve in 2025-2026

The last 18 months reshaped this landscape more than the previous five.

**1. KDB-X Community Edition (Nov 2025) — the biggest event.**
KX, under TA Associates PE ownership, released a free-for-commercial-use kdb+ with caps at 24 cores / 4 secondary threads / 16 GB / 8 connections per process [Source: defconq.tech blog cited above]. Coupled with embedded Parquet + Python + SQL + MCP server modules, KDB-X CE undermines the "open the door to kdb+" pitch that DuckDB/QuestDB/chili used to count on. Now anyone can start with kdb+ for free; the conversion to paid Insights Enterprise is at the *capacity* boundary, not the *zero-dollar* boundary.

**2. KX + OneTick merger (Sept 2025).**
TA Associates is consolidating the closed-source capital-markets analytics segment [Source: https://www.businesswire.com/news/home/20250915723209/en/KX-and-OneTick-Merge..., retrieved 2026-05-06]. This creates a dominant proprietary stack — but consolidation also tends to create open-source counter-movements among customers wary of being locked into a TA-rolled-up vendor. **This dynamic is potentially favorable for chili.**

**3. DuckDB 1.0 (June 2024) → 1.5 (March 2026).**
DuckDB has gone from "experimental laptop OLAP" to "production-stable embedded analytics" with backwards-compatible storage commitment. Corporate adoption (MotherDuck, Databricks dabbling, Snowpark Container Services hosting it) has exploded. **DuckDB is the most likely OSS engine to displace kdb+ for cost-sensitive workloads** that don't need q-syntax.

**4. ClickHouse Cloud + ClickPipes.**
ClickHouse's managed-cloud + native streaming-ingestion (ClickPipes) put cloud-native ingest pipelines at parity with kdb+'s tickerplant pattern, without the q learning curve. Many newer trading shops standardize on ClickHouse + Kafka rather than kdb+ + tickerplant.

**5. Apache Iceberg + Parquet ascendance.**
Iceberg 1.10.x (Dec 2025) and DuckDB/QuestDB/InfluxDB 3 / ClickHouse all converging on **Parquet on object storage with Iceberg metadata** as the open table format [Source: https://github.com/apache/iceberg/releases/latest, retrieved 2026-05-06]. **kdb+'s splayed-table on-disk format is becoming an outlier.** Any shop adopting a data-lake/lakehouse architecture is structurally pulled away from kdb+. Chili's storage layer is also Parquet-based, putting it on the right side of this trend.

**6. Tiger Data rebrand (June 2025) and Kx KDB.AI launch.**
Both incumbents are pivoting toward AI/RAG/agent workloads as their growth narrative, signaling that pure-play timeseries-analytics is a maturing market. Chili's choice to *not* pivot — to stay narrow on capital-markets columnar — is a deliberate counter-bet that the underserved market still has room.

**Strategic implication for chili:** The 2025-2026 landscape is squeezing pure-play TSDBs from above (cloud warehouses crossing in) and below (DuckDB / KDB-X CE at the free-tier bottom). Chili's defensible position has narrowed but not vanished: **above 16 GB**, **needing q-syntax fluency**, **wanting source-modify rights**, **embedded Python use**. That's the four-walled box. Outside that box, every direction has a stronger competitor. Chili's roadmap should explicitly target features that strengthen those walls (large-RAM scale, pepper-language ergonomics, GIL-released Python ops, custom kernel hooks).

---

## 4. Specific implications for chili's roadmap

### 4.1 Which competitor's design choices is chili already mimicking?

| Chili design | Closest analog | Conscious or convergent? |
|---|---|---|
| In-process / single-binary CLI + library | DuckDB | Convergent (both follow MonetDB/X100 lineage via Polars) |
| Polars / Arrow / Parquet substrate | InfluxDB 3 (FDAP) | Different engine (Polars vs DataFusion), same family |
| Python binding via PyO3 | DuckDB Python, Polars Python | Standard pattern |
| Partitioned HDB on disk | kdb+ (splayed tables) | Conscious — chili-mdata explicit kdb+ parity goal |
| `set_column_scale` Int64-quantized prices | mdata-specific (no direct competitor analog) | Bespoke |
| `pepper` q-like surface | kdb+ q | Conscious mimicking |
| `chili` JS-like surface | Idiosyncratic — closest is custom DSLs in DataFusion-extending projects | Bespoke |

**Where this aligns with kdb+ parity:** partitioned HDB, in-memory column store, q-like syntax. **Where it diverges:** open-source license, Python-first embedding, Polars rather than custom kernel, explicit GIL-released-eval semantics for concurrent throughput.

### 4.2 Where is chili's actual differentiation?

**Honest verification of the "uncommon" claim:**

| Property | Chili | DuckDB | QuestDB | KDB-X CE | Shakti | InfluxDB 3 |
|---|---|---|---|---|---|---|
| Open-source (OSI) | Yes (MIT/Apache) | Yes (MIT) | Yes (Apache 2) | No (kc.lic, free-tier only) | No | Yes (MIT/Apache) |
| Rust core | Yes | C++ | Java | C | C | Rust |
| Native Python binding | Yes (PyO3, GIL-released eval) | Yes (DuckDB-Python) | Via psycopg | PyKX (separate product) | No public | Via DataFusion-Python |
| q-like surface | Yes (pepper) | No | No | Yes (q core) | Yes (k9) | No |
| Single-node embedded use | Yes | Yes | No (server) | Yes | Yes | Yes (single-node Core) |
| Polars substrate | Yes | No (own engine) | No | No | No | DataFusion |

**The combination "Open-source + Rust + Polars-substrate + Python binding + q-like DSL" is currently unique to chili.** Each individual axis has a stronger single-axis player; chili's bet is on the combination being more than the sum. The "uncommon" claim survives scrutiny.

**Caveats:**
- The differentiator is feature-bundle, not technical-supremacy. On any single axis, a competitor wins.
- The bundle's audience is small and finance-skewed. Outside capital markets, chili's q-DSL is not valued, and DuckDB or Polars wins.
- The "GIL-released Python eval → 6.10× concurrent throughput" datapoint (CLAUDE.md) is a real lead, but only matters to multi-threaded Python workloads — a narrower audience than "all chili users."

### 4.3 Which competitors should chili monitor as benchmark targets vs ignore?

**Active benchmark targets (chili should keep within ~2× per query class):**
- **DuckDB** — Single-node OLAP. If chili can't match DuckDB on equivalent batch SQL, the in-process pitch fails.
- **QuestDB** — Capital-markets tick patterns specifically (`ASOF JOIN`, `SAMPLE BY`, `HORIZON JOIN`, OHLCV bar). This is chili's home turf.
- **Polars** — Latency floor reference. Chili's own engine *is* Polars; chili-specific overhead vs raw Polars is the parse-cache / partitioned-HDB tax.
- **KDB-X CE** — Aspirational target; if chili can match KDB-X on TSBS-style workloads at-or-below KDB-X's cap, the "open-source alternative" pitch is credible.

**Watch but don't chase:**
- **ClickHouse** — Distributed/cluster-scale; chili won't compete at this shape.
- **InfluxDB 3** — Different audience (observability). Track for FDAP / DataFusion ecosystem developments only.

**Ignore:**
- **Druid, Prometheus, VictoriaMetrics, TimescaleDB, Tiger Data, TDengine, Vaex, Dask, Klong, Snowflake, Databricks** — different audiences, different shapes, or dormant.

### 4.4 Partner vs head-to-head map

| Project | Relationship | Concrete action |
|---|---|---|
| Polars upstream | **Cooperate (existential)** | Stay version-pinned; file issues upstream when chili needs new operators; never reimplement what Polars does |
| Apache Arrow | **Cooperate** | Adopt new IPC formats; track Arrow Flight if streaming becomes a chili topic |
| Apache Parquet ecosystem | **Cooperate** | Track Iceberg integration patterns from DuckDB/InfluxDB 3 |
| growler/k | **Loose cooperation** | Cross-test pepper semantics; no formal partnership needed |
| DuckDB | **Coexist (different audience)** | No need to compete on SQL; cite as "chili for q-shops, DuckDB for SQL shops" |
| QuestDB | **Direct competitor in capital markets** | Match feature parity on `ASOF JOIN`, `SAMPLE BY`, `HORIZON JOIN`, `twap()`, JIT-on-ARM64; benchmark against QuestDB directly |
| KDB-X CE | **Coexist with caveat** | Target the workloads above 16 GB / 24 cores where KDB-X CE caps out. Make this explicit in chili's marketing. |
| KX Insights / kdb+ Enterprise | **Cannot compete on enterprise procurement axis** | Don't try; focus on the bottom-up developer-adoption funnel |
| Shakti / k9 | **Ideological counter, not technical competitor** | Use Shakti's existence as evidence the market wants q-like languages; differentiate on open-source |

---

## 5. Sources index (deduplicated)

- [DuckDB — why_duckdb](https://duckdb.org/why_duckdb)
- [DuckDB releases](https://github.com/duckdb/duckdb/releases)
- [DuckDB 1.0 announcement](https://duckdb.org/2024/06/03/announcing-duckdb-100.html)
- [DuckDB Iceberg extension](https://duckdb.org/docs/current/core_extensions/iceberg/overview.html)
- [ClickHouse intro](https://clickhouse.com/docs/en/intro)
- [ClickHouse releases](https://github.com/ClickHouse/ClickHouse/releases/latest)
- [ClickBench](https://benchmark.clickhouse.com/)
- [QuestDB homepage](https://questdb.com/)
- [QuestDB releases](https://github.com/questdb/questdb/releases/latest)
- [QuestDB vs kdb+ comparison page](https://questdb.com/compare/questdb-vs-kdb/)
- [Apache Druid](https://druid.apache.org/)
- [Apache Druid releases](https://github.com/apache/druid/releases/latest)
- [InfluxDB product page](https://www.influxdata.com/products/influxdb/)
- [InfluxDB releases](https://github.com/influxdata/influxdb/releases)
- [TimescaleDB GitHub](https://github.com/timescale/timescaledb)
- [Tiger Data homepage](https://www.tigerdata.com/)
- [Timescale → Tiger Data rebrand](https://www.tigerdata.com/blog/timescale-becomes-tigerdata)
- [VictoriaMetrics homepage](https://victoriametrics.com/)
- [VictoriaMetrics releases](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/latest)
- [Prometheus homepage](https://prometheus.io/)
- [Prometheus releases](https://github.com/prometheus/prometheus/releases/latest)
- [TDengine releases](https://github.com/taosdata/TDengine/releases/latest)
- [DolphinDB homepage](https://www.dolphindb.com/)
- [DolphinDB community license](https://docs.dolphindb.com/en/Functions/l/license.html)
- [ngn/k Codeberg](https://codeberg.org/ngn/k)
- [growler/k Codeberg](https://codeberg.org/growler/k)
- [Klong (Nils M. Holm)](https://t3x.org/klong/)
- [oK GitHub](https://github.com/JohnEarnest/ok)
- [KDB-X CE GA blog](https://kx.com/blog/kdb-x-ga-built-for-developers/)
- [KDB-X release notes](https://docs.kx.com/public-preview/kdb-x/Releases/release-notes-latest.htm)
- [KDB-X CE caps blog (defconQ)](https://www.defconq.tech/blog/From%20Elite%20to%20Everyone%20-%20KX%20Community%20Edition%20Breaks%20Loose)
- [KX TSBS benchmark on Medium](https://medium.com/kx-systems/benchmarking-kdb-x-vs-questdb-clickhouse-timescaledb-and-influxdb-with-tsbs-2090f4533be0)
- [KxSystems/kdb GitHub (companion files)](https://github.com/KxSystems/kdb)
- [KxSystems/tsbs GitHub (TSBS fork)](https://github.com/KxSystems/tsbs)
- [KX + OneTick merger announcement](https://www.businesswire.com/news/home/20250915723209/en/KX-and-OneTick-Merge-to-Unite-Capital-Markets-Data-Analytics-AI-and-Surveillance-on-One-Platform)
- [KDB.AI homepage](https://kdb.ai/)
- [KX kdb Insights product](https://kx.com/products/kdb-insights/)
- [TimeStored kdb alternatives index](https://www.timestored.com/kdb-guides/kdb-alternatives)
- [TimeStored TSDB benchmark index](https://www.timestored.com/data/time-series-database-benchmarks)
- [opensourcealternative.to kdb+](https://opensourcealternative.to/alternativesto/kdb+)
- [Polars releases](https://github.com/pola-rs/polars/releases)
- [Apache DataFusion repo](https://github.com/apache/datafusion)
- [Vaex GitHub](https://github.com/vaexio/vaex)
- [Dask releases](https://github.com/dask/dask/releases/latest)
- [Apache Iceberg releases](https://github.com/apache/iceberg/releases/latest)
- [HN thread on kdb+ benchmark prohibition](https://news.ycombinator.com/item?id=20762564)
- [DeWitt clause essay](https://dwheeler.com/essays/dewitt-clause.html)
- [Snowflake time-series article](https://hakkoda.io/resources/snowflake-time-series-data-functions/)
- [Kozloski Medium 7-DB benchmark](https://medium.com/@ev_kozloski/timeseries-databases-performance-testing-7-alternatives-56a3415e6e9e)

---

## 6. Open questions for the next research wave

These are gaps this catalog could not close — flagged for follow-up:

1. **DuckDB vs kdb+ direct benchmark.** No public head-to-head exists. Worth attempting one ourselves (chili can run both and publish a methodology-disclosed comparison — chili is permissively licensed so we can publish both endpoints' numbers without DeWitt-clause exposure on chili's side; kdb+'s clause is the reason public numbers are scarce).
2. **KDB-X CE adoption metrics.** No public install/download numbers from KX yet; would clarify how disruptive the free tier really is.
3. **Shakti's actual customer base.** The Shakti deep-dive doc should attempt this; market presence is genuinely hard to assess from public materials.
4. **DolphinDB outside China.** No clear data on Western adoption; possibly significant in Hong Kong / Singapore desks.
5. **KDB.AI traction.** KX's PR is loud but customer-count and ARR for KDB.AI are unpublished; would help judge whether KX's AI pivot is a real threat to chili-aligned shops or PR theater.
6. **Tiger Data's actual revenue split.** Their claim that "majority of cloud workloads are no longer time-series" is publicly stated but not broken out in detail. If true, the pure TSDB market is smaller than chili's positioning assumes.
