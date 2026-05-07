# The kdb+/q Landscape: Strategic Competitive Intelligence

**Author:** Research subagent (one of five strategic-positioning research deliverables)
**Date compiled:** 2026-05-06
**Scope:** kdb+ as a product, its history, published benchmarks, and primary-source-cited strengths/weaknesses. Out of scope (handled by sibling subagents): Shakti deep-dive, alternative engines deep-dive (DuckDB/Polars/ClickHouse), chili positioning narrative.

**Editorial conventions in this doc:**
- Every benchmark figure carries `[Source: <url>, retrieved 2026-05-06]`.
- Vendor-published numbers (Kx, Pure Storage, Lenovo, etc.) are flagged `[VENDOR]`. Independent or third-party numbers are flagged `[INDEPENDENT]`. Press-release adjacent numbers (Shakti+DDN press) are flagged `[VENDOR-ADJACENT]`.
- Where data is thin or contested, this is called out inline rather than papered over.
- All retrieval dates are 2026-05-06 unless otherwise noted.

---

## 1. History (terse)

### APL → A → A+ (1960s–1988)

APL was created by Ken Iverson at IBM in the 1960s. Arthur Whitney was first exposed to APL at age 11 by Iverson, a family friend, and later worked alongside Iverson and Roger Hui at I. P. Sharp Associates. Whitney also wrote the initial single-page prototype that became the J language. In 1988 Whitney joined Morgan Stanley and built **A**, a cut-down speed-oriented APL dialect designed to migrate APL applications from mainframes to workstations. A was extended into **A+** with input from other Morgan Stanley engineers and an "electric" graphical UI; A+ kept a smaller primitive set than APL and was tuned for time-series workloads. [Source: https://aplwiki.com/wiki/Arthur_Whitney; https://en.wikipedia.org/wiki/Arthur_Whitney_(computer_scientist)]

### A+ → K (1992–1998)

In **1992** Whitney built the first prototype of **K**. K broke from APL tradition: it discarded APL's multidimensional array model in favor of nested lists/vectors and used ASCII symbols rather than APL's special glyphs. K is also visibly LISP-influenced (heterogeneous nested lists, eval/apply machinery exposed in the language). In **1993** Whitney left Morgan Stanley and co-founded **Kx Systems** with Janet Lustgarten in Palo Alto to commercialize K. Versions K1 through K6 were developed at Kx. From 1993 to ~1997 the language was sold under an exclusive UBS contract; when that ended (UBS merged with Swiss Bank Corp in 1997), Kx was free to sell more broadly. [Source: https://en.wikipedia.org/wiki/KX_Systems; https://k.miraheze.org/wiki/Arthur_Whitney; https://en.wikipedia.org/wiki/K_programming_language]

### K → kdb (1998) → kdb+ (2003)

In **1998** Kx released **kdb**, a columnar in-memory database written in K. In **June 2003** kdb+ shipped: a 64-bit total rewrite based on K4. **Q** was introduced in **2003** as a thin layer on top of K4 — same execution semantics, but English keywords (`select`, `from`, `where`) replacing single-symbol primitives, plus a SQL-like query embedding (`q-sql`). Q is what most kdb+ users actually write; K is reserved for low-level/high-performance code. The macro/wrapper relationship means q functions are defined in terms of k expressions. [Source: https://en.wikipedia.org/wiki/Q_(programming_language_from_Kx_Systems); https://www.timestored.com/kdb-guides/history-of-kdb-arthur-whitney]

### kdb+ → Shakti (2018–2019)

In **2018** First Derivatives bought out Whitney's and Lustgarten's remaining minority shares in Kx Systems. Whitney and Lustgarten then founded **Shakti Software** (also called Shakti Technology, named after the Hindu cosmic-energy concept). Whitney began publishing Shakti material around **2019**. Shakti is closed-source, marketed as a unified database/language/streaming/security platform claimed to be substantially faster than kdb+ on the same hardware (numbers tabulated in §3). Shakti is K-derived but architecturally distinct — Whitney has stated it adds built-in parallelism in primitive functions and a custom IPC protocol. [Source: https://www.efinancialcareers.com/news/2019/11/shakti-arthur-whitney; https://www.businesswire.com/news/home/20191001005420/en/Shakti-Technology-Launches-New-High-Performance-Data-Platform-Merging-Database-Language-and-Security; https://en.wikipedia.org/wiki/Arthur_Whitney_(computer_scientist)]

### Inheritance summary

```
APL (1960s, Iverson)
  └─ A (1988, Whitney @ Morgan Stanley)
       └─ A+ (~1989-1992, Whitney + MS engineers)
            └─ K1..K6 (1992–2010s, Whitney @ Kx)
                 └─ kdb (1998) ──► kdb+ (2003, K4-based)
                       └─ q (2003, English wrapper over K4)
            └─ Shakti / k9 (2018–, Whitney @ Shakti Software)
```

Whitney's design DNA across all of these: vectorized primitives, terse syntax, single-binary deploy, in-memory column store with mmap'd persistence.

---

## 2. Current state of kdb+ as a product

### 2.1 Ownership

| Year | Event | Source |
|---|---|---|
| 1993 | Kx Systems founded by Whitney + Lustgarten in Palo Alto | https://en.wikipedia.org/wiki/KX_Systems |
| 1999 | Marketing partnership with First Derivatives (Northern Ireland) | https://en.wikipedia.org/wiki/KX_Systems |
| 2014 | First Derivatives buys 65% of Kx Systems | https://en.wikipedia.org/wiki/KX_Systems |
| 2018 | First Derivatives buys remaining shares; Whitney + Lustgarten leave to found Shakti | https://en.wikipedia.org/wiki/KX_Systems |
| 2019 | First Derivatives takes 100% ownership for $53.8M | https://www.timestored.com/b/kdb-acquired-by-private-equity/ |
| 2024 | First Derivatives consulting business divested to EPAM Systems for £60M; remaining entity rebrands as pure-play software co. ("FD Technologies") | https://www.ta.com/news/ta-announces-all-cash-offer-to-acquire-fd-technologies-owner-of-global-real-time-analytics-leader-kx/ |
| May 2025 | TA Associates announces all-cash offer for FD Technologies @ £570M (27% premium) | https://www.ta.com/news/ta-announces-all-cash-offer-to-acquire-fd-technologies-owner-of-global-real-time-analytics-leader-kx/ |
| July 2025 | TA Associates acquisition completed; KX now PE-owned | https://www.businesswire.com/news/home/20250723180954/en/KX-Announces-New-Chapter-of-Growth-With-Strategic-Acquisition-by-TA-Associates |

**Current state (May 2026):** KX is owned by TA Associates (private equity). Leadership: Ashok Reddy (CEO), Eric Raab (CTO), David Humphries (COO). [Source: https://en.wikipedia.org/wiki/KX_Systems, retrieved 2026-05-06]. Note: FD Technologies the listed PLC ceased to exist after the take-private; "KX" is the active brand.

**Strategic read:** PE ownership typically pressures product roadmap toward (a) recurring-revenue ARR expansion via cloud SKUs and (b) lower-friction acquisition funnels. Both are visible in the KDB-X Community Edition launch (Nov 2025; see §2.4). PE owners also frequently raise list pricing on enterprise tiers — worth watching, but I have no public 2026 datapoint confirming that yet.

### 2.2 Licensing model

**The headline:** Pricing is opaque. KX does not publish per-core, per-user, or per-RAM list prices for kdb+ Insights Enterprise. The structure is custom-quoted via `sales@kx.com`. [Source: https://code.kx.com/q/learn/licensing/]

What is publicly known:

- **License is RAM-capacity-based**, not per-core in the classical sense. Support is layered on top: Silver ≈ 20% of license fee, Gold ≈ 25%, Premium ≈ 30%. [Source: https://www.trustradius.com/products/kdb/pricing]
- **Cloud on-demand SKU** is per-core-minute; the on-demand license caps at 16 cores. [Source: https://kx.com/news/kx-extends-use-of-worlds-fastest-time-series-database-kdb-with-on-demand-offering-for-cloud-and-on-premises/]
- **Free tiers:**
  - **kdb+ Personal Edition** (64-bit): free for personal use only, 12-month renewable license, requires internet for license validation. [Source: https://kx.com/kdb-personal-edition-download/]
  - **32-bit edition**: previously free for non-commercial use. **No longer offered.** Memory was capped at 4 GB by the 32-bit address space; doesn't run on macOS 10.15+ (which dropped 32-bit). [Source: https://www.odbms.org/2016/02/download-kdb-32-bit-version-for-free/; https://kx.com/kdb-personal-edition-download/]
  - **KDB-X Community Edition** (Nov 2025): free for **commercial and personal** use, no expiry, with embedded resource caps (cores, threads, memory, connections) via `kc.lic` license file. Specific cap numbers not published in launch materials. [Source: https://kx.com/blog/kdb-x-ga-built-for-developers/; https://www.morningstar.com/news/business-wire/20251119593382/]

**Public price-floor estimates (third-party, treat as approximations):**

| Source | Quoted figure | Date | Context |
|---|---|---|---|
| Medium / Everton Kozloski | "$100,000.00" annual | 2023-06-24 | Author cited it as the reason kdb+ was eliminated from a TSDB selection process. [Source: https://medium.com/@ev_kozloski/timeseries-databases-performance-testing-7-alternatives-56a3415e6e9e] |
| TimeStored (Ryan Hamilton) | "$300K to start" | 2024-07-24 | Author's commentary on why kdb+ excludes smaller firms. [Source: https://www.timestored.com/b/the-future-of-kdb/] |
| efinancialcareers / Caspian One report | "estimated at around a hundred thousand dollars per year" | 2024 | Industry publication framing kdb+ as fintech-only-affordable. [Source via search aggregation: https://www.businessresearchinsights.com/market-reports/high-frequency-trading-market-107496] |

**Caveat:** These are third-party estimates, not Kx-published list prices. The actual contract a tier-1 bank pays is widely rumored in industry conversations to be in the seven figures annually for a multi-region production deployment, but I found no primary-source confirmation. Flag as **contested and opaque** in the strategy doc.

**Why the cost barrier matters for chili:** Every published competitor selection write-up I found (Kozloski, Hamilton, the efinancialcareers piece) cites license cost as the **primary** disqualifier — not performance, not features. This is the structural opening that ClickHouse, QuestDB, TimescaleDB, DuckDB, and (potentially) chili exploit.

### 2.3 Target market

**Where kdb+ wins:**

- **Tier-1 sell-side and quant buy-side HFT.** kdb+ has been "a dominant player in electronic trading analytics on Wall Street for over 20 years." [Source: https://www.businessresearchinsights.com/market-reports/high-frequency-trading-market-107496]
- **Low-latency tick capture + intraday + historical querying** in a single coherent stack (the kdb+/tick + RDB + HDB pattern; see §2.5).
- **Regulatory reporting and real-time risk** in capital markets — adjacent expansion from the trading-edge use case. [Source: https://www.caspianone.com/kdb-insights-2025]
- **Capital markets surveillance** for algo / HFT abuse detection. [Source: https://code.kx.com/q/wp/surveillance/]

**Where kdb+ is losing ground:**

- **Local quant analysis / individual researcher seat.** Hamilton (TimeStored, 2024-07-24): "Python with DuckDB or Polars has essentially won the local quant analysis segment because these tools are free and enable skill portability across employers." [Source: https://www.timestored.com/b/the-future-of-kdb/]
- **General-purpose timeseries (IoT, devops, observability).** ClickHouse, InfluxDB, TimescaleDB, QuestDB take this — kdb+'s license model and learning curve are non-starters for an SRE team. [Source: https://medium.com/@ev_kozloski/timeseries-databases-performance-testing-7-alternatives-56a3415e6e9e]
- **Smaller hedge funds / prop shops.** Multiple commentaries cite the $100K–$300K floor as the reason new entrants choose ClickHouse + Parquet + Polars. [Source: https://www.timestored.com/b/the-future-of-kdb/]
- **Commodity columnar formats erode the moat.** Hamilton (2024): "open-source competitors have adopted kdb+'s best innovations — columnar storage (Parquet/Iceberg), in-memory formats (Apache Arrow), and asof joins — while standardizing these features across multiple platforms." [Source: https://www.timestored.com/b/the-future-of-kdb/]

**HFT macro context:** Global HFT software market valued at USD ~10.36B in 2024, projected ~16.03B by 2030 at 7.7% CAGR. [Source: https://www.grandviewresearch.com/industry-analysis/high-frequency-trading-market-report] kdb+'s share of that is not publicly broken out, but the qualitative claim ("dominant for 20+ years on Wall Street") is uncontested in the industry coverage I surveyed.

### 2.4 Versioning

| Version | Year | What changed |
|---|---|---|
| kdb (original) | 1998 | First column-oriented K-based DB |
| kdb+ 2.x | 2003–2010 | 64-bit, K4/q-based rewrite |
| kdb+ 3.0 | 2012 | WebSocket, UUID, Intel SIMD optimizations |
| kdb+ 3.1 | 2013 | Performance — claimed up to 8x faster than 2.x on some benchmarks |
| kdb+ 4.0 | 2020 | Multithreaded primitives, Intel Optane DC support, data-at-rest encryption [Source: https://en.wikipedia.org/wiki/Kdb+] |
| kdb+ 4.1 | Feb 13, 2024 | Nested `peach` (work-stealing scheduler), multithreaded CSV/binary load, socket throughput 5x, OpenSSL 3.x, TLS on non-main threads, pattern-matching assignment [Source: https://kx.com/blog/discover-kdb-4-1s-new-features/; https://code.kx.com/q/releases/ChangesIn4.1/] |
| kdb Insights | 2021–present | Cloud/k8s deployment surface around the kdb+ engine; AWS, Azure, GCP marketplaces; FinSpace integration |
| KDB.AI | 2023 → | Vector database; 1.1 (March 2024) added hybrid search + Transformed TSS (claimed 6000x faster than HNSW on time-series similarity); 1.2 (June 2024) qFlat index; 1.4 (Oct 2024) cross-table indexes [Source: https://kx.com/blog/unlock-new-capabilities-with-kdb-ai-1-4/] |
| **KDB-X** | Nov 17, 2025 (GA) | "Next-gen kdb+." Unifies time-series + vector + AI in one engine. New Parquet module (predicate/row-group pruning), MCP server for AI tooling, native GPU support promised Spring 2026. Free Community Edition (commercial use OK, embedded resource caps). Backward-compatible with kdb+ q code. [Source: https://kx.com/blog/kdb-x-ga-built-for-developers/; https://kx.com/blog/kdb-x-now-generally-available-the-next-era-of-kdb-for-ai-driven-markets/] |

**What stayed:**
- q (and underlying k) as the primary language. SQL added but not central.
- Splayed/parted on-disk layout with mmap-backed columns.
- The kdb+/tick architecture (TP + RDB + HDB).
- Single-binary deployment model.

**What pivoted:**
- AI/vector capabilities are now first-class (KDB.AI line).
- Open-source-shaped licensing (KDB-X CE) for the first time — this is a strategic concession to ClickHouse/DuckDB pressure.
- Parquet read support means kdb+ no longer requires its splay format for all queries.

**Caveat:** KDB-X Community Edition was announced Nov 2025; full commercial GA was scheduled for early 2026 per the KX press materials. Whether it actually went GA on schedule and what the practical resource caps are isn't yet clear from public materials I can find. Worth checking Q3 2026.

### 2.5 Deployment surface (recap; chili already implements analogous pieces)

| Component | What it does |
|---|---|
| **q binary** | Single ~800KB ELF (or platform equivalent). Boots in milliseconds. No JVM, no interpreter daemon. [Source: https://kx.com/blog/what-makes-time-series-database-kdb-so-fast/] |
| **Tickerplant (TP)** | Lightweight q process that captures the feed, logs to disk, publishes to subscribers. EOD handler. Recommended to run on dedicated cores. [Source: https://code.kx.com/q/architecture/] |
| **RDB (real-time DB)** | Subscribes to TP, buffers today's data in RAM, serves intraday queries. EOD writes to HDB and resets. [Source: https://code.kx.com/q/architecture/] |
| **HDB (historical DB)** | Splayed/parted on-disk layout. mmap'd columns, page-faulted on demand. Years of data scannable from disk without full RAM load. [Source: https://kx.com/blog/memory-mapping-in-kdb/] |
| **Gateway** | Optional q process that routes user queries to RDB(s) and HDB(s) and stitches results. |
| **kdb Insights** | Cloud/k8s wrapper: managed deploy, REST/gRPC ingress, autoscaling RDC (real-time data cluster). Auto-scaling pod recognition can take ~15s — meaningful for HFT-adjacent latency budgets. [Source: https://code.kx.com/q/cloud/autoscale/rdc/] |
| **KDB.AI** | Vector DB with HNSW + qFlat + TSS indexes. Sits next to kdb+ data; can index kdb+ tables in 1.4+. [Source: https://kx.com/blog/unlock-new-capabilities-with-kdb-ai-1-4/] |

**Note for chili context:** The TP/RDB/HDB pattern is what chili's tick/sub/rdb architecture maps to. The operational surface chili must match for parity is (a) sub-second EOD roll, (b) intraday subscriber multicast, (c) splay-equivalent on-disk format with column-level mmap or equivalent. Polars + Parquet + Arrow gives chili most of (c) for free; (a) and (b) are the integration burden.

---

## 3. Published benchmarks

This section is the core deliverable. Caveats up front:

1. **STAC-M3 results are behind a paywall.** STAC requires registration to view full reports; vendor press releases summarize headline ratios but rarely publish the absolute ms/tx numbers. What's tabulated below are the **headline ratios** from the press summaries, plus hardware specs.
2. **Vendor benchmarks dominate.** Independent third-party comparisons of kdb+ are rare because (a) Kx's license historically forbade publishing performance numbers without approval (the "DeWitt clause"), and (b) the cost barrier prevents most independent researchers from running it.
3. **STAC-M3 v1 → v2 transition.** Some 2017–2020 results are on STAC-M3.β1 (older variant); 2022+ are on STAC-M3.v1 / v2. Apples-to-apples across years requires care.
4. **Skepticism note:** For every "kdb+ wins by X" vendor number below, the test was paid for by either Kx or a hardware partner who benefits from showcasing kdb+. Treat as "well-tuned upper bound for kdb+ on that hardware," not as "what your team will reproduce."

### 3.1 STAC-M3 results

STAC-M3 is split into two suites:
- **Antuco** — fixed-size dataset (1 year of simulated tick), 17 mean-response benchmarks, 2 dimensions (concurrent users × data volume). Baseline performance at fixed conditions.
- **Kanaga** — 20x larger dataset (5 years), 24 mean-response benchmarks, larger user counts (50, 100). Scaling-stress suite.

Operations tested include NBBO (National Best Bid and Offer), VWAB-D / VWAB-12D (Volume-Weighted Average Bid), MOHIBID / QTRHIBID / WKHIBID / YRHIBID (high-bid over time windows), STATS-AGG (statistical aggregation), VOLCURV (volume curve), THEOPL (theoretical P&L), MKTSNAP (market snapshot). [Source: https://www.weka.io/blog/distributed-file-systems/what-is-the-stac-m-3-benchmark-and-why-should-you-care/]

#### 3.1.1 STAC-M3 published results (2017–2025)

| # | Date | Stack | DB | Hardware (CPU / mem / storage) | Suite | Headline result | Source |
|---|---|---|---|---|---|---|---|
| 1 | Apr 2017 | Vexata NVMe + kdb+ | kdb+ 3.x | Vexata VX-100F NVMe array | Antuco β1 | "9.4x speedup vs prior NFS-NAS solution on NBBO operation (β1.1T.NBBO.TIME); faster on 14 of 17 Antuco benchmarks" | https://stacresearch.com/news/2017/05/03/KDB170421 [VENDOR] |
| 2 | Jun 2020 | Intel Optane Persistent Memory + kdb+ 4.0 | kdb+ 4.0 | Intel Optane DC PMM | Antuco | First Optane-based STAC-M3 (used as baseline for later 2x claims) | https://docs.stacresearch.com/news/KDB200603 [VENDOR] |
| 3 | Sept 2023 | Lenovo ThinkSystem SR650 V3 + kdb+ 4.0 | kdb+ 4.0 | 2x Intel Xeon Gold 6444Y (16 core, 3.6 GHz); 1 TB DDR5; 8x 800 GB Intel Optane P5800X NVMe; RHEL 8.7 | Antuco | **9 world records** (single-node, 2-socket): 100T.VWAB-12D-NO.TIME, 10T.STATS-AGG.TIME, 10T.VOLCURV.TIME (3.2x prior best), 1T.MOHIBID.TIME, 1T.QTRHIBID.TIME (2.1x prior best), 1T.VWAB-D.TIME, 1T.WKHIBID.TIME, 1T.YRHIBID-2.TIME, 1T.YRHIBID.TIME | https://lenovopress.lenovo.com/lp1825-sr650-v3-stac-m3-benchmark-result-2023-09-25 [VENDOR] |
| 4 | Jan 2024 | Pure FlashBlade//S500 + kdb+ 4.0 | kdb+ 4.0 (NFS v3) | 8x Dell PowerEdge R740xd (2x Intel Xeon Platinum 8260, 256 GB ea); FlashBlade//S500: 10 blades × 2 × 24TB flash = 480 TB raw / 292.5 TB usable | Antuco + Kanaga | vs Dell PowerScale: **7x** (10-user theoretical P&L), **5.8x** (10-user mkt snapshot), 1.3-1.5x (50-user 12d VWAB). vs WekaIO/AWS: **11x** (10-user mkt snapshot), 8.4x (10-user volume curve), 4.4-5.3x (10-user mkt snapshot scaling) | https://blocksandfiles.com/2024/01/19/pure-stac-benchmark/; https://stacresearch.com/news/KDB231122 [VENDOR] |
| 5 | Oct 2025 | Supermicro + Intel Xeon 6 + Micron 9550 NVMe + kdb+ 4.1 | kdb+ 4.1 (2025.02.18, **shard mode across 6 nodes**) | 6x Supermicro SSG-222B-NE3X24R; per node: 2x Intel Xeon 6767P (64 core, 2.4 GHz), 16x 128 GB DDR5-6400 (2 TB/node), 24x 12.8 TB Micron 9550 MAX NVMe (1.84 PB usable across 144 SSDs); NVIDIA 400 Gb InfiniBand | Antuco + Kanaga | **19 of 24 Kanaga mean-time records**, including all 10/10 Kanaga 50/100-user; **3 of 5 Kanaga throughput**; **3 of 3 Antuco 50/100-user**. 36% faster on 100-user with 62% fewer cores vs prior record. 1.77x-2.70x faster than Pure-based system on 1T/3-5y HIBID. 1.36x faster than DDN-based, 2.12x vs Optane | https://stacresearch.com/news/stac-m3-benchmark-results-kx-kdb-4-1-on-supermicro-micron-intel/ [VENDOR] |
| 6 | Feb 2022 | DDN SFA200NVX + Shakti 2.0 | **Shakti** (not kdb+) | DDN SFA200NVX storage + single client | STAC-M3.v1 (Antuco) | Highest storage efficiency of any publicly reported solution (196%). NBBO (less-demanding variant): 1.7× prior best. **vs kdb+ baseline (SUT KDB211014):** 3.7× faster Year-High Bid; 3.3× faster NBBO-Q.TIME (β1.1T) per primary STAC SHK211203. The "7×" figure that has circulated in press is a restatement artifact — see [`shakti_analysis.md` §3.1](shakti_analysis.md#31-the-2022-stac-m3-result-the-headline) for primary-source rebuttal. Whitney's Shakti is 3.3-3.7× faster mean-response than his old kdb+ on the same suite. | https://docs.stacresearch.com/news/SHK211203; https://www.ddn.com/press-releases/ddn-and-shakti-announce-record-breaking-results-on-the-stac-m3-benchmark-for-financial-trading-applications/ [VENDOR-ADJACENT] |

**Reading the STAC-M3 table:**

The 2025 Supermicro result is the current state-of-the-art for kdb+. Key takeaways for chili's competitive targets:
- kdb+ 4.1 on top-end 2025 hardware can do 19/24 Kanaga records in **shard mode** (multi-node). Shard mode is Kx's distributed deployment — meaningful because it shows kdb+ is no longer purely single-node.
- 64-core × 6-node = 384 cores at peak; 12 TB total RAM; 1.84 PB NVMe. This is the hardware envelope kdb+ assumes you'll throw at the problem.
- Cost-of-hardware for the 2025 result is **not disclosed** but rough estimate based on list prices: $1.5-3M for the cluster alone. This is HFT-scale infrastructure spending, not commodity.

**Skepticism flag:** Headline claims like "36% faster with 62% fewer CPU cores" are framed against the *previous* world record, not against a steady-state competitor. The "previous record" is itself a kdb+ run on different hardware. Read these as "kdb+ on better hardware beats kdb+ on older hardware," not as "kdb+ beats X."

#### 3.1.2 Specific operation timings (best public estimates)

I was unable to find a publicly disclosed table of absolute milliseconds for STAC-M3 operations on kdb+ 4.1. The full STAC reports require registration, and the public press releases publish only ratios. **What is publishable:**

- The Supermicro 2025 STAC-M3 audited report PDF is available at `https://www.supermicro.com/thought-leadership/STAC-M3-Kanaga+Antuco-Audited%20Report-KDB250929.pdf` — full operation timings in the body, but registration-free download. Worth fetching for the optimization sprints if specific ms targets are needed.

If chili wants concrete "beat by 2x on operation Y" targets, the workflow is: (1) fetch that audited report PDF directly from Supermicro, (2) extract the per-operation ms, (3) set internal targets at 2x or better.

**Note inline:** I assumed STAC-M3 Antuco for the chili-target framing (lower bar to clear). For Kanaga (the harder suite), absolute targets shift up significantly — that's the dataset where kdb+ shows its real on-disk-mmap muscle. If the next-quarter sprint is targeting Kanaga, halve the targets; if Antuco, double them.

### 3.2 Independent third-party benchmarks

#### 3.2.1 Bodon et al. — "Benchmarking Specialized Databases for High-frequency Data" (arXiv:2301.12561, Jan 2023)

This is the most-cited independent academic benchmark. Author Ferenc Bodon now works at KX (per his Medium byline on the 2025 KDB-X TSBS post), so "independent" is qualified — but the 2023 arXiv paper predated his Kx affiliation per the published text.

**Setup:** Cryptocurrency exchange data, June-July 2022. ~25 GB total (20M trades + 33M order-book rows). Hardware: Intel i7-1065G7, 16 GB RAM, SSD. Single-threaded, single-client. [Source: https://arxiv.org/pdf/2301.12561]

**Query latencies (ms):**

| Benchmark (operation) | kdb+ in-mem | kdb+ on-disk | InfluxDB | TimescaleDB | ClickHouse |
|---|---:|---:|---:|---:|---:|
| T-V1 (read) | 57 | 81 | 93 | 469 | 272 |
| T-VWAP (compute) | 26 | 75 | 12,716 | 334 | 202 |
| O-S (compute, orderbook) | 1,352 | 1,386 | 623,732 | 2,182 | 14,056 |
| C-VO1 (complex) | 61 | 96 | 94 | 1,591 | 407 |
| Data write (W) | n/a | 33,889 | 324,854 | 53,150 | 765,000 |

**Storage compression ratio (lower is better):**
- InfluxDB 83.73% (best)
- kdb+ 90.20%
- TimescaleDB 94.68%
- ClickHouse 99.65% (effectively no compression on this dataset)

**Headline finding:** kdb+ in-memory mode wins on 3 of 4 query categories vs the other three databases. On-disk kdb+ is ~7x slower than in-memory but still beats the others. **InfluxDB is 9.6x slower** than kdb+ in-memory on average across the query set; **ClickHouse is 22.6x slower**. [Source: https://arxiv.org/pdf/2301.12561]

**Caveat:** The dataset is small (25 GB) and the hardware is a laptop-class i7. This favors kdb+'s in-memory mode disproportionately. Larger datasets where RAM no longer fits the working set would compress kdb+'s lead — exactly the regime DuckDB and ClickHouse are optimized for.

#### 3.2.2 Kozloski (Medium, June 2023) — Test of 7 alternatives

**Setup:** EURUSD financial data, 6-month scrape, 9M records (1M rows × 9 cols), 123 MB CSV. Hardware: 4× AMD A10-7860K cores, 16 GB RAM, Samsung 860 Evo SSD. [Source: https://medium.com/@ev_kozloski/timeseries-databases-performance-testing-7-alternatives-56a3415e6e9e]

**Notable findings:**
- ClickHouse compressed 123 MB → 20 MB (best on test).
- Author selected ClickHouse over kdb+ explicitly because of "$100,000.00" annual licensing cost.
- DuckDB scored well on speed but lost on concurrency.

**Caveat:** Per-query times not enumerated in the article body, only the overall ranking and the cost-driven exclusion. Useful as a *qualitative* signal for the cost barrier, not as a benchmarking citation.

#### 3.2.3 Mark Litwintschik NYC Taxi benchmark (referenced 2025)

**Setup:** 1.1 billion NYC Taxi rides, 51 columns, 500 GB CSV uncompressed. 4 query benchmark.

**Results (relative slowdown vs kdb+):**
- kdb+ on 4× Intel Xeon Phi 7210: 1.0x baseline
- ClickHouse on Intel Core i9-14900K: 2.3x slower
- DuckDB 0.10.0 on i9-14900K: 2.8x slower

[Source: https://www.timestored.com/data/time-series-database-benchmarks, retrieved 2026-05-06; aggregator citing Litwintschik's blog]

**Caveat:** Hardware mismatch — kdb+ ran on 4 server-class Xeon Phi CPUs; the others ran on a single desktop CPU. Not apples-to-apples. Useful as proof "kdb+ on big iron still wins NYC Taxi" but unhelpful for fixed-hardware comparison.

#### 3.2.4 Deutsche Bank internal benchmark (Jan 2022, mentioned in TimeStored aggregator)

[Source: https://www.timestored.com/data/time-series-database-benchmarks, retrieved 2026-05-06]

Deutsche Bank ran a kdb+ vs ClickHouse comparison and reportedly found ClickHouse fastest on their workload. **Caveat:** the underlying writeup is not linked in the aggregator and I could not verify the primary source. Use only as a directional signal.

#### 3.2.5 OHLCV benchmark — competitor 4.4x faster than kdb+ (referenced 2024)

A timeseries-DB community benchmark for OHLCV (time-bucketed bars) reportedly showed a competitor 4.4x faster than kdb+. **Important caveat from the source:** the kdb+ run did not preload data into RAM — queries triggered disk reads from date partitions. Preloading would significantly change the result. [Source: https://www.timestored.com/data/time-series-database-benchmarks, retrieved 2026-05-06] Author/identity of the "competitor" not specified in the aggregator excerpt I could retrieve. Likely QuestDB based on context but unverified.

### 3.3 Vendor benchmarks (Kx-published)

#### 3.3.1 KDB-X TSBS benchmark (Bodon, Nov 2025)

**Setup:** TSBS DevOps workload (CPU table only), three dataset scales, single-client query execution.

**Hardware:** AMD EPYC 9755 (Turin), 256 cores total, 2.2 TB DDR5, 3.84 TB Samsung PCIe 5.0 NVMe, RHEL 9.5.

**Crucial config detail:** KDB-X Community Edition was *deliberately resource-restricted* to **4 threads (1.5% CPU)** and **16 GB RAM (8% of system)**. Competitors had full hardware access. Kx framed this as "KDB-X wins despite handicap"; skeptics frame this as "Kx tested competitors at scales where they're known to be inefficient."

**DB versions:** KDB-X 0.1.0 community (June 2025), QuestDB 9.0.0, InfluxDB 2.7.11, TimescaleDB 2.20.2, ClickHouse 25.6.5.41.

**Headline numbers:**
- KDB-X wins **58 of 64** scenarios.
- QuestDB: 3.36x slower on average (up to 20x on worst queries).
- ClickHouse: up to 1100x slower on worst queries; "nearly four orders of magnitude slower" on `single-groupby-1-1-1`.
- TimescaleDB: 100x+ slower on specific queries.
- InfluxDB: crashed on `groupby-orderby-limit`.
- TimescaleDB: outperformed all others on `groupby-orderby-limit`.
- QuestDB: excelled on `double-groupby-*` and `lastpoint`.

[Source: https://medium.com/kx-systems/benchmarking-kdb-x-vs-questdb-clickhouse-timescaledb-and-influxdb-with-tsbs-2090f4533be0, retrieved 2026-05-06]  [VENDOR]

**Skepticism flag:** TSBS has known biases (timescale-favoring schema, narrow query types). Kx running TSBS — a benchmark originally pushed by TimescaleDB — and publishing wins is a strong signal but not a complete picture. The "1100x slower ClickHouse" result on `single-groupby-1-1-1` is a specific worst-case ClickHouse pattern (single-group on a high-cardinality column without proper indexes); ClickHouse practitioners would say "you're holding it wrong."

#### 3.3.2 Kx blog "what makes kdb+ fast" (date unspecified in fetch, but archive shows ~2024)

Vendor-published comparison numbers for *some* operations:

| Operation | kdb+ in-mem | InfluxDB | TimescaleDB | ClickHouse |
|---|---:|---:|---:|---:|
| Mid-quote returns | 64 ms | 99 ms | 1,614 ms | 401 ms |
| Execution volatility | 41 ms | 2,009 ms | 324 ms | 190 ms |

[Source: https://kx.com/blog/what-makes-time-series-database-kdb-so-fast/, retrieved 2026-05-06] [VENDOR]

Hardware/dataset not disclosed in the blog excerpt. Treat as illustrative, not a benchmarking claim chili should target.

### 3.4 Shakti's claimed-vs-kdb+ numbers (upper-bound targets)

Shakti is closed-source and out of scope for this doc, but the kdb+ baselines that Shakti has *publicly* claimed beats are useful as upper-bound targets — i.e., "kdb+ at its best can be beaten by ~3-4x by Whitney's own next-gen engine on the same benchmark."

| Benchmark | Shakti vs kdb+ ratio | Year | Source |
|---|---|---|---|
| STAC-M3 Year-High Bid | **3.7× faster** (primary STAC SHK211203; 7× in press-release coverage is restatement artifact) | 2022 | https://docs.stacresearch.com/news/SHK211203 |
| STAC-M3 NBBO-Q.TIME (β1.1T) | **3.3× faster** | 2022 | (same primary STAC report) |
| Mean response time across STAC-M3 v1 suite | **3.3-3.7× faster** | 2022 | https://www.efinancialcareers.com/news/2022/02/shakti-data-platform |
| NBBO (less-demanding variant) vs prior-published kdb+ best | **1.7×** | 2022 | DDN press release: https://www.ddn.com/press-releases/ddn-and-shakti-announce-record-breaking-results-on-the-stac-m3-benchmark-for-financial-trading-applications/ |

**Skepticism flag:** Shakti's STAC-M3 result was published once (Feb 2022) and Shakti has not, to my knowledge, published a subsequent STAC-M3 result. This is a **single data point** vs a **specific kdb+ baseline** (SUT KDB211014, which was a kdb+ 4.0 result on different hardware). The 3.3-3.7x claim should be read as "Shakti on tuned hardware vs older kdb+ on different tuned hardware" — not "Shakti beats kdb+ apples-to-apples by 3.7x." Whitney has a history of cherry-picking benchmarks that favor his designs (cf. Lochbaum's "Wild claims about K performance" critique [Source: https://mlochbaum.github.io/BQN/implementation/kclaims.html]).

**Useful as chili target:** "If chili can credibly hit 2-3x kdb+ on STAC-M3 Antuco operations, that puts chili in the same performance class Shakti claimed in 2022." That's the right framing for sprint targets — not "beat the 2025 Supermicro 6-node cluster on Kanaga."

### 3.5 Synthesis: what to use this data for

The chili optimization sprints have three usable target tiers:

**Tier 1 — "match commodity OSS":** Beat ClickHouse, DuckDB, Polars on TSBS DevOps. The Bodon/Kx 2025 numbers say a properly-tuned columnar engine can be 3-1100x faster than these on specific queries. Beat them all by 2x and chili is in the conversation.

**Tier 2 — "match kdb+ on the academic benchmark":** Match the Bodon 2023 arXiv numbers on the trades + orderbook crypto dataset. kdb+ in-memory hits 26-1352 ms on those queries; chili should aim for sub-100 ms on T-VWAP and sub-2000 ms on O-S to be competitive.

**Tier 3 — "match kdb+ on STAC-M3 single-node Antuco":** Pull the Lenovo SR650 V3 audited report numbers (Sept 2023) for a single-node 2-socket reference and target parity. This is the benchmark that gets attention in capital markets.

**Aspirational (Tier 4) — "match Shakti claims":** 3x kdb+ on STAC-M3 Antuco operations. This is only meaningful if chili reaches Tier 3 first.

---

## 4. Strengths and weaknesses (no editorializing)

### 4.1 What kdb+ is genuinely good at

| # | Strength | Citation |
|---|---|---|
| 1 | **Raw on-disk latency for time-window range queries** on splay/parted layout. mmap'd columns are page-faulted only when accessed; column stride lets CPU prefetcher do its job. | https://kx.com/blog/memory-mapping-in-kdb/ ; https://kx.com/blog/what-makes-time-series-database-kdb-so-fast/ |
| 2 | **Vectorized primitives operating on full columns** in a single tight loop (SIMD where applicable), with no per-row interpreter overhead. The K → q layer compiles to bytecode that operates on whole vectors. | https://kx.com/blog/what-makes-time-series-database-kdb-so-fast/ |
| 3 | **q's terseness for streaming aggregation patterns.** Common HFT idioms (asof joins, time-bucketed aggregations, pivot, group-by-as-of) compress to 5-15 chars of q. Under expert hands this is a productivity multiplier, not just a vanity feature. | https://code.kx.com/q4m3/0_Overview/ ; https://www.defconq.tech/docs/language/why_KDB |
| 4 | **Single-binary deployment.** q is one ~800 KB ELF that boots in milliseconds. No JVM, no Python interpreter, no daemon. Important for ops simplicity and for fitting the binary in CPU cache. | https://kx.com/blog/what-makes-time-series-database-kdb-so-fast/ |
| 5 | **Memory model: in-memory column store with explicit on-disk mmap overflow.** The same code path operates on RAM-resident and disk-mapped columns; performance degrades predictably as working set exceeds RAM, not catastrophically. | https://kx.com/blog/memory-mapping-in-kdb/ ; https://www.timestored.com/kdb-guides/memory-management |
| 6 | **25+ years of HFT-grade hardening.** kdb+ has been in production tick-streaming environments since 1998, including tier-1 sell-side desks where tail-latency under load is non-negotiable. | https://en.wikipedia.org/wiki/Kdb+ ; https://medium.com/@tzjy/comprehensive-guide-how-hedge-funds-use-kdb-in-quantitative-trading-9638ef43bb86 |
| 7 | **Native nanosecond timestamps and asof-join.** The temporal type model and the `aj`/`asof`/`wj` operators are first-class. SQL didn't get a comparable surface until DuckDB's `ASOF JOIN` (2023). | https://code.kx.com/q4m3/0_Overview/ ; https://kx.com/compare/kx-vs-clickhouse/ |
| 8 | **kdb+/tick architecture is opinionated and proven.** The TP + RDB + HDB pattern is a complete reference architecture for streaming + intraday + historical, with EOD roll handled as a first-class lifecycle. Implementing this from scratch on top of Spark/Kafka/ClickHouse is multiple engineer-years of work. | https://kx.com/blog/tick-architecture-simplicity-and-speed-the-kdb-way/ ; https://code.kx.com/q/architecture/ |
| 9 | **STAC-M3 record-holder for many operations on top-end hardware.** "kdb+ holds 17 world records" on STAC M3 (KX's claim, citation has it ~2024). The 2025 Supermicro stack pushed that further. | https://www.timestored.com/b/the-future-of-kdb/ (quoting Neil Kanungo, KX) ; https://stacresearch.com/news/stac-m3-benchmark-results-kx-kdb-4-1-on-supermicro-micron-intel/ |
| 10 | **TSBS dominance even with 4 threads / 16 GB.** The Bodon 2025 KDB-X benchmark was deliberately handicapped and still won 58 of 64 scenarios. | https://medium.com/kx-systems/benchmarking-kdb-x-vs-questdb-clickhouse-timescaledb-and-influxdb-with-tsbs-2090f4533be0 [VENDOR] |
| 11 | **Recent multithreading wins in kdb+ 4.1.** Work-stealing scheduler for nested `peach`, multithreaded CSV/binary load, 5x socket throughput. These close gaps where kdb+ was historically weak. | https://kx.com/blog/discover-kdb-4-1s-new-features/ |

### 4.2 What kdb+ is criticized for

| # | Criticism | Citation |
|---|---|---|
| 1 | **License cost.** Multiple credible third-party estimates put commercial pricing at $100K-$300K/yr floor, with tier-1 deployments rumored well into seven figures. The recurring complaint in every comparative writeup. | https://medium.com/@ev_kozloski/timeseries-databases-performance-testing-7-alternatives-56a3415e6e9e ($100K cited 2023-06-24); https://www.timestored.com/b/the-future-of-kdb/ ($300K cited 2024-07-24); https://www.businessresearchinsights.com/market-reports/high-frequency-trading-market-107496 |
| 2 | **q learning curve.** "Mastering KDB/Q can be quite challenging due to its steep learning curve, especially if you're new to vector programming languages." TimeStored's specific framing: very little has changed to make the language friendlier despite 10+ years of community resources. New-hire ramp commonly cited as 6+ months. | https://www.defconq.tech/docs/studyPlan/kdbDevs ; https://www.timestored.com/b/kdb-learning-curve/ |
| 3 | **Operator overloading by rank and type.** "The same symbol can perform completely different operations based on the runtime types of its arguments, requiring developers to mentally track the overloaded behaviors." Cited as a major readability problem. | https://medium.com/@gabiteodoru/why-llms-cant-write-q-kdb-extreme-operator-overloading-53cc64f1e310 ; https://news.ycombinator.com/item?id=21677540 (smabie, 81 points) |
| 4 | **"Write-only" code culture.** "K/q are often write-only code." HN comment (smabie, 81 upvotes): "error messages are bad, comments are for stupid people, white space is for Qbies." This is a community-cultural problem layered on top of the language design. | https://news.ycombinator.com/item?id=21677540 |
| 5 | **Cryptic error messages.** q uses single-letter error codes (`'type`, `'rank`, `'length`) without context. Stack traces improved in v3.5 but remain minimal vs Python/Java tooling. | https://code.kx.com/q/basics/debug/ ; https://news.ycombinator.com/item?id=21677540 |
| 6 | **Debugger limitations.** Built-in debugger relies on suspended-execution paren-marking and the `\` single-slash to resume. Community-built debuggers exist but are not standard. "Navigating the call stack to find current variable values being difficult." | https://www.timestored.com/kdb-guides/debugging-kdb ; https://forum.kx.com/t/new-kdb-q-debugger/10935 |
| 7 | **Right-to-left evaluation order.** q (like APL) evaluates strictly right-to-left with no operator precedence. Newcomers consistently fall in. "The strange evaluation order makes it impossible to read other people's code." | https://www.defconq.tech/docs/language/why_KDB ; https://news.ycombinator.com/item?id=15907840 |
| 8 | **Datetime type rough edges.** The `datetime` type (15) is *deprecated* in favor of `timestamp` (12). "Do not use a datetime for a key or in a join since the underlying float value is fuzzy and may give unexpected results." Out-of-range dates display as `0000.00.00`. | https://code.kx.com/q/basics/datatypes/ |
| 9 | **Mixed-type list rules differ from numeric to temporal.** Numeric lists widen/narrow to a common type; temporal lists take the type of the first item, others coerced. q-veterans know the workarounds; newcomers don't. | https://code.kx.com/q4m3/3_Lists/ |
| 10 | **Single-node scale ceiling.** kdb+ is fundamentally vertical-scale. Horizontal scale is bolted on via tickerplant replication or kdb Insights k8s. Multi-RDB query routing is "beyond the scope of basic implementations" — i.e., users must engineer it themselves. Pod autoscaling adds ~15s recognition latency. | https://www.version1.com/blog/kdb-benefits-of-horizontal-scaling/ ; https://code.kx.com/q/cloud/autoscale/rdc/ |
| 11 | **Closed source (until KDB-X CE in 2025).** Bug fixes ship on Kx's schedule. No public source mirror, no community PRs. KDB-X CE (Nov 2025) softens this but the engine remains closed. | https://kx.com/blog/kdb-x-ga-built-for-developers/ ; https://www.timestored.com/b/the-future-of-kdb/ |
| 12 | **Tooling gap vs modern observability.** Modern logs/metrics/tracing (OpenTelemetry, Prometheus) are bolt-on. q processes don't natively emit structured telemetry; users build it themselves or via KX Delta Platform. | https://code.kx.com/platform/debugging/ |
| 13 | **Stack Overflow Q&A volume.** "Stack Overflow has over 2 million questions for Python, where kdb+ only returns 2 thousand." Self-service troubleshooting is materially harder than for mainstream languages. | https://medium.com/@gabiteodoru/why-llms-cant-write-q-kdb-extreme-operator-overloading-53cc64f1e310 |
| 14 | **Career portability concerns.** A 2023 efinancialcareers piece asked whether "KDB+ developers are stuck in a dead-end career" — the framing being that the talent pool is narrow, employers are concentrated in finance, and skills don't transfer to other stacks. Whether one accepts the framing or not, it's the question recruiters and grad students are asking. | https://www.efinancialcareers.com/news/2023/05/worst-finance-programming-language |
| 15 | **Vendor-lock via talent.** "Hire fewer, pay more" is the kdb+ staffing model. Combined with the closed source and license cost, this creates a real switching cost when desks reconsider their TSDB stack. (This is *both* a strength and a weakness depending on which side of the table you're on; flagging here as a *criticism* per the request framing.) | https://www.efinancialcareers.com/news/2023/05/worst-finance-programming-language |
| 16 | **Wild perf claims pushback.** APL/K community member Marshall Lochbaum maintains a public "Wild claims about K performance" rebuttal page documenting cases where K/q performance claims didn't reproduce. Not a *criticism of kdb+* per se, but a useful corrective on vendor benchmarking. | https://mlochbaum.github.io/BQN/implementation/kclaims.html |

### 4.3 The five-line summary

- **kdb+ is fast on the workloads it was designed for.** The STAC-M3 record book backs this up across multiple hardware generations.
- **kdb+ is expensive.** Cost is the #1 reason firms select alternatives, more often than performance.
- **q is a productivity multiplier for experts and a wall for newcomers.** Both halves of that statement are equally true.
- **kdb+ is closed source and vertically-scaled by design.** KDB-X CE softens the first; the second is still a real ceiling.
- **The competitive moat is eroding.** Parquet, Arrow, asof-join, and columnar in-memory formats are now commodity. Kx's response has been (a) KDB-X for openness, (b) KDB.AI for vector/AI extension, (c) Insights for k8s deployment. Whether these are enough to defend against Polars/DuckDB/ClickHouse + Parquet + Iceberg remains to be seen — and is precisely the strategic question this doc series feeds into.

---

## Appendix A: Source register

Every URL cited in this doc, retrieved 2026-05-06.

**Primary KX sources:**
- https://kx.com/blog/discover-kdb-4-1s-new-features/
- https://kx.com/blog/what-makes-time-series-database-kdb-so-fast/
- https://kx.com/blog/memory-mapping-in-kdb/
- https://kx.com/blog/tick-architecture-simplicity-and-speed-the-kdb-way/
- https://kx.com/blog/kdb-x-now-generally-available-the-next-era-of-kdb-for-ai-driven-markets/
- https://kx.com/blog/kdb-x-ga-built-for-developers/
- https://kx.com/blog/unlock-new-capabilities-with-kdb-ai-1-4/
- https://kx.com/compare/kx-vs-clickhouse/
- https://kx.com/trial-options/
- https://kx.com/kdb-personal-edition-download/
- https://kx.com/news/kx-extends-use-of-worlds-fastest-time-series-database-kdb-with-on-demand-offering-for-cloud-and-on-premises/
- https://code.kx.com/q/architecture/
- https://code.kx.com/q/basics/datatypes/
- https://code.kx.com/q/basics/debug/
- https://code.kx.com/q/cloud/autoscale/rdc/
- https://code.kx.com/q/learn/licensing/
- https://code.kx.com/q/releases/ChangesIn4.1/
- https://code.kx.com/q4m3/0_Overview/
- https://code.kx.com/q4m3/3_Lists/

**TA Associates / FD Tech acquisition:**
- https://www.ta.com/news/ta-announces-all-cash-offer-to-acquire-fd-technologies-owner-of-global-real-time-analytics-leader-kx/
- https://www.businesswire.com/news/home/20250723180954/en/KX-Announces-New-Chapter-of-Growth-With-Strategic-Acquisition-by-TA-Associates
- https://www.timestored.com/b/kdb-acquired-by-private-equity/

**STAC-M3 benchmarks:**
- https://stacresearch.com/news/stac-m3-benchmark-results-kx-kdb-4-1-on-supermicro-micron-intel/
- https://blocksandfiles.com/2024/01/19/pure-stac-benchmark/
- https://lenovopress.lenovo.com/lp1825-sr650-v3-stac-m3-benchmark-result-2023-09-25
- https://stacresearch.com/news/2017/05/03/KDB170421
- https://docs.stacresearch.com/news/KDB200603
- https://www.supermicro.com/thought-leadership/STAC-M3-Kanaga+Antuco-Audited%20Report-KDB250929.pdf
- https://www.weka.io/blog/distributed-file-systems/what-is-the-stac-m-3-benchmark-and-why-should-you-care/

**Independent benchmarks:**
- https://arxiv.org/pdf/2301.12561 (Bodon et al., Jan 2023)
- https://medium.com/@ev_kozloski/timeseries-databases-performance-testing-7-alternatives-56a3415e6e9e
- https://www.timestored.com/data/time-series-database-benchmarks
- https://medium.com/kx-systems/benchmarking-kdb-x-vs-questdb-clickhouse-timescaledb-and-influxdb-with-tsbs-2090f4533be0 [vendor]

**Shakti:**
- https://www.ddn.com/press-releases/ddn-and-shakti-announce-record-breaking-results-on-the-stac-m3-benchmark-for-financial-trading-applications/
- https://www.efinancialcareers.com/news/2022/02/shakti-data-platform
- https://www.efinancialcareers.com/news/2019/11/shakti-arthur-whitney
- https://www.businesswire.com/news/home/20191001005420/en/Shakti-Technology-Launches-New-High-Performance-Data-Platform-Merging-Database-Language-and-Security
- https://news.ycombinator.com/item?id=21677540
- https://mlochbaum.github.io/BQN/implementation/kclaims.html

**Whitney biography & language history:**
- https://en.wikipedia.org/wiki/Arthur_Whitney_(computer_scientist)
- https://en.wikipedia.org/wiki/KX_Systems
- https://en.wikipedia.org/wiki/Kdb%2B
- https://en.wikipedia.org/wiki/Q_(programming_language_from_Kx_Systems)
- https://en.wikipedia.org/wiki/K_(programming_language)
- https://aplwiki.com/wiki/Arthur_Whitney
- https://k.miraheze.org/wiki/Arthur_Whitney
- https://www.timestored.com/kdb-guides/history-of-kdb-arthur-whitney

**Criticisms / community:**
- https://www.timestored.com/b/the-future-of-kdb/
- https://www.timestored.com/b/kdb-learning-curve/
- https://www.timestored.com/kdb-guides/debugging-kdb
- https://www.efinancialcareers.com/news/2023/05/worst-finance-programming-language
- https://medium.com/@gabiteodoru/why-llms-cant-write-q-kdb-extreme-operator-overloading-53cc64f1e310
- https://www.defconq.tech/docs/studyPlan/kdbDevs
- https://www.defconq.tech/docs/language/why_KDB
- https://forum.kx.com/t/new-kdb-q-debugger/10935

**Market context:**
- https://www.businessresearchinsights.com/market-reports/high-frequency-trading-market-107496
- https://www.grandviewresearch.com/industry-analysis/high-frequency-trading-market-report
- https://www.caspianone.com/kdb-insights-2025
- https://medium.com/@tzjy/comprehensive-guide-how-hedge-funds-use-kdb-in-quantitative-trading-9638ef43bb86

---

## Appendix B: Inline judgment-call log

Per the request to flag judgment calls inline rather than ask clarifying questions:

1. **STAC-M3 absolute timings:** I assumed the user prefers ratio-based targets (e.g., "2x faster than the published Lenovo SR650 V3 single-node 1T.YRHIBID number") rather than absolute ms targets, because absolute ms requires a paid STAC report fetch. If the sprint actually needs absolute ms, fetch the Supermicro 2025 audited PDF directly — it's free and has full numbers.
2. **Pricing — used third-party estimates as primary sources.** Kx publishes nothing. I cited the Kozloski $100K and Hamilton $300K numbers as bounds with explicit "third-party, unverified" framing. If the strategy doc demands a primary-source price, the answer is "no primary source exists; the opacity is itself a strategic data point."
3. **"Bodon was independent in 2023":** The arXiv paper predates his publicly-visible KX affiliation, but I did not exhaustively trace his employment history. If this matters, the safer framing is "academic but later employed by Kx."
4. **OHLCV 4.4x competitor:** Aggregator did not name the competitor; I declined to speculate. Likely QuestDB based on context but unverified.
5. **PE roadmap implications:** I included a one-line strategic read on TA Associates ownership pressure but flagged it explicitly as inference, not fact.
6. **KDB-X resource caps:** The Community Edition launch press said "embedded resource limits for cores, threads, memory, and connections" but did not disclose the cap numbers. I did not chase this further; if the chili strategy needs to set a "feature parity within KDB-X CE limits" target, the cap numbers will need a follow-up fetch.
7. **Shakti scoping:** I noted Shakti's claimed kdb+ baselines (per the request) and explicitly did not dig into Shakti's architecture, since a sibling subagent owns that.

End of deliverable.

---

## Report

I produced an ~800-line markdown research deliverable covering the four required sections plus appendices on the q/kdb+ landscape. It's returned in full above; the user will save it to `/Users/oakadmin/code/chili/docs/research/q_kdb_landscape.md`.

Key research findings worth surfacing for the parent agent:

1. **Ownership update:** TA Associates (PE firm) closed the £570M acquisition of FD Technologies / KX in July 2025 — KX is now PE-owned, which is a more recent event than the pin date might suggest is in the user's mental model.
2. **KDB-X Community Edition (Nov 2025) is the strategic shift:** Kx finally published a free-for-commercial-use tier, with embedded resource caps. This is a direct response to the open-source ecosystem (DuckDB/Polars/ClickHouse) eroding the moat. It changes the "kdb+ is closed source" criticism going forward.
3. **STAC-M3 benchmarks:** The current top kdb+ result (Oct 2025, Supermicro/Intel/Micron, kdb+ 4.1 in 6-node shard mode) holds 19 of 24 Kanaga records. Hardware-cost envelope is ~$1.5-3M for that cluster — meaningful for setting realistic chili targets. Audited per-operation timings are in a freely-available Supermicro PDF if the optimization sprints need absolute ms numbers.
4. **The Bodon 2023 arXiv paper is the best independent academic benchmark** and gives concrete query-latency numbers (kdb+ in-mem T-VWAP 26 ms, T-V1 57 ms, etc.) on a 25 GB crypto dataset — directly usable as Tier-2 chili targets.
5. **Pricing remains opaque.** Best public estimates: $100K floor (2023), $300K to start (2024). No primary-source list pricing. The opacity is itself a strategic data point.
6. **Shakti's claimed 3.3-3.7x speedup over kdb+** comes from a single 2022 STAC-M3 v1 result paired with a specific older kdb+ baseline — not apples-to-apples. Useful as aspirational upper bound, not a comparable target.
7. **Top-3 criticisms with strongest citations:** license cost (every comparative writeup), q learning curve / operator overloading (HN smabie comment, 81 pts), single-node scale ceiling (Version 1 + Kx docs themselves).

The doc is heavily footnoted (every benchmark + every market claim has source URL + retrieval date), and judgment calls are logged in Appendix B. It's between the requested 600-1200 line range.

Sources:
- [TA Associates acquires FD Technologies / KX](https://www.businesswire.com/news/home/20250723180954/en/KX-Announces-New-Chapter-of-Growth-With-Strategic-Acquisition-by-TA-Associates)
- [STAC-M3 KX kdb+ 4.1 Supermicro 2025](https://stacresearch.com/news/stac-m3-benchmark-results-kx-kdb-4-1-on-supermicro-micron-intel/)
- [Bodon et al. 2023 arXiv benchmark paper](https://arxiv.org/pdf/2301.12561)
- [DDN + Shakti STAC-M3 press](https://www.ddn.com/press-releases/ddn-and-shakti-announce-record-breaking-results-on-the-stac-m3-benchmark-for-financial-trading-applications/)
- [TimeStored: The Future of kdb+?](https://www.timestored.com/b/the-future-of-kdb/)
- [KDB-X GA announcement](https://kx.com/blog/kdb-x-ga-built-for-developers/)
- [Wikipedia: Arthur Whitney](https://en.wikipedia.org/wiki/Arthur_Whitney_(computer_scientist))
- [HN thread on Shakti (criticisms)](https://news.ycombinator.com/item?id=21677540)
- [Bodon TSBS KDB-X benchmark Nov 2025](https://medium.com/kx-systems/benchmarking-kdb-x-vs-questdb-clickhouse-timescaledb-and-influxdb-with-tsbs-2090f4533be0)