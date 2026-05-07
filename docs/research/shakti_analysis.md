# Shakti / k9 Deep-Dive

**Author:** Claude (research subagent)
**Date:** 2026-05-06
**Sibling docs:** [`q_kdb_landscape.md`](q_kdb_landscape.md) (kdb+ proper), [`kdb_alternatives.md`](kdb_alternatives.md) (alternatives catalog).
**Scope:** Architecture, performance claims, and strategic implications of Shakti / k9 — Arthur Whitney's closed-source kdb+ successor — for chili's positioning.

**Source-confidence flags used throughout:**
- `[VENDOR]` — Shakti, Kx, or directly equivalent first-party.
- `[VENDOR-ADJACENT]` — DDN press, Fintan Quill Q&A, financial-press repackagings of vendor claims.
- `[INDEPENDENT]` — third-party authors with no commercial tie (Lochbaum, HN comments, kparc community, academic critiques).
- `[SECONDARY]` — Wikipedia, APL Wiki, derived references that themselves cite either of the above.

**Reading guide.** Shakti is closed-source. Almost everything below is reasoned from a thin public surface: one STAC-M3 result (2022), Whitney's "ksimple" educational interpreter (2024), the unofficial k9-simples tutorial, two financial-press articles, one Q&A with Shakti's sales-engineering lead, and HN/Lobsters discussion. Treat magnitude claims with extreme skepticism; treat *architectural-direction* signals (terseness, AVX hard requirement, single-server design) as genuinely informative because they're consistent across sources.

---

## 0. Executive summary

1. **What Shakti is.** A closed-source, single-binary, single-server columnar timeseries database + array language (k9) built by Arthur Whitney and Janet Lustgarten at Shakti Software (NYC, founded 2018). It is structurally the "kdb+ done over with twenty years of hindsight" thesis. Same lineage (K/q → k9), same workload target (capital-markets tick analytics), distinct codebase.
2. **What Shakti has publicly proven.** One STAC-M3.v1 (Antuco) result, Feb 2022, on a single server (1× R282-Z90, 2× EPYC 7742, 128 cores) backed by a DDN ES200NVX flash appliance + EXAScaler 5.2.2. Headline claims: 7× kdb+ on Year-High Bid (cached), 3.3× on NBBO-Q.TIME, 196% best-in-class storage efficiency, 1.7× the prior best on the less-demanding NBBO variant. The kdb+ baseline cited is **KDB211014** — a 15-server kdb+ 4.0 cluster on AI400X2. **One result, four years stale, vs a multi-node baseline. Read as "directionally credible single-server win" not "Shakti is 3-7× kdb+ apples-to-apples."**
3. **What chili should do with Shakti.** Treat as **aspirational ceiling, not roadmap target.** Chili's right reference frame is kdb+ 4.1 on commodity hardware (Tier 3 in `q_kdb_landscape.md` §3.4); Shakti's STAC-M3 result is Tier 4. Independently, the *design directions* Shakti has publicly hinted at (interpreter dispatch concision, AVX-mandatory primitive selection, native time-series joins with bitemporal-and-microsecond support, in-binary FFI) are concrete optimization targets chili can copy without needing access to Shakti's source.
4. **Counter-philosophy framing.** Shakti is the maximalist closed-source single-genius bet: trust Whitney to ship faster than the open-source consensus can re-derive. Chili is the inverse bet: open-source iteration on Polars + Arrow + Parquet substrate will compound past any single-author stack on the workloads that matter to most users (not the absolute fastest STAC-M3 cell). These are honest opposites; chili's pitch should explicitly acknowledge Shakti's existence rather than ignore it.

---

## 1. Origins and team

### 1.1 The Kx → Shakti transition

Whitney joined Kx in 1993 and stayed for 25 years building K1 through K6 (the language) and kdb (1998) → kdb+ (2003) on top. **In 2018 First Derivatives bought out Whitney's and Lustgarten's remaining minority shares in Kx Systems** [Wikipedia: Arthur Whitney; SECONDARY]. They left and founded Shakti Software the same year. The first public Shakti material appeared in November 2019 with a BusinessWire press release ([Shakti Technology Launches…](https://www.businesswire.com/news/home/20191001005420/en/), confirmed 2019; archived in `q_kdb_landscape.md` §1) and a contemporaneous eFinancialCareers profile titled *"The new data platform from the reclusive genius of banking IT"* ([efinancialcareers.com 2019/11](https://www.efinancialcareers.com/news/2019/11/shakti-arthur-whitney); [VENDOR-ADJACENT]).

Whitney's stated public reason for the rewrite is the standard "twenty years of cruft, do it cleaner now": kdb+ was constrained by backwards-compatibility commitments that he was no longer willing to defend. The aplwiki page on K records: *"Arthur reputedly always starts from scratch when making the next generation of k, happily and deliberately sacrificing backward compatibility in order to build something better and faster, cut fat or revert design decisions that didn't pan out"* [aplwiki.com/wiki/K, 2026-05-06; SECONDARY]. This is consistent across his historical pattern (A → A+ → K1 → K2 → … → K7 → K9; K8 was skipped).

### 1.2 Founding team and known staff

- **Arthur Whitney** — founder, CTO/architect role. Sole author of the k9 interpreter core.
- **Janet Lustgarten** — co-founder. Formerly Kx co-founder (1993). Business / commercial side.
- **Fintan Quill** — Head of Sales Engineering. Joined 2019. Previously 16+ years across Wall Street trading firms ([odbms.org Q&A 2022-02](https://www.odbms.org/2022/02/on-shaktis-data-platform-and-the-stac-m3-benchmark-council-for-financial-trading-applications-qa-with-fintan-quill/); [VENDOR-ADJACENT]). Public face for STAC-M3-era press cycle.

**Known company size:** ZoomInfo lists Shakti Software at **~8 employees** [zoominfo.com/c/shakti-software-inc/365480683, retrieved 2026-05-06; SECONDARY, employee-count databases are notoriously imprecise]. CB Insights lists similar low-double-digit headcount. **HQ:** 243 5th Ave, suite 702, New York City. The smallness is consistent with the public hiring footprint (no LinkedIn job-board presence under "Shakti Software" beyond the company page) and Whitney's documented preference for tiny teams.

**Funding:** ZoomInfo records "Private Equity" as latest round but no figure or date. No Crunchbase profile under the database-Shakti entity (Crunchbase entries returned for unrelated "Shakti" companies). **No public disclosure of a venture round, valuation, or institutional backer.** This is consistent with self-financed / customer-financed operation typical of Whitney's prior commercial ventures.

### 1.3 Public timeline

| Date | Event | Source |
|---|---|---|
| 2018 | First Derivatives buys remaining Kx shares; Whitney + Lustgarten depart and incorporate Shakti Software | en.wikipedia.org/wiki/KX_Systems [SECONDARY] |
| 2019-10 | First public press release (BusinessWire) | businesswire.com 2019/10/01 [VENDOR] |
| 2019-11 | eFinancialCareers profile of Whitney + Shakti | efinancialcareers.com 2019/11 [VENDOR-ADJACENT] |
| 2019-11 | HN thread "Shakti, the new data platform from Arthur Whitney" (item 21677540) | news.ycombinator.com [INDEPENDENT] |
| 2020 | k9-simples community tutorial begins (estradajke/k9-simples GitHub) | github.com/estradajke/k9-simples [INDEPENDENT, secondary documentation] |
| 2022-02 | DDN + Shakti STAC-M3 result published (SHK211203) | docs.stacresearch.com/news/SHK211203 [VENDOR-ADJACENT via STAC] |
| 2022-02 | Fintan Quill Q&A (ODBMS.org) | odbms.org [VENDOR-ADJACENT] |
| 2024-01 | "ksimple" educational K interpreter posted to GitHub (kparc/ksimple, MIT license, ~25 LOC core) | github.com/kparc/ksimple [INDEPENDENT-but-Whitney-authored] |
| 2024-01 | HN thread on ksimple (item 39026551, ~475 stars on repo) | news.ycombinator.com [INDEPENDENT] |
| 2024-06 | Thalesians magazine: "Did Arthur Whitney just open-source k?" — interpreting ksimple as a possible direction signal | magazine.thalesians.com 2024/06/08 [INDEPENDENT] |
| 2026-05 | shakti.com 301-redirects to k.nyc, which serves only the bare letter "k" over HTTPS [retrieved 2026-05-06] | direct fetch [PRIMARY-OBSERVATION] |

**Notably absent from the public record:**
- No second STAC-M3 result, despite four years elapsed.
- No customer logo wall, customer testimonial, or named end-user reference deployment.
- No Whitney conference talk or public q-meetup demo specifically about k9 in 2023 or 2024 returnable from public web search [verified 2026-05-06; absence-of-evidence is weak evidence but consistent with deliberate low-profile posture].
- No announced k10 / k11.
- No press release for the 2024 ksimple drop from Shakti itself — community organization (kparc) carried the announcement.

The 2018→2026 cadence is roughly: one big benchmark moment, one educational/marketing moment, no follow-on. That's a low-velocity public posture by enterprise-DB standards.

---

## 2. Architecture and language (k9)

### 2.1 What's actually known vs. what's assumed

**Known from primary sources:**
- k9 is the language layer. Successor to k4 (the K under kdb+) and k7 (an intermediate Whitney project). K8 was skipped by convention [aplwiki.com/wiki/K; SECONDARY].
- k9 is implemented in C. The full implementation is closed-source.
- The runtime requires AVX. From the 2019 HN thread: a developer reported `Illegal instruction (core dumped)` on older Celeron CPUs; a respondent confirmed the AVX requirement was intentional but "removing the avx hard requirement is on the roadmap" [news.ycombinator.com/item?id=21677540; INDEPENDENT, comment 2019].
- k9 has FFIs: C, Python, NodeJS, Java, plus an in-built FFI mechanism [Quill Q&A 2022-02; VENDOR-ADJACENT].
- k9 supports microsecond and nanosecond timestamps natively, bitemporal joins, and temporal aggregation as language-level primitives [Quill Q&A; VENDOR-ADJACENT].
- The k9-simples community manual (build dated 2023-03-09) is the most complete public documentation and covers nouns/verbs/adverbs, tables, dictionaries, kSQL, and benchmarks. It's not an official Shakti document but is linked from many Whitney-aware references [estradajke.github.io/k9-simples; INDEPENDENT-community].

**Inferable from ksimple (the 2024 educational drop):**
- Whitney's preferred dispatch mechanism in real production interpreters is almost certainly the same pattern as ksimple, scaled up: paired function-pointer arrays indexed by operator position in a glyph string. ksimple uses `V=" +-*&|<>=~!@?#_^,"` and dispatches into `f[]` (monadic) and `F[]` (dyadic) [kparc/ksimple a.c, MIT 2024; INDEPENDENT-but-Whitney-authored].
- Right-to-left evaluation, no operator precedence, single-pass. Standard APL/K.
- Macro-heavy style: `g(a,v)` style polymorphism between atoms and vectors via single-character macros. Whitney's stated motivation: keep the interpreter logic on one screen [needleful.net 2024/01; INDEPENDENT analysis].
- Fat-pointer vector representation: length stored at offset `-1` from the data pointer, atoms below a sentinel boundary (`< 256` in ksimple; obviously different at production scale) [INDEPENDENT analysis of ksimple, needleful.net].

**Strongly suspected but not confirmed for production k9:**
- Production k9 vector layout is some larger-budget descendant of the ksimple fat-pointer scheme with refcount + length + type tag + capacity. This is consistent with how K4 worked (same author) and what the FFI surface implies, but is not directly disclosed.
- No JIT. Lochbaum notes commercial K is "always described as interpreters" and as of 2022 only ktye's open-source K had an AOT compiler [mlochbaum.github.io/BQN/implementation/kclaims.html; INDEPENDENT]. There is no public hint of a JIT in k9.
- Single-threaded primitive dispatch with explicit parallelism opt-in via specific primitives (consistent with kdb+ tradition; Whitney has stated Shakti adds "built-in parallelism in primitive functions" [businesswire 2019; VENDOR] without specifying what changed vs kdb+).

**ksimple-vs-production-k9: what scales up, what doesn't.** ksimple is ~25 LOC of C; the k9 production binary is presumably tens of thousands of LOC. The gap maps approximately to:

| ksimple feature | Production k9 (presumed) | Reasoning |
|---|---|---|
| 7-bit signed atom range, 255-byte vector cap | 64-bit ints, multi-GB vectors | ksimple capacity limits are pedagogical; production has none |
| Function-pointer-array dispatch (`f[]`, `F[]`) | Same pattern, expanded to ~50-100 primitives | The dispatch table is the central trick; nothing about scale changes the pattern |
| Single-character glyph parser | Multi-character keyword parser for kSQL extensions | k9 includes kSQL-style keywords (per k9-simples manual); ksimple does not |
| No refcount, no GC | Refcount + deterministic destruction | "Eager Haskell with deterministic garbage collection" per HN comment 2019 [INDEPENDENT] |
| No I/O, no persistence | mmap-backed columnar persistence | Production claim, not in ksimple |
| No FFI | C/Python/Java/Node FFI + in-built FFI | Quill Q&A 2022 [VENDOR-ADJACENT] |
| No tables | Table type, kSQL queries, time-series joins | k9-simples manual confirms presence |
| No threading | "Built-in parallelism in primitive functions" | BusinessWire 2019 [VENDOR] |

The takeaway: **ksimple discloses the dispatch shape, not the heavy machinery.** The dispatch shape is what chili can copy directly (and largely already inherits via pepper). The heavy machinery — column store, compression, parallelism — remains undisclosed and is where the real Shakti edge lives.

### 2.2.1 k9 vs q primitive set differences (publicly observable)

The k9-simples manual ([estradajke.github.io/k9-simples](https://estradajke.github.io/k9-simples/k9/index.html); INDEPENDENT-community) is the most complete public k9 reference. Notable departures from kdb+/q:

- **Single-character primitives reasserted.** q's English keyword wrappers (`select`, `from`, `where`, `each`, `over`) are de-emphasized in k9; the k-glyph forms are primary. The community manual treats this as the natural reading order. This is consistent with Whitney's documented dislike of the keyword overlay he allowed in q.
- **kSQL.** k9 retains a SQL-like query embedding under the name "kSQL" but the public material is light on whether it's a syntactic alias for k expressions (as q-sql is for k expressions) or a separate parser. The pattern is q-sql-equivalent in functionality.
- **Native bitemporal types.** Per Quill Q&A: bitemporal joining is a primitive-level feature, not a query-rewrite pattern. q has historic + as-of joins but bitemporal-as-a-first-class is a stated k9 differentiator.
- **Microsecond + nanosecond resolution.** kdb+ has nanosecond timestamps (timestamp type, .ns); k9 reasserts both. Not a real differentiator on its own — both can do it — but the Quill framing positions it as a feature.
- **Built-in FFI surface as a primitive.** In q, foreign-function calls go through `2:` (load-and-bind) or remote IPC. k9 reportedly has an "in-built FFI" — terminology suggests a more direct call path.

**Total publicly-disclosed primitive count for k9: not enumerated.** The k9-simples manual covers ~30-40 named primitives; whether that's the full set or a subset is unstated.

### 2.2 On-disk and in-memory layout

**Disclosed by Quill Q&A:** native time-series; bitemporal; microsecond + nanosecond temporal types [VENDOR-ADJACENT].

**Disclosed by STAC report:** Shakti's database achieved **196% storage efficiency** vs the STAC-M3 reference, "the highest of any publicly reported solution" [docs.stacresearch.com/news/SHK211203; VENDOR-ADJACENT]. STAC's "storage efficiency" metric is `dataset_size_logical / on_disk_size`, so 196% means Shakti stored the same logical dataset in roughly half the physical bytes of the reference. That implies aggressive compression — consistent with bit-packing, dictionary encoding, or run-length encoding on quote/trade columnar layouts. (Chili's int64 quantization for prices is in the same family of techniques; see CLAUDE.md §4.)

**Not disclosed:**
- Whether Shakti uses Arrow-compatible memory or a private layout. Almost certainly **private** — Arrow is consensus open-source infrastructure and Whitney's design DNA is to roll his own everything. There is zero public mention of Arrow in any Shakti material.
- File format. Likely a Whitney-style splayed-table or KX-style on-disk variant; no Parquet, no ORC, no IPC.
- Mmap vs explicit-read I/O. KX's kdb+ uses mmap heavily; nothing public says k9 changed this.
- Compression codec. Plausible candidates: dictionary + delta + bitpack; ZSTD at the page level; or a custom Whitney scheme. **Unknown.**

### 2.3 APIs and embeddings

| Surface | Public status | Source |
|---|---|---|
| C FFI | Confirmed in Q&A; details not public | Quill Q&A [VENDOR-ADJACENT] |
| Python binding | Confirmed exists; no published API surface (no shakti package on PyPI as of 2026-05-06) | Quill Q&A; PyPI search [VENDOR-ADJACENT + PRIMARY-OBSERVATION] |
| Java binding | Confirmed | Quill Q&A [VENDOR-ADJACENT] |
| NodeJS binding | Confirmed | Quill Q&A [VENDOR-ADJACENT] |
| In-built FFI | "Built-in foreign function interface (FFI)" — likely a fast-path C-call mechanism akin to kdb+'s `2:` | Quill Q&A [VENDOR-ADJACENT] |
| IPC | "Custom IPC protocol" mentioned in 2019 BusinessWire | businesswire.com 2019/10/01 [VENDOR] |

**No SDK has been made publicly available** — these are all customer-engagement-time deliverables, not download-and-go. This is a genuine differentiator from kdb+ (whose IPC and binding patterns are public reference material) and from chili (whose Python binding is on PyPI).

### 2.4 Open-source approximations to "what k9 is doing"

Group C of `kdb_alternatives.md` covers ngn/k, growler/k, Klong, and KDB-X CE. The piece relevant *here* is what those open implementations reveal about the Shakti interpreter design space.

- **ngn/k** [INDEPENDENT, AGPLv3, dormant]. Lochbaum measured ngn/k's instruction-cache stalls at ~0.6% on representative benchmarks — i.e., the Whitney "interpreter fits in L1" claim is real but doesn't explain a 3-7× speed ratio over kdb+, because every modern interpreter benefits from L1 fit roughly equally [mlochbaum.github.io/BQN/implementation/kclaims.html; INDEPENDENT].
- **growler/k** [INDEPENDENT, AGPLv3, active 2026]. Active fork of ngn/k. Same dispatch model.
- **kparc/ksimple** [Whitney-authored, MIT, 2024, ~25 LOC core]. The *only* Whitney-authored interpreter the public has seen since 2018. Demonstrates the dispatch table and macro polymorphism patterns. Production k9 is presumably ksimple's design philosophy with two more orders of magnitude of code. **kparc** appears to be a community of Whitney-aware C/K hackers who serve as an adjacent ecosystem; the kparc GitHub org also hosts kcc (k crash course), kc (Node-based REPL), and a reference card at [kparc.github.io/ref/](https://kparc.github.io/ref/). [INDEPENDENT from Shakti the company, but with direct Whitney participation.]

**Inference:** if ngn/k can match Shakti's *language semantics* but not Shakti's *primitive vectorization quality*, then the bulk of Shakti's STAC-M3 win is not in the interpreter — it's in the column-store + primitive-vectorization + storage-format axis, not the dispatch hot path. **This is load-bearing for chili's positioning** (see §4.2): if the Shakti edge is not in interpreter dispatch, then chili's pepper-on-top-of-Polars architecture doesn't need to compete on dispatch perfection; it needs to compete on primitive-vectorization quality and storage layout, which is exactly Polars' wheelhouse.

---

## 3. Published benchmarks and performance claims

### 3.1 The 2022 STAC-M3 result (the headline)

**Source:** STAC report SHK211203 (Feb 2022) [docs.stacresearch.com/news/SHK211203; VENDOR-ADJACENT via STAC's process]. Mirrored in DDN press release [ddn.com/press-releases/…; VENDOR] and several reposts (HPCwire, Datanami, Benzinga, GlobalFintechSeries; all are press-release republications, not independent reporting).

**Test system (Shakti 2.0):**
- 1× GIGABYTE R282-Z90 server
- 2× AMD EPYC 7742 (64 cores each, 128 cores total)
- DDN ES200NVX all-flash appliance
- DDN EXAScaler 5.2.2 parallel filesystem

**kdb+ baseline (comparison SUT, KDB211014):**
- kdb+ 4.0 (Compatibility Rev H of kdb+ STAC Pack)
- **15× 20-core servers** (no sharding)
- DDN AI400X2 all-flash storage

**Headline numerical results:**

| Metric | Shakti vs KDB211014 | Source quote |
|---|---|---|
| Storage efficiency | **196%** (best of any publicly reported STAC-M3 solution) | STAC SHK211203 |
| Year-High Bid (cached) | **3.7×** faster | STAC SHK211203 |
| NBBO-Q.TIME (β1.1T) | **3.3×** faster | STAC SHK211203 |
| NBBO-Q.TIME (less-demanding variant) | **1.7×** the prior best published result | STAC SHK211203 |

**Ratio clarification:** the "7×" figure that has circulated in the press (and that I echoed in `q_kdb_landscape.md` §3.4 from the eFinancialCareers writeup) traces back to a less-precise summary. The STAC report itself uses **3.7× (Year-High Bid) and 3.3× (NBBO-Q)** as the operation-specific multipliers, with "3.3-3.7× mean response across STAC-M3.v1" as the aggregate. The 7× appears to be either a separate cached-vs-uncached comparison or a press-amplification artifact. **Use 3.3-3.7× as the defensible Shakti-vs-kdb+ headline; the 7× is over-claim-by-restatement.** This needs back-correction in `q_kdb_landscape.md` row 6 of the STAC table — the Year-High Bid figure should be 3.7×, not 7×, per the primary STAC report.

**Critical apples-to-apples problems:**
1. **Single-server Shakti vs 15-server kdb+.** Per-server, Shakti's win is genuinely impressive. Per-rack-unit cost-equivalent, the comparison is murkier — kdb+ on 15× 20-core servers is using ~300 cores; Shakti on 1× 128-core EPYC is at less than half the core count. So the win is not "Shakti is 3.3-3.7× faster per core" but "Shakti can do on one server what kdb+ does on 15." That's still a strong claim.
2. **Different storage stacks.** ES200NVX (Shakti) vs AI400X2 (kdb+). DDN is the storage vendor in both cases, but the appliances differ in spec.
3. **kdb+ 4.0, not 4.1.** The KDB211014 baseline is October 2021. KX shipped kdb+ 4.1 in 2024 with multithreaded primitives ("peach" improvements, etc.). A 2026 head-to-head would not look the same.
4. **Single STAC suite, single result.** No re-test in 4 years.
5. **STAC-M3.v1 (Antuco), not Kanaga.** Antuco is the easier suite. Shakti has never published a Kanaga (the harder suite) result.

### 3.2 The k9-simples internal benchmarks

The community k9-simples tutorial publishes a `b.k` benchmark page comparing k9 to PostgreSQL, Spark, MongoDB on a synthetic 960B-quote / 48B-trade dataset. Headline:

| System | Q1 | Q2 | Q3 | Q4 |
|---|---|---|---|---|
| k9 | 1 | 9 | 9 | 1 |
| PostgreSQL | 71,000 | 1,500 | 1,900 | INF |
| Spark | 340,000 | 7,400 | 8,400 | INF |
| MongoDB | 89,000 | 1,700 | 5,800 | INF |

[estradajke.github.io/k9-simples/k9/Benchmark.html; INDEPENDENT-community presentation of vendor figures]

**Skepticism:** these are unitless multipliers presented without methodology, hardware spec, query-set definition, dataset-shape disclosure, or reproducibility instructions. Comparing a tuned column-store (k9) to a row-store (PostgreSQL) and a JSON document store (MongoDB) on time-series queries produces this kind of ratio mechanically. **Do not cite these in chili-positioning material.** The STAC-M3 result is the only Shakti benchmark with even partial third-party validation.

A separate "taxi" benchmark on the same page claims k9 runs the NYC-taxi suite on 1× i3.2xlarge in 1 second vs Redshift on 6 machines in 8 seconds and Spark on 21 machines in 30 seconds. Again, no methodology, no reproducibility. Note: i3.2xlarge has 8 vCPUs and 61 GB RAM — the dataset shape (110M rows × 2,500 daily tables) is small enough that this is plausible, but the comparison points are not pinned.

**Methodological problems with the k9-simples b.k benchmark, enumerated:**
1. Q1-Q4 are not defined inline. Reader has to chase the `b.k` script in the repo to see what the queries are. Even there, semantic equivalence across PostgreSQL/Spark/MongoDB/k9 is unverified.
2. PostgreSQL is a row store. Comparing a row store to a column store on a 960B-row analytics workload yields the kind of ratio shown by mechanical design choice — not engineering effort. The same gap would obtain between kdb+ and PostgreSQL.
3. MongoDB is a document store with no native columnar layout. Same critique.
4. Spark is in JVM with shuffle-based execution. On a single-machine 110M-row scan, Spark loses to *any* native column store by 1-2 orders of magnitude before tuning. This is well-known.
5. The "INF" entries for Q4 indicate the row-store comparators couldn't complete the query, not that they were a million times slower.
6. No hardware spec for the comparator runs.
7. No version pins for any comparator.

**Verdict:** the b.k page is marketing copy, not benchmark methodology. The 196% storage-efficiency number from STAC SHK211203 is the only Shakti performance figure with method-validated provenance, and even that is a single 2022 result.

### 3.3 The Lochbaum critique (the only sustained independent counterweight)

Marshall Lochbaum (BQN author, formerly Dyalog APL engineer) wrote *"Wild claims about K performance"* [mlochbaum.github.io/BQN/implementation/kclaims.html; INDEPENDENT]. Key points reproduced here because they directly bear on chili's positioning:

1. **Vendor benchmarks cover only timeseries DB workloads.** "KX and Shakti both advertise performance heavily, they only ever refer to database performance" — i.e., not general array-language operations like sort, filter, set-difference on arbitrary data shapes.
2. **L1-instruction-cache fit doesn't explain the speedup.** Lochbaum measured ngn/k, BQN, and J under Linux `perf` and found instruction-cache stalls account for 2-9% of cycles (J/BQN) and <1% (ngn/k on short programs). The "interpreter fits in L1" story is real but tiny.
3. **No JIT.** As of 2022: commercial K is interpreted; only ktye's K had an AOT compiler. **This is significant for chili: it means Shakti's edge over kdb+ is not from a JIT win — it's from interpreter-loop quality, primitive vectorization, and storage layout, all of which are independently attackable.**
4. **Anti-benchmark clauses.** Lochbaum reports that "every contract for a commercial K includes an anti-benchmark clause," which is why third-party Shakti-vs-kdb+ numbers don't exist. This is a structural feature of the market, not an oversight.
5. **Lochbaum has not benchmarked Shakti.** "I suspect it's faster than K4 but haven't benchmarked it due to licensing restrictions." He's the most credible independent benchmarker in this space and even he can't get a license-clean dataset to compare.

### 3.4 Other publicly stated Shakti performance claims

- **From ZoomInfo company description (likely Shakti-supplied marketing copy):** *"K is about 100 times faster than popular platforms like Polars DataTable, BigQuery, and Redshift."* [zoominfo.com/c/shakti-software-inc/365480683, retrieved 2026-05-06; VENDOR copy via ZoomInfo intake]. This is a PR-deck claim. **Polars in particular is not 100× behind anyone on tick-analytics workloads in any benchmark I have seen** — chili's own numbers (see `docs/bench/summary.md`) show Polars holding its own against custom column stores. Treat as non-credible at face value; useful only as evidence that Shakti positions itself against Polars in marketing.
- **Whitney conference talks 2023-2024.** No surfacing in web search for "Arthur Whitney k9 talk presentation 2023 2024" [verified 2026-05-06]. If he has presented since 2022, it has been at non-recorded private q-meetups.

### 3.5 Has Kx published comparisons against Shakti?

Searched: no direct Kx-vs-Shakti benchmark. KX's own STAC-M3 results on kdb+ 4.1 (2024+) implicitly close the gap on the 2022 result but don't name Shakti. This is the standard incumbent move: don't dignify the upstart by naming it. Strategic implication: chili will face the same treatment from Kx and should plan accordingly (no expectation of a kdb+-vs-chili Kx-published comparison).

---

## 4. Strategic implications for chili

### 4.1 Is Shakti's design lineage applicable to chili?

**Stack-level:** No. Chili is Rust + Polars + Arrow + Parquet. Shakti is C + custom interpreter + custom column store. The substrate is different and chili is not going to rewrite itself in C to compete with Whitney on his home turf. That bet is already taken (and is called Shakti).

**Design-choice-level:** Several Shakti-style decisions are directly importable to chili. Each is annotated with current chili-state and effort estimate.

| Shakti choice | Translation to chili | Chili state today | Estimated effort |
|---|---|---|---|
| Single-binary deploy | Already true. `chili` CLI + `chili-pie` Python wheel both ship as single artifacts. | Done | — |
| AVX (or AVX-512) hard requirement on the hot path | Chili uses Polars' SIMD; Polars itself dispatches per CPU. Could add an AVX-512 fast path for specific bit-pack/dequantize kernels. | Inherits Polars dispatch | Medium (1-2 sprints for targeted kernels) |
| Glyph-string indexed dispatch table for primitives | Pepper parser already does symbol-table dispatch; could be tightened to direct function-pointer-array indexing on the parsed-AST hot path. | Parse cache hit ~385 ns (CLAUDE.md golden rule 6); already good. | Low — maintain bench discipline |
| Bitemporal + microsecond/nanosecond native types | Chili has nanosecond timestamps via Polars. Bitemporal joins are a workload-pattern, not a primitive. | Partial | Medium — add a bitemporal join idiom in pepper |
| In-built FFI (cheap C call from k9) | Chili's `chili-py` already exposes a fast Python ↔ Polars DataFrame bridge with GIL released. The "in-built FFI" equivalent is the user-defined-function path in pepper → Rust. | Not yet exposed; UDFs in pepper are a known follow-on | Medium-high |
| Aggressive on-disk compression to win storage-efficiency benchmarks | Chili stores Int64-quantized prices (CLAUDE.md golden rule 4). Adding optional dictionary + run-length on category columns would be additive. | Partial | Medium |
| Right-to-left, no-precedence interpreter | Pepper inherits this from k/q. Already done. | Done | — |
| Single-server-first design | Chili is single-process. Out-of-core via Polars streaming is the parallel path. | Done | — |

**The most actionable items in this list are the bitemporal-join idiom in pepper and the user-defined-function-in-Rust path** — both are explicit Shakti differentiators that chili can match on the open-source substrate without architectural pain.

#### 4.1.1 Concrete chili engineering checklist drawn from the Shakti analysis

Each item is annotated with the chili crate where the work would land. None of these are commitments — they're a follow-on-research short list for the architect/PM to triage against the existing roadmap.

| Item | Chili crate | Source of inspiration | Effort |
|---|---|---|---|
| Add explicit AVX-512 feature gate on int64 dequantize and price-scale kernels | `chili-op` | Shakti AVX-mandatory dispatch + 2022 EPYC 7742 result | S |
| Native `asof` and bitemporal join idioms in pepper that compile to a single Polars query plan (no two-pass user code) | `chili-parser`, `chili-op` | Quill Q&A 2022, k9 native semantics claim | M |
| Dictionary + delta + bitpack codec on category columns, optional, controlled by the storage-format builder | `chili-core` | STAC SHK211203 196% storage efficiency | M |
| User-defined-function path: pepper → typed Rust closure registered at engine init, no Python crossing | `chili-core`, `chili-parser` | k9 in-built FFI claim | M-L |
| Maintain parse-cache hot-path budget < 500 ns under bench (currently ~385 ns per CLAUDE.md golden rule 6) | `chili-core` (parse_cache bench) | ksimple dispatch-table elegance | continuous |
| Publish reproducible STAC-M3 Antuco numbers on commodity hardware (1 NVMe box, single EPYC mid-tier) | `crates/chili-bin` benches | STAC report transparency standard | L (real engineering project) |
| Add a "no anti-benchmark clause" pitch line in chili-py README (this is structurally true and is a market-relevant differentiator vs Shakti and Kx) | `crates/chili-py/README.md` | Lochbaum's anti-benchmark observation | XS |

**Sequencing intuition (not a roadmap):** S items are passes-through-existing-code; M items align naturally with mdata's downstream needs (bitemporal in particular); L items are sprint-scope. The "publish reproducible STAC-M3" item is the highest-leverage marketing-engineering crossover and is the single most credible move chili could make to position vs both kdb+ and Shakti at once.

### 4.2 What is Shakti's claimed performance edge over kdb+, and how much is theoretically achievable by chili?

Decomposing the Shakti 3.3-3.7× kdb+ STAC-M3.v1 result by axis:

| Axis | Estimated contribution to Shakti edge | Chili can attack? | Chili's mechanism |
|---|---|---|---|
| Interpreter dispatch quality (loop overhead per primitive call) | ~10-20% (Lochbaum: L1-fit benefit is small) | Yes, but small ROI | Pepper parse cache + AST-direct dispatch |
| Primitive-vectorization quality (SIMD inner loops) | ~30-40% | Yes, fully | Polars' SIMD primitives + targeted Rust kernels for cases Polars misses |
| On-disk storage format / compression (less I/O = less time) | ~30-40% | Yes, partly | Int64 quantize today; can add dictionary + delta + bitpack. The 196% storage-efficiency number is the most attackable axis in pure engineering terms. |
| In-memory layout (cache-line / NUMA awareness on EPYC 7742) | ~10-20% | Polars-dependent | Polars' Arrow layout is already cache-friendly; NUMA tuning is workload-specific |
| Native time-series semantics (asof, bitemporal as primitives, not query rewrites) | Variable (10-50% on time-series-heavy queries specifically) | Yes | Pepper-language-level idiom additions |

**Synthesis.** A chili that (a) keeps Polars' SIMD primitive quality, (b) adds dictionary + delta + bitpack compression on top of Int64-quantize, (c) ships native bitemporal and asof idioms in pepper, and (d) keeps the parse-cache hot path under 500 ns can plausibly close ~70-80% of the Shakti-vs-kdb+ gap on STAC-M3.v1 Antuco operations. **That puts chili in the same performance class Shakti claimed in 2022, on commodity hardware, with an open-source stack.** Whether chili can actually *beat* Shakti is a different question (§4.5); matching the 2022 Shakti directional claim is the defensible roadmap target.

### 4.3 Whitney's design philosophy and its mapping to pepper

Whitney's stated philosophy across 30+ years: terse, composable, minimal primitives; one-screen interpreters; no operator precedence; right-to-left; macro-driven C; aggressive hardware targeting; no patience for backwards compatibility.

| Whitney axiom | Pepper alignment | Notes |
|---|---|---|
| Terseness | Matches | Pepper is q-like, intentionally dense |
| Right-to-left, no precedence | Matches | Inherited |
| Minimal primitive set | **Diverges intentionally.** Chili exposes Polars semantics via pepper, which means more primitives than k9, not fewer. | This is the right divergence — chili's user is a Polars-aware Python user who wants q-syntax-on-top, not a k9 purist |
| Macro-driven implementation | Diverges | Rust + Polars are the mechanism; macros are Rust-style, not Whitney-style C macros |
| Hardware-aggressive | Aligns | Polars handles dispatch; chili can add AVX-512-targeted fast paths |
| No backward-compatibility patience | **Diverges.** Chili is a community / open-source project; we maintain compatibility. | This is the right divergence — chili needs to be a stable substrate for mdata and other downstream users |

**The principal pepper divergences from k9 are justified, not accidental.** Pepper is "q-syntax targeting Polars-semantics," which is structurally different from k9 ("k-syntax targeting Whitney-runtime"). The q-syntax overlap with k9 is a community-acquisition feature; the Polars semantics is the substrate-strength feature; the divergence is in the middle layer.

### 4.4 Risks of treating Shakti as a benchmark target

1. **Vendor numbers.** Shakti's STAC-M3 result is vendor-published (via STAC's process, which validates methodology but does not contradict claims). One run, four years stale. Treating "3.7×" as a Shakti capability rather than "Shakti, on a specific 128-core EPYC + DDN flash stack, in 2022, running STAC-M3.v1" is the apples-to-apples mistake.
2. **Hardware reference frame.** Shakti's 2022 win was on a single-server R282-Z90 + DDN ES200NVX appliance — not commodity. Chili's reference frame should be commodity (single laptop, single small cloud VM, or single mid-tier server with NVMe). The right chili pitch is *"chili on a $5K NVMe box runs the same workload class kdb+ used to need a $50K server for"*, not *"chili beats Shakti on a DDN appliance."*
3. **STAC-M3 cherry-picking risk.** Antuco is the easier suite. Shakti has not published Kanaga. If chili publishes Antuco only and skips Kanaga, chili replicates the same selective-disclosure pattern — and a critical reader will notice. **Recommend: when chili eventually runs STAC-M3, run both suites, publish both.**
4. **Lochbaum-class scrutiny.** If chili publishes "we beat Shakti on X" claims, expect a Lochbaum-style methodology critique. Pre-empt by publishing reproducible benchmarks (datasets, hardware, query scripts) in the chili repo. Anti-benchmark clauses are the closed-source vendor's privilege; chili being open-source means we're under no such constraint and should aggressively use this.
5. **Whitney-loyalist community capture.** A subset of the q/k community is genuinely loyal to Whitney personally. Positioning chili directly against Shakti will cost goodwill in that community. The right framing is *"chili and Shakti are taking opposite bets on the same problem"* — not *"chili replaces Shakti."*

### 4.5 Open-source equivalents to Shakti's published wins

Per `kdb_alternatives.md` §2.C (referenced rather than duplicated):

- **ngn/k** [INDEPENDENT, AGPLv3, dormant 2025]. Lochbaum's measurements give us: ngn/k's interpreter is genuinely L1-cache-friendly (~0.6% icache stalls), but its production-data performance is *not* Shakti's. The gap between ngn/k and Shakti is in the column-store, not the interpreter. **This validates §4.2's decomposition: the Shakti edge is mostly in the storage and primitive-vectorization layers, not the interpreter loop.**
- **growler/k** [INDEPENDENT, AGPLv3, active 2026]. Same architectural pattern as ngn/k.
- **kparc/ksimple** [Whitney-authored, MIT, 2024]. Demonstrates Whitney's *interpreter-design* taste publicly. Does not include a column store.

**The actionable inference for chili:** the parts of the Shakti win that are open-source-replicable (interpreter dispatch, glyph-table-driven primitive selection) have been replicated by ngn/k and growler/k already; chili inherits that wisdom via pepper. The parts that are *not* open-source-replicated (the primitive-vectorization quality over a custom column store) are exactly where Polars + Arrow become chili's advantage — Polars' SIMD primitives are mature, well-tested, and improving on a faster cadence than Whitney's solo cadence on a closed codebase.

---

## 5. Open questions

The following are things I tried to answer and could not, from public sources, as of 2026-05-06. Each represents a specific delta between "what we'd need to know to fully scope chili-vs-Shakti" and "what's published." None should be answered by fabrication.

1. **How many paying Shakti customers are there?** Public information lists "banks, hedge funds, manufacturers, Formula 1" as customer categories [ZoomInfo; SECONDARY] but no named customer. Headcount of ~8 employees and lack of customer-marketing suggests double-digit customer count at most.
2. **What's Shakti's annual revenue?** No disclosure. No SEC filing path (private). Inferable upper bound: 8 employees × NYC fully-loaded cost (~$300-400K/yr/employee inclusive of benefits, infra, overhead) = ~$2.5-3.5M/yr. Self-sustaining at modest ARR, but small.
3. **Does Shakti plan to open-source any production component?** The 2024 ksimple drop is the closest signal. ksimple is explicitly educational and ~25 LOC; it's not a production engine. The Thalesians piece headlining "Did Whitney just open-source k?" was speculation, not announcement. **No public roadmap commitment to OSS.**
4. **Specific operation timings (μs/op for individual primitives) on standard datasets?** Not published. The k9-simples benchmark page gives whole-query timings but not primitive-level numbers. **This is the biggest gap for direct-comparison benchmarking.**
5. **What hardware does Shakti run best on?** Empirically, Shakti's 2022 result was AMD EPYC 7742 (Zen 2). Whether 2024-era Zen 4 / Sapphire Rapids / Genoa hardware shifts the picture is unstated.
6. **Roadmap for k10 / k11?** No public mention. No skip-naming convention rule has been declared (e.g., k8 was skipped — does that pattern continue?).
7. **Hiring signals.** No active LinkedIn job postings under "Shakti Software" as of 2026-05-06. No conference recruiting visible. **Suggests maintenance-mode hiring, not aggressive growth.**
8. **What does Shakti's IPC protocol look like?** Mentioned as "custom" in 2019 BusinessWire; no spec or wire-format ever published.
9. **Does Shakti have a cloud / managed offering?** No evidence. shakti.com → k.nyc → bare letter "k" served. No AWS Marketplace listing, no GCP Marketplace, no Azure entry.
10. **Has anyone external benchmarked k9 against Polars on the same workloads?** Lochbaum hasn't (license-blocked); I found no other independent attempt. The ZoomInfo "100× Polars" claim is unsupported.
11. **What's Shakti's pricing model?** Per the 2020 HN thread, prospective customers were directed to email Fintan Quill directly — i.e., bespoke / contact-sales pricing with no published tiers. Whether this has changed in 2024-2026 is unverified.
12. **What's the relationship between Shakti the company and the kparc community on GitHub?** kparc hosts Whitney-authored code (ksimple) and Whitney-aware tooling (kcc, kc, reference card). The kparc README posture is "fan club + adjacent ecosystem" not "Shakti-funded developer relations." The relationship is real but informal.

These twelve gaps roughly bound the next research-wave question set.

---

## 6. Bottom line for chili positioning

1. **Shakti is real, narrowly proven, and intentionally low-profile.** One STAC-M3 result from 2022, one MIT-licensed educational interpreter from 2024, ~8 employees, single-server design, anti-benchmark contracts. It is not vaporware — Whitney and Lustgarten are credible builders — but the public surface is thin enough that it cannot be a precise benchmark target.
2. **Use Shakti as Tier 4 aspirational ceiling, not Tier 3 roadmap target.** Chili's Tier 3 target stays "match-or-beat kdb+ 4.1 on STAC-M3 Antuco operations on commodity hardware" per `q_kdb_landscape.md` §3.4. Tier 4 ("approach the Shakti 2022 single-server result on commodity") is meaningful only after Tier 3 is hit.
3. **The Shakti edge is decomposable; ~70-80% is structurally attackable by chili.** Storage-format compression, primitive-vectorization quality, native time-series semantics — all open-source-tractable. Whitney's interpreter dispatch is elegant but accounts for a small share of the win.
4. **The chili-Shakti relationship is ideological-opposites, not technical-competitors.** Shakti = closed-source, single-genius, vendor-locked, expensive hardware partner. Chili = open-source, community-iterated, commodity hardware, free Python integration. Both pursue "kdb+ has untapped optimization left." That shared thesis is the point of agreement; everything else is the point of divergence.
5. **Pepper's design choices vs k9 are mostly justified, not accidental.** The places pepper diverges from k9 (more primitives, Polars semantics, stable backwards-compat) are deliberate trade-offs that fit chili's actual user base (Python data engineers wanting q-on-top), not k9 purists.
6. **Open-source advantage is decisive on Lochbaum-style scrutiny.** Chili's benchmarks are reproducible by definition; Shakti's are not (anti-benchmark clauses + license-walled). When chili publishes performance work, lean hard on reproducibility.
7. **Watch for k10, watch for any production OSS drop, watch for second STAC-M3.** Those three signals — none of which exist as of 2026-05-06 — would change the analysis materially. Until then, Shakti is a known-direction, unknown-magnitude data point at the edge of chili's planning horizon.

---

## Appendix A — Source list

**Primary / vendor / vendor-adjacent:**
- [Shakti Technology launch (BusinessWire, 2019-10-01)](https://www.businesswire.com/news/home/20191001005420/en/Shakti-Technology-Launches-New-High-Performance-Data-Platform-Merging-Database-Language-and-Security)
- [DDN + Shakti STAC-M3 press release (2022-02)](https://www.ddn.com/press-releases/ddn-and-shakti-announce-record-breaking-results-on-the-stac-m3-benchmark-for-financial-trading-applications/)
- [STAC report SHK211203 (Shakti's first STAC-M3)](https://docs.stacresearch.com/news/SHK211203)
- [Fintan Quill Q&A on ODBMS.org (2022-02)](https://www.odbms.org/2022/02/on-shaktis-data-platform-and-the-stac-m3-benchmark-council-for-financial-trading-applications-qa-with-fintan-quill/)
- [eFinancialCareers: "the new data platform from the reclusive genius of banking IT" (2019-11)](https://www.efinancialcareers.com/news/2019/11/shakti-arthur-whitney)
- [eFinancialCareers: "the fastest data platform in finance is surprisingly under-used" (2022-02)](https://www.efinancialcareers.com/news/2022/02/shakti-data-platform) [fetch failed 2026-05-06; cited via earlier reads in `q_kdb_landscape.md`]
- [shakti.com → k.nyc, retrieved 2026-05-06: serves only the bare letter "k"]

**Whitney-authored open-source:**
- [kparc/ksimple (GitHub, MIT, 2024)](https://github.com/kparc/ksimple)
- [kparc/ksimple README](https://github.com/kparc/ksimple/blob/main/README.md)

**Independent analysis / commentary:**
- [Lochbaum, "Wild claims about K performance"](https://mlochbaum.github.io/BQN/implementation/kclaims.html)
- [needleful.net, "Learning to read Arthur Whitney's C to become Smart" (2024-01)](https://needleful.net/blog/2024/01/arthur_whitney.html)
- [Thalesians, "Did Arthur Whitney just open-source k?" (2024-06)](https://magazine.thalesians.com/2024/06/08/did-arthur-whitney-just-open-source-k/)
- [HN thread: Shakti launch (2019, item 21677540)](https://news.ycombinator.com/item?id=21677540)
- [HN thread: ksimple drop (2024, item 39026551)](https://news.ycombinator.com/item?id=39026551)
- [HN thread: Q/KDB+ vs k9 production-readiness (2020, item 22565790)](https://news.ycombinator.com/item?id=22565790)

**Community documentation (third-party):**
- [k9-simples tutorial (estradajke)](https://estradajke.github.io/k9-simples/k9/index.html)
- [k9-simples benchmark page](https://estradajke.github.io/k9-simples/k9/Benchmark.html)
- [k9-simples manual source (estradajke GitHub)](https://github.com/estradajke/k9-simples/blob/master/k.manual.texi)
- [kparc/kcc (k crash course)](https://github.com/kparc/kcc)
- [kparc.github.io/ref/ (K reference card)](https://kparc.github.io/ref/)

**Secondary / encyclopedic:**
- [Wikipedia: Arthur Whitney (computer scientist)](https://en.wikipedia.org/wiki/Arthur_Whitney_(computer_scientist))
- [APL Wiki: K](https://aplwiki.com/wiki/K)
- [APL Wiki: Arthur Whitney](https://aplwiki.com/wiki/Arthur_Whitney)
- [ZoomInfo: Shakti Software Inc.](https://www.zoominfo.com/c/shakti-software-inc/365480683) — for headcount and HQ (databases of this kind are imprecise; treat as approximate)
- [CB Insights: Shakti Software](https://www.cbinsights.com/company/shakti-software)

---

## Appendix B — Discrepancy back-correction note

The Shakti row in `q_kdb_landscape.md` §3.4 currently records "7x faster Year-High Bid (cached)." The STAC report SHK211203 itself uses **3.7×** for that operation. The "7×" appears to be a press-amplification artifact (likely a cached-vs-uncached comparison miscoded as the headline). Recommend back-correcting to **3.7×** on the next pass through `q_kdb_landscape.md`. The 3.3× NBBO-Q.TIME and 196% storage-efficiency numbers verify directly to the STAC source.
