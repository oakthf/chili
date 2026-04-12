![chili](https://github.com/purple-chili/chili/blob/main/icon.png?raw=true)

![PyPI - Version](https://img.shields.io/pypi/v/chili-pie?style=for-the-badge&color=%2321759b) ![Visual Studio Marketplace Version](https://img.shields.io/visual-studio-marketplace/v/jshinonome.vscode-chili?style=for-the-badge&label=vscode%20ext&color=%23007ACC) ![PyPI - Version](https://img.shields.io/pypi/v/chiz?style=for-the-badge&label=chiz&color=%23FFA600)

# Chili: for data analysis and engineering.

## Language syntaxes

[Chili](https://purple-chili.github.io/) uses [Polars](https://www.pola.rs/) for data manipulation and [Arrow](https://arrow.apache.org/)/[Parquet](https://parquet.apache.org/) for data storage. It provides two language syntaxes:

- chili: a modern programming language similar to `javascript`.
- pepper: a vintage programming language similar to `q`.

## Performance

After the 2026-04-12 optimization sweep (Phases 1-7, 14 individual proposals — see `docs/bench/summary.md` and `CHANGELOG.md`):

| Workload | Before sweep | After sweep | Speedup |
|---|---:|---:|---:|
| Single-date equality query | 2.81 ms | 2.25 ms | 1.25× |
| Narrow range (5 partitions) | 11.05 ms | 5.78 ms | 1.91× |
| Wide range (500 partitions) | 988 ms | 363 ms | **2.72×** |
| Group-by aggregation | 7.66 ms | 3.30 ms | **2.32×** |
| Small `select * where date=X` | 1.52 ms | 365 µs | **4.16×** |
| Multi-table HDB load | 2.87 ms | 1.53 ms | **1.88×** |
| Repeat-query parse (cache hit) | 374 µs | **385 ns** | **970×** |
| Python concurrent throughput (8 threads) | 519 q/s | **3168 q/s** | **6.10×** |

The Python concurrent number is the headline: chili-py released the GIL around `Engine::eval`, so an 8-thread Python client can now drive the engine in parallel instead of serializing through the GIL. Pre-sweep, 8 threads were *slower* than 1 thread (0.92× speedup); post-sweep, 8 threads scale to 2.47× single-thread.

Run the benchmarks yourself:
```bash
cargo bench -p chili-op --bench scan -- --save-baseline mine
cargo bench -p chili-op --bench eval
cargo bench -p chili-op --bench load_par_df
cargo bench -p chili-op --bench write_partition
cargo bench -p chili-core --bench parse_cache
cd crates/chili-py && uv run python tests/bench_concurrent.py --save mine
```

## References

- [Polars](https://www.pola.rs/): an open-source library for data manipulation.
- [chumsky](https://github.com/zesterer/chumsky): a high performance parser.
- [kola](https://github.com/jshinonome/kola): source code is reused for interfacing with `q`.
- [Nushell](https://www.nushell.sh/): a new type of shell.

Thanks to the authors of these projects and all other open source projects for their great work.
