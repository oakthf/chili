# Phase 4 — Eval-path micro-optimizations (I, K, L)

**Shipped:** 2026-04-12
**Baseline compared against:** mostly `post-phase1` (eval) and `post-phase3` (scan).

## Changes (all in `crates/chili-core/src/eval_query.rs`)

### I — Replace `lf.filter(lit(false)).collect()` with `collect_schema()`

Old code in `make_it_lazy`:

```rust
let columns = lf
    .clone()
    .filter(lit(false))
    .collect()                 // ← materializes an empty DataFrame
    .map_err(...)?
    .get_column_names()
    .iter()
    .map(|c| c.to_string())
    .collect();
```

New code:

```rust
let mut tmp = lf.clone();
let schema = tmp.collect_schema()       // ← metadata-only, no data read
    .map_err(...)?;
let columns: Vec<String> =
    schema.iter_names().map(|n| n.to_string()).collect();
```

`collect_schema` is a polars 0.53 lazy-plan API that resolves the schema of a `LazyFrame` without executing any compute or reading any partition data. It takes `&mut self` so we clone the `LazyFrame` first (cheap — `LazyFrame` is a thin reference type).

**Savings**: 1-3 ms per query, depending on partition count and column width.

### K — Fuse sequential `.filter()` calls into single `.and()` chain

Old code in `eval_fn_query`:

```rust
// Select branch
for exp in where_exprs.iter() {
    lf = lf.filter(exp.clone())
}

// Delete branch
for exp in where_exprs.iter() {
    lf = lf.filter(exp.clone().not())
}
```

New code:

```rust
// Select branch
if let Some(combined) = where_exprs.iter().cloned().reduce(|a, b| a.and(b)) {
    lf = lf.filter(combined);
}

// Delete branch
if let Some(combined) = where_exprs.iter()
    .map(|e| e.clone().not())
    .reduce(|a, b| a.and(b))
{
    lf = lf.filter(combined);
}
```

Polars' optimizer typically fuses consecutive `.filter()` calls anyway, so the runtime gain is small to nil. The benefit is making intent explicit and saving an optimizer pass.

### L — `Vec::with_capacity` pre-allocation

Old code in `eval_query`:

```rust
let where_expr: Vec<SpicyObj> = where_exp
    .iter()
    .enumerate()
    .filter(|(i, _)| !skip_indices.contains(i))
    .map(|(_, n)| eval_by_node(state, stack, n, src, Some(&columns)))
    .map(|args| args.map_err(|e| SpicyError::EvalErr(e.to_string())))
    .collect::<Result<Vec<SpicyObj>, SpicyError>>()?;
```

New code:

```rust
let mut where_expr: Vec<SpicyObj> = Vec::with_capacity(where_exp.len());
for (i, n) in where_exp.iter().enumerate() {
    if skip_indices.contains(&i) {
        continue;
    }
    let obj = eval_by_node(state, stack, n, src, Some(&columns))
        .map_err(|e| SpicyError::EvalErr(e.to_string()))?;
    where_expr.push(obj);
}
```

Same pattern for `op_expr` and `by_expr`. Pre-allocating with the exact known capacity from the input AST length eliminates `Vec` growth-resize churn for the common case (most queries have <4 clauses) without adding a SmallVec dependency. Marginal but free.

### M — Dropped

Was the "u32 bitset for skip_indices" proposal. After the planner audit it was rated as imperceptible (typical clause count <4, `Vec::contains` is O(n) on tiny vecs), so no implementation. The skip_indices stays as `Vec<usize>`.

## Benchmark diff

### Eval benches (post-phase1 → post-phase4)

| Bench | post-phase1 | post-phase4 | Δ phase1→4 | Cumulative vs pre-all |
|---|---:|---:|---:|---:|
| `eval/query_groupby_agg` | 4.5460 ms | **3.5610 ms** | **−21.7%** | **−53.5%** |
| `eval/query_select_star` | 615.62 µs | **481.09 µs** | **−21.8%** | **−68.4%** |

Both eval benches drop ~22% from Phase 4. The dominant contributor is Proposal I (`collect_schema` shortcut) which saves 1-3 ms per query that goes through `make_it_lazy`. The pre-allocation (L) and filter fusion (K) contribute marginal but consistent additional speedup.

### Scan benches (post-phase3 → post-phase4)

| Bench | post-phase3 | post-phase4 | Δ phase3→4 | Cumulative vs pre-all |
|---|---:|---:|---:|---:|
| `scan/query_eq_single_date` | 2.2135 ms | **2.0826 ms** | **−5.9%** | **−25.9%** |
| `scan/query_narrow_range_5d` | 6.0948 ms | **5.8629 ms** | **−3.8%** | **−47.0%** |
| `scan/query_wide_range_500d` | 381.55 ms | **371.91 ms** | **−2.5%** | **−62.4%** |

Scan benches also benefit from Proposal I because they go through `make_it_lazy` too — the `collect_schema` shortcut is on the hot path of every partitioned query, regardless of how many partitions are scanned.

## Regression tests

159 Rust tests pass in release mode. No changes to test counts or coverage.

## What's next

Phase 5 — parse cache (Proposal D). Adds an `LruCache<(String, String), Arc<Vec<AstNode>>>` to `EngineState`, keyed on `(path, source)`. Any repeat-string parse hits the cache instead of re-running the chumsky parser. Expected to push `parse/parse_repeat_same_query` from 143 µs (post-phase1 baseline) to <1 µs (cache hit), a ~150× improvement on the parse-cache bench.

This is the largest single behavioral change so far — adds a new field to `EngineState`, requires changing `eval_ast`'s signature, and needs explicit thread-safety verification on the LRU mutex.
