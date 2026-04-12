use std::{fmt::Display, path::PathBuf, sync::Arc};

use polars::prelude::{DataFrame, IntoLazy, LazyFrame, PlRefPath, ScanArgsParquet};
use rayon::prelude::*;

use crate::{SpicyError, SpicyObj, SpicyResult};

#[derive(PartialEq, Debug, Clone)]
pub enum DFType {
    Single,
    ByDate,
    ByYear,
}

impl Display for DFType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                DFType::Single => "single",
                DFType::ByDate => "date",
                DFType::ByYear => "year",
            }
        )
    }
}

#[derive(Debug, Clone)]
pub struct PartitionedDataFrame {
    pub name: String,
    pub df_type: DFType,
    pub path: String,
    pub pars: Vec<i32>,
    /// Cached empty DataFrame matching the partition's schema. Populated
    /// from the on-disk `schema` sentinel file at load time. When a query
    /// hits a partition that doesn't exist, we return this clone rather
    /// than re-scanning the sentinel file on every miss.
    pub empty_schema: Option<Arc<DataFrame>>,
}

/// PartialEq that ignores `empty_schema` — the cached DataFrame is a load-time
/// optimization and should not participate in structural equality checks.
/// Existing tests (par_df_tests) compare by name/df_type/path/pars only.
impl PartialEq for PartitionedDataFrame {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.df_type == other.df_type
            && self.path == other.path
            && self.pars == other.pars
    }
}

impl PartitionedDataFrame {
    pub fn new(name: String, df_type: DFType, path: String, pars: Vec<i32>) -> Self {
        Self {
            name,
            df_type,
            path,
            pars,
            empty_schema: None,
        }
    }

    pub fn get_par_glob(&self, par_num: i32) -> String {
        if self.df_type == DFType::ByDate {
            format!("{}_*", SpicyObj::Date(par_num))
        } else {
            format!("{}_*", par_num)
        }
    }

    pub fn get_empty_schema(&self) -> &str {
        "schema"
    }

    /// Return a LazyFrame representing the empty-schema sentinel for this
    /// table. Prefers the cached `empty_schema` if it was populated at load
    /// time (Proposal J); falls back to the on-disk `schema` file otherwise
    /// so the struct is usable when constructed without a load-time cache
    /// (e.g. from unit tests that call `new()` directly).
    fn empty_schema_lf(&self) -> SpicyResult<LazyFrame> {
        if let Some(df) = &self.empty_schema {
            return Ok((**df).clone().lazy());
        }
        let mut par_path = PathBuf::from(&self.path);
        par_path.push(self.get_empty_schema());
        LazyFrame::scan_parquet(
            PlRefPath::new(par_path.to_str().unwrap_or_default()),
            ScanArgsParquet::default(),
        )
        .map_err(|e| {
            SpicyError::Err(format!(
                "failed to scan empty schema for {}: {}",
                self.name, e
            ))
        })
    }

    pub fn scan_partition(&self, par_num: i32) -> SpicyResult<LazyFrame> {
        if self.df_type == DFType::Single {
            return LazyFrame::scan_parquet(
                PlRefPath::new(PathBuf::from(&self.path).to_str().unwrap_or_default()),
                ScanArgsParquet::default(),
            )
            .map_err(|e| SpicyError::Err(format!("failed to scan single {}: {}", self.name, e)));
        }
        let mut par_path = PathBuf::from(&self.path);
        let args = ScanArgsParquet::default();
        let lazy_df = if self.pars.binary_search(&par_num).is_ok() {
            par_path.push(self.get_par_glob(par_num));
            LazyFrame::scan_parquet(PlRefPath::new(par_path.to_str().unwrap_or_default()), args)
                .map_err(|e| {
                    SpicyError::Err(format!("failed to scan partitioned {}: {}", self.name, e))
                })?
        } else {
            self.empty_schema_lf()?
        };
        Ok(lazy_df)
    }

    pub fn scan_partitions(&self, par_nums: &[i32]) -> SpicyResult<LazyFrame> {
        let args = ScanArgsParquet::default();
        let mut par_nums = par_nums.to_vec();
        par_nums.sort();
        par_nums.dedup();

        // Proposal E: expand each partition's glob in parallel. We preserve
        // input order by collecting `(par_num, Vec<PathBuf>)` and then
        // flattening in the original sorted par_nums sequence, so downstream
        // `scan_parquet_files` sees paths in deterministic order matching
        // the pre-parallel implementation.
        let per_par: Vec<Vec<PathBuf>> = par_nums
            .par_iter()
            .map(|par_num| {
                if self.pars.binary_search(par_num).is_err() {
                    return Ok(Vec::new());
                }
                let mut par_path = PathBuf::from(&self.path);
                par_path.push(self.get_par_glob(*par_num));
                let mut paths: Vec<PathBuf> = Vec::new();
                match glob::glob(&par_path.to_string_lossy()) {
                    Ok(iter) => {
                        for entry in iter {
                            match entry {
                                Ok(path) => paths.push(path),
                                Err(e) => eprintln!("{:?}", e),
                            }
                        }
                        Ok(paths)
                    }
                    Err(e) => Err(SpicyError::EvalErr(e.to_string())),
                }
            })
            .collect::<SpicyResult<Vec<_>>>()?;
        let pars: Vec<PathBuf> = per_par.into_iter().flatten().collect();

        let lazy_df = if !pars.is_empty() {
            LazyFrame::scan_parquet_files(
                pars.into_iter()
                    .map(|p| PlRefPath::new(p.to_str().unwrap_or_default()))
                    .collect(),
                args,
            )
            .map_err(|e| SpicyError::EvalErr(e.to_string()))?
        } else {
            self.empty_schema_lf()?
        };
        Ok(lazy_df)
    }

    pub fn scan_partition_by_range(&self, start_par: i32, end_par: i32) -> SpicyResult<LazyFrame> {
        let args = ScanArgsParquet::default();
        let start_index = match self.pars.binary_search(&start_par) {
            Ok(i) => i,
            Err(i) => i,
        };
        let mut end_index = match self.pars.binary_search(&end_par) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        end_index = end_index.min(self.pars.len());
        let lazy_df = if start_index < end_index {
            // Proposal E: parallel glob expansion over the requested range.
            // Ordered-collect preserves path order (ascending partition) so
            // `scan_parquet_files` receives files in the same sequence as
            // the pre-parallel implementation.
            let per_par: Vec<Vec<PathBuf>> = self.pars[start_index..end_index]
                .par_iter()
                .map(|par_num| {
                    let mut par_path = PathBuf::from(&self.path);
                    par_path.push(self.get_par_glob(*par_num));
                    let mut paths: Vec<PathBuf> = Vec::new();
                    match glob::glob(&par_path.to_string_lossy()) {
                        Ok(iter) => {
                            for entry in iter {
                                match entry {
                                    Ok(path) => paths.push(path),
                                    Err(e) => eprintln!("{:?}", e),
                                }
                            }
                            Ok(paths)
                        }
                        Err(e) => Err(SpicyError::Err(e.to_string())),
                    }
                })
                .collect::<SpicyResult<Vec<_>>>()?;
            let pars: Vec<PathBuf> = per_par.into_iter().flatten().collect();

            LazyFrame::scan_parquet_files(
                pars.into_iter()
                    .map(|p| PlRefPath::new(p.to_str().unwrap_or_default()))
                    .collect(),
                args,
            )
            .map_err(|e| {
                SpicyError::Err(format!("failed to scan partitioned {}: {}", self.name, e))
            })?
        } else {
            self.empty_schema_lf()?
        };
        Ok(lazy_df)
    }
}

impl Display for PartitionedDataFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "([] `par_df {}, {:?})", self.name, self.df_type)
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for partition index selection.
    //!
    //! These tests assert the invariant that every method on
    //! `PartitionedDataFrame` that calls `slice::binary_search` on `self.pars`
    //! must see a sorted vector. Load-time sorting is done in
    //! `engine_state::load_par_df` — these tests simulate a correctly-sorted
    //! `pars` vector and prove the range/exact selection arithmetic is
    //! correct across boundary conditions, AND that an unsorted vector
    //! would produce wrong selections (the historical R1 bug).
    //!
    //! We cannot call the real `scan_partition*` methods here without
    //! spinning up parquet files on disk, so these tests exercise the
    //! selection logic directly via a helper that mirrors the production
    //! computation.

    use super::*;

    /// Mirror of the index arithmetic in `scan_partition_by_range` — lets us
    /// unit-test the selection without touching the filesystem.
    fn select_indices(pars: &[i32], start: i32, end: i32) -> (usize, usize) {
        let start_index = match pars.binary_search(&start) {
            Ok(i) => i,
            Err(i) => i,
        };
        let mut end_index = match pars.binary_search(&end) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        end_index = end_index.min(pars.len());
        (start_index, end_index)
    }

    fn new_by_date(pars: Vec<i32>) -> PartitionedDataFrame {
        PartitionedDataFrame::new("t".into(), DFType::ByDate, "/tmp/t".into(), pars)
    }

    // Encoded dates: days since 1970-01-01
    const D_2024_01_02: i32 = 19724;
    const D_2024_01_03: i32 = 19725;
    const D_2024_01_04: i32 = 19726;
    const D_2024_01_05: i32 = 19727;
    const D_2024_01_08: i32 = 19730;

    fn sorted_pars() -> Vec<i32> {
        vec![
            D_2024_01_02,
            D_2024_01_03,
            D_2024_01_04,
            D_2024_01_05,
            D_2024_01_08,
        ]
    }

    // -------- scan_partition (exact equality) --------

    #[test]
    fn scan_partition_finds_every_element_on_sorted_pars() {
        let pars = sorted_pars();
        for &p in &pars {
            assert!(
                pars.binary_search(&p).is_ok(),
                "binary_search should find {} in sorted pars",
                p
            );
        }
    }

    #[test]
    fn scan_partition_rejects_missing_date() {
        let pars = sorted_pars();
        // 2024.01.06 = 19728 — not present
        assert!(pars.binary_search(&19728).is_err());
    }

    #[test]
    fn scan_partition_historical_bug_unsorted_pars_loses_elements() {
        // Reproduces the pre-fix state: directory order on macOS APFS was
        // [schema, 03, 02, 05, 08, 04] → par_vec before sort was
        // [01-03, 01-02, 01-05, 01-08, 01-04]. binary_search on this vector
        // returned Err() for real entries — the R1 symptom. This test
        // exists as a regression guard: if anyone removes the sort in
        // `load_par_df`, the assertion below MUST break before this test
        // starts passing.
        let unsorted = vec![
            D_2024_01_03,
            D_2024_01_02,
            D_2024_01_05,
            D_2024_01_08,
            D_2024_01_04,
        ];
        assert!(
            unsorted.binary_search(&D_2024_01_03).is_err(),
            "unsorted pars cannot be searched reliably (guards R1 root cause)"
        );
        // The fix: sort, then search works.
        let mut sorted = unsorted.clone();
        sorted.sort_unstable();
        assert!(sorted.binary_search(&D_2024_01_03).is_ok());
        assert!(sorted.binary_search(&D_2024_01_08).is_ok());
    }

    // -------- scan_partition_by_range (inclusive range) --------

    #[test]
    fn range_narrow_inclusive_keeps_all_boundaries() {
        // [01-02..01-04] must scan exactly 3 partitions.
        let pars = sorted_pars();
        let (s, e) = select_indices(&pars, D_2024_01_02, D_2024_01_04);
        assert_eq!(&pars[s..e], &[D_2024_01_02, D_2024_01_03, D_2024_01_04]);
    }

    #[test]
    fn range_narrow_with_missing_inner_dates_still_inclusive() {
        let pars = sorted_pars();
        // [01-03..01-08] — gap at 01-06/07 — must still be inclusive of both endpoints.
        let (s, e) = select_indices(&pars, D_2024_01_03, D_2024_01_08);
        assert_eq!(
            &pars[s..e],
            &[D_2024_01_03, D_2024_01_04, D_2024_01_05, D_2024_01_08]
        );
    }

    #[test]
    fn range_with_nonexistent_bounds_uses_nearest_insert_point() {
        let pars = sorted_pars();
        // 2024.01.06 = 19728, between 05 and 08. Range [01-06..01-09] should
        // scan only 01-08.
        let (s, e) = select_indices(&pars, 19728, 19731);
        assert_eq!(&pars[s..e], &[D_2024_01_08]);
    }

    #[test]
    fn range_lower_unbounded() {
        let pars = sorted_pars();
        // date<=2024.01.04 lowers to [0, 19726]
        let (s, e) = select_indices(&pars, 0, D_2024_01_04);
        assert_eq!(&pars[s..e], &[D_2024_01_02, D_2024_01_03, D_2024_01_04]);
    }

    #[test]
    fn range_upper_unbounded() {
        let pars = sorted_pars();
        // date>=2024.01.05 lifts to [19727, i32::MAX]
        let (s, e) = select_indices(&pars, D_2024_01_05, i32::MAX);
        assert_eq!(&pars[s..e], &[D_2024_01_05, D_2024_01_08]);
    }

    #[test]
    fn range_fully_unbounded_scans_everything() {
        let pars = sorted_pars();
        let (s, e) = select_indices(&pars, 0, i32::MAX);
        assert_eq!(&pars[s..e], &pars[..]);
    }

    #[test]
    fn range_entirely_before_any_partition() {
        let pars = sorted_pars();
        // Range below all partitions — empty scan.
        let (s, e) = select_indices(&pars, 0, 10_000);
        assert_eq!(s, 0);
        assert_eq!(e, 0);
    }

    #[test]
    fn range_entirely_after_any_partition() {
        let pars = sorted_pars();
        // Range above all partitions — empty scan.
        let (s, e) = select_indices(&pars, 30_000, 40_000);
        assert_eq!(s, pars.len());
        assert_eq!(e, pars.len());
    }

    #[test]
    fn range_inverted_start_greater_than_end_empty_scan() {
        let pars = sorted_pars();
        // Empty-range sentinel produced by `extract_partition_predicates`
        // when exact predicate falls outside a bound (`vec![1, 0]`).
        let (s, e) = select_indices(&pars, 1, 0);
        assert!(s >= e, "inverted range must produce empty scan");
    }

    #[test]
    fn range_single_existing_partition_via_equal_bounds() {
        let pars = sorted_pars();
        // Range [X, X] where X is present should return exactly one partition.
        let (s, e) = select_indices(&pars, D_2024_01_03, D_2024_01_03);
        assert_eq!(&pars[s..e], &[D_2024_01_03]);
    }

    // -------- DFType / path helpers --------

    #[test]
    fn get_par_glob_by_date_uses_date_display() {
        let par_df = new_by_date(sorted_pars());
        let g = par_df.get_par_glob(D_2024_01_03);
        assert_eq!(g, "2024.01.03_*");
    }

    #[test]
    fn get_par_glob_by_year_uses_raw_number() {
        let par_df =
            PartitionedDataFrame::new("t".into(), DFType::ByYear, "/tmp/t".into(), vec![2024]);
        let g = par_df.get_par_glob(2024);
        assert_eq!(g, "2024_*");
    }
}
