# Dev Setup — chili

Required local environment to make the pre-commit gate pass on `claude-2`. Five
minutes one-time setup; documented here because pyo3 0.27 + uv-managed standalone
Python interpreters + macOS create a non-obvious env requirement that's not
auto-detected by cargo.

## Prerequisites

- macOS (Apple Silicon or Intel) or Linux
- Rust toolchain — stable ≥ **1.95** (edition 2024). Required since the 2026-05-13 merge of upstream main brought in `sysinfo = "0.39"`, which requires rustc 1.95. No `rust-toolchain.toml` pin in-tree; run `rustup update stable` if you're behind.
- [uv](https://docs.astral.sh/uv/) ≥ 0.5

## Setup steps

### 1. Install Python ≥ 3.10 via uv

```bash
uv python install 3.12
uv python find 3.12
# Outputs an absolute path like:
#   ~/.local/share/uv/python/cpython-3.12.13-macos-aarch64-none/bin/python3.12
```

### 2. Set up `crates/chili-py/.venv`

```bash
cd crates/chili-py
uv venv --python 3.12
uv pip install -e .                  # editable install for pytest
cd ../..
```

### 3. Create `.cargo/config.toml` from the template

```bash
cp .cargo/config.toml.example .cargo/config.toml
```

Then edit `.cargo/config.toml` and replace placeholders:

- `<UV_PYTHON_PATH>` → output of `uv python find 3.12`
- `<UV_PYTHON_LIB_DIR>` → the `lib` sibling of `bin` in that path

For example, if `uv python find 3.12` outputs:
`/Users/foo/.local/share/uv/python/cpython-3.12.13-macos-aarch64-none/bin/python3.12`

Then:
- PYO3_PYTHON = the full path above
- DYLD_FALLBACK_LIBRARY_PATH = `/Users/foo/.local/share/uv/python/cpython-3.12.13-macos-aarch64-none/lib`

`.cargo/config.toml` is gitignored — never commit it (paths are machine-specific).

### 4. Verify the gate

```bash
cargo fmt --all -- --check && \
cargo clippy --all-targets -- -D warnings && \
cargo test --workspace --exclude chili-py
```

All three should pass. If `cargo test` hits `_PyExc_BaseException not found in flat
namespace` at runtime, your DYLD_FALLBACK_LIBRARY_PATH is wrong — verify
`libpython3.12.dylib` exists at the path you set.

For the chili-py side:

```bash
cd crates/chili-py && uv run maturin develop && uv run pytest
```

### 5. Install the `chili` CLI onto PATH (manual copy — no auto-tracking)

`cargo build --release -p chili-bin` produces `target/release/chili`, but that
path is **not** on PATH. The shell `chili` command resolves to
`~/.local/bin/chili`, which is a **plain copied binary**, not a symlink. After
every release build whose CLI you want to use, re-copy it:

```bash
cargo build --release -p chili-bin
cp -f target/release/chili ~/.local/bin/chili
chili --version   # confirm it matches the just-built version
```

Because it is a copy, not a symlink, it does **not** track rebuilds — skipping
the re-copy is how the on-PATH CLI silently went stale to `0.8.1` for an entire
Sprint-18/19 cycle while `target/release/chili` was current `0.8.6`. Treat
`cp -f target/release/chili ~/.local/bin/chili` as the final step of any
release-build that should be reachable from the shell.

## Why `--workspace --exclude chili-py` is the gate (load-bearing)

`chili-py`'s `Cargo.toml` declares `pyo3 = { features = ["extension-module"] }`. The
`extension-module` feature tells pyo3 NOT to emit `cargo:rustc-link-lib=python3.12`
or `cargo:rustc-link-search=...` — because the resulting `.so` is loaded by an
existing Python interpreter at runtime, not run as a standalone binary.

Cargo's workspace feature unification (even with `resolver = "2"`) merges
`extension-module` into pyo3's effective feature set whenever `cargo test`
builds the whole workspace. This means `chili-core`/`chili-op` standalone test
binaries — which DO need to link libpython since they're regular executables —
inherit the feature and fail to link with:

```
Undefined symbols for architecture arm64:
  "__Py_NoneStruct", referenced from: pyo3::err::PyErr::from_value::...
  "__Py_DecRef", referenced from: ...
  "__Py_IncRef", referenced from: ...
ld: symbol(s) not found for architecture arm64
```

Excluding `chili-py` from the cargo test invocation (`--workspace --exclude
chili-py`) prevents the feature unification from polluting non-Python crates.
chili-py's tests still run via `uv run pytest` after `maturin develop`, which
uses Python's runtime loading — no link-time `-lpython` needed there.

## Known issues

- **`Taskfile.yml` `env:` block uses `distutils.sysconfig` which is removed in
  Python 3.12.** `task test` will fail until that block is rewritten using
  `sysconfig.get_config_var('LIBDIR')`. Workaround: invoke the cargo command
  directly per "Verify the gate" above. Fix queued for a future port sprint.

- **The `.cargo/config.toml` paths are machine-specific.** If you switch Python
  versions or rebuild your uv environment, you must update these paths.
  `.cargo/config.toml.example` documents the expected shape.

## Troubleshooting

- **`cargo test` fails at link with `__Py_NoneStruct` undefined:** PYO3_PYTHON is
  unset or points at Python < 3.10. Verify `cat .cargo/config.toml` and
  `cargo build -p chili-core --tests` succeeds.
- **`cargo test` succeeds at link but SIGABRTs at runtime with `_PyExc_BaseException
  not found in flat namespace`:** DYLD_FALLBACK_LIBRARY_PATH is unset or wrong.
  Verify `ls $(grep DYLD .cargo/config.toml | cut -d'"' -f2)/libpython3.12.dylib`.
- **`uv run pytest` fails with `ImportError: chili module not found`:** maturin
  didn't install the wheel. Re-run `cd crates/chili-py && uv run maturin develop`.
- **`chili --version` shows an old version after a fresh release build:**
  `~/.local/bin/chili` is a stale copy — it does not track `target/release/`.
  Re-run `cp -f target/release/chili ~/.local/bin/chili` (see step 5).

## Cross-references

- `CLAUDE.md` — Pre-commit gate section (reference)
- `.cargo/config.toml.example` — the actual template
- Taskfile.yml — `task test` runner (currently broken on Python 3.12; see Known issues)
- `~/code/chili/docs/standards/iteration_lessons.md` — lesson 4 (cherry-pick conflict
  accumulation), the pivot whose Part A productionized this setup.
