//! Sprint 22 W1 — `eval_str` pepper builtin guard tests (chili-core side).
//!
//! `eval_str` is the new (Sprint 22) side-effect builtin that takes a
//! pepper-source STRING and evaluates it on the engine, returning the
//! raw `SpicyObj` (no stringification, no row-limit) — unlike `evalc`
//! (returns `SpicyObj::String(obj.to_string())`) and `evali` (row-limits
//! tabular results). Designed for mdata's chili-IPC qcon REPL — see
//! `docs/sync/mdata_wishlist_2026-05-23_remote-eval-surface.md`.
//!
//! NOTE: arithmetic / operator builtins (`+`, `-`, `*`, ...) live in
//! `chili-op::BUILT_IN_FN`, which `EngineState::initialize()` does NOT
//! load (chili-py's ChiliEngine ctor does — see `crates/chili-py/src/lib.rs:362-363`).
//! So these tests use literal-and-assign expressions only; the end-to-end
//! arithmetic-eval contract is covered by the Python pytest
//! `crates/chili-py/tests/test_eval_str.py` (the MC-4 closure gate).

use chili_core::{EngineState, SpicyObj};

fn engine_chili() -> EngineState {
    EngineState::initialize()
}

fn engine_pepper() -> EngineState {
    let mut s = EngineState::initialize();
    s.enable_pepper();
    s
}

/// `fn_call("eval_str", &[&String("42")])` must return `I64(42)`, NOT
/// `String("42")`. This is the contract delta vs `evalc`.
#[test]
fn eval_str_returns_raw_literal_not_stringified() {
    let e = engine_chili();
    let res = e
        .fn_call("eval_str", &[&SpicyObj::String("42".to_owned())])
        .expect("eval_str on '42' must succeed");
    assert_eq!(
        res,
        SpicyObj::I64(42),
        "eval_str must return the raw evaluated I64, not a stringified one — \
         that is the contract delta vs evalc. Got: {res:?}"
    );
}

/// `eval_str` evaluates assignment + variable lookup (no operators needed),
/// confirming the parse + eval_ast chain works end-to-end and state is
/// mutated as expected.
#[test]
fn eval_str_assign_then_read_returns_raw_value() {
    let e = engine_chili();
    let res = e
        .fn_call("eval_str", &[&SpicyObj::String("x: 100; x".to_owned())])
        .expect("eval_str on 'x: 100; x' must succeed");
    assert_eq!(
        res,
        SpicyObj::I64(100),
        "eval_str must return the raw value (I64), not a stringified one. Got: {res:?}"
    );
    // Side-effect on the engine state: `x` is now bound.
    assert_eq!(
        e.get_var("x").expect("x must be bound after eval_str"),
        SpicyObj::I64(100),
    );
}

/// Same contract verified in pepper syntax (mdata uses pepper via
/// `ChiliEngine(pepper=True)`). Mirror the
/// `crates/chili-py/tests/test_pepper_syntax.py:42` convention.
#[test]
fn eval_str_works_in_pepper_syntax() {
    let e = engine_pepper();
    let res = e
        .fn_call("eval_str", &[&SpicyObj::String("a: 7; a".to_owned())])
        .expect("eval_str on pepper 'a: 7; a' must succeed");
    assert_eq!(
        res,
        SpicyObj::I64(7),
        "eval_str must return raw I64 under pepper syntax too. Got: {res:?}"
    );
}

/// `eval_str` must error (NOT panic) on a non-string-like argument; the
/// `validate_args(args, &[ArgType::StrOrSym])` call is the gate. Symbol
/// is ACCEPTED (chili-py FFI sends Symbol for Python str — see comment in
/// eval.rs). Numeric / List / etc. are rejected.
#[test]
fn eval_str_errors_on_non_string_arg() {
    let e = engine_chili();
    let res = e.fn_call("eval_str", &[&SpicyObj::I64(42)]);
    assert!(
        res.is_err(),
        "eval_str must reject a non-string-like arg; got Ok({res:?})"
    );
}

/// `eval_str` accepts `SpicyObj::Symbol` as source — this is the actual
/// over-the-wire shape from chili-py's Python `str` → `Symbol` FFI
/// conversion (see crates/chili-py/src/lib.rs:111).
#[test]
fn eval_str_accepts_symbol_source() {
    let e = engine_chili();
    let res = e
        .fn_call("eval_str", &[&SpicyObj::Symbol("42".to_owned())])
        .expect("eval_str must accept Symbol source (FFI-shape from chili-py)");
    assert_eq!(res, SpicyObj::I64(42));
}

/// `eval_str` must error (NOT panic) when the source fails to parse.
/// The parse error is mapped to `SpicyError::EvalErr` so the caller
/// (mdata) sees a `ChiliError`, never a Rust panic.
#[test]
fn eval_str_errors_on_parse_failure() {
    let e = engine_chili();
    let res = e.fn_call("eval_str", &[&SpicyObj::String("invalid )(*&".to_owned())]);
    assert!(
        res.is_err(),
        "eval_str must surface a parse error as Err, not panic. Got Ok({res:?})"
    );
}
