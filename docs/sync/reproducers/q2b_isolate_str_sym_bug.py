"""Q2b — isolate where the str-vs-sym bug lives on main 0.9.0.

The path that errors:
  roll_tick_log -> .tick.rollLog (pepper) -> .handle.rotate[.tick.msgHandle; .tick.logFile]
  -> Rust rotate_handle expects str, gets sym.

Question: is the bug in (a) .handle.rotate being too strict, or (b) the
pepper concat producing a sym from logDir + filename?
"""
import tempfile
from datetime import date

from chili import ChiliEngine

eng = ChiliEngine(pepper=True)

# Direct call to .handle.rotate with explicit String literals via bytes
with tempfile.TemporaryDirectory() as log_dir:
    target_a = log_dir + "/target_a"
    eng.eval(f'.h.test: .handle.open "file://{target_a}"')
    print(f"opened handle to {target_a}")

    # Verify the handle exists and check the type of the URI it has
    h = eng.get_var(".h.test")
    print(f".h.test = {h!r}")

    # Now try rotate via pepper, passing the URI as a String literal (in pepper, "..." is str)
    target_b = log_dir + "/target_b"
    print(f"\nattempting .handle.rotate via pepper str literal...")
    try:
        eng.eval(f'.handle.rotate[.h.test; "file://{target_b}"]')
        print(f"  SUCCESS — .handle.rotate accepts str literals from pepper")
    except Exception as e:
        print(f"  RAISED: {e}")

    # Now try via Python — passing a Python str
    target_c = log_dir + "/target_c"
    print(f"\nattempting via Python — fn_call('.handle.rotate', [h, str])...")
    try:
        eng.fn_call(".handle.rotate", [h, f"file://{target_c}"])
        print(f"  SUCCESS — fn_call works")
    except Exception as e:
        print(f"  RAISED: {e}")

    # Try via Python bytes
    target_d = log_dir + "/target_d"
    print(f"\nattempting via Python — fn_call('.handle.rotate', [h, bytes])...")
    try:
        eng.fn_call(".handle.rotate", [h, f"file://{target_d}".encode()])
        print(f"  SUCCESS — bytes works")
    except Exception as e:
        print(f"  RAISED: {e}")

    # Type-introspect what `.tick.msgLog`-style concat produces
    print(f"\ntype inspection of pepper string concat...")
    eng.eval('.test.s1: "foo"')          # pepper str
    eng.eval('.test.s2: "bar"')          # pepper str
    eng.eval('.test.concat: .test.s1 + .test.s2')  # what type?
    res = eng.eval('.test.concat')
    print(f"  '\"foo\" + \"bar\"' (pepper) -> Python {type(res).__name__}: {res!r}")

    # Now from Python (passing in a sym via Python str)
    eng.set_var(".test.py_str", "abc")  # Python str -> SpicyObj::Symbol
    eng.eval('.test.py_str_kind: $["str";.test.py_str]')  # peek
    eng.eval('.test.concat2: "prefix-" + .test.py_str')  # str + sym = ???
    try:
        res2 = eng.eval('.test.concat2')
        print(f"  '\"prefix-\" + (py str=sym)': Python {type(res2).__name__}: {res2!r}")
    except Exception as e:
        print(f"  '\"prefix-\" + (py str=sym)' RAISED: {e}")
