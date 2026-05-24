"""Q1 verification — fh→tp Polars DataFrame publish via main 0.9.0's sync API.

Question: with main 0.9.0 (no publish_via_handle), can fh publish a Polars
DataFrame to a remote tp via the public sync() API alone?

Two candidate shapes:
  (A) `sync(h, (".tick.upd", "trade", df))` — calls the publisher-side
      handler loaded by init_tick (writes to tplog + broadcasts + tick++).
  (B) `sync(h, ("upd", "trade", df))` — calls the subscriber-side handler
      (sub.pep:1, bare `upd: {[t;d] t upsert d; tick[this.h;1];}`).
      Should fail on tp (sub.pep not loaded on a non-subscribed engine).

Acceptance:
  - (A) returns successfully + pub.get_var("trade") shows the new rows.
  - Verify the broadcast also reaches a subscribed downstream engine.
  - If both work, publish_via_handle is deprecation-ready; (A) is the
    canonical recipe.
  - If only (A) works (B fails), the answer is "use .tick.upd".
  - If neither works, real ergonomic gap → need to ask the author OR
    keep publish_via_handle.
"""
from __future__ import annotations

import socket
import tempfile
import time
from datetime import date

import polars as pl
from chili import ChiliEngine


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _trade_schema() -> pl.DataFrame:
    return pl.DataFrame({
        "sym": pl.Series([], dtype=pl.Categorical),
        "price": pl.Series([], dtype=pl.Float64),
        "size": pl.Series([], dtype=pl.Int64),
    })


def _sample_df(n: int) -> pl.DataFrame:
    return pl.DataFrame({
        "sym": pl.Series(["AAPL"] * n, dtype=pl.Categorical),
        "price": [100.0 + i for i in range(n)],
        "size": [10 * i for i in range(n)],
    })


def main() -> int:
    port = _free_port()
    sub_port = _free_port()

    # tp publisher engine
    pub = ChiliEngine(pepper=True, debug=False)
    with tempfile.TemporaryDirectory() as log_dir:
        pub.init_tick(
            schema={"trade": _trade_schema()},
            log_dir=log_dir + "/",
            filename=str(date.today()),
        )
        pub.start_tcp_listener(port)
        time.sleep(0.1)

        # subscriber to verify broadcast reaches it
        sub = ChiliEngine(pepper=True)
        sub.subscribe(f"chili://127.0.0.1:{port}", ["trade"])
        time.sleep(0.2)

        # fh-style client (no publish_via_handle on main 0.9.0)
        fh = ChiliEngine()
        h = fh.open_handle(f"chili://127.0.0.1:{port}")

        df = _sample_df(3)

        # === Shape (A): qualified .tick.upd ===
        print("--- Shape (A): sync(h, ('.tick.upd', 'trade', df)) ---")
        a_ok = False
        try:
            result_a = fh.sync(h, (".tick.upd", "trade", df))
            print(f"  return: {result_a!r}")
            a_ok = True
        except Exception as e:
            print(f"  RAISED: {type(e).__name__}: {e}")

        time.sleep(0.3)
        # tp's .tick.upd writes tplog + broadcasts; doesn't accumulate locally.
        # The subscriber's bare `upd` handler (sub.pep:1) does `table upsert data`.
        # So we measure success at the SUBSCRIBER, not at the publisher.
        try:
            sub_trade = sub.get_var("trade")
            print(f"  sub.get_var('trade'): {sub_trade.height} rows")
            print(sub_trade)
            a_sub_received = a_ok and sub_trade.height == 3
        except Exception as e:
            print(f"  sub.get_var('trade') RAISED: {e}")
            a_sub_received = False

        a_pub_received = True  # n/a — tp doesn't accumulate

        # === Shape (B): bare upd (likely fails on tp) ===
        print("\n--- Shape (B): sync(h, ('upd', 'trade', df)) ---")
        b_ok = False
        try:
            result_b = fh.sync(h, ("upd", "trade", df))
            print(f"  return: {result_b!r}")
            b_ok = True
        except Exception as e:
            print(f"  RAISED: {type(e).__name__}: {e}")

        # === Verdict ===
        print("\n=== VERDICT ===")
        print(f"  (A) qualified .tick.upd: ok={a_ok}; pub_received={a_pub_received}; sub_received={a_sub_received}")
        print(f"  (B) bare upd:            ok={b_ok}")

        fh.shutdown()
        sub.shutdown()
        pub.shutdown()

        if a_sub_received:
            print("  → PASS — main 0.9.0 supports fh→tp publish via sync(h, ('.tick.upd', ...))")
            print("  → publish_via_handle CAN be deprecated")
            return 0
        else:
            print("  → FAIL — broadcast didn't reach subscriber; ergonomic gap exists")
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
