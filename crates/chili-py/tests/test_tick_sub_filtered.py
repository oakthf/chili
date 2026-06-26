"""FR-1 — per-handle FILTERED subscribe (TorQ `.u.sub[t;syms]`).

A publisher broadcasts a multi-symbol frame. A filtered subscriber (syms
[AAPL, GOOG]) receives ONLY the AAPL/GOOG rows on the live broadcast path;
an unfiltered subscriber receives every row. This is the additive,
backward-compatible filter accumulated on the chili `torq-frs` branch.
"""

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


class TestTickSubFiltered:
    def test_filtered_vs_unfiltered_subscribe(self):
        port = _free_port()

        t = ChiliEngine(pepper=True, debug=True)
        with tempfile.TemporaryDirectory() as log_dir:
            trade_schema = pl.DataFrame(
                {
                    "sym": pl.Series([], dtype=pl.Categorical),
                    "price": pl.Series([], dtype=pl.Float64),
                    "size": pl.Series([], dtype=pl.Int64),
                }
            )
            t.init_tick(
                schema={"trade": trade_schema},
                log_dir=log_dir + "/",
                filename=date.today(),
            )
            t.start_tcp_listener(port)
            time.sleep(0.1)

            # -- unfiltered subscriber --
            s_all = ChiliEngine(pepper=True, debug=True)
            s_all.subscribe(f"chili://127.0.0.1:{port}", ["trade"])
            time.sleep(0.1)

            # -- FILTERED subscriber: only AAPL, GOOG --
            s_flt = ChiliEngine(pepper=True, debug=True)
            s_flt.subscribe(
                f"chili://127.0.0.1:{port}",
                filters={"trade": ("sym", ["AAPL", "GOOG"])},
            )
            time.sleep(0.1)

            assert s_flt.get_var(".sub.filterColumn") == "sym"
            assert sorted(s_flt.get_var(".sub.filterValues")) == ["AAPL", "GOOG"]

            # Live broadcast: 4 symbols, only 2 match the filter.
            batch = pl.DataFrame(
                {
                    "sym": pl.Series(
                        ["AAPL", "MSFT", "GOOG", "TSLA"], dtype=pl.Categorical
                    ),
                    "price": [150.0, 300.0, 2800.0, 700.0],
                    "size": [100, 50, 200, 75],
                }
            )
            t.publish("trade", batch)
            time.sleep(0.2)

            all_trade = s_all.get_var("trade")
            flt_trade = s_flt.get_var("trade")
            print(f"\n[DIAG] unfiltered:\n{all_trade}")
            print(f"\n[DIAG] filtered:\n{flt_trade}")

            # Unfiltered subscriber sees all 4 rows.
            assert all_trade.shape[0] == 4, f"unfiltered expected 4:\n{all_trade}"

            # Filtered subscriber sees ONLY AAPL + GOOG (2 rows).
            assert flt_trade.shape[0] == 2, f"filtered expected 2:\n{flt_trade}"
            got = sorted(str(x) for x in flt_trade["sym"].to_list())
            assert got == ["AAPL", "GOOG"], f"filtered syms wrong: {got}"

            # Schema preserved on the filtered frame (filter drops rows only).
            assert flt_trade.columns == ["sym", "price", "size"]

            # Second live batch — filter persists across publishes.
            batch2 = pl.DataFrame(
                {
                    "sym": pl.Series(["GOOG", "NVDA"], dtype=pl.Categorical),
                    "price": [2810.0, 900.0],
                    "size": [10, 20],
                }
            )
            t.publish("trade", batch2)
            time.sleep(0.2)

            all_trade = s_all.get_var("trade")
            flt_trade = s_flt.get_var("trade")
            assert all_trade.shape[0] == 6, f"unfiltered expected 6:\n{all_trade}"
            # Only the GOOG row from batch2 passes -> 2 (prev) + 1 = 3.
            assert flt_trade.shape[0] == 3, f"filtered expected 3:\n{flt_trade}"

            s_flt.shutdown()
            s_all.shutdown()
        t.shutdown()
