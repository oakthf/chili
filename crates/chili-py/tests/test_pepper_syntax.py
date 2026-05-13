"""Sprint 16 Part C — pepper `::` null-literal disambiguation tests.

mdata wishlist Q2 lock-in 2026-05-13: the bug is specifically the `::`
null-literal vs `:: <expr>` binary-arg ambiguity. The parser previously
greedy-consumed tokens after `::` as binary-arg continuation, causing
`x: ::; y: 1` to reject with `found 'Punc';' expected arguments`.

This file holds the chili-side regression tests; mdata maintains the
parallel ``test_null_literal_semicolon_disambiguation`` on their side
against the same surface.
"""

import pytest
from chili import ChiliEngine


class TestNullLiteralSemicolonDisambiguation:
    """`::` as a standalone null-literal in atom position."""

    def test_minimal_repro_xy(self):
        """``x: ::; y: 1`` parses; x is None, y is 1."""
        e = ChiliEngine(pepper=True)
        e.eval("x: ::; y: 1")
        assert e.get_var("x") is None
        assert e.get_var("y") == 1

    def test_wishlist_exact_form(self):
        """mdata's wishlist example: ``.sub.eod.fired: ::; eod: {[msg] .sub.eod.fired: msg};``"""
        e = ChiliEngine(pepper=True)
        e.eval(".sub.eod.fired: ::; eod: {[msg] .sub.eod.fired: msg};")
        # After eval, .sub.eod.fired is None (set to :: which is null).
        assert e.get_var(".sub.eod.fired") is None
        # eod is defined as a function; calling it sets .sub.eod.fired.
        e.fn_call("eod", ["hello"])
        assert e.get_var(".sub.eod.fired") == "hello"

    def test_standalone_null_literal(self):
        """``::`` alone parses without args/RHS."""
        e = ChiliEngine(pepper=True)
        # Single :: expression — evaluates to null.
        result = e.eval("::")
        assert result is None

    def test_general_multistatement_unchanged(self):
        """The non-`::` general case still parses (regression check on Q2's case 1)."""
        e = ChiliEngine(pepper=True)
        e.eval("a: 1; b: 2; c: a + b")
        assert e.get_var("c") == 3
