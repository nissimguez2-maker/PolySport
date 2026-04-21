"""Tests for Polymarket CLOB book parsing.

`fetch_book` uses max()/min() on bids/asks because the CLOB payload's sort
direction has historically been unreliable. These tests guarantee best-bid
is the highest price and best-ask is the lowest, regardless of input order.
Must pass before any live execution module touches BookTop.
"""

from __future__ import annotations

import httpx

from polysport.feeds.polymarket import BookTop, fetch_book


class _FakeResp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.last_params: dict | None = None

    def get(self, url: str, *, params: dict, timeout: float) -> _FakeResp:  # noqa: ARG002
        self.last_params = params
        return _FakeResp(self._payload)


def test_fetch_book_picks_best_regardless_of_sort() -> None:
    """Asks ascending, bids descending — both must yield correct extremes."""
    payload = {
        "bids": [
            {"price": "0.47", "size": "100"},
            {"price": "0.49", "size": "500"},   # best bid
            {"price": "0.45", "size": "200"},
        ],
        "asks": [
            {"price": "0.55", "size": "300"},
            {"price": "0.52", "size": "400"},   # best ask
            {"price": "0.58", "size": "100"},
        ],
    }
    client = _FakeClient(payload)
    book = fetch_book(client, token_id="tok-123")  # type: ignore[arg-type]
    assert isinstance(book, BookTop)
    assert book.best_bid == 0.49
    assert book.best_ask == 0.52
    assert book.bid_size_shares == 500.0
    assert book.ask_size_shares == 400.0
    assert client.last_params == {"token_id": "tok-123"}


def test_fetch_book_ascending_bids() -> None:
    """Sanity check: ascending-sorted bids still resolve to the top."""
    payload = {
        "bids": [
            {"price": "0.30", "size": "10"},
            {"price": "0.40", "size": "20"},
            {"price": "0.50", "size": "30"},  # best bid (highest price)
        ],
        "asks": [
            {"price": "0.60", "size": "40"},  # best ask (lowest price)
            {"price": "0.70", "size": "50"},
        ],
    }
    book = fetch_book(_FakeClient(payload), token_id="x")  # type: ignore[arg-type]
    assert book.best_bid == 0.50
    assert book.best_ask == 0.60


def test_fetch_book_empty_side() -> None:
    """Missing side of book returns None without raising."""
    payload = {"bids": [], "asks": [{"price": "0.6", "size": "10"}]}
    book = fetch_book(_FakeClient(payload), token_id="x")  # type: ignore[arg-type]
    assert book.best_bid is None
    assert book.bid_size_shares is None
    assert book.best_ask == 0.6
    assert book.ask_size_shares == 10.0


def test_fetch_book_fully_empty() -> None:
    """An empty book should never raise — caller decides what to do."""
    book = fetch_book(_FakeClient({"bids": [], "asks": []}), token_id="x")  # type: ignore[arg-type]
    assert book.best_bid is None
    assert book.best_ask is None
