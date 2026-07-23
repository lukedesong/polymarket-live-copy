import pytest

from wallet_copy_paper import ACCOUNTS, DataUnavailable, PublicClient


@pytest.mark.live
def test_approved_wallets_and_current_book_are_publicly_readable():
    client = PublicClient()
    all_rows = []
    for account in ACCOUNTS:
        rows = client.trades(account.wallet)
        assert all(item.wallet == account.wallet.lower() for item in rows)
        all_rows.extend(rows)
    assert all_rows
    for candidate in sorted(all_rows, key=lambda item: item.timestamp, reverse=True):
        try:
            current_book = client.book(candidate.asset)
            market_params = client.market_params(candidate.condition_id)
        except DataUnavailable:
            continue
        assert current_book.asset == candidate.asset
        assert current_book.condition_id == candidate.condition_id
        assert current_book.min_order_size == market_params.min_order_size
        break
    else:
        pytest.fail("no recent wallet trade referenced a currently readable CLOB book")
