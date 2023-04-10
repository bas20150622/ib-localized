from decimal import Decimal
from models.sqla import OpenPositions, ForexBalance, ChangeInDividendAccruals
from sqlalchemy import func


def print_balance(
    session, STATEMENT_END_DATE_STR, BASE_CURRENCY, LOCAL_CURRENCY, EOY_BASE_LOCAL
):
    """
    Calculate the balance of equities and cash at statement end date
    """
    QUANTIZE_FIAT = Decimal("1.00")
    sum_equity_base = sum_cash_base = sum_change_in_dividends_base = Decimal(0)
    print(f"BALANCE AT {STATEMENT_END_DATE_STR}\n")
    print("*** EQUITY BALANCE ***")
    q = session.query(
        OpenPositions.Symbol,
        OpenPositions.Quantity,
        OpenPositions.Mult,
        OpenPositions.Value,
    ).order_by(OpenPositions.Symbol)
    for symbol, quantity, multiplier, value in q:
        sum_equity_base += value
        val_local_currency = value * EOY_BASE_LOCAL

        print(
            f"{quantity} {symbol} @ {value} {BASE_CURRENCY}/ "
            f"{val_local_currency.quantize(Decimal('1.00'))} {LOCAL_CURRENCY}"
        )
    print("-------------------------------")
    print(
        f"SUB TOTAL EQUITY {sum_equity_base} {BASE_CURRENCY}/ "
        f"{(sum_equity_base * EOY_BASE_LOCAL).quantize(QUANTIZE_FIAT)} {LOCAL_CURRENCY}"
    )
    print()
    print("*** CASH BALANCE ***")
    for currency, qty, close_value_in_base_at_statement_end in session.query(
        ForexBalance.Description, ForexBalance.Quantity, ForexBalance.Close_Price
    ):
        val_base = qty * close_value_in_base_at_statement_end
        val_local_currency = val_base * EOY_BASE_LOCAL
        sum_cash_base += val_base
        print(
            f"{qty} {currency}: {val_base.quantize(Decimal('1.00'))} {BASE_CURRENCY}/ "
            f"{val_local_currency.quantize(Decimal('1.00'))} {LOCAL_CURRENCY}"
        )
    print("-------------------------------")
    print(
        f"SUB TOTAL CASH {sum_cash_base} {BASE_CURRENCY}/ "
        f"{(sum_cash_base * EOY_BASE_LOCAL).quantize(QUANTIZE_FIAT)} {LOCAL_CURRENCY}"
    )
    print()
    print("*** CHANGE IN DIVIDEND ACCRUALS ***")
    sum_change_in_dividends_base = session.query(
        func.sum(ChangeInDividendAccruals.Net_Amount)
    ).scalar()
    print(sum_change_in_dividends_base)
    print()
    totals = sum_equity_base + sum_cash_base + sum_change_in_dividends_base
    print(
        f"TOTAL: {totals.quantize(QUANTIZE_FIAT)} {BASE_CURRENCY}/ "
        f"{(totals * EOY_BASE_LOCAL).quantize(QUANTIZE_FIAT)} {LOCAL_CURRENCY}"
    )
