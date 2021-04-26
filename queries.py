# Module containing the key queries that recreate the balance and pnl statement in local currency
# WORK IN PROGRESS !!!
from models.sqla import NameValue, Trade, OpenPositions, ForexBalance, ChangeInDividendAccruals, DepositsWithdrawals
from settings import LOCAL_CURRENCY
from forex import forex_rate
from enums import NameValueType
from datetime import datetime
from decimal import Decimal
from sqlalchemy import not_, func
db = None


def calc_balance():
    """
    Calculate the balance of equities and cash at statement end date
    """
    QUANTIZE_FIAT = Decimal('1.00')
    sum_equity_base = sum_cash_base = sum_change_in_dividends_base = Decimal(0)
    print(f"BALANCE AT {STATEMENT_END_DATE_STR}\n")
    print("*** EQUITY BALANCE ***")
    q = (
        db.session
        .query(OpenPositions.Symbol, OpenPositions.Quantity, OpenPositions.Mult, OpenPositions.Value)
        .order_by(OpenPositions.Symbol)
    )
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
        f"{(sum_equity_base * EOY_BASE_LOCAL).quantize(QUANTIZE_FIAT)} {LOCAL_CURRENCY}")
    print()
    print("*** CASH BALANCE ***")
    for currency, qty, close_value_in_base_at_statement_end in (
            db.session.query(ForexBalance.Description, ForexBalance.Quantity, ForexBalance.Close_Price)):
        val_base = qty * close_value_in_base_at_statement_end
        val_local_currency = val_base * EOY_BASE_LOCAL
        sum_cash_base += val_base
        print(
            f"{qty} {currency}: {val_base.quantize(Decimal('1.00'))} {BASE_CURRENCY}/ "
            f"{val_local_currency.quantize(Decimal('1.00'))} {LOCAL_CURRENCY}")
    print("-------------------------------")
    print(
        f"SUB TOTAL CASH {sum_cash_base} {BASE_CURRENCY}/ "
        f"{(sum_cash_base * EOY_BASE_LOCAL).quantize(QUANTIZE_FIAT)} {LOCAL_CURRENCY}")
    print()
    print("*** CHANGE IN DIVIDEND ACCRUALS ***")
    sum_change_in_dividends_base = db.session.query(
        func.sum(ChangeInDividendAccruals.Net_Amount)).scalar()
    print(sum_change_in_dividends_base)
    print()
    totals = sum_equity_base + sum_cash_base + sum_change_in_dividends_base
    print(
        f"TOTAL: {totals.quantize(QUANTIZE_FIAT)} {BASE_CURRENCY}/ "
        f"{(totals * EOY_BASE_LOCAL).quantize(QUANTIZE_FIAT)} {LOCAL_CURRENCY}")


def show_trade_deltas():
    """
    Show the per symbol trade quantities sorted by date
    while tracking running total of balance
    """
    q = (
        db.session.query(
            Trade.Symbol, Trade.DateTime, Trade.Quantity,
            func.sum(Trade.Quantity).over(
                partition_by=Trade.Symbol, order_by=(Trade.DateTime)).label("Balance"),
            Trade.QuoteInLocalCurrency, Trade.Proceeds, Trade.CommOrFee
        )
    )
    for row in q:
        print(row)


# Statement and Account Information
db.session.query(NameValue.type, NameValue.Name).all()

# Infer Base Currency
BASE_CURRENCY = db.session.query(NameValue.Value).filter(
    NameValue.type == NameValueType.ACCOUNT_INFORMATION, NameValue.Name == "Base Currency").scalar()

# Infer balance date from statement
STATEMENT_END_DATE_STR = db.session.query(NameValue.Value).filter(
    NameValue.type == NameValueType.STATEMENT, NameValue.Name == "Period").scalar()
STATEMENT_END_DATE = datetime.strptime(
    STATEMENT_END_DATE_STR.split(" - ")[-1], "%B %d, %Y")

# infer FIAT trades
ForexPairs = set()
FiatTrades = set()
symbols = db.session.query(Trade.Symbol).filter(
    Trade.Asset_Category.like("Forex%")).group_by(Trade.Symbol).all()
for symbol in symbols:
    ForexPairs.add(symbol[0])
    base, quote = symbol[0].split(".")
    FiatTrades.add(base)
    FiatTrades.add(quote)
print(f"Forex pairs: {ForexPairs}")
print(f"Fiat traded: {FiatTrades}")
EOY_FOREX_RATES = {}
for currency in FiatTrades:
    EOY_FOREX_RATES[currency] = forex_rate(
        currency, LOCAL_CURRENCY, STATEMENT_END_DATE)
print(
    f"Forex rates vs {LOCAL_CURRENCY} @ {STATEMENT_END_DATE}: {EOY_FOREX_RATES}")

# alt: trade symbol holdings with qty <> 0 ("short or long position open at balance date")
q = (db.session
     .query(Trade.Symbol, func.sum(Trade.Quantity).label("sum"))
     .group_by(Trade.Symbol)
     .order_by(Trade.Symbol).subquery()
     )
q2 = db.session.query(q.c.Symbol, q.c.sum).filter(not_(q.c.sum == Decimal(0)))
for r in q2:
    print(r)

# Withdrawals and Deposits
db.session.query(DepositsWithdrawals).all()

# Open positions
db.session.query(OpenPositions.Symbol, OpenPositions.Quantity).order_by(
    OpenPositions.Symbol).all()

# trade symbols totals
q = (db.session
     .query(
         Trade.Symbol,
         func.sum(Trade.Quantity).label("quantity"),
         func.sum(Trade.Proceeds).label("proceeds"),
         func.sum(Trade.CommOrFee).label("comm_or_fee"),
         func.sum(Trade.Basis).label("basis"),
         func.sum(Trade.Realized_PnL).label("realized"),
         func.sum(Trade.Realized_PnL_pct).label("realized_pct"),
         func.sum(Trade.MTM_PnL).label("mtm_pnl"),
         func.sum(Trade.Comm_in_USD).label("comm_USD"),
         func.sum(Trade.MTM_in_USD).label("MTM_USD"),
     )
     .group_by(Trade.Symbol)
     .order_by(Trade.Symbol)
     )
for r in q:
    print(r)

# Base in Local (Quote) currency @ statement end date
EOY_BASE_LOCAL = forex_rate(BASE_CURRENCY, LOCAL_CURRENCY, STATEMENT_END_DATE)
print(
    f"Base currency quote in local currency {BASE_CURRENCY}.{LOCAL_CURRENCY} = {EOY_BASE_LOCAL}")
