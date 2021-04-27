# Module containing the key queries that recreate the balance and pnl statement in local currency
# WORK IN PROGRESS !!!
from models.sqla import NameValue, Trade, OpenPositions, ForexBalance, ChangeInDividendAccruals
from models.sqla import tradePosition, DepositsWithdrawals
from settings import LOCAL_CURRENCY
from enums import Direction, TradePositionStatus
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


def calc_pnl(closing_principle: str = "FIFO"):
    """
    Add pnl data to the database and calculate the pnl on the basis of the trades
    :param closing_principle: one of ["LIFO", "FIFO"], i.e. last-in-first-out or first-in-first-out,
    when closing a position, this indicates whether to close the lastest opened (LIFO) or earlierst
    opened (FIFO) position first
    """
    BASE_CURRENCY = (
        db.session.query(NameValue.Value)
        .filter(NameValue.type == NameValueType.ACCOUNT_INFORMATION, NameValue.Name == "Base Currency")
        .scalar()
    )

    def _open_position(
        qty: Decimal,
        open_dt: datetime,
        asset: str,
        base_c: str,
        local_c: str,
        basis: Decimal,
        open_forex: Decimal,
        commit: bool = True
    ):
        """
        add an open position to the database
        :param qty: equity quantity
        :param open_dt: datetime for opening position
        :param asset: name of the asset / symbol
        :param base_c: base currency
        :param local_c: local currency
        :param basis: total value in base currency for the opening trade
        :param open_forex: base_currency.local_currency exchange rate at opening time
        :param commit: if True, commit to db after adding, else dont

        """
        print(f"qty {qty}/ basis {basis}")
        db.session.add(
            tradePosition(
                qty=qty,
                open_dt=open_dt,
                status=TradePositionStatus.OPEN,
                asset=asset,
                base_c=base_c,
                local_c=local_c,
                open_price=basis / qty,
                open_forex=open_forex
            ))
        db.session.commit()

    def _close_position(trade, closing_principle) -> bool:
        """
        Check if a position can be closed and execute if possible
        return True when succesfull, else False
        """

        # check sufficient balance up to dt
        qty_available_at_dt = (
            db.session
            .query(func.sum(tradePosition.qty))
            .filter(
                tradePosition.asset == trade.Symbol,
                tradePosition.open_dt < trade.DateTime,
                tradePosition.status == TradePositionStatus.OPEN)
            .scalar()
        )

        # qty_available_at_dt = q.scalar()

        if not qty_available_at_dt:
            # no balance at this dt
            return False
            # raise ValueError(f"Not enough {asset} qty available at {close_dt}")

        trade_qty = -trade.Quantity  # inverse sign

        if qty_available_at_dt >= trade_qty:
            # start finding open positions and reduce qty
            q = (
                db.session
                .query(tradePosition)
                .filter(
                    tradePosition.open_dt <= trade.DateTime, tradePosition.asset == trade.Symbol,
                    tradePosition.status == TradePositionStatus.OPEN)
            )

            if closing_principle == "LIFO":
                fq = q.order_by(tradePosition.open_dt.desc())
            else:
                fq = q.order_by(tradePosition.open_dt)

            remaining_qty_to_close = trade_qty
            for open_pos in fq:
                if remaining_qty_to_close >= open_pos.qty:
                    print(trade.QuoteInLocalCurrency)
                    # fully consume and close
                    open_pos.status = TradePositionStatus.CLOSED
                    open_pos.close_dt = trade.DateTime
                    basis = trade.Basis
                    if not basis:
                        basis = trade.Proceeds - trade.Comm_in_USD
                    open_pos.close_price = basis / trade_qty
                    open_pos.close_forex = trade.QuoteInLocalCurrency

                    pnl_base = open_pos.qty * \
                        (basis / trade_qty - open_pos.open_price)
                    open_pos.pnl_base = pnl_base
                    open_pos.pnl_local = open_pos.qty * (
                        basis / trade_qty * trade.QuoteInLocalCurrency -
                        open_pos.open_price * open_pos.open_forex)
                    db.session.commit()
                    remaining_qty_to_close -= open_pos.qty
                else:
                    # consume partly and close and make new open pos for remainder
                    _open_position(
                        asset=open_pos.asset,
                        qty=open_pos.qty - remaining_qty_to_close,
                        basis=open_pos.open_price *
                        (open_pos.qty - remaining_qty_to_close),
                        open_dt=open_pos.open_dt,
                        open_forex=open_pos.open_forex,
                        base_c=open_pos.base_c,
                        local_c=open_pos.local_c,
                        commit=False
                    )
                    open_pos.qty = remaining_qty_to_close
                    open_pos.status = TradePositionStatus.CLOSED
                    open_pos.close_dt = trade.DateTime
                    basis = trade.Basis
                    if not basis:
                        basis = trade.Proceeds - trade.Comm_in_USD
                    open_pos.close_price = basis / trade_qty,
                    open_pos.close_forex = trade.QuoteInLocalCurrency,
                    pnl_base = remaining_qty_to_close * \
                        (basis / trade_qty - open_pos.open_price)
                    open_pos.pnl_base = pnl_base,
                    open_pos.pnl_local = remaining_qty_to_close * (
                        basis / trade_qty * trade.QuoteInLocalCurrency -
                        open_pos.open_price * open_pos.open_forex)

                    print(open_pos.close_forex)  # THIS IS THE ERROR
                    print(open_pos.close_price)
                    db.session.commit()
                    break
            return True

        else:
            # insufficient qty available at this point
            return False

    for trade in db.session.query(Trade).order_by(Trade.DateTime):
        direction = Direction.BUY if trade.Quantity > Decimal(
            0) else Direction.SELL
        print(trade.QuoteInLocalCurrency)

        if direction == Direction.BUY:
            # open a position
            basis = trade.Basis
            if not basis:
                basis = trade.Proceeds - trade.Comm_in_USD
            assert basis > Decimal(0), f"ERROR: {trade.id}"
            _open_position(qty=trade.Quantity,
                           open_dt=trade.DateTime,
                           asset=trade.Symbol,
                           base_c=BASE_CURRENCY,
                           local_c=LOCAL_CURRENCY,
                           basis=basis,
                           open_forex=trade.QuoteInLocalCurrency)
        elif direction == Direction.SELL:
            # close position
            # pass
            _close_position(trade, closing_principle=closing_principle)
        else:
            raise NotImplementedError(
                "ERROR: incorrect Direction type received...")


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
