# Module containing the key queries that recreate the balance and pnl statement in local currency
# WORK IN PROGRESS !!!
from models.sqla import (
    NameValue,
    Trade,
    OpenPositions,
    ForexBalance,
    ChangeInDividendAccruals,
    Fees,
)
from models.sqla import (
    tradePosition,
    Dividends,
    WitholdingTax,
    DepositsWithdrawals,
    Transfer,
    Mark2Market,
)
from settings import LOCAL_CURRENCY
from enums import Direction, TradePositionStatus, CategoryType
import enums

# from forex import forex_rate
from enums import NameValueType, ModelType
from datetime import datetime, timedelta
from decimal import Decimal
from sqlalchemy import func
from sqlalchemy import case, literal, null, cast, Numeric
from sqlalchemy.orm import Query
from sqlalchemy.orm.session import Session
from typing import Optional
from forex import ForexRate, forex_rate
from db import object_as_dict
from collections import defaultdict


def _base_query(session: Session) -> Query:
    """
    USED IN MAIN BRANCH PNL CALCULATION USING PROCESS_BASE

    Return the base deltas based on trade, transfers (deposits withdrawals),
    dividend and witholding tax,
    sorted by asset and by date
    :returns selectable: base, dt, qty, balance, quote, cost, forex
    """
    d1 = session.query(  # trade base excluding forex
        # literal(0).label("type"),  # trade type == 0
        Trade.Quantity.label("qty"),
        Trade.Symbol.label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
        Trade.Currency.label("quote"),
        case(
            [
                (Trade.Proceeds, Trade.Proceeds + Trade.CommOrFee),
                (Trade.Notional_Value, Trade.Notional_Value + Trade.CommOrFee),
            ],
            else_=literal(Decimal(0)),
        ).label("cost"),
        Trade.type.label("t"),
    ).filter(Trade.type != CategoryType.FOREX)
    d2 = session.query(  # trade base exclusively forex
        # literal(0).label("type"),  # trade type == 0
        Trade.Quantity.label("qty"),
        func.substr(Trade.Symbol, 1, func.instr(Trade.Symbol, ".") - 1).label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
        Trade.Currency.label("quote"),
        (Trade.Proceeds + Trade.Comm_in_USD).label("cost"),
        Trade.type.label("t"),
    ).filter(Trade.type == CategoryType.FOREX)
    d5 = session.query(
        # literal(1).label("type"),  # transfer type == 0
        DepositsWithdrawals.Amount.label("qty"),
        DepositsWithdrawals.Currency.label("base"),
        DepositsWithdrawals.DateTime.label("dt"),
        DepositsWithdrawals.QuoteInLocalCurrency.label("forex"),
        null().label("quote"),
        null().label("cost"),
        literal(CategoryType.STOCKS).label("t"),
    )
    u = d1.union_all(d2, d5).subquery()
    q1 = session.query(
        u.c.base,
        u.c.dt,
        u.c.qty,
        func.sum(u.c.qty).over(partition_by=u.c.base, order_by=u.c.dt).label("balance"),
        u.c.quote,
        u.c.cost,
        u.c.forex,
        u.c.t,
    ).order_by(
        u.c.dt
    )  # .cte()

    return q1


def asset_delta_query(session: Session, LOCAL_CURRENCY=LOCAL_CURRENCY) -> Query:
    """
    Return the asset deltas based on trade, transfers, deposits & withdrawals,
    dividend, witholding tax, fees and starting balance for period.
    sorted by account, asset and by date
    :returns selectable: asset, dt, qty, balance, forex, asset type, account, orm model type and orm model id
    """
    # Infer balance start date from statement
    STATEMENT_PERIOD = (
        session.query(NameValue.Value)
        .filter(NameValue.type == NameValueType.STATEMENT, NameValue.Name == "Period")
        .first()[0]
    )
    STATEMENT_PERIOD_SPLIT = STATEMENT_PERIOD.split(" - ")
    STATEMENT_START_DATE = datetime.strptime(STATEMENT_PERIOD_SPLIT[0], "%B %d, %Y")
    STATEMENT_END_DATE = datetime.strptime(STATEMENT_PERIOD_SPLIT[-1], "%B %d, %Y")

    BASE_CURRENCY = (
        session.query(NameValue.Value)
        .filter(
            NameValue.type == NameValueType.ACCOUNT_INFORMATION,
            NameValue.Name == "Base Currency",
        )
        .first()[0]
    )

    ForexPairs = set()
    FiatTrades = set()
    symbols = (
        session.query(Trade.Symbol)
        .filter(Trade.Asset_Category.like("Forex%"))
        .group_by(Trade.Symbol)
        .all()
    )
    for symbol in symbols:
        ForexPairs.add(symbol[0])
        base, quote = symbol[0].split(".")
        FiatTrades.add(base)
        FiatTrades.add(quote)
    EOY_FOREX_RATES, BOY_FOREX_RATES = {}, {}
    for currency in FiatTrades:
        BOY_FOREX_RATES[currency] = forex_rate(
            currency, LOCAL_CURRENCY, STATEMENT_START_DATE
        )
        EOY_FOREX_RATES[currency] = forex_rate(
            currency, LOCAL_CURRENCY, STATEMENT_END_DATE
        )

    DEDICATED_TRADE_IMPLEMENTATIONS = [CategoryType.FOREX, CategoryType.FOREX_CFDs]

    trade_base = session.query(  # trade base excluding forex and forex_cfd
        Trade.Account.label("account"),
        Trade.Quantity.label("qty"),
        Trade.Symbol.label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
        Trade.type.label("t"),
        literal(ModelType.TRADE.name).label("m"),
        Trade.id.label("_id"),
    ).filter(Trade.type.notin_(DEDICATED_TRADE_IMPLEMENTATIONS))
    trade_quote = session.query(  # trade quote excluding forex and forex_cfd
        Trade.Account.label("account"),
        case(
            [
                (Trade.Proceeds, Trade.Proceeds + Trade.CommOrFee),
                (Trade.Notional_Value, Trade.Notional_Value + Trade.CommOrFee),
            ],
            else_=literal(Decimal(0)),
        ).label("qty"),
        Trade.Currency.label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
        Trade.type.label("t"),
        literal(ModelType.TRADE.name).label("m"),
        Trade.id.label("_id"),
    ).filter(Trade.type.notin_(DEDICATED_TRADE_IMPLEMENTATIONS))
    forex_cfd_quote = session.query(  # forex cfd quote
        # if currency == split -1, use notional value + realized_pnl
        # elif currency == split 0, raise not implemented (not seen yet)
        Trade.Account.label("account"),
        case(
            [
                (
                    Trade.Currency
                    == func.substr(Trade.Symbol, func.instr(Trade.Symbol, ".") + 1),
                    Trade.Notional_Value,
                ),
            ],
            else_=literal(Decimal(0)),
        ).label("qty"),
        Trade.Currency.label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
        Trade.type.label("t"),
        literal(ModelType.TRADE.name).label("m"),
        Trade.id.label("_id"),
    ).filter(Trade.type == CategoryType.FOREX_CFDs)
    forex_cfd_base = session.query(  # forex cfd base
        # if currency == split -1, use qty
        # elif currency == split 0, raise not implemented (not seen yet)
        Trade.Account.label("account"),
        Trade.Quantity.label("qty"),
        func.substr(Trade.Symbol, 1, func.instr(Trade.Symbol, ".") - 1).label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
        Trade.type.label("t"),
        literal(ModelType.TRADE.name).label("m"),
        Trade.id.label("_id"),
    ).filter(Trade.type == CategoryType.FOREX_CFDs)

    forex_cfd_fee = session.query(  # forex cfd fee
        # if currency == split -1, use qty
        # elif currency == split 0, raise not implemented (not seen yet)
        Trade.Account.label("account"),
        Trade.CommOrFee.label("qty"),
        literal("USD").label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
        Trade.type.label("t"),
        literal(ModelType.TRADE.name).label("m"),
        Trade.id.label("_id"),
    ).filter(Trade.type == CategoryType.FOREX_CFDs)

    forex_trade_base = session.query(  # trade base exclusively forex
        Trade.Account.label("account"),
        Trade.Quantity.label("qty"),
        func.substr(Trade.Symbol, 1, func.instr(Trade.Symbol, ".") - 1).label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
        Trade.type.label("t"),
        literal(ModelType.TRADE.name).label("m"),
        Trade.id.label("_id"),
    ).filter(Trade.type == CategoryType.FOREX)
    forex_trade_quote = session.query(  # trade quote exclusively forex
        Trade.Account.label("account"),
        Trade.Proceeds.label("qty"),
        func.substr(Trade.Symbol, func.instr(Trade.Symbol, ".") + 1).label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
        Trade.type.label("t"),
        literal(ModelType.TRADE.name).label("m"),
        Trade.id.label("_id"),
    ).filter(Trade.type == CategoryType.FOREX)
    forex_trade_fee = session.query(  # trade quote exclusively USD fee
        Trade.Account.label("account"),
        Trade.Comm_in_USD.label("qty"),
        literal("USD").label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
        Trade.type.label("t"),
        literal(ModelType.TRADE.name).label("m"),
        Trade.id.label("_id"),
    ).filter(Trade.type == CategoryType.FOREX)
    deposit_withdrawals = session.query(
        # literal(1).label("type"),  # transfer type == 0
        DepositsWithdrawals.Account.label("account"),
        DepositsWithdrawals.Amount.label("qty"),
        DepositsWithdrawals.Currency.label("base"),
        DepositsWithdrawals.DateTime.label("dt"),
        DepositsWithdrawals.QuoteInLocalCurrency.label("forex"),
        literal(CategoryType.FOREX).label("t"),
        literal(ModelType.DEPOSITWITHDRAWAL.name).label("m"),
        DepositsWithdrawals.id.label("_id"),
    )
    transfers = session.query(
        # literal(1).label("type"),  # transfer type == 0
        Transfer.Account.label("account"),
        Transfer.Qty.label("qty"),
        Transfer.Symbol.label("base"),
        Transfer.DateTime.label("dt"),
        Transfer.QuoteInLocalCurrency.label("forex"),
        Transfer.type.label("t"),
        literal(ModelType.TRANSFER.name).label("m"),
        Transfer.id.label("_id"),
    )
    starting_balances = session.query(
        Mark2Market.Account.label("account"),
        Mark2Market.Prior_Quantity.label("qty"),
        Mark2Market.Symbol.label("base"),
        literal(STATEMENT_START_DATE).label("dt"),
        literal(BOY_FOREX_RATES.get(BASE_CURRENCY)).label("forex"),
        Mark2Market.type.label("t"),
        literal(ModelType.MARK2MARKET.name).label("m"),
        Mark2Market.id.label("_id"),
    ).filter(Mark2Market.Prior_Quantity > Decimal(0))
    dividends = session.query(
        Dividends.Account.label("account"),
        Dividends.Amount.label("qty"),
        Dividends.Currency.label("base"),
        Dividends.DateTime.label("dt"),
        Dividends.QuoteInLocalCurrency.label("forex"),
        literal(CategoryType.STOCKS).label("t"),
        literal(ModelType.DIVIDEND.name).label("m"),
        Dividends.id.label("_id"),
    )
    witholdingtax = session.query(
        WitholdingTax.Account.label("account"),
        WitholdingTax.Amount.label("qty"),
        WitholdingTax.Currency.label("base"),
        WitholdingTax.DateTime.label("dt"),
        WitholdingTax.QuoteInLocalCurrency.label("forex"),
        literal(CategoryType.STOCKS).label("t"),
        literal(ModelType.WITHOLDINGTAX.name).label("m"),
        WitholdingTax.id.label("_id"),
    )
    fees = session.query(
        Fees.Account.label("account"),
        Fees.Amount.label("qty"),
        Fees.Currency.label("base"),
        Fees.DateTime.label("dt"),
        Fees.QuoteInLocalCurrency.label("forex"),
        literal(CategoryType.NONE).label("t"),
        literal(ModelType.FEE.name).label("m"),
        Fees.id.label("_id"),
    )
    u = trade_base.union_all(
        trade_quote,
        forex_trade_base,
        forex_trade_quote,
        forex_trade_fee,
        forex_cfd_quote,
        forex_cfd_base,
        forex_cfd_fee,
        deposit_withdrawals,
        transfers,
        starting_balances,
        dividends,
        witholdingtax,
        fees,
    ).subquery()
    q1 = session.query(
        u.c.base,
        u.c.dt,
        u.c.qty,
        func.sum(u.c.qty)
        .over(partition_by=(u.c.account, u.c.base), order_by=u.c.dt)
        .label("balance"),
        u.c.forex,
        u.c.t,
        u.c.account,
        u.c.m,
        u.c._id,
    ).order_by(u.c.dt, u.c.base, u.c.account)

    return q1


def calc_balance(
    session: Session,
    STATEMENT_END_DATE_STR: str,
    BASE_CURRENCY: str,
    LOCAL_CURRENCY: str,
    EOY_BASE_LOCAL,
):
    """
    Calculate the balance of equities and cash at statement end date
    """
    QUANTIZE_FIAT = Decimal("1.00")
    sum_equity_base, sum_cash_base, sum_change_in_dividends_base = (
        Decimal(0),
        Decimal(0),
        Decimal(0),
    )
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


def process_base_query(
    in_session: Session, out_session: Session, forex_rate: ForexRate
):
    """
    process the base query rows as tradepositions
    :param in_session: session to apply base query onto
    :param out_session: session to export tradePositions onto
    :param forex_rate: forex utility to fetch fiat asset USD values
    """
    ROUND_ERROR_MARGIN = Decimal("0.99")
    # relevant for closing trades (assuming CFDs/ shorting not subject to namechange)
    ISIN_CHANGE = {"IPOB": "OPEN"}
    _revISIN = {val: key for key, val in ISIN_CHANGE.items()}

    def _open_position(
        dt: datetime,
        asset: str,
        qty: Decimal,
        value: Decimal,
        forex: Decimal,
        multiplier: int = 1,
    ):
        """
        open a tradePosition
        """
        if qty.is_zero():  # e.g. options expiring OTM
            return
        out_session.add(
            tradePosition(
                qty=qty,
                open_dt=dt,
                status=TradePositionStatus.OPEN,
                asset=asset,
                o_price_base=value / qty,
                o_price_local=value / qty * forex,
                multiplier=multiplier,
            )
        )
        out_session.commit()
        return

    def _consume_position(
        open_pos: tradePosition,
        close_dt: datetime,
        forex_rate: Optional[Decimal] = None,
        exit_value: Optional[Decimal] = None,
        with_pnl: bool = True,
    ):
        """
        Provided an open_pos, fully consume it by closing the position
        add pnl if with_pnl==True otherwise close without pnl
        :param open_pos: open tradePosition to consume
        :param close_dt: time for closing position
        :param exit_value: exit value for trade (if relevant)
        :param forex_rate: valid forex rate at time of closing position
        :param with_pnl: flag to indicate whether to realize pnl or not
        """
        if not with_pnl:
            id = open_pos.id
            out_session.query.filter_by(id=id).delete()
        else:
            # fully consume and close
            open_pos.status = TradePositionStatus.CLOSED
            open_pos.close_dt = close_dt
            open_pos.c_price_base = exit_value / open_pos.qty
            open_pos.c_price_local = exit_value / open_pos.qty * forex_rate
            open_pos.pnl_base = (
                exit_value - open_pos.qty * open_pos.o_price_base
            ) * open_pos.multiplier
            open_pos.pnl_local = (
                exit_value * forex_rate - open_pos.qty * open_pos.o_price_local
            ) * open_pos.multiplier
        out_session.commit()

    def _reduce_position(
        open_pos: tradePosition,
        close_dt: datetime,
        reduce_with_qty: Decimal,
        forex_rate: Optional[Decimal] = None,
        exit_value: Optional[Decimal] = None,
        with_pnl: bool = False,
    ):
        """
        Provided an open_pos.qty with reduce_with_qty by updating the qty and creating
        a closed position with or without pnl for qty==reduce_with_qty
        :param open_pos: open tradePosition to consume
        :param forex_rate: foreign exchange rate
        :param dt: time for closing position
        :reduce_with_qty: quantity to reduce position with
        :param with_pnl: flag to indicate whether to realize pnl or not
        :optional param forex_rate: forex rate @ dt
        :optional param exit_value: value in base currency of qty position
        """
        # reduce the open position
        open_pos_new_q = open_pos.qty - reduce_with_qty
        open_pos.qty = open_pos_new_q

        if with_pnl:
            c_price_base = exit_value / reduce_with_qty
            c_price_local = exit_value / reduce_with_qty * forex_rate
            pnl_base = (
                exit_value - reduce_with_qty * open_pos.o_price_base
            ) * open_pos.multiplier
            pnl_local = (
                exit_value * forex_rate - reduce_with_qty * open_pos.o_price_local
            ) * open_pos.multiplier

            # create closed position
            out_session.add(
                tradePosition(
                    c_price_base=c_price_base,
                    c_price_local=c_price_local,
                    pnl_base=pnl_base,
                    pnl_local=pnl_local,
                    close_dt=close_dt,
                    open_dt=open_pos.open_dt,
                    status=TradePositionStatus.CLOSED,
                    asset=open_pos.asset,
                    qty=reduce_with_qty,
                    multiplier=open_pos.multiplier,
                )
            )
        out_session.commit()

    def _close_position(
        dt: datetime, asset: str, qty: Decimal, value: Decimal, forex: Decimal
    ) -> bool:
        """
        Check if a position can be closed and execute if possible
        return True when succesfull, else False
        """
        if asset in _revISIN.keys():
            print(f"CHANGED TICKER FOR {asset} to {_revISIN.get(asset)}")
            asset = _revISIN.get(asset)
        if qty.is_zero():
            raise ValueError("ERROR: qty==0")

        # check sufficient balance up to dt
        q = out_session.query(func.sum(tradePosition.qty)).filter(
            tradePosition.asset == asset,
            tradePosition.open_dt < dt,
            tradePosition.status == TradePositionStatus.OPEN,
        )

        qty_available_at_dt = q.scalar()

        if not qty_available_at_dt:
            # no balance at this dt
            raise ValueError(f"ERROR: insufficient balance for {asset}")

        ASSUMED_ROUNDING_ERROR_MARGIN = (
            qty_available_at_dt > ROUND_ERROR_MARGIN * qty
        ) and (qty_available_at_dt < qty)
        if (qty_available_at_dt >= qty) or ASSUMED_ROUNDING_ERROR_MARGIN:
            if ASSUMED_ROUNDING_ERROR_MARGIN:
                qty = qty_available_at_dt
            # start finding open positions and reduce qty
            q = (
                out_session.query(tradePosition)
                .filter(
                    tradePosition.open_dt <= dt,
                    tradePosition.asset == asset,
                    tradePosition.status == TradePositionStatus.OPEN,
                )
                .order_by(tradePosition.open_dt)  # FIFO
            )

            avg_price = value / qty
            remaining_qty_to_close = qty
            for trade_pos in q:
                # either trade_pos.qty < remaining, close pos and continue
                # or trade_pos.qty == remaining, close pos and exit
                # or trade_pos.qty > remaining, reduce pos and exit
                if remaining_qty_to_close.is_zero():
                    return True

                if remaining_qty_to_close >= trade_pos.qty:
                    # fully consume and close
                    _consume_position(
                        open_pos=trade_pos,
                        close_dt=dt,
                        forex_rate=forex,
                        exit_value=trade_pos.qty * avg_price,
                        with_pnl=True,
                    )
                    remaining_qty_to_close -= trade_pos.qty
                else:
                    # consume partly and close and make new open pos for remainder
                    _reduce_position(
                        open_pos=trade_pos,
                        close_dt=dt,
                        forex_rate=forex,
                        exit_value=remaining_qty_to_close * avg_price,
                        with_pnl=True,
                        reduce_with_qty=remaining_qty_to_close,
                    )
                    return True
        else:
            # fail to close this iteration
            raise ValueError(f"ERROR: insufficient balance for {asset}")
            return False

    baseq = _base_query(in_session)
    for base, dt, qty, balance, quote, cost, forex, _type in baseq:
        multiplier = 1  # default for long positions

        if _type == CategoryType.CFDs:
            # CFDs are short or long positions, direction is determined by qty sign and balance
            if qty < 0 and balance < 0:
                # open short pos
                multiplier = -1

            if qty > 0 and balance <= 0:
                # close short pos
                multiplier = -1

        if qty * multiplier > Decimal(0):
            if quote:
                assert quote == "USD"
                _close_position(dt=dt, asset=quote, qty=-cost, value=-cost, forex=forex)
            else:

                USDNOK = forex_rate("USD", "NOK", dt)
                base_USD = forex_rate(base, "USD", dt)
                cost = -qty * base_USD
                forex = USDNOK
            _open_position(
                dt=dt,
                asset=base,
                qty=qty * multiplier,
                value=-cost * multiplier,
                forex=forex,
                multiplier=multiplier,
            )

        else:
            assert quote == "USD", f"error: quote=={quote}"
            _open_position(
                dt=dt, asset=quote, qty=cost, value=cost, forex=forex, multiplier=1
            )
            _close_position(
                dt=dt,
                asset=base,
                qty=-qty * multiplier,
                value=cost * multiplier,
                forex=forex,
            )


def calc_pnl_simplified(session, STATEMENT_END_DATE_STR):
    """
    Calculate the pnl in base and local in a simplified manner, i.e. multiply pnl with the current exrate
    meaning that the difference in exchange rate between buying and selling is not taken into account
    """
    BASE_CURRENCY = (
        session.query(NameValue.Value)
        .filter(
            NameValue.type == NameValueType.ACCOUNT_INFORMATION,
            NameValue.Name == "Base Currency",
        )
        .scalar()
    )
    q = session.query(
        Trade.Symbol,
        func.sum(Trade.Realized_PnL),
        func.sum(Trade.Realized_PnL * Trade.QuoteInLocalCurrency),
    ).group_by(Trade.Symbol)

    q_total = session.query(
        func.sum(Trade.Realized_PnL),
        func.sum(Trade.Realized_PnL * Trade.QuoteInLocalCurrency),
    )

    print(f"PROFIT AND LOSS AT {STATEMENT_END_DATE_STR}\n")
    for _asset, _pnl_base, _pnl_local in q:
        if _pnl_base:
            print(
                f"{_asset}: {_pnl_base} {BASE_CURRENCY}, {_pnl_local} {LOCAL_CURRENCY}"
            )

    print()
    for _pnl_base, _pnl_local in q_total:
        if _pnl_base:
            print(f"TOTAL: {_pnl_base} {BASE_CURRENCY}, {_pnl_local} {LOCAL_CURRENCY}")


def calc_net_dividend(session, STATEMENT_END_DATE_STR, BASE_CURRENCY):
    """
    Calc sum of dividend - witholding tax
    """
    dividends = session.query(
        Dividends.Symbol.label("asset"),
        Dividends.Currency.label("currency"),
        Dividends.Amount.label("amount"),
        (Dividends.Amount * Dividends.QuoteInLocalCurrency).label("amount_local"),
    )
    witholdings = session.query(
        WitholdingTax.Symbol.label("asset"),
        WitholdingTax.Currency.label("currency"),
        WitholdingTax.Amount.label("amount"),
        (WitholdingTax.Amount * WitholdingTax.QuoteInLocalCurrency).label(
            "amount_local"
        ),
    )
    net_dividend = dividends.union_all(witholdings).subquery()
    q = (
        session.query(
            net_dividend.c.asset.label("asset"),
            func.sum(net_dividend.c.amount).label("net_dividend"),
            func.sum(net_dividend.c.amount_local).label("net_dividend_local"),
        )
        .group_by(net_dividend.c.asset)
        .order_by(net_dividend.c.asset)
    )

    print(f"NET DIVIDENDS (DIVIDEND - WITHOLDING TAX) AT {STATEMENT_END_DATE_STR}\n")
    for row in q:
        print(
            f"{row.asset}: {row.net_dividend} {BASE_CURRENCY}, {row.net_dividend_local} {LOCAL_CURRENCY}"
        )


def cost_basis_query(session):
    """
    Return query that provides id, asset, delta, balance, average cost price in base
    currency and average cost price in local currency
    Provides a means to derive pnl_base and pnl_local from
    """
    q = (
        session.query(
            Trade.id.label("id"),
            Trade.DateTime.label("dt"),
            Trade.Symbol.label("asset"),
            Trade.Quantity.label("delta"),
            # Trade.QuoteInLocalCurrency.label('forex_rate'),
            func.sum(Trade.Quantity)
            .over(partition_by=Trade.Symbol, order_by=Trade.DateTime)
            .label("balance"),
            (
                (
                    func.sum(Trade.Quantity).over(
                        partition_by=Trade.Symbol, order_by=Trade.DateTime
                    )
                    - Trade.Quantity
                ).label("prev_balance")
            ),
            case(
                [
                    (Trade.Quantity > Decimal(0), Trade.Basis),
                ],
                else_=func.lag(Trade.Basis, 1, 0).over(
                    partition_by=Trade.Symbol, order_by=Trade.DateTime
                ),
            ).label("last_buy_basis"),
            case(
                [
                    (
                        Trade.Quantity > Decimal(0),
                        Trade.Basis * Trade.QuoteInLocalCurrency,
                    ),
                ],
                else_=func.lag(Trade.Basis * Trade.QuoteInLocalCurrency, 1, 0).over(
                    partition_by=Trade.Symbol, order_by=Trade.DateTime
                ),
            ).label("last_buy_basis_local"),
            case(
                [
                    (Trade.Quantity > Decimal(0), Trade.Basis / Trade.Quantity),
                ],
                else_=(
                    func.lag(Trade.Basis / Trade.Quantity, 1, 0).over(
                        partition_by=Trade.Symbol, order_by=Trade.DateTime
                    )
                ),
            ).label("last_avg_buy_price"),
            case(
                [
                    (
                        Trade.Quantity > Decimal(0),
                        Trade.Basis * Trade.QuoteInLocalCurrency / Trade.Quantity,
                    ),
                ],
                else_=(
                    func.lag(
                        Trade.Basis * Trade.QuoteInLocalCurrency / Trade.Quantity, 1, 0
                    ).over(partition_by=Trade.Symbol, order_by=Trade.DateTime)
                ),
            ).label("last_avg_buy_price_local"),
        ).order_by(Trade.Symbol)
    ).subquery()
    q2 = (
        session.query(
            q.c.id.label("id"),
            # q.c.forex_rate.label('forex_rate'),
            q.c.dt.label("dt"),
            q.c.asset.label("asset"),
            q.c.delta.label("delta"),
            q.c.balance.label("balance"),
            q.c.last_buy_basis.label("last_buy_basis"),
            q.c.prev_balance.label("prev_balance"),
            q.c.last_buy_basis_local.label("last_buy_basis_local"),
            q.c.last_avg_buy_price_local.label("last_avg_buy_price_local"),
            func.lag(q.c.last_avg_buy_price, 1, 0)
            .over(partition_by=q.c.asset, order_by=q.c.dt)
            .label("prev_buy_price"),
            func.lag(q.c.last_avg_buy_price_local, 1, 0)
            .over(partition_by=q.c.asset, order_by=q.c.dt)
            .label("prev_buy_price_local"),
            case(
                [
                    (
                        q.c.delta > Decimal(0),
                        (
                            q.c.last_buy_basis
                            + q.c.prev_balance
                            * func.lag(q.c.last_avg_buy_price, 1, 0).over(
                                partition_by=q.c.asset, order_by=q.c.dt
                            )
                        )
                        / q.c.balance,
                    ),
                ],  # noqa
                else_=literal(0),
            ).label("last_avg"),
            case(
                [
                    (
                        q.c.delta > Decimal(0),
                        (
                            q.c.last_buy_basis_local
                            + q.c.prev_balance
                            * func.lag(q.c.last_avg_buy_price_local, 1, 0).over(
                                partition_by=q.c.asset, order_by=q.c.dt
                            )
                        )
                        / q.c.balance,
                    ),
                ],  # noqa
                else_=literal(0),
            ).label("last_avg_local"),
        ).order_by(q.c.asset)
    ).subquery()
    q3 = session.query(
        q2.c.id.label("id"),
        # q2.c.forex_rate.label('forex_rate')
        q2.c.asset.label("asset"),
        q2.c.delta.label("delta"),
        q2.c.balance.label("balance"),
        case(
            [
                (q2.c.last_avg != literal(0), q2.c.last_avg),
            ],
            else_=func.lag(q2.c.last_avg, 1, 0).over(
                partition_by=q2.c.asset, order_by=q2.c.dt
            ),
        ).label("cost_price_base"),
        case(
            [
                (q2.c.last_avg_local != literal(0), q2.c.last_avg_local),
            ],
            else_=func.lag(q2.c.last_avg_local, 1, 0).over(
                partition_by=q2.c.asset, order_by=q2.c.dt
            ),
        ).label("cost_price_local"),
    ).order_by(q2.c.asset)
    return q3


def pnl_asset_query(session: Session) -> Query:
    """
    Return the change in traded asset quantities that affect trading position value, i.e. any changes in
    assets quantities that affect profit and loss
    Also track FIAT quantity changes with exchange rate to be able to calculate forex related losses

    consists of mark2market (starting balances), trades, transfers (to be able to transfer open tradePositions
    between accounts), deposits and withdrawals
    sorted by asset and by date

    since the quote currency forex exchange rate is unknown during add to db stage,
    the fee for forex trades is ignored and has to be added manually (use get_forex_trade_fees....)
    :returns selectable: account, base (currency), dt, qty, balance,
    quote currency, cost (in quote currency), forex rate, type and model type
    sorted by date, asset and account
    :param session: session object towards database containing asset movements
    :returns Query: SQLAlchemy query result
    """
    # Infer Base Currency
    BASE_CURRENCY = (
        session.query(NameValue.Value)
        .filter(
            NameValue.type == NameValueType.ACCOUNT_INFORMATION,
            NameValue.Name == "Base Currency",
        )
        .first()[0]
    )

    # Infer balance date from statement
    STATEMENT_PERIOD = (
        session.query(NameValue.Value)
        .filter(NameValue.type == NameValueType.STATEMENT, NameValue.Name == "Period")
        .first()[0]
    )
    STATEMENT_PERIOD_SPLIT = STATEMENT_PERIOD.split(" - ")
    STATEMENT_START_DATE = datetime.strptime(STATEMENT_PERIOD_SPLIT[0], "%B %d, %Y")
    STATEMENT_END_DATE = datetime.strptime(
        STATEMENT_PERIOD_SPLIT[-1], "%B %d, %Y"
    ) + timedelta(hours=23, minutes=59, seconds=59)
    # infer FOREX trades and currencies required for Rates
    ForexPairs = set()
    FiatTrades = set()
    symbols = (
        session.query(Trade.Symbol)
        .filter(Trade.Asset_Category.like("Forex%"))
        .group_by(Trade.Symbol)
        .all()
    )
    for symbol in symbols:
        ForexPairs.add(symbol[0])
        base, quote = symbol[0].split(".")
        FiatTrades.add(base)
        FiatTrades.add(quote)

    EOY_FOREX_RATES, BOY_FOREX_RATES = {}, {}
    for currency in FiatTrades:
        BOY_FOREX_RATES[currency] = forex_rate(
            currency, LOCAL_CURRENCY, STATEMENT_START_DATE
        )
        EOY_FOREX_RATES[currency] = forex_rate(
            currency, LOCAL_CURRENCY, STATEMENT_END_DATE
        )

    DEDICATED_TRADE_IMPLEMENTATIONS = [
        enums.CategoryType.FOREX,
        enums.CategoryType.FOREX_CFDs,
    ]

    # SHOULD INCLUDE STARTING BALANCE
    starting_balances = session.query(
        Mark2Market.Account.label("account"),
        Mark2Market.Prior_Quantity.label("qty"),
        Mark2Market.Symbol.label("base"),
        literal(STATEMENT_START_DATE).label("dt"),
        literal(BOY_FOREX_RATES.get(BASE_CURRENCY)).label("forex"),
        null().label("quote"),
        null().label("cost"),
        Mark2Market.type.label("t"),
        literal(enums.ModelType.MARK2MARKET.name).label("m"),
    ).filter(Mark2Market.Prior_Quantity > Decimal(0))
    trade1 = session.query(  # stock, option and CFD trades
        Trade.Account.label("account"),
        Trade.Quantity.label("qty"),
        Trade.Symbol.label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
        Trade.Currency.label("quote"),
        case(
            [
                (Trade.Proceeds, Trade.Proceeds + Trade.CommOrFee),
                (Trade.Notional_Value, Trade.Notional_Value + Trade.CommOrFee),
            ],
            else_=literal(Decimal(0)),
        ).label("cost"),
        Trade.type.label("t"),
        literal(ModelType.TRADE.name).label("m"),
    ).filter(Trade.type.notin_(DEDICATED_TRADE_IMPLEMENTATIONS))
    forex_cfd = session.query(
        Trade.Account.label("account"),
        Trade.Quantity.label("qty"),
        func.substr(Trade.Symbol, 1, func.instr(Trade.Symbol, ".") - 1).label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
        Trade.Currency.label("quote"),
        case(  # COST
            [
                (
                    Trade.Currency
                    == func.substr(Trade.Symbol, func.instr(Trade.Symbol, ".") + 1),
                    Trade.Notional_Value,
                ),
            ],
            else_=literal(Decimal(0)),
        ).label("cost"),
        Trade.type.label("t"),
        literal(ModelType.TRADE.name).label("m"),
    ).filter(Trade.type == CategoryType.FOREX_CFDs)
    forex_cfd_fee = session.query(  # forex cfd fee
        # if currency == split -1, use qty
        # elif currency == split 0, raise not implemented (not seen yet)
        Trade.Account.label("account"),
        Trade.CommOrFee.label("qty"),
        literal("USD").label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
        null().label("quote"),
        null().label("cost"),
        Trade.type.label("t"),
        literal(ModelType.TRADE.name).label("m"),
    ).filter(Trade.type == CategoryType.FOREX_CFDs)
    forex_trade = session.query(  # trade base exclusively forex
        Trade.Account.label("account"),
        Trade.Quantity.label("qty"),
        func.substr(Trade.Symbol, 1, func.instr(Trade.Symbol, ".") - 1).label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
        func.substr(Trade.Symbol, func.instr(Trade.Symbol, ".") + 1).label("quote"),
        Trade.Proceeds.label("cost"),
        Trade.type.label("t"),
        literal(ModelType.TRADE.name).label("m"),
    ).filter(Trade.type == CategoryType.FOREX)
    forex_trade_fee = session.query(  # trade quote exclusively USD fee
        Trade.Account.label("account"),
        Trade.Comm_in_USD.label("qty"),
        literal("USD").label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
        null().label("quote"),
        null().label("cost"),
        Trade.type.label("t"),
        literal(ModelType.TRADE.name).label("m"),
    ).filter(Trade.type == CategoryType.FOREX, Trade.Comm_in_USD < Decimal(0))
    depowith = session.query(
        DepositsWithdrawals.Account.label("account"),
        DepositsWithdrawals.Amount.label("qty"),
        DepositsWithdrawals.Currency.label("base"),
        DepositsWithdrawals.DateTime.label("dt"),
        DepositsWithdrawals.QuoteInLocalCurrency.label("forex"),
        null().label("quote"),
        null().label("cost"),
        literal(enums.CategoryType.FOREX).label("t"),
        literal(enums.ModelType.DEPOSITWITHDRAWAL.name).label("m"),
    )
    transfers = session.query(
        Transfer.Account.label("account"),
        Transfer.Qty.label("qty"),
        Transfer.Symbol.label("base"),
        Transfer.DateTime.label("dt"),
        Transfer.QuoteInLocalCurrency.label("forex"),
        null().label("quote"),
        null().label("cost"),
        Transfer.type.label("t"),
        literal(enums.ModelType.TRANSFER.name).label("m"),
    )
    dividends = session.query(
        Dividends.Account.label("account"),
        Dividends.Amount.label("qty"),
        Dividends.Currency.label("base"),
        Dividends.DateTime.label("dt"),
        Dividends.QuoteInLocalCurrency.label("forex"),
        null().label("quote"),
        null().label("cost"),
        literal(CategoryType.STOCKS).label("t"),
        literal(ModelType.DIVIDEND.name).label("m"),
    )
    witholdingtax = session.query(
        WitholdingTax.Account.label("account"),
        WitholdingTax.Amount.label("qty"),
        WitholdingTax.Currency.label("base"),
        WitholdingTax.DateTime.label("dt"),
        WitholdingTax.QuoteInLocalCurrency.label("forex"),
        null().label("quote"),
        null().label("cost"),
        literal(CategoryType.STOCKS).label("t"),
        literal(ModelType.WITHOLDINGTAX.name).label("m"),
    )
    fees = session.query(
        Fees.Account.label("account"),
        Fees.Amount.label("qty"),
        Fees.Currency.label("base"),
        Fees.DateTime.label("dt"),
        Fees.QuoteInLocalCurrency.label("forex"),
        null().label("quote"),
        null().label("cost"),
        literal(CategoryType.NONE).label("t"),
        literal(ModelType.FEE.name).label("m"),
    )
    u = starting_balances.union_all(
        trade1,
        forex_cfd,
        forex_cfd_fee,
        forex_trade,
        forex_trade_fee,
        depowith,
        transfers,
        dividends,
        witholdingtax,
        fees,
    ).subquery()
    q1 = session.query(
        u.c.account,
        u.c.base,
        u.c.dt,
        u.c.qty,
        func.sum(u.c.qty)
        .over(partition_by=(u.c.account, u.c.base), order_by=u.c.dt)
        .label("balance"),
        u.c.quote,
        cast(u.c.cost, Numeric(8, 4)).label("cost"),
        u.c.forex,
        u.c.t,
        u.c.m,
    ).order_by(
        u.c.dt, u.c.base, u.c.account
    )  # .cte()

    return q1


def calc_tradepositions(
    in_session: Session,
    out_session: Session,
    forex_rate: ForexRate,
    debug: bool = False,
) -> None:
    """
    process the pnl asset query rows to create and update tradepositions

    *** NOTE: ISIN changes must be tracked, see Corporate Actions ***
    *** NOTE: Stock splits not implemented ***
    *** NOTE: margin accounts require margin - due to this specific cash account implementation,
        negative FIAT may be possible
    ***

    :param in_session: session to apply base query onto
        ***** should contain TRADEPOSITIONS carried over from previous period!!!! *****
    :param out_session: session to export tradePositions onto
    :param forex_rate: forex utility to fetch fiat asset USD values
    """
    DEDICATED_TRADE_IMPLEMENTATIONS = [
        enums.CategoryType.FOREX,
        enums.CategoryType.FOREX_CFDs,
    ]
    TRANSFER_PAIRS = defaultdict(
        list
    )  # used to track matching transfers between accounts
    FIAT = ["USD", "NOK", "GBP", "EUR"]

    ROUND_ERROR_MARGIN = Decimal("0.99")
    # relevant for closing trades (assuming CFDs/ shorting not subject to namechange)
    ISIN_CHANGE = {}  # list for ticker changes
    _revISIN = {val: key for key, val in ISIN_CHANGE.items()}

    def _open_position(
        account: str,
        dt: datetime,
        asset: str,
        qty: Decimal,
        value: Decimal,
        forex: Decimal,
        multiplier: int = 1,
    ):
        """
        open a tradePosition
        """
        if qty < Decimal(0):
            raise ValueError(f"{account} {dt} {asset} {qty} {value} {multiplier}")

        assert value >= Decimal("0"), "ERROR: value < 0"

        if not value:
            print(f"WARNING: None value {asset} {value} {dt}")
        if value == Decimal(0):
            print(f"WARNING: opening 0 value {asset} {value} {dt}")

        if qty.is_zero():
            print(f"SKIP 0 QTY OPEN {asset} @ {dt}")
            return
        # print(f"OPEN {asset} @ {dt}")

        # CHECK IF THERE IS A DEFICIT FIAT POSITION TO REDUCE
        if asset in FIAT:
            deficit_totals = (
                out_session.query(func.sum(tradePosition.qty))
                .filter(
                    tradePosition.status == enums.TradePositionStatus.OPEN,
                    tradePosition.qty < Decimal(0),
                    tradePosition.open_dt < dt,
                    tradePosition.Account == account,
                    tradePosition.asset == asset,
                )
                .scalar()
            )
            if deficit_totals:
                deficits = out_session.query(tradePosition).filter(
                    tradePosition.status == enums.TradePositionStatus.OPEN,
                    tradePosition.qty < Decimal(0),
                    tradePosition.open_dt < dt,
                    tradePosition.Account == account,
                    tradePosition.asset == asset,
                )

                print(f"CLOSING DEFICITS FOR {asset} @ {dt} (deficit {deficit_totals})")
                remaining_deficits_to_fill = qty
                for deficit in deficits:
                    if -deficit.qty < remaining_deficits_to_fill:
                        remaining_deficits_to_fill += deficit.qty
                        # consume this deficit
                        _consume_position(
                            open_pos=deficit,
                            close_dt=dt,
                            forex_rate=forex,
                            exit_value=-deficit.qty * value / qty,
                            with_pnl=True,
                        )
                    else:
                        # reduce with remaining_deficits_to_fill
                        _reduce_position(
                            open_pos=deficit,
                            close_dt=dt,
                            forex_rate=forex,
                            exit_value=remaining_deficits_to_fill * value / qty,
                            with_pnl=True,
                            reduce_with_qty=remaining_deficits_to_fill,
                        )
                        remaining_deficits_to_fill -= remaining_deficits_to_fill
                    if remaining_deficits_to_fill.is_zero():
                        print(
                            f"DEFICITS RESOLVED FOR {asset}, opening {remaining_deficits_to_fill}"
                        )
                        return  # done!
                qty = remaining_deficits_to_fill  # if here, all deficits have been resolved but there is still a remaining qty to open

        out_session.add(
            tradePosition(
                Account=account,
                qty=qty,
                open_dt=dt,
                status=enums.TradePositionStatus.OPEN,
                asset=asset,
                o_price_base=value / qty,
                o_price_local=value / qty * forex,
                multiplier=multiplier,
            )
        )
        out_session.commit()

        return

    def _consume_position(
        open_pos: tradePosition,
        close_dt: datetime,
        forex_rate: Optional[Decimal] = None,
        exit_value: Optional[Decimal] = None,
        with_pnl: bool = True,
    ):
        """
        Provided an open_pos, fully consume it by closing the position
        add pnl if with_pnl==True otherwise close without pnl
        :param open_pos: open tradePosition to consume
        :param close_dt: time for closing position
        :param exit_value: exit value for trade (if relevant)
        :param forex_rate: valid forex rate at time of closing position
        :param with_pnl: flag to indicate whether to realize pnl or not
        """
        if not with_pnl:
            id = open_pos.id
            out_session.query.filter_by(id=id).delete()
        else:
            # fully consume and close
            qty = abs(open_pos.qty)  # NEW TO HANDLE FIAT DEFICITS
            open_pos.status = enums.TradePositionStatus.CLOSED
            open_pos.close_dt = close_dt
            open_pos.c_price_base = exit_value / qty
            open_pos.c_price_local = exit_value / qty * forex_rate
            open_pos.pnl_base = (
                exit_value - qty * open_pos.o_price_base
            ) * open_pos.multiplier
            open_pos.pnl_local = (
                exit_value * forex_rate - qty * open_pos.o_price_local
            ) * open_pos.multiplier
        out_session.commit()

    def _reduce_position(
        open_pos: tradePosition,
        close_dt: datetime,
        reduce_with_qty: Decimal,
        forex_rate: Optional[Decimal] = None,
        exit_value: Optional[Decimal] = None,
        with_pnl: bool = False,
    ):
        """
        Provided an open_pos.qty with reduce_with_qty by updating the qty and creating
        a closed position with or without pnl for qty==reduce_with_qty
        :param open_pos: open tradePosition to consume
        :param forex_rate: foreign exchange rate
        :param dt: time for closing position
        :reduce_with_qty: quantity to reduce position with
        :param with_pnl: flag to indicate whether to realize pnl or not
        :optional param forex_rate: forex rate @ dt
        :optional param exit_value: value in base currency of qty position
        """
        # print(f"REDUCE {open_pos.asset} @ {close_dt}")
        # reduce the open position
        if open_pos.qty > Decimal(0):
            open_pos_new_q = open_pos.qty - reduce_with_qty
        else:
            open_pos_new_q = (
                open_pos.qty + reduce_with_qty
            )  # NEW TO HANDLE WITH DEFICITS
        open_pos.qty = open_pos_new_q

        if with_pnl:
            # print(f"CALC PNL for {open_pos.asset} @ {close_dt} having exit_value {exit_value}")
            c_price_base = exit_value / reduce_with_qty
            c_price_local = exit_value / reduce_with_qty * forex_rate
            pnl_base = (
                exit_value - reduce_with_qty * open_pos.o_price_base
            ) * open_pos.multiplier
            pnl_local = (
                exit_value * forex_rate - reduce_with_qty * open_pos.o_price_local
            ) * open_pos.multiplier

            # create closed position
            out_session.add(
                tradePosition(
                    Account=open_pos.Account,
                    o_price_base=open_pos.o_price_base,
                    o_price_local=open_pos.o_price_local,
                    c_price_base=c_price_base,
                    c_price_local=c_price_local,
                    pnl_base=pnl_base,
                    pnl_local=pnl_local,
                    close_dt=close_dt,
                    open_dt=open_pos.open_dt,
                    status=enums.TradePositionStatus.CLOSED,
                    asset=open_pos.asset,
                    qty=reduce_with_qty,
                    multiplier=open_pos.multiplier,
                )
            )
        out_session.commit()

    def _close_position(
        account: str,
        dt: datetime,
        asset: str,
        qty: Decimal,
        value: Decimal,
        forex: Decimal,
        multiplier: int = 1,
    ) -> bool:
        """
        Check if a position can be closed and execute if possible
        return True when succesfull, else False
        """
        # if asset in FIAT:
        #    forex = forex_rate(asset, "USD", dt)
        #    #assert forex == _forex, f"ERROR: {asset} {forex} != {_forex}"
        if qty < Decimal(0):
            raise ValueError(f"{account} {dt} {asset} {qty} {value}")

        # Track ISIN changes
        if asset in _revISIN.keys():
            print(f"CHANGED TICKER FOR {asset} to {_revISIN.get(asset)}")
            asset = _revISIN.get(asset)

        # Track zero qty positions - should not occur
        if qty.is_zero():
            raise ValueError("ERROR: qty==0")

        assert value >= Decimal("0"), "ERROR: value < 0"

        # check open tradeposition asset balance up to dt
        pos_balance_at_dt = (
            out_session.query(
                func.sum(tradePosition.qty)
            )  # ALSO TAKES INTO ACCOUNT DEFICITS!
            .filter(
                tradePosition.Account == account,
                tradePosition.asset == asset,
                tradePosition.open_dt <= dt,
                tradePosition.status == enums.TradePositionStatus.OPEN,
            )
            .scalar()
        )

        if not pos_balance_at_dt:
            if asset in FIAT:
                pos_balance_at_dt = Decimal(0)  # 0 balance
            else:
                raise ValueError("NO BALANCE AVAILABLE FOR {asset} @ {dt}")

        ASSUMED_ROUNDING_ERROR_MARGIN = (
            pos_balance_at_dt > ROUND_ERROR_MARGIN * qty
        ) and (pos_balance_at_dt < qty)
        # evaluates to TRUE if error*margin * qty < pos_balance_at_dt < qty
        if ASSUMED_ROUNDING_ERROR_MARGIN:
            qty = pos_balance_at_dt

        # start finding open positions and reduce qty
        avg_price = value / qty
        remaining_qty_to_close = qty
        q = (
            out_session.query(tradePosition)
            .filter(
                tradePosition.Account == account,
                tradePosition.open_dt <= dt,
                tradePosition.asset == asset,
                tradePosition.qty
                > Decimal(0),  # skip deficits - they can not be closed
                tradePosition.status == enums.TradePositionStatus.OPEN,
            )
            .order_by(tradePosition.open_dt)  # FIFO
        )

        for trade_pos in q:  # may be empty
            # either trade_pos.qty < remaining, close pos and continue
            # or trade_pos.qty == remaining, close pos and exit
            # or trade_pos.qty > remaining, reduce pos and exit
            if remaining_qty_to_close.is_zero():
                # print("remaining qty to close is zero - break")
                break  # DONE

            # IF THIS POS HAS A NEGATIVE QTY! SKIP IT!
            # if trade_pos.qty < Decimal(0):
            #    continue # skip this - must be a FIAT deficit, should only be dealed with by opening positions

            if remaining_qty_to_close >= trade_pos.qty:
                # fully consume and close
                remaining_qty_to_close -= trade_pos.qty
                _consume_position(
                    open_pos=trade_pos,
                    close_dt=dt,
                    forex_rate=forex,
                    exit_value=trade_pos.qty * avg_price,
                    with_pnl=True,
                )

            else:
                # consume partly and close and make new open pos for remainder
                _reduce_position(
                    open_pos=trade_pos,
                    close_dt=dt,
                    forex_rate=forex,
                    exit_value=remaining_qty_to_close * avg_price,
                    with_pnl=True,
                    reduce_with_qty=remaining_qty_to_close,
                )
                remaining_qty_to_close -= remaining_qty_to_close
        if remaining_qty_to_close.is_zero():
            return True
        elif asset in FIAT:
            # open deficit position (-qty)
            print(f"OPENING {remaining_qty_to_close} DEFICIT FOR {asset} @ {dt}")
            out_session.add(
                tradePosition(
                    Account=account,
                    open_dt=dt,
                    qty=-remaining_qty_to_close,
                    status=enums.TradePositionStatus.OPEN,
                    asset=asset,
                    multiplier=multiplier,
                    o_price_base=value / qty,
                    o_price_local=value / qty * forex,
                )
            )
            out_session.commit()
            return True
        else:
            raise ValueError(
                "DID NOT EXPECT TO BE HERE - STILL REMAINING QTY TO CLOSE..."
            )

    def _process_transfer_tradepos(key) -> bool:
        """
        Process a transfer, i.e. close an existing account tradePosition from which transfer
        is initiated, and open tradePosition for  account to which transfer is made

        Note: transfers have date vs datetime timestamp, i.e. trades can happen on same day.
        Transfers OUT should be artificially timestamped from date to end of date datetime and
        Transfers IN should be artificially timestamped to beginning of day

        :param key: key of (dt, asset) indicating which transfer pair in TRANSFER_PAIRS dict to process
        """
        # print(f"TRANSFER: {key}, {TRANSFER_PAIRS[key]}")
        dt, asset = key
        (acc1, qty1), (acc2, qty2) = TRANSFER_PAIRS[key]  # (acc1, qty1), (acc2, qty2)
        assert qty1 == -qty2, f"ERROR: {qty1} != - {qty2}"
        # find the tradepos for the -qty account and transfer it to new account
        if qty1 < Decimal(0):
            from_acc = acc1
            from_qty = qty1  # note the sign is negative
            to_acc = acc2
            to_qty = qty2
        else:
            from_acc = acc2
            from_qty = qty2  # note the sign is negative
            to_acc = acc1
            to_qty = qty1

        # print(f"from {from_acc} {from_qty}, to {to_acc} {to_qty}")

        out_session.flush()
        qty_available_at_dt = (
            out_session.query(func.sum(tradePosition.qty))
            .filter(
                tradePosition.Account == from_acc,  # 44
                tradePosition.asset == asset,
                tradePosition.open_dt <= dt + timedelta(days=1),
                tradePosition.status == enums.TradePositionStatus.OPEN,
            )
            .scalar()
        )

        # print(f"TRANSFER {to_qty} from {from_acc} to {to_acc} having {qty_available_at_dt} @ {dt} + 23hrs")

        if not qty_available_at_dt:
            out_session.commit()
            # print(f"CANT FIND SUFFICIENT TRADEPOS BALANCE FOR {asset}")
            # print(f"dt {dt} from_qty {from_qty}")
            qty_a = (
                out_session.query(func.sum(tradePosition.qty))
                .filter(
                    tradePosition.asset == asset,
                    tradePosition.open_dt
                    <= datetime(2021, 1, 13),  # (dt + timedelta(hours=23, minutes=59)),
                    tradePosition.status == enums.TradePositionStatus.OPEN,
                )
                .scalar()
            )
            # print(f"qty_a {qty_a}")
            raise ValueError

        ASSUMED_ROUNDING_ERROR_MARGIN = (
            qty_available_at_dt > ROUND_ERROR_MARGIN * -from_qty
        ) and (qty_available_at_dt < -from_qty)
        if (qty_available_at_dt >= -from_qty) or ASSUMED_ROUNDING_ERROR_MARGIN:
            if ASSUMED_ROUNDING_ERROR_MARGIN:
                qty = qty_available_at_dt
            else:
                qty = -from_qty

            # start finding open tradePositions to transfer from
            q = (
                out_session.query(tradePosition)
                .filter(
                    tradePosition.Account == account,
                    tradePosition.open_dt <= dt + timedelta(days=1),
                    tradePosition.asset == asset,
                    tradePosition.status == enums.TradePositionStatus.OPEN,
                )
                .order_by(tradePosition.open_dt)  # FIFO
            )

            remaining_qty_to_transfer = qty
            for trade_pos in q:
                # either trade_pos.qty < qty, transfer this whole pos and continue
                # or trade_pos.qty == remaining, transfer pos and exit
                # or trade_pos.qty > remaining, reduce tradepos w qty, open tradepos for qty with to_account and exit
                if remaining_qty_to_transfer.is_zero():
                    return True

                if remaining_qty_to_transfer >= trade_pos.qty:
                    # fully transfer
                    trade_pos.Account = to_acc
                    out_session.commit()
                    remaining_qty_to_transfer -= trade_pos.qty
                else:
                    # reduce partly and close and make new open pos for remainder
                    trade_pos.qty -= remaining_qty_to_transfer
                    out_session.commit()
                    _d = object_as_dict(trade_pos)
                    _d.pop("id")
                    _d["qty"] = remaining_qty_to_transfer
                    _d["Account"] = to_acc
                    out_session.add(tradePosition(**_d))
                    out_session.commit()

        else:
            # fail to close this iteration
            raise ValueError(f"ERROR: insufficient balance for {asset}")
            return False
        # out_session.commit()

        return True

    # ----- ENTRY POINT
    # export open tradePositions from in_session to out_session db
    for tradepos in in_session.query(tradePosition).filter(
        tradePosition.status == enums.TradePositionStatus.OPEN
    ):
        tp_dict = object_as_dict(tradepos)
        tp_dict.pop("id")
        out_session.add(tradePosition(**tp_dict))
    out_session.commit()

    # verify open tradepos qty vs starting_balances
    starting_balances = in_session.query(
        Mark2Market.Account.label("account"),
        Mark2Market.Prior_Quantity.label("qty"),
        Mark2Market.Symbol.label("base"),
    ).filter(Mark2Market.Prior_Quantity > Decimal(0))
    for account, qty, base in starting_balances:
        tpq = (
            out_session.query(func.sum(tradePosition.qty))
            .filter(
                tradePosition.asset == base,
                tradePosition.Account == account,
                tradePosition.status == enums.TradePositionStatus.OPEN,
            )
            .scalar()
        )
        if not (qty - tpq).is_zero():
            # adjust the last opened open tradePos to qty equal to starting balances for account
            pct = (100 * ((qty - tpq) / qty)).quantize(Decimal("1.00"))
            if debug:
                print(f"{account} {qty} {base}, lacks {qty-tpq} ({pct}%) ... updating")
            tp = (
                out_session.query(tradePosition)
                .filter(
                    tradePosition.Account == account,
                    tradePosition.asset == base,
                    tradePosition.status == enums.TradePositionStatus.OPEN,
                )
                .order_by(-tradePosition.open_dt)
                .first()
            )
            tp.qty = tp.qty + qty - tpq
            out_session.commit()

    # fetch pnl base and forex fees in quote currency
    # baseq = pnl_base_query_without_forex_fees(session = in_session)
    baseq = pnl_asset_query(session=in_session)
    # forex_fees = get_forex_trade_fees_in_quote_currency(session = in_session, forex_rate=forex_rate)

    # process tradepositions for base query rows
    for account, base, dt, qty, balance, quote, cost, forex, _type, _model in baseq:

        if debug:
            print(
                f"{account}. {base}, {dt}, {qty}, {balance}, {quote}, {cost}, {forex}, {_type}, {_model}"
            )
        multiplier = 1  # default for long positions

        if _model == "MARK2MARKET":
            continue  # starting balances do not contribute to pnl

        # check for transfers that require dedicated tradePosition creation
        if _model == "TRANSFER":
            key = (
                dt.date(),
                base,
            )  # the pairs key, defaults to the date, i.e. always before any trade occurring same day
            TRANSFER_PAIRS[key].append((account, qty))
            if len(TRANSFER_PAIRS[key]) == 2:
                # print("GOT 2 TRANSFERS FOR SAME ASSET AND DATE!!! PROCESSING!!!!! -------")
                _process_transfer_tradepos(key)
                TRANSFER_PAIRS.pop(key)

            continue  # done with this row if here...

        qib = Decimal(1)  # quote in base
        USDNOK = forex_rate("USD", "NOK", dt)
        if quote and quote != "USD":
            qib = forex_rate(quote, "USD", dt)

        if qty < 0:
            if (
                balance < 0 and quote and base not in FIAT
            ):  # add and quote to avoid FIAT insufficient balances allow below 0 balance
                multiplier = -1
            # open quote if exists
            if quote:
                # sell USD.NOK, forex==1 (quote inlocalcurrenyc) - but need to open cost NOK at value
                if debug:
                    print(f"OPENING QUOTE qty {cost} value {cost} multi {1}")
                _open_position(
                    account=account,
                    dt=dt,
                    asset=quote,
                    qty=cost,
                    value=cost * qib,
                    forex=USDNOK,
                    multiplier=1,  # QUOTE DOES NOT REQUIRE NEGATIVE MULTIPLIER
                )
            else:
                # withdrawal
                base_USD = forex_rate(base, "USD", dt)
                cost = (
                    -qty * base_USD
                )  # positive in order to align with short sell and long buy

        elif qty > 0:
            if balance <= 0 and quote and base not in FIAT:
                multiplier = -1
            # close quote if exists
            if quote:
                if debug:
                    print(f"CLOSING QUOTE qty {-cost} value {-cost} multi {1}")
                _close_position(
                    account=account,
                    dt=dt,
                    asset=quote,
                    qty=-cost,
                    value=-cost * qib,
                    forex=USDNOK,
                    multiplier=1,  # QUOTE DOES NOT REQUIRE NEGATIVE MULTIPLIER
                )
            else:
                # deposit FIAT, open a position with price in USD
                base_USD = forex_rate(base, "USD", dt)
                cost = (
                    -qty * base_USD
                )  # negative in order to align with short buy and long sell

        if qty * multiplier > Decimal(0):
            if debug:
                print(
                    f"OPEN BASE qty {qty*multiplier} value {-cost*multiplier} multi {multiplier}"
                )
                if multiplier == -1:
                    print("*****------------******--------")
            # opening short pos (sell), qty < 0, multi < 0, cost > 0: multi*qty>0, multi*cost < 0,
            # or buying long pos, qty > 0, multi > 0, cost < 0, multi*cost < 0
            # deposit, qty > 0, multi > 0, cost SHOULD BE NEGATIVE; ergo -qty * base_USD in above calc, then also multi*cost < 0
            _open_position(
                account=account,
                dt=dt,
                asset=base,
                qty=qty * multiplier,
                value=-cost * multiplier * qib,
                forex=USDNOK,
                multiplier=multiplier,
            )
        else:
            # closing short pos (buy), qty > 0, multi = -1, cost < 0, qty*multi < 0, cost*multi>0
            # or selling long pos, qty < 0, cost > 0, multi = 1, qty*multi < 0, cost*multi>0
            # withdrawal, qty < 0, multi = 1, cost * multi must be > 0, i.e. cost must be > 0
            if debug:
                print(
                    f"CLOSE BASE qty {-qty*multiplier} value {cost*multiplier} multi {multiplier}"
                )
                if multiplier == -1:
                    print("*****------------******--------")
            _close_position(
                account=account,
                dt=dt,
                asset=base,
                qty=-qty * multiplier,
                value=cost * multiplier * qib,
                forex=USDNOK,
                multiplier=multiplier,
            )

    assert len(TRANSFER_PAIRS) == 0, "ERROR: unprocessed transfer pairs remaining"

    return


def calc_pnl(session):
    """
    DEPRECATED!!
    calculate the pnl for trades on the basis of the average cost basis (buy) price
    """
    print("DEPRECATED! use calc_tradepositions")
    cb = cost_basis_query(session).subquery()
    sells = (
        session.query(
            Trade.id,
            Trade.DateTime,
            Trade.T_Price.label("sell_price"),
            Trade.QuoteInLocalCurrency.label("forex"),
            cb.c.asset,
            cb.c.delta,
            cb.c.balance,
            cb.c.cost_price_base,
            cb.c.cost_price_local,
        )
        .filter(Trade.Quantity < Decimal(0))
        .outerjoin(cb, cb.c.id == Trade.id)
    )
    for (
        _id,
        _dt,
        _sell_price,
        _forex,
        _asset,
        _delta,
        _balance,
        _cost_price_base,
        _cost_price_local,
    ) in sells:
        print(
            f"TRADE {_id} {_delta} {_asset} PNL {_delta * (_sell_price - _cost_price_base)}"
            f" PNL LOCAL {_delta * (_sell_price * _forex - _cost_price_local)}"
        )


def __dca_price_query(session):
    """
    Calculates the dollar cost average price for traded symbols
    :param session: active database session object
    :returns q: query object containing trade symbol, trade dt and dca_price for traded symbol @ dt
    """
    q = session.query(
        Trade.Symbol.label("symbol"),
        Trade.DateTime.label("dt"),
        (
            func.sum(Trade.Basis).over(
                partition_by=Trade.Symbol, order_by=(Trade.DateTime)
            )
            / func.sum(Trade.Quantity).over(
                partition_by=Trade.Symbol, order_by=(Trade.DateTime)
            )
        ).label("dca_price"),
    )
    return q


def __avg_entry_price_query(session):
    """
    Calculates the average entry price exclusively on the basis of bought symbols
    :param session: active database session object
    :returns q: query object containing trade symbol, trade.dt asset avg_entry_price @ dt,
    the latter being a buy quantity weighted average price which can be used to calculate pnl against
    """
    q = (
        session.query(
            Trade.DateTime.label("dt"),
            Trade.Symbol.label("asset"),
            Trade.Quantity.label("delta"),
            func.sum(Trade.Quantity)
            .over(partition_by=Trade.Symbol, order_by=Trade.DateTime)
            .label("balance"),
            (
                (
                    func.sum(Trade.Quantity).over(
                        partition_by=Trade.Symbol, order_by=Trade.DateTime
                    )
                    - Trade.Quantity
                ).label("prev_balance")
            ),
            case(
                [
                    (Trade.Quantity > Decimal(0), Trade.Basis),
                ],
                else_=func.lag(Trade.Basis, 1, 0).over(
                    partition_by=Trade.Symbol, order_by=Trade.DateTime
                ),
            ).label("last_buy_basis"),
            case(
                [
                    (Trade.Quantity > Decimal(0), Trade.Basis / Trade.Quantity),
                ],
                else_=(
                    func.lag(Trade.Basis / Trade.Quantity, 1, 0).over(
                        partition_by=Trade.Symbol, order_by=Trade.DateTime
                    )
                ),
            ).label("last_avg_buy_price"),
        ).order_by(Trade.Symbol)
    ).subquery()
    q2 = (
        session.query(
            q.c.dt.label("dt"),
            q.c.asset.label("asset"),
            q.c.delta.label("delta"),
            q.c.balance.label("balance"),
            q.c.last_buy_basis.label("last_buy_basis"),
            q.c.prev_balance.label("prev_balance"),
            func.lag(q.c.last_avg_buy_price, 1, 0)
            .over(partition_by=q.c.asset, order_by=q.c.dt)
            .label("prev_buy_price"),
            case(
                [
                    (
                        q.c.delta > Decimal(0),
                        (
                            q.c.last_buy_basis
                            + q.c.prev_balance
                            * func.lag(q.c.last_avg_buy_price, 1, 0).over(
                                partition_by=q.c.asset, order_by=q.c.dt
                            )
                        )
                        / q.c.balance,
                    ),
                ],
                else_=literal(0),
            ).label("last_avg"),
        ).order_by(q.c.asset)
    ).subquery()
    q3 = session.query(
        q2.c.asset.label("symbol"),
        q2.c.dt.label("dt"),
        case(
            [
                (q2.c.last_avg != literal(0), q2.c.last_avg),
            ],
            else_=func.lag(q2.c.last_avg, 1, 0).over(
                partition_by=q2.c.asset, order_by=q2.c.dt
            ),
        ).label("avg_entry_price"),
    ).order_by(q2.c.asset)
    return q3


def __calc_pnl(
    session, closing_principle: str = "FIFO", BASE_CURRENCY="USD", LOCAL_CURRENCY="NOK"
):
    """
    Add pnl data to the database and calculate the pnl on the basis of the trades
    :param closing_principle: one of ["LIFO", "FIFO"], i.e. last-in-first-out or first-in-first-out,
    when closing a position, this indicates whether to close the lastest opened (LIFO) or earlierst
    opened (FIFO) position first
    """
    BASE_CURRENCY = (
        session.query(NameValue.Value)
        .filter(
            NameValue.type == NameValueType.ACCOUNT_INFORMATION,
            NameValue.Name == "Base Currency",
        )
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
        commit: bool = True,
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
        session.add(
            tradePosition(
                qty=qty,
                open_dt=open_dt,
                status=TradePositionStatus.OPEN,
                asset=asset,
                base_c=base_c,
                local_c=local_c,
                open_price=basis / qty,
                open_forex=open_forex,
            )
        )
        session.commit()

    def _close_position(session, trade, closing_principle) -> bool:
        """
        Check if a position can be closed and execute if possible
        return True when succesfull, else False
        """

        # check sufficient balance up to dt
        qty_available_at_dt = (
            session.query(func.sum(tradePosition.qty))
            .filter(
                tradePosition.asset == trade.Symbol,
                tradePosition.open_dt < trade.DateTime,
                tradePosition.status == TradePositionStatus.OPEN,
            )
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
            q = session.query(tradePosition).filter(
                tradePosition.open_dt <= trade.DateTime,
                tradePosition.asset == trade.Symbol,
                tradePosition.status == TradePositionStatus.OPEN,
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

                    pnl_base = open_pos.qty * (basis / trade_qty - open_pos.open_price)
                    open_pos.pnl_base = pnl_base
                    open_pos.pnl_local = open_pos.qty * (
                        basis / trade_qty * trade.QuoteInLocalCurrency
                        - open_pos.open_price * open_pos.open_forex
                    )
                    session.commit()
                    remaining_qty_to_close -= open_pos.qty
                else:
                    # consume partly and close and make new open pos for remainder
                    _open_position(
                        asset=open_pos.asset,
                        qty=open_pos.qty - remaining_qty_to_close,
                        basis=open_pos.open_price
                        * (open_pos.qty - remaining_qty_to_close),
                        open_dt=open_pos.open_dt,
                        open_forex=open_pos.open_forex,
                        base_c=open_pos.base_c,
                        local_c=open_pos.local_c,
                        commit=False,
                    )
                    open_pos.qty = remaining_qty_to_close
                    open_pos.status = TradePositionStatus.CLOSED
                    open_pos.close_dt = trade.DateTime
                    basis = trade.Basis
                    if not basis:
                        basis = trade.Proceeds - trade.Comm_in_USD
                    open_pos.close_price = (basis / trade_qty,)
                    open_pos.close_forex = (trade.QuoteInLocalCurrency,)
                    pnl_base = remaining_qty_to_close * (
                        basis / trade_qty - open_pos.open_price
                    )
                    open_pos.pnl_base = (pnl_base,)
                    open_pos.pnl_local = remaining_qty_to_close * (
                        basis / trade_qty * trade.QuoteInLocalCurrency
                        - open_pos.open_price * open_pos.open_forex
                    )

                    print(open_pos.close_forex)  # THIS IS THE ERROR
                    print(open_pos.close_price)
                    session.commit()
                    break
            return True

        else:
            # insufficient qty available at this point
            return False

    for trade in session.query(Trade).order_by(Trade.DateTime):
        direction = Direction.BUY if trade.Quantity > Decimal(0) else Direction.SELL
        print(trade.QuoteInLocalCurrency)

        if direction == Direction.BUY:
            # open a position
            basis = trade.Basis
            if not basis:
                basis = trade.Proceeds - trade.Comm_in_USD
            assert basis > Decimal(0), f"ERROR: {trade.id}"
            _open_position(
                qty=trade.Quantity,
                open_dt=trade.DateTime,
                asset=trade.Symbol,
                base_c=BASE_CURRENCY,
                local_c=LOCAL_CURRENCY,
                basis=basis,
                open_forex=trade.QuoteInLocalCurrency,
            )
        elif direction == Direction.SELL:
            # close position
            # pass
            _close_position(session, trade, closing_principle=closing_principle)
        else:
            raise NotImplementedError("ERROR: incorrect Direction type received...")


def show_trade_deltas(session):
    """
    Show the per symbol trade quantities sorted by date
    while tracking running total of balance
    """
    q = session.query(
        Trade.Symbol,
        Trade.DateTime,
        Trade.Quantity,
        func.sum(Trade.Quantity)
        .over(partition_by=Trade.Symbol, order_by=(Trade.DateTime))
        .label("Balance"),
        Trade.QuoteInLocalCurrency,
        Trade.Proceeds,
        Trade.CommOrFee,
    )
    for row in q:
        print(row)


"""
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
"""


def total_asset_delta_query(session: Session, LOCAL_CURRENCY=LOCAL_CURRENCY) -> Query:
    """
    Return the asset deltas based on trade, transfers, deposits & withdrawals,
    dividend, witholding tax, fees and starting balances for the period
    sorted by account & asset
    :returns selectable: asset, dt, qty, average forex rate, account
    """
    # Infer balance start date from statement
    # Infer balance start date from statement
    STATEMENT_PERIOD = (
        session.query(NameValue.Value)
        .filter(NameValue.type == NameValueType.STATEMENT, NameValue.Name == "Period")
        .first()[0]
    )
    STATEMENT_PERIOD_SPLIT = STATEMENT_PERIOD.split(" - ")
    STATEMENT_START_DATE = datetime.strptime(STATEMENT_PERIOD_SPLIT[0], "%B %d, %Y")
    STATEMENT_END_DATE = datetime.strptime(STATEMENT_PERIOD_SPLIT[-1], "%B %d, %Y")

    BASE_CURRENCY = (
        session.query(NameValue.Value)
        .filter(
            NameValue.type == NameValueType.ACCOUNT_INFORMATION,
            NameValue.Name == "Base Currency",
        )
        .first()[0]
    )

    ForexPairs = set()
    FiatTrades = set()
    symbols = (
        session.query(Trade.Symbol)
        .filter(Trade.Asset_Category.like("Forex%"))
        .group_by(Trade.Symbol)
        .all()
    )
    for symbol in symbols:
        ForexPairs.add(symbol[0])
        base, quote = symbol[0].split(".")
        FiatTrades.add(base)
        FiatTrades.add(quote)
    EOY_FOREX_RATES, BOY_FOREX_RATES = {}, {}
    for currency in FiatTrades:
        BOY_FOREX_RATES[currency] = forex_rate(
            currency, LOCAL_CURRENCY, STATEMENT_START_DATE
        )
        EOY_FOREX_RATES[currency] = forex_rate(
            currency, LOCAL_CURRENCY, STATEMENT_END_DATE
        )

    DEDICATED_TRADE_IMPLEMENTATIONS = [CategoryType.FOREX, CategoryType.FOREX_CFDs]

    trade_base = session.query(  # trade base excluding forex and forex_cfd
        Trade.Account.label("account"),
        Trade.Quantity.label("qty"),
        Trade.Symbol.label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
    ).filter(Trade.type.notin_(DEDICATED_TRADE_IMPLEMENTATIONS))
    trade_quote = session.query(  # trade quote excluding forex and forex_cfd
        Trade.Account.label("account"),
        case(
            [
                (Trade.Proceeds, Trade.Proceeds + Trade.CommOrFee),
                (Trade.Notional_Value, Trade.Notional_Value + Trade.CommOrFee),
            ],
            else_=literal(Decimal(0)),
        ).label("qty"),
        Trade.Currency.label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
    ).filter(Trade.type.notin_(DEDICATED_TRADE_IMPLEMENTATIONS))
    forex_cfd_quote = session.query(  # forex cfd quote
        # if currency == split -1, use notional value + realized_pnl
        # elif currency == split 0, raise not implemented (not seen yet)
        Trade.Account.label("account"),
        case(
            [
                (
                    Trade.Currency
                    == func.substr(Trade.Symbol, func.instr(Trade.Symbol, ".") + 1),
                    Trade.Notional_Value,
                ),
            ],
            else_=literal(Decimal(0)),
        ).label("qty"),
        Trade.Currency.label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
    ).filter(Trade.type == CategoryType.FOREX_CFDs)
    forex_cfd_base = session.query(  # forex cfd base
        # if currency == split -1, use qty
        # elif currency == split 0, raise not implemented (not seen yet)
        Trade.Account.label("account"),
        Trade.Quantity.label("qty"),
        func.substr(Trade.Symbol, 1, func.instr(Trade.Symbol, ".") - 1).label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
    ).filter(Trade.type == CategoryType.FOREX_CFDs)

    forex_cfd_fee = session.query(  # forex cfd fee
        # if currency == split -1, use qty
        # elif currency == split 0, raise not implemented (not seen yet)
        Trade.Account.label("account"),
        Trade.CommOrFee.label("qty"),
        literal("USD").label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
    ).filter(Trade.type == CategoryType.FOREX_CFDs)

    forex_trade_base = session.query(  # trade base exclusively forex
        Trade.Account.label("account"),
        Trade.Quantity.label("qty"),
        func.substr(Trade.Symbol, 1, func.instr(Trade.Symbol, ".") - 1).label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
    ).filter(Trade.type == CategoryType.FOREX)
    forex_trade_quote = session.query(  # trade quote exclusively forex
        Trade.Account.label("account"),
        Trade.Proceeds.label("qty"),
        func.substr(Trade.Symbol, func.instr(Trade.Symbol, ".") + 1).label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
    ).filter(Trade.type == CategoryType.FOREX)
    forex_trade_fee = session.query(  # trade quote exclusively USD fee
        Trade.Account.label("account"),
        Trade.Comm_in_USD.label("qty"),
        literal("USD").label("base"),
        Trade.DateTime.label("dt"),
        Trade.QuoteInLocalCurrency.label("forex"),
    ).filter(Trade.type == CategoryType.FOREX)
    deposit_withdrawals = session.query(
        # literal(1).label("type"),  # transfer type == 0
        DepositsWithdrawals.Account.label("account"),
        DepositsWithdrawals.Amount.label("qty"),
        DepositsWithdrawals.Currency.label("base"),
        DepositsWithdrawals.DateTime.label("dt"),
        DepositsWithdrawals.QuoteInLocalCurrency.label("forex"),
    )
    transfers = session.query(
        # literal(1).label("type"),  # transfer type == 0
        Transfer.Account.label("account"),
        Transfer.Qty.label("qty"),
        Transfer.Symbol.label("base"),
        Transfer.DateTime.label("dt"),
        Transfer.QuoteInLocalCurrency.label("forex"),
    )
    starting_balances = session.query(
        Mark2Market.Account.label("account"),
        Mark2Market.Prior_Quantity.label("qty"),
        Mark2Market.Symbol.label("base"),
        literal(STATEMENT_START_DATE).label("dt"),
        literal(BOY_FOREX_RATES.get(BASE_CURRENCY)).label("forex"),
    ).filter(Mark2Market.Prior_Quantity > Decimal(0))
    dividends = session.query(
        Dividends.Account.label("account"),
        Dividends.Amount.label("qty"),
        Dividends.Currency.label("base"),
        Dividends.DateTime.label("dt"),
        Dividends.QuoteInLocalCurrency.label("forex"),
    )
    witholdingtax = session.query(
        WitholdingTax.Account.label("account"),
        WitholdingTax.Amount.label("qty"),
        WitholdingTax.Currency.label("base"),
        WitholdingTax.DateTime.label("dt"),
        WitholdingTax.QuoteInLocalCurrency.label("forex"),
    )
    fees = session.query(
        Fees.Account.label("account"),
        Fees.Amount.label("qty"),
        Fees.Currency.label("base"),
        Fees.DateTime.label("dt"),
        Fees.QuoteInLocalCurrency.label("forex"),
    )
    u = trade_base.union_all(
        trade_quote,
        forex_trade_base,
        forex_trade_quote,
        forex_trade_fee,
        forex_cfd_quote,
        forex_cfd_base,
        forex_cfd_fee,
        deposit_withdrawals,
        transfers,
        starting_balances,
        dividends,
        witholdingtax,
        fees,
    ).subquery()
    q1 = (
        session.query(
            u.c.base,
            u.c.dt,
            func.sum(u.c.qty),
            func.sum(u.c.qty * u.c.forex) / func.sum(u.c.forex),
            u.c.account,
        )
        .group_by(u.c.account, u.c.base)
        .order_by(u.c.account, u.c.base)
    )

    return q1
