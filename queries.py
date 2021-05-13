# Module containing the key queries that recreate the balance and pnl statement in local currency
# WORK IN PROGRESS !!!
from models.sqla import NameValue, Trade, OpenPositions, ForexBalance, ChangeInDividendAccruals
from models.sqla import tradePosition, Dividends, WitholdingTax, DepositsWithdrawals
from settings import LOCAL_CURRENCY
from enums import Direction, TradePositionStatus, TradeType
# from forex import forex_rate
from enums import NameValueType
from datetime import datetime
from decimal import Decimal
from sqlalchemy import func
from sqlalchemy import case, literal, null
from sqlalchemy.orm import Query
from sqlalchemy.orm.session import Session
from typing import Optional
from forex import ForexRate


def base_query(session: Session) -> Query:
    """
    Return the base deltas based on trade, transfers (deposits withdrawals),
    dividend and witholding tax,
    sorted by asset and by date
    :returns selectable: base, dt, qty, balance, quote, cost, forex
    """
    d1 = (  # trade base excluding forex
        session.query(
            # literal(0).label("type"),  # trade type == 0
            Trade.Quantity.label("qty"),
            Trade.Symbol.label("base"),
            Trade.DateTime.label("dt"),
            Trade.QuoteInLocalCurrency.label("forex"),
            Trade.Currency.label("quote"),
            case(
                [
                    (Trade.Proceeds, Trade.Proceeds + Trade.CommOrFee),
                    (Trade.Notional_Value, Trade.Notional_Value + Trade.CommOrFee)
                ], else_=literal(Decimal(0))
            ).label("cost"),
            Trade.type.label("t")
        )
        .filter(Trade.type != TradeType.FOREX)
    )
    d2 = (  # trade base exclusively forex
        session.query(
            # literal(0).label("type"),  # trade type == 0
            Trade.Quantity.label("qty"),
            func.substr(Trade.Symbol, 1, func.instr(
                Trade.Symbol, '.')-1).label("base"),
            Trade.DateTime.label("dt"),
            Trade.QuoteInLocalCurrency.label("forex"),
            Trade.Currency.label("quote"),
            (Trade.Proceeds + Trade.Comm_in_USD).label("cost"),
            Trade.type.label("t")
        )
        .filter(Trade.type == TradeType.FOREX)
    )
    d5 = (
        session.query(
            # literal(1).label("type"),  # transfer type == 0
            DepositsWithdrawals.Amount.label("qty"),
            DepositsWithdrawals.Currency.label("base"),
            DepositsWithdrawals.DateTime.label("dt"),
            DepositsWithdrawals.QuoteInLocalCurrency.label("forex"),
            null().label("quote"),
            null().label("cost"),
            literal(TradeType.STOCKS).label("t")
        )
    )
    u = d1.union_all(d2, d5).subquery()
    q1 = (
        session.query(
            u.c.base, u.c.dt, u.c.qty,
            func.sum(u.c.qty).over(partition_by=u.c.base,
                                   order_by=u.c.dt).label("balance"),
            u.c.quote, u.c.cost,
            u.c.forex, u.c.t
        )
        .order_by(u.c.dt)
    )  # .cte()

    return q1


def calc_balance(
        session: Session, STATEMENT_END_DATE_STR: str, BASE_CURRENCY: str, LOCAL_CURRENCY: str, EOY_BASE_LOCAL):
    """
    Calculate the balance of equities and cash at statement end date
    """
    QUANTIZE_FIAT = Decimal('1.00')
    sum_equity_base = sum_cash_base = sum_change_in_dividends_base = Decimal(0)
    print(f"BALANCE AT {STATEMENT_END_DATE_STR}\n")
    print("*** EQUITY BALANCE ***")
    q = (
        session
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
            session.query(ForexBalance.Description, ForexBalance.Quantity, ForexBalance.Close_Price)):
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
    sum_change_in_dividends_base = session.query(
        func.sum(ChangeInDividendAccruals.Net_Amount)).scalar()
    print(sum_change_in_dividends_base)
    print()
    totals = sum_equity_base + sum_cash_base + sum_change_in_dividends_base
    print(
        f"TOTAL: {totals.quantize(QUANTIZE_FIAT)} {BASE_CURRENCY}/ "
        f"{(totals * EOY_BASE_LOCAL).quantize(QUANTIZE_FIAT)} {LOCAL_CURRENCY}")


def process_base_query(in_session: Session, out_session: Session, forex_rate: ForexRate):
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

    def _open_position(dt: datetime, asset: str, qty: Decimal, value: Decimal, forex: Decimal, multiplier: int = 1):
        """
        open a tradePosition
        """
        if qty.is_zero():  # e.g. options expiring OTM
            return
        out_session.add(tradePosition(
            qty=qty,
            open_dt=dt,
            status=TradePositionStatus.OPEN,
            asset=asset,
            o_price_base=value / qty,
            o_price_local=value / qty * forex,
            multiplier=multiplier
        ))
        out_session.commit()
        return

    def _consume_position(
        open_pos: tradePosition, close_dt: datetime,
        forex_rate: Optional[Decimal] = None,
        exit_value: Optional[Decimal] = None, with_pnl: bool = True
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
            open_pos.pnl_base = (exit_value - open_pos.qty *
                                 open_pos.o_price_base) * open_pos.multiplier
            open_pos.pnl_local = (exit_value * forex_rate -
                                  open_pos.qty * open_pos.o_price_local) * open_pos.multiplier
        out_session.commit()

    def _reduce_position(
            open_pos: tradePosition,
            close_dt: datetime, reduce_with_qty: Decimal,
            forex_rate: Optional[Decimal] = None, exit_value: Optional[Decimal] = None,
            with_pnl: bool = False):
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
            pnl_base = (exit_value - reduce_with_qty *
                        open_pos.o_price_base) * open_pos.multiplier
            pnl_local = (exit_value * forex_rate -
                         reduce_with_qty * open_pos.o_price_local) * open_pos.multiplier

            # create closed position
            out_session.add(tradePosition(
                c_price_base=c_price_base,
                c_price_local=c_price_local,
                pnl_base=pnl_base,
                pnl_local=pnl_local,
                close_dt=close_dt,
                open_dt=open_pos.open_dt,
                status=TradePositionStatus.CLOSED,
                asset=open_pos.asset,
                qty=reduce_with_qty,
                multiplier=open_pos.multiplier
            ))
        out_session.commit()

    def _close_position(
        dt: datetime, asset: str, qty: Decimal,
        value: Decimal, forex: Decimal
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
        q = (
            out_session
            .query(func.sum(tradePosition.qty))
            .filter(
                tradePosition.asset == asset,
                tradePosition.open_dt < dt,
                tradePosition.status == TradePositionStatus.OPEN)
        )

        qty_available_at_dt = q.scalar()

        if not qty_available_at_dt:
            # no balance at this dt
            raise ValueError(f"ERROR: insufficient balance for {asset}")

        ASSUMED_ROUNDING_ERROR_MARGIN = (
            qty_available_at_dt > ROUND_ERROR_MARGIN * qty) and (qty_available_at_dt < qty)
        if (qty_available_at_dt >= qty) or ASSUMED_ROUNDING_ERROR_MARGIN:
            if ASSUMED_ROUNDING_ERROR_MARGIN:
                qty = qty_available_at_dt
            # start finding open positions and reduce qty
            q = (
                out_session
                .query(tradePosition)
                .filter(
                    tradePosition.open_dt <= dt, tradePosition.asset == asset,
                    tradePosition.status == TradePositionStatus.OPEN)
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
                        with_pnl=True
                    )
                    remaining_qty_to_close -= trade_pos.qty
                else:
                    # consume partly and close and make new open pos for remainder
                    _reduce_position(
                        open_pos=trade_pos,
                        close_dt=dt,
                        forex_rate=forex,
                        exit_value=remaining_qty_to_close*avg_price,
                        with_pnl=True,
                        reduce_with_qty=remaining_qty_to_close
                    )
                    return True
        else:
            # fail to close this iteration
            raise ValueError(f"ERROR: insufficient balance for {asset}")
            return False

    base = base_query(in_session)
    for base, dt, qty, balance, quote, cost, forex, _type in base:
        multiplier = 1  # default for long positions

        if _type == TradeType.CFDs:
            # CFDs are short or long positions, direction is determined by qty sign and balance
            if qty < 0 and balance < 0:
                # open short pos
                multiplier = -1

            if qty > 0 and balance <= 0:
                # close short pos
                multiplier = -1

        if qty*multiplier > Decimal(0):
            if quote:
                assert quote == "USD"
                _close_position(dt=dt, asset=quote, qty=-
                                cost, value=-cost, forex=forex)
            else:

                USDNOK = forex_rate("USD", "NOK", dt)
                base_USD = forex_rate(base, "USD", dt)
                cost = -qty * base_USD
                forex = USDNOK
            _open_position(dt=dt, asset=base, qty=qty*multiplier,
                           value=-cost*multiplier, forex=forex, multiplier=multiplier)

        else:
            assert quote == "USD", f"error: quote=={quote}"
            _open_position(dt=dt, asset=quote, qty=cost,
                           value=cost, forex=forex, multiplier=1)
            _close_position(dt=dt, asset=base, qty=-qty *
                            multiplier, value=cost*multiplier, forex=forex)


def calc_pnl_simplified(session, STATEMENT_END_DATE_STR):
    """
    Calculate the pnl in base and local in a simplified manner, i.e. multiply pnl with the current exrate
    """
    BASE_CURRENCY = (
        session.query(NameValue.Value)
        .filter(NameValue.type == NameValueType.ACCOUNT_INFORMATION, NameValue.Name == "Base Currency")
        .scalar()
    )
    q = (
        session.query(
            Trade.Symbol, func.sum(Trade.Realized_PnL),
            func.sum(Trade.Realized_PnL * Trade.QuoteInLocalCurrency)
        )
        .group_by(Trade.Symbol)
    )

    q_total = (
        session.query(
            func.sum(Trade.Realized_PnL),
            func.sum(Trade.Realized_PnL * Trade.QuoteInLocalCurrency)
        )
    )

    print(f"PROFIT AND LOSS AT {STATEMENT_END_DATE_STR}\n")
    for _asset, _pnl_base, _pnl_local in q:
        if _pnl_base:
            print(
                f"{_asset}: {_pnl_base} {BASE_CURRENCY}, {_pnl_local} {LOCAL_CURRENCY}")

    print()
    for _pnl_base, _pnl_local in q_total:
        if _pnl_base:
            print(
                f"TOTAL: {_pnl_base} {BASE_CURRENCY}, {_pnl_local} {LOCAL_CURRENCY}")


def calc_net_dividend(session, STATEMENT_END_DATE_STR, BASE_CURRENCY):
    """
    Calc sum of dividend - witholding tax
    """
    dividends = session.query(
        Dividends.Symbol.label("asset"), Dividends.Currency.label("currency"),
        Dividends.Amount.label("amount"),
        (Dividends.Amount * Dividends.QuoteInLocalCurrency).label("amount_local")
    )
    witholdings = session.query(
        WitholdingTax.Symbol.label(
            "asset"), WitholdingTax.Currency.label("currency"),
        WitholdingTax.Amount.label("amount"),
        (WitholdingTax.Amount * WitholdingTax.QuoteInLocalCurrency).label("amount_local")
    )
    net_dividend = dividends.union_all(witholdings).subquery()
    q = (
        session.query(
            net_dividend.c.asset.label("asset"),
            func.sum(net_dividend.c.amount).label("net_dividend"),
            func.sum(net_dividend.c.amount_local).label("net_dividend_local")
        )
        .group_by(net_dividend.c.asset)
        .order_by(net_dividend.c.asset)
    )

    print(
        f"NET DIVIDENDS (DIVIDEND - WITHOLDING TAX) AT {STATEMENT_END_DATE_STR}\n")
    for row in q:
        print(f"{row.asset}: {row.net_dividend} {BASE_CURRENCY}, {row.net_dividend_local} {LOCAL_CURRENCY}")


def cost_basis_query(session):
    """
    Return query that provides id, asset, delta, balance, average cost price in base
    currency and average cost price in local currency
    Provides a means to derive pnl_base and pnl_local from
    """
    q = (
        session.query(
            Trade.id.label('id'),
            Trade.DateTime.label('dt'),
            Trade.Symbol.label('asset'),
            Trade.Quantity.label('delta'),
            # Trade.QuoteInLocalCurrency.label('forex_rate'),
            func.sum(Trade.Quantity).over(partition_by=Trade.Symbol,
                                          order_by=Trade.DateTime).label('balance'),
            (
                (func.sum(Trade.Quantity)
                     .over(partition_by=Trade.Symbol, order_by=Trade.DateTime) - Trade.Quantity).label('prev_balance')
            ),
            case([(Trade.Quantity > Decimal(0), Trade.Basis), ],
                 else_=func.lag(Trade.Basis, 1, 0).over(
                     partition_by=Trade.Symbol, order_by=Trade.DateTime)
                 ).label('last_buy_basis'),
            case([(Trade.Quantity > Decimal(0), Trade.Basis * Trade.QuoteInLocalCurrency), ],
                 else_=func.lag(Trade.Basis * Trade.QuoteInLocalCurrency, 1,
                                0).over(partition_by=Trade.Symbol, order_by=Trade.DateTime)
                 ).label('last_buy_basis_local'),
            case([(Trade.Quantity > Decimal(0), Trade.Basis / Trade.Quantity), ],
                 else_=(
                func.lag(Trade.Basis / Trade.Quantity, 1, 0)
                .over(
                    partition_by=Trade.Symbol,
                    order_by=Trade.DateTime))
                 ).label('last_avg_buy_price'),
            case([(Trade.Quantity > Decimal(0), Trade.Basis * Trade.QuoteInLocalCurrency / Trade.Quantity), ],
                 else_=(
                func.lag(
                    Trade.Basis * Trade.QuoteInLocalCurrency / Trade.Quantity, 1, 0)
                .over(
                    partition_by=Trade.Symbol,
                    order_by=Trade.DateTime))
                 ).label('last_avg_buy_price_local'),
        )
        .order_by(Trade.Symbol)

    ).subquery()
    q2 = (
        session.query(
            q.c.id.label('id'),
            # q.c.forex_rate.label('forex_rate'),
            q.c.dt.label('dt'), q.c.asset.label('asset'), q.c.delta.label(
                'delta'), q.c.balance.label('balance'),
            q.c.last_buy_basis.label(
                'last_buy_basis'), q.c.prev_balance.label('prev_balance'),
            q.c.last_buy_basis_local.label('last_buy_basis_local'),
            q.c.last_avg_buy_price_local.label('last_avg_buy_price_local'),
            func.lag(q.c.last_avg_buy_price, 1, 0).over(
                partition_by=q.c.asset, order_by=q.c.dt).label('prev_buy_price'),
            func.lag(q.c.last_avg_buy_price_local, 1, 0).over(
                partition_by=q.c.asset, order_by=q.c.dt).label('prev_buy_price_local'),
            case([(
                q.c.delta > Decimal(0),
                (q.c.last_buy_basis + q.c.prev_balance * func.lag(q.c.last_avg_buy_price, 1, 0).over(partition_by=q.c.asset, order_by=q.c.dt)) / q.c.balance), ],  # noqa
                else_=literal(0)).label('last_avg'),
            case([(
                q.c.delta > Decimal(0),
                (q.c.last_buy_basis_local + q.c.prev_balance * func.lag(q.c.last_avg_buy_price_local, 1, 0).over(partition_by=q.c.asset, order_by=q.c.dt)) / q.c.balance), ],  # noqa
                else_=literal(0)).label('last_avg_local')
        )
        .order_by(q.c.asset)
    ).subquery()
    q3 = (
        session.query(
            q2.c.id.label('id'),
            # q2.c.forex_rate.label('forex_rate')
            q2.c.asset.label('asset'),
            q2.c.delta.label('delta'),
            q2.c.balance.label('balance'),
            case([(
                q2.c.last_avg != literal(0), q2.c.last_avg), ],
                else_=func.lag(q2.c.last_avg, 1, 0).over(
                    partition_by=q2.c.asset, order_by=q2.c.dt)
            ).label('cost_price_base'),
            case([(
                q2.c.last_avg_local != literal(0), q2.c.last_avg_local), ],
                else_=func.lag(q2.c.last_avg_local, 1, 0).over(
                    partition_by=q2.c.asset, order_by=q2.c.dt)
            ).label('cost_price_local')
        )
        .order_by(q2.c.asset)
    )
    return q3


def calc_pnl(session):
    """
    calculate the pnl for trades on the basis of the average cost basis (buy) price
    """
    cb = cost_basis_query(session).subquery()
    sells = (
        session.query(
            Trade.id, Trade.DateTime,
            Trade.T_Price.label("sell_price"),
            Trade.QuoteInLocalCurrency.label("forex"),
            cb.c.asset, cb.c.delta, cb.c.balance, cb.c.cost_price_base, cb.c.cost_price_local
        )
        .filter(Trade.Quantity < Decimal(0))
        .outerjoin(cb, cb.c.id == Trade.id)
    )
    for _id, _dt, _sell_price, _forex, _asset, _delta, _balance, _cost_price_base, _cost_price_local in sells:
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
    q = (
        session.query(
            Trade.Symbol.label('symbol'),
            Trade.DateTime.label('dt'),
            (func.sum(Trade.Basis).over(
                partition_by=Trade.Symbol, order_by=(Trade.DateTime)) /
             func.sum(Trade.Quantity).over(
                partition_by=Trade.Symbol, order_by=(Trade.DateTime))).label('dca_price')
        )
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
            Trade.DateTime.label('dt'),
            Trade.Symbol.label('asset'),
            Trade.Quantity.label('delta'),
            func.sum(Trade.Quantity).over(partition_by=Trade.Symbol,
                                          order_by=Trade.DateTime).label('balance'),
            (
                (func.sum(Trade.Quantity)
                 .over(partition_by=Trade.Symbol, order_by=Trade.DateTime) - Trade.Quantity).label('prev_balance')
            ),
            case([(Trade.Quantity > Decimal(0), Trade.Basis), ],
                 else_=func.lag(Trade.Basis, 1, 0).over(
                     partition_by=Trade.Symbol, order_by=Trade.DateTime)
                 ).label('last_buy_basis'),
            case([(Trade.Quantity > Decimal(0), Trade.Basis / Trade.Quantity), ],
                 else_=(
                func.lag(Trade.Basis / Trade.Quantity, 1, 0)
                .over(
                    partition_by=Trade.Symbol,
                    order_by=Trade.DateTime))
                 ).label('last_avg_buy_price'),
        )
        .order_by(Trade.Symbol)

    ).subquery()
    q2 = (
        session.query(
            q.c.dt.label('dt'), q.c.asset.label('asset'), q.c.delta.label(
                'delta'), q.c.balance.label('balance'),
            q.c.last_buy_basis.label(
                'last_buy_basis'), q.c.prev_balance.label('prev_balance'),
            func.lag(q.c.last_avg_buy_price, 1, 0).over(
                partition_by=q.c.asset, order_by=q.c.dt).label('prev_buy_price'),
            case([(
                q.c.delta > Decimal(0),
                (
                    q.c.last_buy_basis + q.c.prev_balance *
                    func.lag(q.c.last_avg_buy_price, 1, 0)
                    .over(partition_by=q.c.asset, order_by=q.c.dt)) / q.c.balance), ],
                 else_=literal(0)).label('last_avg')
        )
        .order_by(q.c.asset)
    ).subquery()
    q3 = (
        session.query(
            q2.c.asset.label('symbol'),
            q2.c.dt.label('dt'),
            case([(
                q2.c.last_avg != literal(0), q2.c.last_avg), ],
                else_=func.lag(q2.c.last_avg, 1, 0).over(
                    partition_by=q2.c.asset, order_by=q2.c.dt)
            ).label('avg_entry_price')
        )
        .order_by(q2.c.asset)
    )
    return q3


def __calc_pnl(session, closing_principle: str = "FIFO", BASE_CURRENCY="USD", LOCAL_CURRENCY="NOK"):
    """
    Add pnl data to the database and calculate the pnl on the basis of the trades
    :param closing_principle: one of ["LIFO", "FIFO"], i.e. last-in-first-out or first-in-first-out,
    when closing a position, this indicates whether to close the lastest opened (LIFO) or earlierst
    opened (FIFO) position first
    """
    BASE_CURRENCY = (
        session.query(NameValue.Value)
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
        session.add(
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
        session.commit()

    def _close_position(session, trade, closing_principle) -> bool:
        """
        Check if a position can be closed and execute if possible
        return True when succesfull, else False
        """

        # check sufficient balance up to dt
        qty_available_at_dt = (
            session
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
                session
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
                    session.commit()
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
                    session.commit()
                    break
            return True

        else:
            # insufficient qty available at this point
            return False

    for trade in session.query(Trade).order_by(Trade.DateTime):
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
            _close_position(
                session, trade, closing_principle=closing_principle)
        else:
            raise NotImplementedError(
                "ERROR: incorrect Direction type received...")


def show_trade_deltas(session):
    """
    Show the per symbol trade quantities sorted by date
    while tracking running total of balance
    """
    q = (
        session.query(
            Trade.Symbol, Trade.DateTime, Trade.Quantity,
            func.sum(Trade.Quantity).over(
                partition_by=Trade.Symbol, order_by=(Trade.DateTime)).label("Balance"),
            Trade.QuoteInLocalCurrency, Trade.Proceeds, Trade.CommOrFee
        )
    )
    for row in q:
        print(row)


'''
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
'''
