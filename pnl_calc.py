from collections import defaultdict
from models.sqla import (
    tradePosition,
    Account,
    CorporateActions,
    Mark2Market,
    RealizedUnrealizedPerformance,
)
from decimal import Decimal
from sqlalchemy.orm.session import Session
from typing import Union, Dict, List, Any, Optional
import enums
from datetime import datetime, timedelta
from sqlalchemy import null, alias, case, literal, func
from queries import pnl_asset_query
from db import object_as_dict
from forex import ForexRate


def analyze_corporate_actions(session):
    """
    Parse corporate action descriptions for ISIN changes and splits
    outputs dict with key=dt, val=(OPERATION, FROM_TICKER, TO_TICKER, RATIO)
    """
    change_dict = defaultdict(set)
    actions = session.query(
        CorporateActions.Account,
        CorporateActions.DateTime,
        CorporateActions.Description,
        CorporateActions.Quantity,
    )
    for acc, dt, desc, qty in actions:
        if "Split" in desc:
            # a stock split
            # need to divide open tradePos qty by split and multiply open tradePos prices by split
            # to find the split, do regex "#num for" and regex "for #num", ratio is the split
            # print(f"SPLIT {desc} @ {dt}")
            split_parts = desc.split("Split ")
            split_parts_details = split_parts[1].split(" ")
            num1 = split_parts_details[0]
            num2 = split_parts_details[2]
            ticker = desc.split("(")[0]

            change_dict[dt].add(("SPLIT", ticker, Decimal(num2) / Decimal(num1)))
        elif "ISIN Change" in desc:
            # an ISIN Change
            # need to update tradePosition names for open positions
            # print(f"CHANGE {desc} @ {dt}")
            splitted = desc.split("(")
            ticker = splitted[0]
            ticker_to = splitted[-1].split(",")[0]
            if ".OLD" in ticker_to:
                # drop the old ticker
                continue
            change_dict[dt].add(("CHANGE", ticker, ticker_to))
        elif "Delisted" in desc:
            # treat as bankruptcy - not able to trade anymore
            sp1 = desc.split("Delisted ")
            assert (
                len(sp1) == 2
            ), "Error: need to revise delisted mechanism to filter out ticker"
            sp2 = sp1[-1].split("(")[1]
            ticker = sp2.split(".")[0]
            change_dict[dt].add(("DELIST", ticker, Decimal(qty)))

    print("INFERRED CORPORATE ACTIONS - PLS VERIFY!")
    return change_dict


def calc_tradepositions(
    in_session: Session,
    out_session: Session,
    forex_rate: ForexRate,
    corporate_actions: dict = {},
    track_symbols: List = [],
    debug: bool = False,
) -> None:
    """
    process the pnl asset query rows to create and update tradepositions

    *** NOTE: margin accounts require margin - due to this specific cash account implementation,
        negative FIAT may be possible
    ***

    :param in_session: session to apply base query onto
        ***** should contain TRADEPOSITIONS carried over from previous period!!!! *****
    :param out_session: session to export tradePositions onto
    :param forex_rate: forex utility to fetch fiat asset USD values
    :param corp_actions: dictionary containing splits and ISIN changes and delistings as produced by
    analyze_corporate_actions
    :param track_symbols: List of symbols to specifically output during processing
    """
    print(f"TRACKING {len(corporate_actions)} CORPORATE ACTIONS")

    # the below is used to HACK incorrect pnl_asset_query balances resulting from not being able to
    # track balance changes from corporate actions (renames, splits, delists)
    allow_negative_balance_for_ca = [
        vals[2]
        for k, v in corporate_actions.items()
        for vals in v
        if type(vals[2]) == str
    ]
    allow_negative_balance_for_ca.extend(
        [vals[1] for k, v in corporate_actions.items() for vals in v]
    )
    print(
        f"INFO: allowing negative balances for corporate actions related tickers {allow_negative_balance_for_ca}"
    )

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
        if asset in track_symbols:
            print(
                f"Opening {account} {dt} {qty} {asset} value {value} multi {multiplier}"
            )
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
        if asset in track_symbols:
            print(
                f"Closing {account} {dt} {qty} {asset} value {value} multi {multiplier}"
            )
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
        if asset in track_symbols:
            print(f"Processing transfer {TRANSFER_PAIRS[key]} {dt} {asset}")
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

    def split(dt, ticker, ratio):
        """
        split all tradePos for ticker open at dt with ratio
        """
        if ticker in track_symbols:
            print(f"Splitting {dt} {ticker} ratio {ratio}")
        sumq = (
            out_session.query(func.sum(tradePosition.qty))
            .filter(
                tradePosition.status == enums.TradePositionStatus.OPEN,
                tradePosition.asset == ticker,
                tradePosition.open_dt <= dt,
            )
            .scalar()
        )
        print(f"FOUND {sumq} QTY TO SPLIT USING {dt}, {ticker}, {ratio}, {type(ratio)}")
        q = out_session.query(tradePosition).filter(
            tradePosition.status == enums.TradePositionStatus.OPEN,
            tradePosition.asset == ticker,
            tradePosition.open_dt <= dt,
        )

        for tp in q:
            tp.qty = tp.qty / ratio
            tp.o_price_base = (tp.o_price_base * ratio,)
            if isinstance(tp.o_price_base, tuple):  # NOT SURE HOW A TUPLE CAN OCCUR!!!!
                tp.o_price_base = tp.o_price_base[0]
            tp.o_price_local = tp.o_price_local * ratio
            if isinstance(
                tp.o_price_local, tuple
            ):  # NOT SURE HOW A TUPLE CAN OCCUR!!!!
                tp.o_price_local = tp.o_price_local[0]

        out_session.commit()

        return

    def change(dt, from_ticker, to_ticker):
        """
        change of ISIN requires renaming of from_ticker to to_ticker for all open tradepositions up to dt
        """
        if from_ticker in track_symbols or to_ticker in track_symbols:
            print(f"Changing {dt} {from_ticker} to {to_ticker}")
        sumq = (
            out_session.query(func.sum(tradePosition.qty))
            .filter(
                tradePosition.status == enums.TradePositionStatus.OPEN,
                tradePosition.asset == from_ticker,
                tradePosition.open_dt <= dt,
            )
            .scalar()
        )
        print(f"FOUND {sumq} QTY TO CHANGE USING {dt}, {from_ticker}, {to_ticker}")
        q = out_session.query(tradePosition).filter(
            tradePosition.status == enums.TradePositionStatus.OPEN,
            tradePosition.asset == from_ticker,
            tradePosition.open_dt <= dt,
        )

        for tp in q:
            tp.asset = to_ticker
        out_session.commit()

        return

    def delist(dt, ticker, qty):
        """
        delisting requires closing at loss for all open tradepositions up to dt
        """
        print(f"DELISTING {ticker}: CLOSE AT FULL LOSS ")
        q = out_session.query(tradePosition).filter(
            tradePosition.status == enums.TradePositionStatus.OPEN,
            tradePosition.asset == ticker,
            tradePosition.open_dt <= dt,
        )

        for tp in q:
            tp.close_dt = dt
            tp.status = enums.TradePositionStatus.CLOSED
            tp.c_price_local = Decimal(0)
            tp.c_price_base = Decimal(0)
            tp.pnl_base = -tp.qty * tp.o_price_base
            tp.pnl_local = -tp.qty * tp.o_price_local
        out_session.commit()

        return

    # ----- ENTRY POINT
    # export open tradePositions from in_session to out_session db
    """
    for tradepos in in_session.query(tradePosition).filter(tradePosition.status==enums.TradePositionStatus.OPEN):
        tp_dict = object_as_dict(tradepos)
        tp_dict.pop("id")
        out_session.add(tradePosition(**tp_dict))
    out_session.commit()
    """

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
        if tpq is None:
            print(
                f"!!!!Issue: tqp == None for account {account}, qty {qty}, base {base}"
            )
            continue

        if not (qty - tpq).is_zero():
            # adjust the last opened open tradePos to qty equal to starting balances for account
            pct = (100 * ((qty - tpq) / qty)).quantize(Decimal("1.00"))
            # if debug:
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
    ca = corporate_actions.copy()
    for account, base, dt, qty, balance, quote, cost, forex, _type, _model in baseq:
        # verify if there is a corporate action for this date
        if ca:
            ca_dates = list(ca.keys())
            for d in ca_dates:
                if d <= dt:
                    # process this corporate action
                    for action, ticker, f2 in corporate_actions.get(d):
                        if action == "SPLIT":
                            ratio = f2
                            # DO THE SPLITTING
                            print(f"{d} SPLITTING {ticker} by {ratio}")
                            split(dt=d, ticker=ticker, ratio=ratio)
                        elif action == "CHANGE":
                            to_ticker = f2
                            # DO THE RENAMING
                            print(f"{d} RENAMING {ticker} to {to_ticker}")
                            change(dt=d, from_ticker=ticker, to_ticker=to_ticker)
                        elif action == "DELIST":
                            # DO THE DELISTING
                            print(f"{d} DELISTING {ticker}")
                            delist(dt=d, ticker=ticker, qty=f2)
                        else:
                            raise NotImplementedError(f"UNEXPECTED ACTION {action}")
                    ca.pop(d)

        if debug or base in track_symbols:
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

        if qty < 0:  # i.e. sell BASE
            # THE BELOW IS INCORRECT
            # "USD" and "EUR" not in fiat == False
            # "AAA" and "EUR" not in fiat == False
            # "USD" and "AAA" not in fiat == True
            if (
                balance < 0 and quote and base not in FIAT
            ):  # add and quote to avoid FIAT insufficient balances allow below 0 balance
                multiplier = -1
                if base in allow_negative_balance_for_ca:  # HACK
                    print(f"HACK! {base}, multiplier reset from -1 to 1")
                    multiplier = 1
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


def show_pnl(session, in_session):
    """
    SHOW THE SUM PNL PER ACCOUNT AND PER ASSET
    """
    q = (
        session.query(
            tradePosition.Account,
            tradePosition.asset,
            func.sum(tradePosition.pnl_base),
            func.sum(tradePosition.pnl_local),
        )
        .filter(tradePosition.status == enums.TradePositionStatus.CLOSED)
        .group_by(tradePosition.Account, tradePosition.asset)
        .order_by(tradePosition.Account, tradePosition.asset)
    )
    tot_pnl = Decimal(0)
    tot_pnl_local = Decimal(0)
    for acc, asset, pnl_base, pnl_local in q:
        # query mark2market total pnl for this asset
        r_ur = (
            in_session.query(RealizedUnrealizedPerformance.Realized_Total)
            .filter(
                RealizedUnrealizedPerformance.Symbol == asset,
                RealizedUnrealizedPerformance.Account == acc,
            )
            .scalar()
        )
        if r_ur:
            delta = pnl_base - r_ur
        else:
            delta = "MISSING"
        print(
            f"{acc} {asset} pnl {pnl_base} / pnl local {pnl_local} .. deviation from Realized {delta}"
        )
        if r_ur:
            tot_pnl += r_ur
        tot_pnl_local += pnl_local
    print("-------- TOTALS ------")
    print(f"PNL BASE: {tot_pnl}")
    print(f"PNL LOCAL: {tot_pnl_local}")
