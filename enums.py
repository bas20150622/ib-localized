from enum import IntEnum

# ********** IntEnum definitions


class TradeType(IntEnum):
    STOCKS = 1
    CFDs = 2
    OPTIONS = 3
    FOREX = 4


class NameValueType(IntEnum):
    STATEMENT = 1
    ACCOUNT_INFORMATION = 2


class Status(IntEnum):
    REQUESTED = 1  # user request
    PENDING_ENTRY = 2  # Only set by exchange handler
    REQUEST_CHANGE = 3  # user request
    ACTIVE = 4  # Only set by exchange handler, after entry completed
    REQUEST_CANCEL = 5  # user request
    CANCELLED = 6  # Only set by exchange handler
    CLOSED = 7  # Only set by exchange handler
    # Impossible request leads to this status, i.e. request trade with insufficient balance.
    REJECTED = 8  # Only exchange handler sets this status


class Direction(IntEnum):
    """ Order, Transfer and Trade related """
    BUY = 1
    SELL = 2
    DEPOSIT = 3
    WITHDRAWAL = 4
    LONG = 5
    SHORT = 6


class TransactionType(IntEnum):
    """ Transaction related"""
    TRANSFER = 1
    TRADE = 2
    OTHER = 3
    PNL = 4  # bitmex
    INTEREST = 5
    MANUAL_ADJUSTMENT = 6


class DeltaType(IntEnum):
    """ delta balance type definitions """
    INTEREST = 1
    MANUAL_ADJUSTMENT = 2
    MINED_QTY = 3
    AIRDROP = 4
    HARDFORK = 5
    LIQUID_SWAP = 6


class TradePositionStatus(IntEnum):
    """ tradeposition status """
    CLOSED = 0
    OPEN = 1
