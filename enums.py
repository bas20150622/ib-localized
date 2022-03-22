from enum import IntEnum

# ********** IntEnum definitions


class CategoryType(IntEnum):
    STOCKS = 1
    CFDs = 2
    OPTIONS = 3
    FOREX = 4
    FOREX_CFDs = 5


class NameValueType(IntEnum):
    STATEMENT = 1
    ACCOUNT_INFORMATION = 2


class TradePositionStatus(IntEnum):
    """ tradeposition status """
    CLOSED = 0
    OPEN = 1


class Direction(IntEnum):
    """ Order, Transfer and Trade related """
    BUY = 1
    SELL = 2
    DEPOSIT = 3
    WITHDRAWAL = 4
    LONG = 5
    SHORT = 6
