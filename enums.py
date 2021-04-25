from enum import IntEnum

# ********** IntEnum definitions


class TradeType(IntEnum):
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
