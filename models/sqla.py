from enums import TradePositionStatus, NameValueType, TradeType
from sqlalchemy import Column, Integer, Numeric, UniqueConstraint
from sqlalchemy import ForeignKey, DateTime
from sqlalchemy import String
from uuid import uuid4
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import types
from sqlalchemy.schema import MetaData

# Recommended naming convention used by Alembic, as various different database
# providers will autogenerate vastly different names making migrations more
# difficult. See: http://alembic.zzzcomputing.com/en/latest/naming.html
# Implementation Note: this requires boolean columns to have a "name" property
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}
metadata = MetaData(naming_convention=NAMING_CONVENTION)
Base = declarative_base(metadata=metadata)

# ---- CUSTOM TYPE FOR SQLALCHEMY TABLE INTENUM CAPABLE FIELD


class SAIntEnum(types.TypeDecorator):
    impl = Integer

    def __init__(self, enumtype, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._enumtype = enumtype

    def process_bind_param(self, value, dialect):
        # loading: convert to int
        if isinstance(value, int):
            return value

        if isinstance(value, str):
            return self._enumtype[value].value

        return value.value

    def process_result_value(self, value, dialect):
        # dumping: int to IntEnum
        return self._enumtype(value)


class User(Base):
    __tablename__ = "user"
    id = Column(Integer, primary_key=True)
    user_name = Column(String(255), nullable=False, unique=True)
    identity = Column(String(32), default=uuid4().hex)

    def __repr__(self):
        return f"User {self.id}: ({self.user_name}, {self.identity})"


class Broker(Base):
    __tablename__ = 'broker'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)

    def __repr__(self):
        return f"Broker {self.id}: ({self.name})"


class Account(Base):
    """ Used as a key to match with api details stored in local.secret """
    __tablename__ = "account"
    __table_args__ = (
        UniqueConstraint('broker', 'user_name', 'account_id'),
    )
    id = Column(Integer, primary_key=True, index=True)
    broker = Column(ForeignKey(
        'broker.name', ondelete="CASCADE"), index=True, nullable=True)
    user_name = Column(ForeignKey(
        'user.user_name', ondelete="CASCADE"), index=True, nullable=False)
    account_id = Column(String(255), nullable=False, unique=True)

    def __repr__(self):
        return f"Account {self.id}/ {self.account_id}: {self.user_name} @ {self.broker}"


class Trade(Base):
    __tablename__ = 'trade'
    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(ForeignKey(
        'account.id', ondelete="CASCADE"), nullable=True)
    Realized_PnL = Column(Numeric(6, 2))
    DataDiscriminator = Column(String(255), nullable=False)
    Code = Column(String(16), nullable=False)
    Realized_PnL_pct = Column(Numeric(6, 2))
    CommOrFee = Column(Numeric(6, 2))
    Quantity = Column(Numeric(6, 2))
    Proceeds = Column(Numeric(6, 2))
    Currency = Column(String(16), nullable=False)
    MTM_PnL = Column(Numeric(6, 2))
    MTM_in_USD = Column(Numeric(6, 2))
    Comm_in_USD = Column(Numeric(6, 2))
    Asset_Category = Column(String(255), nullable=False)
    Symbol = Column(String(16), nullable=False)
    Notional_Value = Column(Numeric(6, 2))
    C_Price = Column(Numeric(6, 2))  # current price
    DateTime = Column(DateTime, nullable=False)
    Basis = Column(Numeric(6, 2))
    T_Price = Column(Numeric(6, 2))  # trade price
    QuoteInLocalCurrency = Column(Numeric(6, 2))
    type = Column(SAIntEnum(TradeType), nullable=False)

    def __repr__(self):
        return (
            f"TRADE ID {self.id}"
        )


class ForexBalance(Base):
    __tablename__ = 'forexbalance'
    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(ForeignKey(
        'account.id', ondelete="CASCADE"), nullable=True)

    Asset_Category = Column(String(255), nullable=False)
    Currency = Column(String(16), nullable=False)
    Description = Column(String(256))
    Quantity = Column(Numeric(6, 2))
    Cost_Price = Column(Numeric(6, 2))
    Unrealized_PnL_in_USD = Column(Numeric(6, 2))
    Value_in_USD = Column(Numeric(6, 2))
    Close_Price = Column(Numeric(6, 2))
    Cost_Basis_in_USD = Column(Numeric(6, 2))
    Code = Column(String(16))

    def __repr__(self):
        return (
            f"FOREX BALANCE ID {self.id}"
        )


class NetAssetValue(Base):
    __tablename__ = 'netassetvalue'
    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(ForeignKey(
        'account.id', ondelete="CASCADE"), nullable=True)

    Asset_Class = Column(String(255), nullable=False)
    Prior_Total = Column(Numeric(6, 2))
    Current_Long = Column(Numeric(6, 2))
    Current_Short = Column(Numeric(6, 2))
    Current_Total = Column(Numeric(6, 2))
    Change = Column(Numeric(6, 2))

    def __repr__(self):
        return (
            f"NET ASSET VALUE ID {self.id}"
        )


class OpenPositions(Base):
    __tablename__ = 'openpositions'
    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(ForeignKey(
        'account.id', ondelete="CASCADE"), nullable=True)
    Symbol = Column(String(16), nullable=False)
    Quantity = Column(Numeric(6, 2))
    Mult = Column(Integer(), nullable=False)
    Cost_Price = Column(Numeric(6, 2))
    Cost_Basis = Column(Numeric(6, 2))
    Close_Price = Column(Numeric(6, 2))
    Value = Column(Numeric(6, 2))
    Unrealized_PnL = Column(Numeric(6, 2))
    Unrealized_PnL_pct = Column(Numeric(6, 2))
    Code = Column(String(16))

    def __repr__(self):
        return (
            f"OPEN POSITION ID {self.id}"
        )


class DepositsWithdrawals(Base):
    __tablename__ = 'depositswithdrawals'
    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(ForeignKey(
        'account.id', ondelete="CASCADE"), nullable=True)

    Currency = Column(String(16), nullable=False)
    Description = Column(String(256))
    DateTime = Column(DateTime, nullable=False)
    Amount = Column(Numeric(6, 2))
    QuoteInLocalCurrency = Column(Numeric(6, 2))

    def __repr__(self):
        return (
            f"DEPOSITS WITHDRAWALS ID {self.id}")


class Dividends(Base):
    __tablename__ = 'dividends'
    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(ForeignKey(
        'account.id', ondelete="CASCADE"), nullable=True)

    Currency = Column(String(16), nullable=False)
    Description = Column(String(256))
    DateTime = Column(DateTime, nullable=False)
    Amount = Column(Numeric(6, 2))
    QuoteInLocalCurrency = Column(Numeric(6, 2))

    def __repr__(self):
        return (
            f"DIVIDEND ID {self.id}")


class WitholdingTax(Base):
    __tablename__ = 'witholdingtax'
    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(ForeignKey(
        'account.id', ondelete="CASCADE"), nullable=True)

    Currency = Column(String(16), nullable=False)
    Description = Column(String(256))
    DateTime = Column(DateTime, nullable=False)
    Amount = Column(Numeric(6, 2))
    Code = Column(String(16))
    QuoteInLocalCurrency = Column(Numeric(6, 2))

    def __repr__(self):
        return (
            f"WITHOLDING TAX ID {self.id}")


class NameValue(Base):
    __tablename__ = 'namevalue'
    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(ForeignKey(
        'account.id', ondelete="CASCADE"), nullable=True)

    Name = Column(String(256), nullable=False)
    Value = Column(String(256), nullable=False)
    type = Column(SAIntEnum(NameValueType), nullable=False)

    def __repr__(self):
        return (
            f"NAME_VALUE ID {self.id}")


class tradePosition(Base):
    __tablename__ = "tradepos"
    id = Column(Integer, primary_key=True, index=True)
    qty = Column(Numeric(5, 2))
    opened = Column(DateTime, nullable=False)
    closed = Column(DateTime, nullable=True)
    status = Column(SAIntEnum(TradePositionStatus),
                    default=TradePositionStatus.OPEN)
    asset = Column(String(16))
    currency = Column(String(16))
    openedPrice = Column(Numeric(15, 7))  # currency
    closedPrice = Column(Numeric(15, 7))  # currency
    openedPrice_NOK = Column(Numeric(15, 7))  # NOK
    closedPrice_NOK = Column(Numeric(15, 7))  # NOK
    pnl = Column(Numeric(5, 2))
    pnl_NOK = Column(Numeric(5, 2))

    def __repr__(self):
        return (
            f'TRADEPOS {self.id} {self.status.name} {self.asset} {self.qty} @ {self.openedPrice} '
            f'- opened {self.opened}/ closed {self.closed} pnl {self.pnl if self.pnl else "N/A"}'
        )
