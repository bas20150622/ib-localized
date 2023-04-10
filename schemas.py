from marshmallow import Schema, fields, EXCLUDE, pre_dump, post_dump
from decimal import Decimal
from xcoder import DecimalEncoder
from datetime import datetime, timedelta
from enums import NameValueType, CategoryType


# --- Custom field definitions
class MMIntEnum(fields.Field):
    """Marshmallow IntEnum field mirroring SQAlchemy IntEnum field
    dump converts str or IntEnum to str
    load converts str to IntEnum
    :param enumtype: IntEnum instance"""

    def __init__(self, enumtype, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._enumtype = enumtype

    def _serialize(self, value, attr, obj):
        # dump: from str to IntEnum
        # if isinstance(value, str):
        #    return value

        if isinstance(value, self._enumtype):
            return value

    def _deserialize(self, value, attr, data, **kwargs):
        # Loading: from str to IntEnum
        # return self._enumtype[value]
        return value


class ThousandField(fields.Field):
    def _serialize(self, value, attr, obj, **kwargs):
        # dump method
        _val = value.replace(",", "")
        return Decimal(_val)

    def _deserialize(self, value, attr, data, **kwargs):
        # load method
        return str(value)


class ToDateTimeField(fields.Field):
    def _serialize(self, value, attr, obj, **kwargs):
        # dump method - return naive datetime
        DT_TEXT_FMT = "%Y-%m-%d, %H:%M:%S"  # "2020-11-13, 10:05:39"

        return datetime.strptime(value, DT_TEXT_FMT)

    def _deserialize(self, value, attr, data, **kwargs):
        # load method
        return value


class ToDateField(fields.Field):
    def _serialize(self, value, attr, obj, **kwargs):
        # dump method - return naive datetime
        DT_TEXT_FMT = "%Y-%m-%d"  # "2020-11-13"
        try:
            dt = datetime.strptime(value, DT_TEXT_FMT)
        except:
            return None

        return dt

    def _deserialize(self, value, attr, data, **kwargs):
        # load method
        return value


class DashDashEmptyThousandDecimalField(fields.Field):
    # field that can be "--", "", "xxx,yyy.zz" convert to Decimal
    def _serialize(self, value, attr, obj, **kwargs):
        # dump method for "--" vs "decimalstring"
        if value in ["", "--"]:

            return "0"
        _val = value.replace(",", "")

        return _val

    def _deserialize(self, value, attr, data, **kwargs):
        # load method
        return str(value)


class Percentage(fields.Field):
    """Field that serializes to a decimal and deserializes a string with % sign to a decimal fraction.
    i.e. .dump("34.5%") == Decimal(0.345), .load(Decimal(0.345) == "34.5%")
    """

    def _serialize(self, value, attr, obj, **kwargs):
        assert "%" == value[-1]
        value = value[:-1]
        value = value.rstrip()
        return Decimal(value) * Decimal(1e-2)

    def _deserialize(self, value, attr, data, **kwargs):

        return str(100 * value) + "%"


# --- Schema definitions
class BaseSchema(Schema):
    # Custom json decimalencoder

    def dumps(self, obj, **kwargs):
        return super().dumps(obj, cls=DecimalEncoder, **kwargs)


class Mark2MarketSchema(BaseSchema):
    Asset_Category = fields.Str()
    Symbol = fields.Str()
    Prior_Quantity = DashDashEmptyThousandDecimalField()
    Current_Quantity = DashDashEmptyThousandDecimalField()
    Prior_Price = DashDashEmptyThousandDecimalField()
    Current_Price = DashDashEmptyThousandDecimalField()
    Mark_to_Market_PnL_Position = DashDashEmptyThousandDecimalField()
    Mark_to_Market_PnL_Transaction = DashDashEmptyThousandDecimalField()
    Mark_to_Market_PnL_Commissions = DashDashEmptyThousandDecimalField()
    Mark_to_Market_PnL_Other = DashDashEmptyThousandDecimalField()
    Mark_to_Market_PnL_Total = DashDashEmptyThousandDecimalField()
    Code = fields.Str()
    type = MMIntEnum(CategoryType)

    @pre_dump
    def add_category_type(self, data, **kwargs):
        mapper = {
            "Stocks": CategoryType.STOCKS,
            "Equity and Index Options": CategoryType.OPTIONS,
            "CFDs": CategoryType.CFDs,
            "Forex CFDs": CategoryType.FOREX_CFDs,
            "Forex": CategoryType.FOREX,
            "Other Fees": CategoryType.FEES,
        }
        data_type = data.get("Asset_Category")
        data["type"] = CategoryType.UNKNOWN
        for match_str, trade_type in mapper.items():
            if data_type[: len(match_str)] == match_str:
                data["type"] = trade_type
        return data


class RealizedUnrealizedPerformanceSchema(BaseSchema):
    """
    Schema for selectively processing csv fee info
    dump creates dict
    """

    class Meta:
        unknown = EXCLUDE

    Account = fields.Str()
    Asset_Category = fields.Str()
    Symbol = fields.Str()
    Cost_Adj = fields.Decimal()
    Realized_ST_Profit = fields.Decimal()
    Realized_ST_Loss = fields.Decimal()
    Realized_LT_Profit = fields.Decimal()
    Realized_LT_Loss = fields.Decimal()
    Realized_Total = fields.Decimal()
    Unrealized_ST_Profit = fields.Decimal()
    Unrealized_ST_Loss = fields.Decimal()
    Unrealized_LT_Profit = fields.Decimal()
    Unrealized_LT_Loss = fields.Decimal()
    Unrealized_Total = fields.Decimal()
    Total = fields.Decimal()
    Code = fields.Str()
    type = MMIntEnum(CategoryType)

    @pre_dump
    def check_and_remove_total(self, data, many, **kwargs):
        if data.get("Asset_Category")[:5] == "Total":
            return
        return data

    @pre_dump
    def add_category_type(self, data, **kwargs):
        # NOTE ON EXPANSION - assure the below mapper categories sort equal beginning strings with longest first, see implementation of break statement
        mapper = {
            "Stocks": CategoryType.STOCKS,
            "Equity and Index Options": CategoryType.OPTIONS,
            "CFDs": CategoryType.CFDs,
            "Forex CFDs": CategoryType.FOREX_CFDs,
            "Forex": CategoryType.FOREX,
        }
        data_type = data.get("Asset_Category")
        for match_str, trade_type in mapper.items():
            if data_type[: len(match_str)] == match_str:
                data["type"] = trade_type
                break
        return data


class TradeInputSchema(BaseSchema):
    """
    Schema for selectively processing csv trade info
    dump creates dict
    """

    class Meta:
        unknown = EXCLUDE

    Realized_PnL = fields.Decimal()
    DataDiscriminator = fields.Str()
    Code = fields.Str()
    Realized_PnL_pct = fields.Decimal()
    CommOrFee = fields.Decimal()
    Quantity = ThousandField()
    Proceeds = fields.Decimal()
    Currency = fields.Str()
    Account = fields.Str()
    MTM_PnL = fields.Decimal()
    MTM_in_USD = fields.Decimal()
    Comm_in_USD = fields.Decimal()
    Asset_Category = fields.Str()
    Symbol = fields.Str()
    Notional_Value = fields.Decimal()
    C_Price = fields.Decimal()  # current price
    DateTime = ToDateTimeField()
    Basis = fields.Decimal()
    T_Price = fields.Decimal()  # trade price
    QuoteInLocalCurrency = fields.Decimal()
    type = MMIntEnum(CategoryType)

    @pre_dump
    def add_category_type(self, data, **kwargs):
        # NOTE ON EXPANSION - assure the below mapper categories sort equal beginning strings with longest first, see implementation of break statement
        mapper = {
            "Stocks": CategoryType.STOCKS,
            "Equity and Index Options": CategoryType.OPTIONS,
            "CFDs": CategoryType.CFDs,
            "Forex CFDs": CategoryType.FOREX_CFDs,
            "Forex": CategoryType.FOREX,
        }
        data_type = data.get("Asset_Category")
        for match_str, trade_type in mapper.items():
            if data_type[: len(match_str)] == match_str:
                data["type"] = trade_type
                break
        return data


class CashReportSchema(BaseSchema):
    Currency_Summary = fields.Str()
    Account = fields.Str()
    Currency = fields.Str()
    Total = fields.Decimal()
    Securities = fields.Decimal()
    Futures = fields.Decimal()
    IB_UKL = fields.Decimal()
    QuoteInLocalCurrency = fields.Decimal()


class CorporateActionsSchema(BaseSchema):
    Asset_Category = fields.Str()
    Account = fields.Str()
    Currency = fields.Str()
    Report_Date = ToDateField()
    DateTime = ToDateTimeField()
    Description = fields.Str()
    Quantity = fields.Decimal()
    Proceeds = fields.Decimal()
    Value = fields.Decimal()
    Realized_PnL = fields.Decimal()
    Code = fields.Str()
    type = MMIntEnum(CategoryType)

    @pre_dump
    def add_category_type(self, data, **kwargs):
        mapper = {
            "Stocks": CategoryType.STOCKS,
            "Equity and Index Options": CategoryType.OPTIONS,
            "CFDs": CategoryType.CFDs,
            "Forex CFDs": CategoryType.FOREX_CFDs,
            "Forex": CategoryType.FOREX,
            "Other Fees": CategoryType.FEES,
        }
        data_type = data.get("Asset_Category")
        data["type"] = CategoryType.UNKNOWN
        for match_str, trade_type in mapper.items():
            if data_type[: len(match_str)] == match_str:
                data["type"] = trade_type
        return data

    @pre_dump
    def check_and_remove_total(self, data, many, **kwargs):
        if data.get("Asset_Category")[:5] == "Total":
            return
        return data


class TransferSchema(BaseSchema):
    class Meta:
        unknown = EXCLUDE

    Asset_Category = fields.Str()
    Currency = fields.Str()
    Account = fields.Str()
    Symbol = fields.Str()
    DateTime = ToDateField(attribute="Date")
    _Type = fields.Str(attribute="Type")
    Direction = fields.Str()
    Xfer_Company = fields.Str()
    Xfer_Account = fields.Str()
    Qty = fields.Decimal()
    Xfer_Price = DashDashEmptyThousandDecimalField()
    Market_Value = fields.Decimal()
    Realized_PnL = fields.Decimal()
    Cash_Amount = fields.Decimal()
    Code = fields.Str()
    QuoteInLocalCurrency = fields.Decimal()

    type = MMIntEnum(CategoryType)

    @pre_dump
    def add_category_type(self, data, **kwargs):
        mapper = {
            "Stocks": CategoryType.STOCKS,
            "Equity and Index Options": CategoryType.OPTIONS,
            "CFDs": CategoryType.CFDs,
            "Forex CFDs": CategoryType.FOREX_CFDs,
            "Forex": CategoryType.FOREX,
        }
        data_type = data.get("Asset_Category")
        for match_str, trade_type in mapper.items():
            if data_type[: len(match_str)] == match_str:
                data["type"] = trade_type
        return data

    @post_dump
    def add_timedelta_to_transfer_out(self, data, **kwargs):
        # Add a timedelta to outgoing transfers so that any buys occurring on same day precede the transfer out which avoids tradeposition insufficient balance issues when calculating pnl
        if data.get("Qty") < Decimal(0):
            new_dt = data.get("DateTime") + timedelta(hours=23, minutes=59)
            data["DateTime"] = new_dt

        return data


class ForexBalancesSchema(BaseSchema):
    """
    Schema for selectively processing csv forex balance info
    dump creates dict
    """

    class Meta:
        unknown = EXCLUDE

    Asset_Category = fields.Str()
    Currency = fields.Str()
    Description = fields.Str()
    Quantity = ThousandField()
    Cost_Price = ThousandField()
    Unrealized_PnL_in_USD = fields.Decimal()
    Value_in_USD = fields.Decimal()
    Close_Price = fields.Decimal()
    Cost_Basis_in_USD = fields.Decimal()
    Code = fields.Str()

    @pre_dump
    def check_and_remove_total(self, data, many, **kwargs):
        if data.get("Asset_Category") == "Total":
            return
        return data


class NetAssetValueSchema(BaseSchema):
    """
    Schema for selectively processing csv net asset value info
    dump creates dict
    """

    class Meta:
        unknown = EXCLUDE

    Asset_Class = fields.Str()
    Prior_Total = ThousandField()
    Current_Long = ThousandField()
    Current_Short = ThousandField()
    Current_Total = ThousandField()
    Change = fields.Decimal()

    @pre_dump
    def check_and_remove_total(self, data, many, **kwargs):
        if data.get("Asset_Class") == "Total":
            return
        return data


class OpenPositionsSchema(BaseSchema):
    """
    Schema for selectively processing csv open positions info
    dump creates dict
    """

    class Meta:
        unknown = EXCLUDE

    Symbol = fields.Str()
    Quantity = ThousandField()
    Mult = fields.Int()
    Cost_Price = ThousandField()
    Cost_Basis = fields.Decimal()
    Close_Price = fields.Decimal()
    Value = fields.Decimal()
    Unrealized_PnL = fields.Decimal()
    Unrealized_PnL_pct = fields.Decimal()
    Code = fields.Str()

    @pre_dump
    def check_and_remove_total(self, data, many, **kwargs):
        if data.get("Asset_Category") == "Total":
            return
        return data


class DepositsWithdrawalsSchema(BaseSchema):
    """
    Schema for selectively processing csv open positions info
    dump creates dict
    """

    class Meta:
        unknown = EXCLUDE

    Currency = fields.Str()
    Account = fields.Str()
    DateTime = ToDateField(attribute="Settle_Date")
    Description = fields.Str()
    Amount = ThousandField()

    @pre_dump
    def check_and_remove_total(self, data, many, **kwargs):
        if data.get("Currency")[:5] == "Total":
            return
        return data

    @post_dump
    def add_timedelta_to_withdrawals(self, data, **kwargs):
        """
        Adds a timedelta to withdrawals in order to avoid insufficient balance warnings
        when calculating pnl via tradePositions - in case trades occur on same day as withdrawals
        """
        if data.get("Amount") < Decimal(0):
            new_dt = data.get("DateTime") + timedelta(hours=23, minutes=59)
            data["DateTime"] = new_dt

        return data


class FeeSchema(BaseSchema):
    """
    Schema for selectively processing csv fee info
    dump creates dict
    """

    class Meta:
        unknown = EXCLUDE

    Subtitle = fields.Str()
    Currency = fields.Str()
    Account = fields.Str()
    DateTime = ToDateField(attribute="Date")
    Description = fields.Str()
    Amount = ThousandField()

    @pre_dump
    def check_and_remove_total(self, data, many, **kwargs):
        if data.get("Subtitle")[:5] == "Total":
            return
        return data


class DividendsSchema(BaseSchema):
    """
    Schema for selectively processing csv dividends info
    dump creates dict
    """

    class Meta:
        unknown = EXCLUDE

    Currency = fields.Str()
    Account = fields.Str()
    DateTime = ToDateField(attribute="Date")
    Description = fields.Str()
    Symbol = fields.Str()
    Amount = ThousandField()

    @pre_dump
    def check_and_remove_total(self, data, many, **kwargs):
        if data.get("Currency")[:5] == "Total":
            return
        return data

    @pre_dump
    def add_symbol(self, data, many, **kwargs):
        """
        Derive symbol from description that always seems to start with SYMBOL(CODE)
        """
        symbol = data.get("Description").split("(")[0]
        data["Symbol"] = symbol
        return data


class AccountSummarySchema(BaseSchema):
    Currency = fields.Str()
    Account = fields.Str()
    Account_Alias = fields.Str()
    Name = fields.Str()
    Prior_NAV = fields.Decimal()
    Current_NAV = fields.Decimal()
    TWR = Percentage()


class WitholdingTaxSchema(BaseSchema):
    """
    Schema for selectively processing csv dividends info
    dump creates dict
    """

    class Meta:
        unknown = EXCLUDE

    Currency = fields.Str()
    Account = fields.Str()
    DateTime = ToDateField(attribute="Date")
    Description = fields.Str()
    Symbol = fields.Str()
    Amount = ThousandField()
    Code = fields.Str()

    @pre_dump
    def check_and_remove_total(self, data, many, **kwargs):
        if data.get("Currency")[:5] == "Total":
            return
        return data

    @pre_dump
    def add_symbol(self, data, many, **kwargs):
        """
        Derive symbol from description that always seems to start with SYMBOL(CODE)
        """
        symbol = data.get("Description").split("(")[0]
        data["Symbol"] = symbol
        return data


class _NameValueSchema(BaseSchema):
    """
    Schema for selectively processing Field Name: Field Value
    type row data (Statement and Account Information)
    dump creates dict
    """

    class Meta:
        unknown = EXCLUDE

    Name = fields.Str(attribute="Field_Name")
    Value = fields.Str(attribute="Field_Value")
    type = MMIntEnum(NameValueType)

    @pre_dump
    def add_type(self, in_data, **kwargs):
        in_data["type"] = self.context.get("type")
        return in_data


StatementSchema = _NameValueSchema()
StatementSchema.context = {"type": NameValueType.STATEMENT}

AccountInformationSchema = _NameValueSchema()
AccountInformationSchema.context = {"type": NameValueType.ACCOUNT_INFORMATION}


class ChangeInDividendAccrualsSchema(BaseSchema):
    """
    Schema for selectively processing csv change in dividend accruals info
    dump creates dict
    """

    class Meta:
        unknown = EXCLUDE

    Asset_Category = fields.Str()
    Currency = fields.Str()
    Account = fields.Str()
    Symbol = fields.Str()
    Date = ToDateField(attribute="Date")
    Ex_Date = ToDateField(attribute="Ex_Date")
    Pay_Date = ToDateField(attribute="Pay_Date")
    Quantity = fields.Decimal()
    Tax = fields.Decimal()
    Fee = fields.Decimal()
    Gross_Rate = fields.Decimal()
    Gross_Amount = ThousandField()
    Net_Amount = ThousandField()
    Code = fields.Str()

    @pre_dump
    def check_and_remove_total(self, data, many, **kwargs):
        if data.get("Currency")[:5] == "Total":
            return
        return data
