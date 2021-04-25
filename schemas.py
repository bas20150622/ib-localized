from marshmallow import Schema, fields, EXCLUDE, pre_dump
from decimal import Decimal
from xcoder import DecimalEncoder
from datetime import datetime
from enums import NameValueType, TradeType


# --- Custom field definitions
class MMIntEnum(fields.Field):
    """ Marshmallow IntEnum field mirroring SQAlchemy IntEnum field
        dump converts str or IntEnum to str
        load converts str to IntEnum
        :param enumtype: IntEnum instance """

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
        DT_TEXT_FMT = "%Y-%m-%d, %H:%M:%S"   # "2020-11-13, 10:05:39"

        return datetime.strptime(value, DT_TEXT_FMT)

    def _deserialize(self, value, attr, data, **kwargs):
        # load method
        return value


class ToDateField(fields.Field):
    def _serialize(self, value, attr, obj, **kwargs):
        # dump method - return naive datetime
        DT_TEXT_FMT = "%Y-%m-%d"   # "2020-11-13"

        return datetime.strptime(value, DT_TEXT_FMT)

    def _deserialize(self, value, attr, data, **kwargs):
        # load method
        return value


# --- Schema definitions
class BaseSchema(Schema):
    # Custom json decimalencoder

    def dumps(self, obj, **kwargs):
        return super().dumps(obj, cls=DecimalEncoder, **kwargs)


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
    type = MMIntEnum(TradeType)

    @pre_dump
    def add_trade_type(self, data, **kwargs):
        mapper = {
            "Stocks": TradeType.STOCKS,
            "Equity and Index Options": TradeType.OPTIONS,
            "CFDs": TradeType.CFDs,
            "Forex": TradeType.FOREX
        }
        data_type = data.get("Asset_Category")
        for match_str, trade_type in mapper.items():
            if data_type[:len(match_str)] == match_str:
                data["type"] = trade_type
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
    DateTime = ToDateField(attribute="Settle_Date")
    Description = fields.Str()
    Amount = ThousandField()

    @pre_dump
    def check_and_remove_total(self, data, many, **kwargs):
        if data.get("Currency")[:5] == "Total":
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
    DateTime = ToDateField(attribute="Date")
    Description = fields.Str()
    Amount = ThousandField()

    @pre_dump
    def check_and_remove_total(self, data, many, **kwargs):
        if data.get("Currency")[:5] == "Total":
            return
        return data


class WitholdingTaxSchema(BaseSchema):
    """
    Schema for selectively processing csv dividends info
    dump creates dict
    """
    class Meta:
        unknown = EXCLUDE

    Currency = fields.Str()
    DateTime = ToDateField(attribute="Date")
    Description = fields.Str()
    Amount = ThousandField()
    Code = fields.Str()

    @pre_dump
    def check_and_remove_total(self, data, many, **kwargs):
        if data.get("Currency")[:5] == "Total":
            return
        return data


class NameValueSchema(BaseSchema):
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
        in_data['type'] = self.context.get("type")
        return in_data


StatementSchema = NameValueSchema()
StatementSchema.context = {"type": NameValueType.STATEMENT}

AccountInformationSchema = NameValueSchema()
AccountInformationSchema.context = {"type": NameValueType.ACCOUNT_INFORMATION}
