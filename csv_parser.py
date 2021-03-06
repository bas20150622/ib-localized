import csv
from pytz import timezone
from schemas import TradeInputSchema, NetAssetValueSchema, OpenPositionsSchema, ForexBalancesSchema
from schemas import DepositsWithdrawalsSchema, DividendsSchema, WitholdingTaxSchema
from schemas import StatementSchema, AccountInformationSchema, ChangeInDividendAccrualsSchema
from forex import forex_rate
from typing import Union, Dict, List, Any
from pathlib import Path
from collections import defaultdict


EDT = timezone('US/Eastern')


class DataPassThroughSchema():
    @classmethod
    def dump(cls, in_data, many: bool = False, **kwargs):
        '''if many:
            # return a list
            if many and isinstance(in_data, list):
                return [a_dict for a_dict in in_data]'''

        return in_data


_data_type_schemas = {   # NOTE: class schemas require () to indicate class method call required
    "Statement": StatementSchema,  # instance schema
    "Disclaimer": None,
    "Account Information": AccountInformationSchema,  # instance schema
    "Net Asset Value": NetAssetValueSchema(),  # class schema
    "Change in NAV": None,
    "Mark-to-Market Performance Summary": None,
    "Total P/L for Statement Period": None,
    "Realized & Unrealized Performance Summary": None,
    "Cash Report": None,
    "Open Positions": OpenPositionsSchema(),
    "Trades": TradeInputSchema(),
    "Transfers": None,
    "Forex Balances": ForexBalancesSchema(),
    "Corporate Actions": None,
    "Deposits & Withdrawals": DepositsWithdrawalsSchema(),
    "Fees": None,
    "Dividends": DividendsSchema(),
    "Withholding Tax": WitholdingTaxSchema(),
    "Change in Dividend Accruals": ChangeInDividendAccrualsSchema(),
    "IBKR Managed Securities Collateral Held at IBSS (Stock Yield Enhancement Program)": None,
    "Financial Instrument Information": None,
    "Codes": None,
    "Notes/Legal Notes": None
}


def add_local_base(forex_rate: forex_rate, data_row_dump_dict: dict) -> dict:
    """
    Verify presence of DateTime key in indict, and if found add USD.NOK exchange rate
    - requires preprocessing of header keays to strip / char
    :param forex_rate: forex rate fetcher
    :param data_row_dump_dict: dict obtained from schema.dump method for csv data row types
    """
    from settings import LOCAL_CURRENCY

    DATETIME_IDENTIFIER_KEY = "DateTime"
    CURRENCY_IDENTIFIER_KEY = "Currency"
    dict_keys = data_row_dump_dict.keys()
    if DATETIME_IDENTIFIER_KEY in dict_keys and CURRENCY_IDENTIFIER_KEY in dict_keys:
        dt = data_row_dump_dict.get(DATETIME_IDENTIFIER_KEY)
        try:
            quote = data_row_dump_dict[CURRENCY_IDENTIFIER_KEY].upper()
            data_row_dump_dict.update(
                {"QuoteInLocalCurrency": forex_rate(quote, LOCAL_CURRENCY, dt)})
        except ValueError:
            print(
                "ERROR, cant get quote in {LOCAL_CURRENCY} for {quote} @ {dt}")
            # pass
    return data_row_dump_dict


def preprocess_header_keys(header_row: list) -> dict:
    """
    Rename/ replace/ remove characters from header_row raw csv key data
    to enable serializing and deserializing using marshmallow
    """
    cleaned_row = []
    for key in header_row:
        new_key = (
            key
            .replace(" ", "_")
            .replace("%", "pct")
            .replace("P/L", "PnL")
            .replace(".", "")
            .replace("Date/Time", "DateTime")
            .replace("Comm/Fee", "CommOrFee")
            .replace("(", "")
            .replace(")", "")
        )
        if new_key == "":
            new_key = "_blank_"
        cleaned_row.append(new_key)
    return cleaned_row


def process_csv(
        datafile: Union[Path, str],
        data_types_to_process: list = None, suppress_not_implemented: bool = True) -> Dict[str, List[Dict[str, Any]]]:
    """
    function to process rows in datafile
    :param datafile: input csv file path
    :param data_types_to_process: list of data types to fetch, fetch all implemented data types if not provided
    :param suppress_not_implemented: bool indicating whether to suppress not implemented data row types
    :returns result: dictionary with key==data type and val== list of dictionaries containing row data
    """
    result = defaultdict(list)
    header = None
    row_count: int = 0

    with open(datafile, newline='') as csvfile:
        datareader = csv.reader(csvfile, delimiter=',')
        print("Parsing csv file...")
        for row in datareader:
            data_type = row[0]
            row_count += 1
            # escape unicode byte order mark
            data_type = data_type.split('\ufeff')[-1]
            if data_types_to_process:
                if data_type not in data_types_to_process:
                    continue
            row_schema = _data_type_schemas.get(data_type, None)
            if not (row_schema or suppress_not_implemented):
                print(f"SKIPPING NOT IMPLEMENTED DATA ROW TYPE {data_type}")
            if row[1] == "Header":
                header = preprocess_header_keys(row[2:])
            elif row[1] == "Data" and header:
                data_dict = dict(zip(header, row[2:]))
                if row_schema:
                    try:
                        dump_dict = row_schema.dump(data_dict)
                        dump_dict = add_local_base(forex_rate, dump_dict)
                    except:  # noqa - can be many schema/ field related types
                        dump_dict = None
                        if not suppress_not_implemented:
                            print("ERROR ON THE BELOW **************************")
                            print(data_dict)
                    if dump_dict:
                        result[data_type].append(dump_dict)

                else:
                    if not suppress_not_implemented:
                        print(f"NOT ABLE TO DUMP! {data_dict}")
            else:
                pass
    print(f"Finished parsing {row_count} rows ...")
    return result
