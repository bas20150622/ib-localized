# FOREX.py
# Module for obtaining historical forex rates
# current implementation for NOK/EUR/USD only
# TODO: expand to generic native currency

from functools import partial
from decimal import Decimal
from datetime import datetime
from typing import Tuple
import requests
import pandas as pd

QUANTIZE_FIAT = Decimal("0.0001")


class ForexRate():
    """ Class for fetching FIAT exchangerates with
    FIAT being one of ['USD','EUR','NOK'] """

    def __init__(self):
        # self.logger = get_logger(type(self).__name__)
        # self.logger.info('initializing exrates')
        url = "https://data.norges-bank.no/api/data/EXR/B.USD+EUR+.NOK.SP"
        querystring = {
            "startPeriod": "2017-05-19",
            "endPeriod": datetime.strftime(datetime.now(), format="%Y-%m-%d"), "format": "sdmx-json", "locale": "no"}
        payload = ""
        headers = {
            'cache-control': "no-cache",
            'Postman-Token': "51510138-8f7b-4297-a8a1-665306bc1cdc"
        }
        response = requests.request(
            "GET", url, data=payload, headers=headers, params=querystring)

        # ISSUE: return positional order is not guaranteed by URL positional order
        BASE_ORDER = [val['values'] for val in
                      response.json()['data']['structure']['dimensions']['series'] if val['id'] == 'BASE_CUR']
        BASE_ORDER = [val['id'] for val in BASE_ORDER[0]]
        EURPOS = BASE_ORDER.index('EUR')
        USDPOS = BASE_ORDER.index('USD')

        USD = [float(val[0]) for key, val in
               response.json()['data']['dataSets'][0]['series']['0:'+str(USDPOS)+':0:0']['observations'].items()]
        EUR = [float(val[0]) for key, val in
               response.json()['data']['dataSets'][0]['series']['0:'+str(EURPOS)+':0:0']['observations'].items()]
        DATES = [d['id'] for d in response.json()['data']['structure']['dimensions']
                 ['observation'][0]['values']]

        self.df = pd.DataFrame(index=pd.to_datetime(
            DATES), data={'USD': USD, 'EUR': EUR})

    def return_formatted(self, rate: Decimal, curr: str, short_format: bool):
        """ using short_format either return only rate or Tuple[rate, curr] """
        if short_format:
            return rate
        return rate, curr

    def getForexRate(
            self, base: str, quote: str = "USD", dt: datetime = datetime.now(),
            short_format: bool = False) -> Tuple[Decimal, str]:
        '''
        outputs historical fiat base/quote rate for datetime date
        as a tuple of str(base.quote), Decimal
        '''
        # check for missing input
        if not isinstance(quote, str):
            raise ValueError(
                "Error on quote: provide base=str, quote=str[default USD], dt=datetime")
        FIAT = ["USD", "EUR", "NOK"]
        assert base in FIAT, "Error, base not in FIAT"
        assert quote in FIAT, "Error, quote not in FIAT"

        if base == quote:
            return self.return_formatted(
                Decimal(1).quantize(QUANTIZE_FIAT),
                base.upper() + '.' + quote.upper(),
                short_format)

        quote_ix, base_ix = None, None
        if quote == 'USD':
            quote_ix = 0
        elif quote == "EUR":
            quote_ix = 1
        if base == "USD":
            base_ix = 0
        elif base == 'EUR':
            base_ix = 1

        # closest index of time less or equal
        le_ix = self.df[self.df.index <= dt.replace(tzinfo=None)].index[-1]
        # closest index of time greater or equal
        ge_ix = self.df[self.df.index >= dt.replace(tzinfo=None)].index[0]
        # select nearest price
        dtl = dt.replace(tzinfo=None).timestamp() - le_ix.timestamp()
        dtr = ge_ix.timestamp() - dt.replace(tzinfo=None).timestamp()
        if dtl < dtr:
            ix = le_ix
        else:
            ix = ge_ix
        quote_val = Decimal(self.df.loc[ix][quote_ix]
                            ) if quote_ix is not None else Decimal("1")
        base_val = Decimal(self.df.loc[ix][base_ix]
                           ) if base_ix is not None else Decimal("1")
        # print(f"quote {quote} base {base}")
        return self.return_formatted(
            (base_val / quote_val).quantize(QUANTIZE_FIAT),
            base.upper() + '.' + quote.upper(),
            short_format)


forex_instance = ForexRate()
forex_rate = partial(forex_instance.getForexRate, short_format=True)
