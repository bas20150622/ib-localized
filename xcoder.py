# ********* json encoder/ decoder definitions
import json
from decimal import Decimal
from enums import Status
from datetime import datetime


class DecimalEncoder(json.JSONEncoder):
    """ Custom json encoder converting decimal to float.
        Useage: Schema().dumps(obj, cls=DecimalEncoder) """

    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


def jsonStatusDecoder(indict: dict) -> dict:
    """ json decoder object_hook for converting ***_status fields to Status intenum field """
    keys = indict.keys()
    for key in keys:
        if "status" in key:
            indict.update({key: Status(indict.get(key))})
    return indict


class SQLEncoder(json.JSONEncoder):
    """ Custom json encoder converting decimal to float and datetime to isoformat.
        Useage: Schema().dumps(obj, cls=DecimalEncoder) """

    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return super(DecimalEncoder, self).default(o)
