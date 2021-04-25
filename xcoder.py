# ********* json encoder/ decoder definitions
import json
from decimal import Decimal
from datetime import datetime


class DecimalEncoder(json.JSONEncoder):
    """ Custom json encoder converting decimal to float.
        Useage: Schema().dumps(obj, cls=DecimalEncoder) """

    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


class SQLEncoder(json.JSONEncoder):
    """ Custom json encoder converting decimal to float and datetime to isoformat.
        Useage: Schema().dumps(obj, cls=DecimalEncoder) """

    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return super(DecimalEncoder, self).default(o)
