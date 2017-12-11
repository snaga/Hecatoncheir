import datetime
import json

class DbProfilerJSONEncoder(json.JSONEncoder):
    def default(self, o):
        try:
            iterable = iter(o)
        except TypeError:
            pass
        else:
            return list(iterable)

        if isinstance(o, datetime.datetime):
            return o.isoformat()
        elif isinstance(o, datetime.date):
            return o.isoformat()
        elif isinstance(o, decimal.Decimal):
            return (str(o) for o in [o])
        return super(DbProfilerJSONEncoder, self).default(o)

def jsonize(data):
    return json.dumps(data, cls=DbProfilerJSONEncoder, sort_keys=True,
                      indent=2)
