import inspect
from collections import OrderedDict, Mapping


class DbRow(OrderedDict):
    '''
    Represents the results of a single database row. Behaves like an
    OrderedDict with column values accessible as by keys in addition to
    standard object attributes.
    '''
    def __init__(self, *args, **kwargs):
        od = OrderedDict(*args, **kwargs)
        for key, val in od.items():
            if isinstance(val, Mapping):
                val = DotDict(val)
            self[key] = val

    def __delattr__(self, name):
        try:
            del self[name]
        except KeyError as ex:
            raise AttributeError(f"No attribute called: {name}") from ex

    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError as ex:
            raise AttributeError(f"No attribute called: {k}") from ex

    __setattr__ = OrderedDict.__setitem__

    @staticmethod
    def from_object(obj):
        ''' Converts an object to a DotDict '''
        if isinstance(obj, DbRow):
            return obj
        elif isinstance(obj, Mapping):
            return DbRow(obj)
        else:
            result = DbRow()
            for name in dir(obj):
                value = getattr(obj, name)
                if not name.startswith('__') and not inspect.ismethod(value):
                    result[name] = value
            return result


def dbrow_factory(cur, row):
    '''Helper to turn rows from a cursor into DbRow objects'''
    result = DbRow()
    for idx, col in enumerate(cur.description):
        result[col[0]] = row[idx]
    return result
