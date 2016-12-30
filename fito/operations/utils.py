def general_new(iterable):
    return type(iterable)()


def general_append(iterable, k, v):
    if isinstance(iterable, list):
        assert k == len(iterable)
        iterable.append(v)
    elif isinstance(iterable, dict):
        iterable[k] = v
    elif isinstance(iterable, tuple):
        assert k == len(iterable)
        iterable = iterable + (v,)
    else:
        raise ValueError()
    return iterable


def general_iterator(iterable):
    if isinstance(iterable, list) or isinstance(iterable, tuple):
        return enumerate(iterable)
    elif isinstance(iterable, dict):
        return iterable.iteritems()
    else:
        raise ValueError()


def is_iterable(obj):
    return isinstance(obj, list) or isinstance(obj, dict) or isinstance(obj, tuple)


def recursive_map(iterable, callable, recursion_condition=None):
    recursion_condition = recursion_condition or is_iterable
    res = general_new(iterable)
    for k, v in general_iterator(iterable):
        if recursion_condition(v):
            res = general_append(res, k, recursive_map(v, callable, recursion_condition))
        else:
            res = general_append(res, k, callable(v))
    return res

