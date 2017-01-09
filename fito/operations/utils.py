"""
Helper functions that allows you to receive and handle collections in a uniform way
"""
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
    """
    Provides a map that works on lists, dicts and tuples, and is recursive into sub collections (default behaviour)

    :param recursion_condition: Predicate specifying on which cases the function should recurse.
    Default: :py:func:`is_iterable`

    :return: A similar structure to the given iterable where :param callable: was applied

    Example:
    >>> recursive_map([[1], {"some": 3, "stuff":10}], lambda x:x+1)
    >>> [[2], {'some': 4, 'stuff': 11}]
    """
    recursion_condition = recursion_condition or is_iterable
    res = general_new(iterable)
    for k, v in general_iterator(iterable):
        if recursion_condition(v):
            res = general_append(res, k, recursive_map(v, callable, recursion_condition))
        else:
            res = general_append(res, k, callable(v))
    return res

