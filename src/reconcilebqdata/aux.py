from datetime import date, datetime, timezone

import reconcilebqdata.dbtools
import time


def get_dict_symmetric_difference(dict_a, dict_b) -> dict:
    """Example: input { 1: 'a', 2: 'b', }, { 2: 'b', 3: 'c' } yields {1:'a', 3:'c'}

    Args:
        dict_a: A dictionary
        dict_b: A dictionary

    Returns:
        result: A dictionary whose keys are the symmetric difference of dict_a.keys() and dict_b.keys().

    """
    result = dict_a.copy()
    result.update(dict_b)

    for key in dict_a.keys() & dict_b.keys():
        del result[key]
    return result


def get_current_timestamp() -> str:
    now = datetime.now().replace(microsecond=0, tzinfo=timezone.utc).isoformat()
    return now


def time_function(func):
    """Decorator that logs the elapsed time of a function"""

    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed_seconds = round(time.time() - start, 2)
        print(f"{func.__name__}; elapsed time {elapsed_seconds} sec")
        return result

    return wrapper


if __name__ == "__main__":
    pass
