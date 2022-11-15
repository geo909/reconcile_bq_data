import time
import datetime


def time_function(func):
    """Decorator that logs the elapsed time of a function"""

    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed_seconds = round(time.time() - start, 2)
        print(f"Elapsed: {elapsed_seconds}")
        return result

    return wrapper


def get_timestamp() -> str:
    now = (
        datetime.datetime.now()
        .replace(microsecond=0, tzinfo=datetime.timezone.utc)
        .isoformat()
    )
    return now
