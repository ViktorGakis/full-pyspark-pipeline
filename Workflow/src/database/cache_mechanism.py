from datetime import datetime, timezone
from typing import Optional, Union

from pyspark.sql import DataFrame


def cache_query(seconds=5):  # -> Callable[..., Callable[..., DataFrame]]:
    def decorator(fetch_function):  # -> Callable[..., DataFrame]:
        cache: dict[str, Union[Optional[datetime], Optional[DataFrame]]] = {
            "last_updated": None,
            "data": None,
        }

        def wrapper(*args, **kwargs) -> DataFrame:
            current_time: datetime = datetime.now(timezone.utc)
            if (
                cache["last_updated"] is None
                or (current_time - cache["last_updated"]).total_seconds() > seconds  # type: ignore
            ):
                if cache["data"] is not None:
                    cache["data"].unpersist()  # type: ignore # Clear previous cache
                cache["data"] = fetch_function(*args, **kwargs)
                cache["data"].cache()  # Cache the new DataFrame
                cache["last_updated"] = current_time
            return cache["data"]  # type: ignore

        return wrapper

    return decorator
