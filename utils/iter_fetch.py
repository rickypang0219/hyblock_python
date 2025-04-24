from typing import Any, Callable
from functools import wraps
import time


def iterative_fetch(time_column: str, enabled: bool = True) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(params: dict[str, Any], *args, **kwargs) -> list[dict[str, Any]]:
            if not enabled:
                return func(params, *args, **kwargs)

            all_results = []
            start_time = params.get("startTime", 0) * 1000
            if not start_time:
                raise ValueError("startTime parameter is required")
            end_time = params.get("endTime", int(time.time() * 1000))
            limit = params.get("limit", 1000)
            current_start_time = start_time

            while current_start_time < end_time:
                current_params = {
                    **params,
                    "startTime": current_start_time,
                    "endTime": end_time,
                    "limit": limit,
                }
                result = func(current_params, *args, **kwargs)
                if not result or not isinstance(result, list) or len(result) == 0:
                    break
                all_results.extend(result)
                latest_time = max(int(entry[time_column]) for entry in result)
                current_start_time = latest_time + 1
                if len(result) < limit:
                    break
            return all_results

        return wrapper

    return decorator
