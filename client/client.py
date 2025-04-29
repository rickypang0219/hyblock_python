import requests
import time
from typing import Any, Callable
from functools import wraps
import logging
import polars as pl
from itertools import product

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def iterative_fetch(time_column: str = "openDate", enabled: bool = True) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(
            self,
            endpoint: str,
            query_params: dict[str, Any] | None = None,
            *args,
            **kwargs,
        ) -> list[dict[str, Any]] | None:
            time_set = set()
            if not enabled:
                result = func(self, endpoint, query_params, *args, **kwargs)
                return result.get("data", []) if isinstance(result, dict) else []

            all_results = []
            params = query_params or {}
            start_time = params.get("startTime", 0)
            if not start_time:
                raise ValueError("startTime parameter is required")
            end_time = (
                params.get("endTime", int(time.time()))
                if params.get("endTime") is not None
                else int(time.time())
            )
            limit = params.get("limit", 1000)
            current_start_time = start_time

            while current_start_time < end_time:
                current_params = {
                    **params,
                    "startTime": current_start_time,
                    "endTime": end_time,
                    "limit": limit,
                }
                result = func(self, endpoint, current_params, *args, **kwargs)

                if not result or not isinstance(result, dict) or "data" not in result:
                    logging.info(
                        f"No data returned for {endpoint} at startTime={current_start_time}"
                    )
                    break

                data = result["data"]
                if not data or not isinstance(data, list):
                    logging.info(
                        f"Empty data list for {endpoint} at startTime={current_start_time}"
                    )
                    break

                # exclude the last data to avoid duplicates
                all_results.extend(data[:-1])
                # logging.info(f"Fetched {len(data)} data points from the endpoint")

                try:
                    latest_time = max(int(entry[time_column]) for entry in data)
                    current_start_time = latest_time + 1
                except KeyError as e:
                    logging.error(
                        f"Time column '{time_column}' not found in response: {e}"
                    )
                    return all_results
                except ValueError as e:
                    logging.error(f"Invalid timestamp in '{time_column}': {e}")
                    return all_results

                # Last fetching loop
                if len(data) == 1:
                    if data[-1][time_column] not in time_set:
                        all_results.extend(data)
                        logging.info(f"Received {len(data)} results stopping iteration")
                        current_start_time = latest_time + 3 * 86400  # jump days
                        time_set.add(data[-1][time_column])
                        continue
                    else:
                        break

            return all_results

        return wrapper

    return decorator


def union_join_timestamp(
    df_1: pl.DataFrame,
    df_2: pl.DataFrame,
    time_col: str,
) -> pl.DataFrame:
    all_timestamps = pl.DataFrame(
        {f"{time_col}": sorted(set(df_1[f"{time_col}"]).union(df_2[f"{time_col}"]))}
    )

    df_result = all_timestamps.join(df_1, on=f"{time_col}", how="left")

    df_result = df_result.join(
        df_2,
        on=f"{time_col}",
        how="left",
    )

    return df_result


class HyblockConsumer:
    def __init__(self, client_id: str, client_secret: str, api_key: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_key = api_key
        self.auth_url = "https://auth-api.hyblockcapital.com/oauth2/token"
        self.base_url = "https://api1.hyblockcapital.com/v1"
        self.access_token = None

    def update_access_token(self) -> str:
        auth_data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        try:
            response = requests.post(self.auth_url, data=auth_data, headers=headers)
            response.raise_for_status()
            token_data = response.json()
            if "access_token" not in token_data:
                raise Exception(f"Access token not found in response: {token_data}")
            self.access_token = token_data["access_token"]
            logging.info(f"Access token retrieved: {self.access_token[:10]}...")
            return self.access_token
        except requests.RequestException as e:
            logging.error(f"Authentication request failed: {str(e)}")
            raise Exception(f"Authentication failed: {str(e)}")
        except ValueError as e:
            logging.error(f"Invalid JSON response: {str(e)}")
            raise Exception(f"Invalid JSON response: {str(e)}")

    @iterative_fetch(time_column="openDate", enabled=True)
    def get_api_request(
        self, endpoint: str, query_params: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        if not self.access_token:
            self.update_access_token()

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "x-api-key": self.api_key,
            "Content-Type": "application/json",
        }
        url = self.base_url + endpoint
        # logging.info(f"Making request to {url} with params {query_params}")

        try:
            response = requests.get(
                url,
                params=query_params,
                headers=headers,
                timeout=60,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Error {response.status_code}: {response.text}")
            return None
        except ValueError as e:
            logging.error(f"Error parsing JSON: {str(e)}")
            return None


def iter_helper(**kwargs) -> list[dict[str, Any]]:
    key_list = list(kwargs)
    value_lists = [
        kwargs[key] if isinstance(kwargs[key], list) else [kwargs[key]]
        for key in key_list
    ]
    combinations = list(product(*value_lists))

    param_list = [
        {key_list[i]: combo[i] for i in range(len(key_list))} for combo in combinations
    ]

    seen = set()
    unique_param_list = [
        param
        for param in param_list
        if not (
            tuple(sorted(param.items())) in seen
            or seen.add(tuple(sorted(param.items())))
        )
    ]

    return unique_param_list


def download_hyblock_data(
    endpoint_list: list[str],
    coin_list: list[str],
    timeframe_list: list[str],
    exchange_list: list[str],
    client_id: str,
    client_secret: str,
    api_key: str,
    start_time: int | None = None,
    end_time: int | None = None,
    limit: int = 1000,
    **kwargs,
) -> pl.DataFrame:
    for param, name in [
        (endpoint_list, "endpoint_list"),
        (coin_list, "coin_list"),
        (timeframe_list, "timeframe_list"),
        (exchange_list, "exchange_list"),
    ]:
        if not isinstance(param, list):
            raise ValueError(f"{name} must be a list")

    hyblock_consumer = HyblockConsumer(client_id, client_secret, api_key)
    df_lists: list = []

    base_params = {
        "coin": coin_list,
        "timeframe": timeframe_list,
        "exchange": exchange_list,
    }

    base_params.update(kwargs)
    param_combinations = iter_helper(**base_params)

    for endpoint in endpoint_list:
        for params in param_combinations:
            query_params = {
                "limit": limit,
                "startTime": start_time,
                "endTime": end_time,
                **params,
            }

            param_str = "_".join(f"{v}" for v in params.values())

            data = hyblock_consumer.get_api_request(endpoint, query_params)
            if not data:  # incase the endpoints no data
                continue

            df = pl.DataFrame(data)
            if isinstance(df, str):
                print(f"Error for {endpoint} with params {params}: {df}")
                continue

            if len(df.columns) == 1:
                df.columns = [f"{endpoint.strip('/')}_{param_str}"]
            else:
                df.columns = [
                    f"{endpoint.strip('/')}_{param_str}_{col}"
                    if "openDate" not in col
                    else col
                    for col in df.columns
                ]
            df_lists.append(df)

    if not df_lists:  # if the endpoint does not have data, return null dataframe
        return pl.DataFrame()

    merged_df = df_lists[0]
    for df in df_lists[1:]:
        merged_df = union_join_timestamp(merged_df, df, "openDate")
    return merged_df


if __name__ == "__main__":
    import os
    import matplotlib.pyplot as plt

    CLIENT_ID = os.getenv("CLIENT_ID", "")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")
    API_KEY = os.getenv("API_KEY", "")
    hyblock_consumer = HyblockConsumer(CLIENT_ID, CLIENT_SECRET, API_KEY)

    data = hyblock_consumer.get_api_request(
        "/bidAsk",
        {
            "exchange": "Binance",
            "coin": "BTC",
            "startTime": 1673136300,
            "endTime": 1735718400,
            # "startTime": 1745366400,
            "limit": 1000,
            "timeframe": "1m",
        },
    )
    df = pl.DataFrame(data)
    df.write_csv("test.csv")
    plt.plot(df["openDate"], df["bid"])
    plt.show()
