import os
from client.client import download_hyblock_data


CLIENT_ID = os.getenv("CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")
API_KEY = os.getenv("API_KEY", "")


if __name__ == "__main__":
    client_id = CLIENT_ID
    client_secret = CLIENT_SECRET
    api_key = API_KEY

    # consider single endpoint only
    endpoint_list = [
        "/bidAsk",
    ]
    coin_list = [
        "BTC",
        # "ETH",
    ]
    timeframe_list = [
        "1d",
        "4h",
    ]
    exchange_list = [
        "Binance",
        # "Deribit",
    ]

    limit = 1000

    df = download_hyblock_data(
        endpoint_list=endpoint_list,
        coin_list=coin_list,
        timeframe_list=timeframe_list,
        exchange_list=exchange_list,
        limit=limit,
        client_id=client_id,
        client_secret=client_secret,
        api_key=api_key,
        start_time=1672531200,
        depth="0,20",  # kwargs for bidAsk endpoint
    )
    print(df.head(10))

    print(df)
