import requests
import os
import polars as pl

CLIENT_ID = os.getenv("CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")
API_KEY = os.getenv("API_KEY", "")

grant_type = "client_credentials"  # this is constant
amazon_auth_url = "https://auth-api.hyblockcapital.com/oauth2/token"
auth_data = {
    "grant_type": grant_type,
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
}


def update_access_token():
    auth_response = requests.post(
        amazon_auth_url,
        data=auth_data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    return auth_response.json()


def get_data(path, base_url, query_params):
    auth_response_json = update_access_token()
    auth_token_header = {
        "Authorization": "Bearer %s" % auth_response_json["access_token"],
        "x-api-key": API_KEY,
    }

    url = base_url + path
    response = requests.get(url, params=query_params, headers=auth_token_header)
    print(response.json())
    return response.json()


base_url = "https://api1.hyblockcapital.com/v1"
orderbook_path = "/bidAsk"

query = {
    "timeframe": "1m",
    "coin": "BTC",
    "exchange": "Binance",
    "limit": 5,
}

data = get_data(orderbook_path, base_url, query)

print(pl.DataFrame(data))
