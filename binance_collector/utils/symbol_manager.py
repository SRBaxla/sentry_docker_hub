import requests


def get_active_binance_symbols():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url)
    data = response.json()
    print(data)
    symbols = [
        s['symbol'] for s in data['symbols']
        if s['status'] == 'TRADING'
    ]
    return symbols
    # result = query_api.query_data_frame(flux)
    # return result["_value"].dropna().unique().tolist()
