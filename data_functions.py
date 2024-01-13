import requests
import pandas as pd
import numpy as np
import time

def download_crypto_data(crypto_id='bitcoin', vs_currency='usd', days='max', interval='daily', retries=3, delay=45):
    """
    Fetch historical data for a specified cryptocurrency from CoinGecko and saves it into csv file.

    Parameters:
    - crypto_id: (str) The id of the cryptocurrency on CoinGecko (e.g., 'bitcoin', 'ethereum', etc.)
    - vs_currency: (str) The target currency of market data (usd, eur, jpy, etc.)
    - days: (str) Number of days to retrieve data for (e.g., '1', '14', '30', 'max')
    - interval: (str) Data interval (e.g., 'minute', 'hourly', 'daily')

    Returns:
    - A pandas DataFrame with the historical data.
    """
    url = f"https://api.coingecko.com/api/v3/coins/{crypto_id}/market_chart"
    params = {
        "vs_currency": vs_currency,
        "days": days,
        "interval": interval
    }
    
    for _ in range(retries):
        try:
            

            response = requests.get(url, params=params)
            # Check for valid response
            response.raise_for_status()
            data = response.json()
            df = pd.DataFrame(data["prices"], columns=["Timestamp", "Price"])
            df.rename(columns={'Price': crypto_id}, inplace=True)
            # Convert the Timestamp from milliseconds to datetime format
            df["Timestamp"] = pd.to_datetime(df["Timestamp"], unit='ms')
            df['Timestamp'] = df['Timestamp'].dt.strftime('%Y-%m-%d')
            df = df.drop_duplicates(subset=['Timestamp'], keep='last')
            df.set_index(df.Timestamp, inplace=True)
            df = df[[crypto_id]]

            df.to_csv(f'{crypto_id}.csv')

            print(f"Data successfully saved to {crypto_id}.csv")
            return df
        
        except requests.HTTPError as e:
            print(f"HTTP error occurred: {e}")
        except requests.ConnectionError as e:
            print(f"Connection error occurred: {e}")
        except requests.Timeout as e:
            print(f"Timeout error occurred: {e}")
        except requests.RequestException as e:
            print(f"An unexpected error occurred: {e}")

        # If we haven't returned by this point, we've hit an error. Wait and then try again.
        print(f"Retrying in {delay} seconds...")
        time.sleep(delay)
        
        
    # If we've exhausted all retries and still haven't succeeded, return None
    print("Failed to download data after several retries.")
    return None
    


def fetch_top_cryptos_by_market_cap(vs_currency='usd', limit=50):
    """
    Fetch a list of the top cryptocurrencies from CoinGecko based on market capitalization.

    Parameters:
    - vs_currency: The target currency of market data (usd, eur, jpy, etc.)
    - limit: Number of top cryptocurrencies to retrieve based on market cap.

    Returns:
    - A pandas DataFrame with the top cryptocurrencies by market cap.
    """
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "order_by": "market_cap",  # Order by market cap
        "per_page": limit,  # Limit the number of results
        "page": 1,  # Starting page
        "sparkline": False,  # We don't need sparkline data
        "price_change_percentage": '24h',  # 24h price change
    }

    response = requests.get(url, params=params)
    data = response.json()

    # Convert the data into a DataFrame
    df = pd.DataFrame(data)
    
    # Select relevant columns for clarity
    df = df[['id', 'symbol', 'name', 'current_price', 'market_cap', 'price_change_percentage_24h_in_currency']]
    
    return df