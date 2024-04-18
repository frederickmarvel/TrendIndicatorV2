import os
from typing import Union
from fastapi import FastAPI
import pymysql
from pymysql.err import MySQLError
from dotenv import load_dotenv
import requests
from datetime import datetime
import pandas as pd
app = FastAPI()

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')


lambdas = [0.5, 0.757858283, 0.870550563, 0.933032992, 0.965936329, 0.982820599]
nfs = [1.0000, 1.0000, 1.0000, 1.0000, 1.0020, 1.0462]


def get_binance_data(ticker):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": ticker,
        "interval": "1d",
        "limit": 180
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = pd.DataFrame(response.json(), columns=["Open time", "Open", "High", "Low", "Close", "Volume", "Close time", "Quote asset volume", "Number of trades", "Taker buy base asset volume", "Taker buy quote asset volume", "Ignore"])
        data['Close'] = data['Close'].astype(float)  # Convert close prices to float
        return data
    else:
        print("Failed to retrieve data: Status code", response.status_code)
        return None

def calculate_ewma(df, lambdas, nf):
    ewma_results = []
    for i in range(len(df)):
        ewma_results.append((1 - lambdas) * pow(lambdas, i) * float(df['Close'][i]) * nf)
    return sum(ewma_results)

def sign(x):
    return 1 if x >= 0 else -1

def calculate_trend_indicator(data, lambdas, nfs):
    MA1 = calculate_ewma(data, lambdas[0], nfs[0])  # 1 day
    MA2_5 = calculate_ewma(data, lambdas[1], nfs[1])  # 2.5 days
    MA5 = calculate_ewma(data, lambdas[2], nfs[2])  # 5 days
    MA10 = calculate_ewma(data, lambdas[3], nfs[3])  # 10 days
    MA20 = calculate_ewma(data, lambdas[4], nfs[4])  # 20 days
    MA40 = calculate_ewma(data, lambdas[5], nfs[5])  # 40 days
    MAP1 = sign(MA1 - MA5)
    MAP2 = sign(MA2_5 - MA10)
    MAP3 = sign(MA5 - MA20)
    MAP4 = sign(MA10 - MA40)
    result = MAP1 + MAP2 + MAP3 + MAP4
    return result/4

def get_trend_indicator(ticker):
    data = get_binance_data(ticker)
    trend_indicator = calculate_trend_indicator(data, lambdas, nfs)
    return int(trend_indicator)

@app.get("/trend/update")
def fetch_and_update():
    current_timestamp = datetime.now()
    ticker_eth="ETHUSDT"
    trend_indicator_eth = get_trend_indicator(ticker_eth)
    ticker_btc="BTCUSDT"
    trend_indicator_btc = get_trend_indicator(ticker_btc)
    ticker_btc="SOLUSDT"
    trend_indicator_sol = get_trend_indicator(ticker_btc)
    try:
        conn = pymysql.connect(host=db_host, db=db_name, user=db_user, password=db_password)
        cursor = conn.cursor()
        
        # Prepare insert query
        insert_query = "INSERT INTO trend_indicator (bitcoin_trend, ethereum_trend, solana_trend, timestamp) VALUES (%s, %s, %s, %s, %s)"
        values =(int(trend_indicator_btc), int(trend_indicator_eth), int(trend_indicator_sol),current_timestamp)

        # Execute insert
        cursor.execute(insert_query, values) # Use execute for single record
        conn.commit()
        cursor.close()
        conn.close()
    except MySQLError as e:
        print(f"Error: {e}")