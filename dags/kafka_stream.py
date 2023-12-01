from datetime import datetime
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from kafka import KafkaProducer
import logging

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 10, 19, 10, 00)
}

def getDatafromAPI():
    import yfinance as yf
    import pandas as pd
    from datetime import date
    import json

    start_date = '2023-01-01'
    end_date = date.today()

    ETF = ['QQQ', 'SPY', 'DIA', 'IWM', 'KWEB', 'AXP', 'AAPL']
    ETFInformation = yf.download(tickers=ETF, start=start_date, end=end_date, interval='1d')
    # ETFInformation: <class 'pandas.core.frame.DataFrame'>

    # file_name = 'ETF_data.xlsx'
    # ETFInformation.to_excel(file_name)

    return ETFInformation

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    ETFInformation = getDatafromAPI()
    technical = ['Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']
    ETF_reorder = ['AAPL', 'AXP', 'DIA', 'IWM', 'KWEB', 'QQQ', 'SPY']
    data = {}
    row_num = ETFInformation.shape[0]
    ETF_num = len(ETF_reorder)
    index = 0
    for row_index in range(row_num):
        for index in range(ETF_num):
            data['Date'] = ETFInformation.iloc[row_index, :].name.strftime("%Y-%m-%d %H:%M:%S")
            data['ETF'] = ETF_reorder[index]
            for i in range(len(technical)):
                data[technical[i]] = ETFInformation.iloc[row_index, index + i * ETF_num]

            # change int64 to int before serialization
            data[technical[-1]] = int(data[technical[-1]])
            print(json.dumps(data).encode('utf-8'))
            try:
                producer.send('assets', json.dumps(data).encode('utf-8'))
            except Exception as e:
                logging.error(f'An error occurred: {e}')
                continue


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )