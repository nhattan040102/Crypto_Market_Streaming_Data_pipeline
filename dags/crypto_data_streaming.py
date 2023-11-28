import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from kafka.errors import KafkaError
import requests
import json
from kafka import KafkaProducer
import time
import logging


default_args = {
    'owner': 'nhattan',
    'start_date': datetime(2023, 9, 3, 10, 00)
}


def check_response(res):
    return True


def get_crypto_data(**kwargs):
    URL = "https://data-api.binance.vision"
    END_POINT = "/api/v3/avgPrice"

    symbols_to_fetch = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"]

    data = []

    for symbol in symbols_to_fetch:
        params = { 'symbol': symbol}

        res = requests.get(f"{URL}{END_POINT}", params=params)

        if res.status_code == 200 and check_response(res):
            data.append(res.json())    
        else:
            print(f"Failed to retrieve data for symbol {symbol}. Status code: {res.status_code}")

    if len(data) == 6:
        kwargs['ti'].xcom_push(key='raw_crypto_data', value=data)
        return data
    else:
        return AirflowException("Failed to retrieve crypto market data!")
        

def process_raw_data(**kwargs):
    crypto_data = kwargs['ti'].xcom_pull(task_ids='get_crypto_data_task', key='raw_crypto_data')
    cur_time = int(datetime.utcnow().timestamp())
    processed_crypto_data = {}
    
    
    symbols_list = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"]
    
    for idx, data in enumerate(crypto_data):
        cur_symbol = symbols_list[idx]
        processed_crypto_data[cur_symbol] = data['price']

    processed_crypto_data['timestamp'] = cur_time    

    kwargs['ti'].xcom_push(key='processed_crypto_data', value=processed_crypto_data)

    

def load_crypto_data_to_kafka(**kwargs):
    try:
        producer = KafkaProducer(bootstrap_servers=['broker:29092'],
                                max_block_ms=5000,
                                value_serializer=lambda v: json.dumps(v).encode('utf-8')
                                )

        data = kwargs['ti'].xcom_pull(task_ids='process_data_task', key='processed_crypto_data') 
        producer.send('cryptoData', data)
    except KafkaError as e:
        raise(f'Error: {e}')


    
    
    
    

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='*/5 * * * *',
         catchup=False) as dag:

    get_crypto_data_task = PythonOperator(
    task_id='get_crypto_data_task',
    python_callable=get_crypto_data,
    provide_context=True,
    retries=3,  # Number of retries upon failure
    retry_delay=timedelta(seconds=10),  # Delay between retries
    dag=dag,
    )
    
    process_data_task = PythonOperator(
        task_id = "process_data_task",
        python_callable=process_raw_data,
        provide_context=True,
        dag=dag
    )

    load_data_to_kafka_task = PythonOperator(
        task_id = "load_data_task",
        python_callable=load_crypto_data_to_kafka,
        provide_context=True,
        dag=dag
    )

    get_crypto_data_task >> process_data_task >> load_data_to_kafka_task
    