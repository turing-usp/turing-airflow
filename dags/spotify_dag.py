import datetime
from datetime import timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import awswrangler as wr

default_args = {
    'owner': 'murilokrebsky',
    'depends_on_past': False,
    'start_date': days_ago(0, 0, 0, 0, 0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='My first DAG',
    schedule_interval=timedelta(days=1)
)


def check_valid(df: pd.DataFrame) -> bool:
    if df.empty:
        print(
            'No songs were listened in the past 24 hours. Finishing execution'
        )
    # primary key check
    if pd.Series(df['played_at']).is_unique():
        pass
    else:
        raise Exception('Primary key check not valid')

    if df.insull().values.any():
        raise Exception('Null value found')

    # checking the date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hout=0, minute=0, second=0, microsecond=0)

    timestamps = df['timestamp'].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception('Datetime invalid')

    return True


def extraction():
    TOKEN = 'BQAHFSflxiv4JEQB2xt2yIdVW3_5o3Kg4pASKzxO6-4XigKpIcJsFo5O3NEMth78h'
    'actJuAArcERuGum7gl4Jdgnm4J6Ovm87Pm7ZopqAZ0yKTqo27crPma7o_v4ntR4WXx2R6BNpu'
    '2vp75uPtqlHfop'

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {token}'.format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get(
        'https://api.spotify.com/v1/me/player/'
        'recently-played?after={time}'.
        format(time=yesterday_unix_timestamp), headers=headers
    )

    data = r.json()

    song_names = []
    artist_names = []
    played_at = []
    timestamps = []

    for song in data['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['album']['artists'][0]['name'])
        played_at.append(song['played_at'])
        timestamps.append(song['played_at'][:10])

    song_dict = {
        'song_name': song_names,
        'artist_name': artist_names,
        'played_at': played_at,
        'timestamp': timestamps
    }

    df = pd.DataFrame(
        song_dict, columns=[
            'song_name', 'artist_name', 'played_at', 'timestamp'
        ]
    )

    wr.s3.to_parquet(
        df=df,
        path='s3://turing-datalake/processados/spotify/',
        dataset=True,
        database='processados',
        table='spotify'
    )


extract = PythonOperator(
    task_id='extract',
    python_callable=extraction,
    dag=dag,
)

extract
