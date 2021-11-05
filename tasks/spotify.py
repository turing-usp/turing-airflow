import datetime

import pandas as pd
import psycopg2
import requests


class Spotify:
    def extract(self):
            TOKEN = 'BQAwMPVvPufren5gO7MBDqwgqkBdIluQUJIrD8_Ox850YQoUPSvF8FBb'
            'XhH8CMpVrG2DFqj-0xsELrKi-4z2YTp33t-fJpOp2pkcjWkTh78YvHZyUASI7JXeL-5u'
            'xZFyL8P9iWU63ZRTqInw_XMtaQr'

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

        insert_query = """
            INSERT INTO public.spotify_data (song_name, artist_name,
                                            played_at, date_played)
            VALUES (%s, %s, %s, %s)
        """

        try:
            conn = psycopg2.connect(
                host='localhost',
                database='spotify',
                user='postgres',
                password='shobusAs16'
            )
            cursor = conn.cursor()

            for index, row in df.iterrows():
                cursor.execute(insert_query, (
                    row['song_name'], row['artist_name'],
                    row['played_at'], row['timestamp'])
                )
        except Exception:
            conn.rollback()
            conn.close()

        conn.commit()
