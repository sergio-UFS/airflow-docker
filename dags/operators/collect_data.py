import requests
import datetime
from dotenv import load_dotenv
import os
import pandas as pd
from utils.utils import Utils


class Collect_data_operator():


    def __init__():
        pass


    def collect_daily_spotify():

        load_dotenv()
        spotify_key = "BQCYWQErry4hlp-zuuXQYQFBASq_ITSoHdgiMQa7aSHHIgoz0yx-OL6Na5eOuFCbD2qM30Qqnsy--kOWfPVwMqLFAN9unrU0MJHKL4uBnADLk8RGRQJng6qtEWjdk7jEeTKLj8e59GBOO_I-sA2k_Ol-Vy9LSXK5q3I9sVD8Tl1gp6w-WeATDWMBRsfmdCKt81otsEzH6jAHEIjM-qmzc-Hdr6WNHEL3Yy64gg"

        url = f'https://charts-spotify-com-service.spotify.com/auth/v0/charts/regional-global-daily/{datetime.date.today()-datetime.timedelta(days=1)}'
        headers = {"Authorization": f'Bearer {spotify_key}'}

        try:
            r = requests.get(url, headers=headers)
            data = r.json()
            return data['entries']
    
        except Exception as e:
            print(e)

        
    def create_df_spotify_data(data: dict):
        df_songs = pd.DataFrame(columns=["date","track_name","streams_daily"])
        list_songs=[]
        for song in data:
            df_music = []
            chart_info = song['chartEntryData']
            track_info = song['trackMetadata']

            df_music.append(datetime.date.today())
            df_music.append(track_info['trackName'])
            df_music.append(chart_info['rankingMetric']['value'])
            artists = []
            for i in track_info['artists']:
                artists.append(i['name'])
            df_music.append(artists)

            df_music.append(track_info["releaseDate"])
            df_music.append(chart_info["currentRank"])
            df_music.track_info["peakRank"]
            df_music.chart_info['appearancesOnChart']

            list_songs.append(df_music)

        df_songs = pd.DataFrame(list_songs,columns=["date","track_name","streams_daily",'artists',"release_date","current_rank", "peak_rank",'appearances_on_Chart'])
        Utils.save_csv(df_songs)
        return df_songs
    



a = Collect_data_operator.create_df_spotify_data(Collect_data_operator.collect_daily_spotify())


