import requests
import datetime
from dotenv import load_dotenv
from . import adjustingdetails as adj
import os
import pandas as pd
import json
import time


class Collect_data_operator():

    def __init__(self):
        pass

    def data_steam_games():
        df_jogos_app = pd.read_csv('/home/sergiomanhaesmfilho/airflow/dags/data/jogos_app.csv')
        with open('../data/offset.json', 'r') as f:
            offset = json.load(f)
            f.close()

        df_registrados = pd.read_csv('../data/steam_games.csv')
        df_naojogos = pd.read_csv('../data/steam_add_content.csv')

        df_jogos_app = df_jogos_app.dropna()
        df_jogos_app = df_jogos_app[~df_jogos_app['appid'].isin(df_registrados['steam_appid'])]
        df_jogos_app = df_jogos_app[df_jogos_app['name'] != '']
        df_jogos_app = df_jogos_app[~df_jogos_app['name'].str.endswith('Demo')]
        df_jogos_app = df_jogos_app[~df_jogos_app['name'].str.endswith('Playtest')]

        filtrar_apenas_jogos = ["DLC", "Soundtrack", "Expansion Pack", "Playtest"]

        filtered_games = [
            row['appid'] for index, row in df_jogos_app.iterrows()
            if not any(keyword.lower() in row["name"].lower() for keyword in filtrar_apenas_jogos)
        ]
        filtered_games = [game for game in filtered_games if game not in df_naojogos['steam_appid']]

        print(len(filtered_games))

        games_id = filtered_games

        url = "https://store.steampowered.com/api/appdetails?appids="
        games = []
        aditional_content = []

        num_offset = pd.read_csv('../data/steam_games.csv')

        num_offset = len(num_offset)
        i = offset['offset']
        for game_id in games_id[i:i+30]:
            response = requests.get(url + str(game_id)+"&cc=us")
            if response.status_code == 200:
                try:
                    data = response.json()
                    if data[str(game_id)]['success']:

                        if(data[str(game_id)]['data']['type'] == 'game'):
                            games.append(adj.formatting_details(data[str(game_id)]['data']))
                        else:
                            aditional_content.append(adj.formatting_details(data[str(game_id)]['data']))
                            
                except:
                    print("Erro no formato da saída", game_id)
                        
            else:
                print("Erro:", response.status_code)
                if response.status_code == 429:
                    print("Atingiu limite de requisições")
                    time.sleep(60)

            games = [game for game in games if game is not None]
            games = [game for game in games if not game['name'].endswith('Test')]

            df_games = games
            df = pd.read_csv('../data/steam_games.csv')
            df_games = pd.DataFrame(df_games)
            df = pd.concat([df, df_games], ignore_index=True)
            df.to_csv('../data/steam_games.csv', index=False)


            offset['offset'] += 30
            with open('../data/offset.json', 'w') as f:
                json.dump(offset, f)
                f.close()

            games = []
            aditional_content = []