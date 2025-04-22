import json
import os
import pandas as pd
from datetime import datetime
import requests
import traceback
import numpy as np

def formatting_details(game):
    try:
            new_game = {}
            game_ids = pd.read_csv("../data/jogos_app.csv")
            new_game["steam_appid"] = int(game["steam_appid"])
            new_game["name"] = game["name"]
            new_game["is_free"] = bool(game["is_free"])

            if game["ratings"] == None:
                new_game["required_age"] = 0
            else:
                if game["ratings"] == None:
                    new_game["required_age"] = 0
                else:
                    try:
                        new_game["required_age"] = game["ratings"]["dejus"]["required_age"]
                    except:
                        new_game["required_age"] = 0

            achievements = game.get("achievements")
            if achievements is not None:
                new_game["n_achievements"] = game["achievements"]["total"]
            else:
                new_game["n_achievements"] = 0
            
            new_game['platforms'] = [platform for platform in game['platforms'].keys() if game['platforms'][platform] == True]

            categories = game.get("categories")
            if categories is not None:
                new_game["categories"] = [category['description'] for category in game['categories']]
            else:
                new_game["categories"] = []


            genres = game.get("genres")
            if genres is not None:
                new_game["genres"] = [genre["description"] for genre in game["genres"]]
            else:
                new_game["genres"] = []

            dlc = []

            new_game["is_released"] = not bool(game["release_date"]["coming_soon"])

            if(new_game["is_released"]):
                date_object = datetime.strptime(game["release_date"]["date"], "%b %d, %Y")
                new_game["release_date"] = date_object
            else:
                new_game["release_date"] = None

            publishers = game.get("publishers")
            if publishers is not None:
                new_game["publishers"] = game["publishers"]
            else:
                new_game["publishers"] = []

            new_game["additional_content"] = list()


            developers = game.get("developers")
            if developers is not None:
                new_game["developers"] = game["developers"]
            else:
                new_game["developers"] = []

            dlc = game.get("dlc")
            if dlc is not None:
                for i in dlc:
                    try:
                        new_game["additional_content"].append(game_ids[game_ids["appid"] == i]["name"].values[0])
                    except:
                        pass

            else:
                new_game["additional_content"] = []
            
            review = get_review_summary(new_game["steam_appid"])

            if review is not None:
                new_game["total_reviews"] = review["total_reviews"]
                new_game["total_positive"] = review["total_positive"]
                new_game["total_negative"] = review["total_negative"]
                new_game["review_score"] = review["review_score"]
                new_game["review_score_desc"] = review["review_score_desc"]
                if review["total_reviews"] == 0:
                    new_game["positive_percentual"] = 0
                else:
                    new_game["positive_percentual"] = round(((review["total_positive"] / review["total_reviews"])*100),1)

            price_initial = game.get("price_overview")

            if price_initial is not None:
                new_game["price_initial (USD)"] = price_initial["initial"]/100
            else:
                new_game["price_initial (USD)"] = 0

            meta = game.get("metacritic")
            if meta is not None:
                new_game["metacritic"] = int(game["metacritic"]["score"])
            else:
                new_game["metacritic"] = 0
            
            
            new_game = {k: new_game[k] for k in ['steam_appid', 'name', 'developers','publishers', 'categories', 'genres', 'required_age', 'n_achievements', 'platforms', 'is_released', 'release_date', 'additional_content', 'total_reviews', 'total_positive', 'total_negative', 'review_score', 'review_score_desc', 'positive_percentual', 'metacritic', 'is_free', 'price_initial (USD)']}
            return new_game
    except Exception as e:
        print("Deu ruim no: ", e)
        traceback.print_exc()  # Imprime o traceback completo

    
def get_review_summary(steam_id):
    url = f"https://store.steampowered.com/appreviews/{steam_id}?json=1"
    game = requests.get(url)
    if game.status_code == 200:
        game = game.json()
        if game["success"]:
            return game['query_summary']

def remover_duplicatas():
    game_ids = pd.read_csv("../data/jogos_app.csv")
    game_ids = game_ids.drop_duplicates(subset="appid")
    game_ids.to_csv("../data/jogos_app.csv", index=False)