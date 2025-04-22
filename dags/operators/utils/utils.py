import requests
import base64
from dotenv import load_dotenv
import os
import datetime

class Utils():
    
    def __init__(self):
        pass

    def get_acess_token():
        load_dotenv()
        TOKEN_URL = "https://accounts.spotify.com/api/token"
        CLIENT_ID = os.getenv("CLIENT_ID")
        CLIENT_SECRET = os.getenv("CLIENT_SECRET")

        auth_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
        auth_header =  base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")

        headers = {
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded"
        }

        data = {"grant_type": "client_credentials"}

        response = requests.post(TOKEN_URL, headers=headers, data=data)

        if response.status_code == 200:
            token_info = response.json()
            return token_info["access_token"]
        else:
            raise Exception(f"Erro ao obter token: {response.text}")
        
    def save_csv(df):
        file_name = f'./files/spotify_trends-{datetime.date.today()}'
        df.to_csv(file_name, index = False)