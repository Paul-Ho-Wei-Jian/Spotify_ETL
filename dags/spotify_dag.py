from airflow import Dataset
from airflow.decorators import dag, task
import pendulum
from pendulum import datetime
import requests

import pandas as pd 
import requests
import json
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime, timedelta
import os


import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from snowflake.snowpark.functions import col
from snowflake.snowpark import Session


def create_spotify_oauth():
        return SpotifyOAuth(    
            client_id = 'your_client_id',
            client_secret = 'your_client_secret',
            redirect_uri = "http://127.0.0.1:3000",
            scope='user-read-recently-played'
        )
        # This method returns an instance 

def check_if_valid_data(df: pd.DataFrame) -> bool:

    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False 

    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    # check if timestamps contains only > yesterday timestamp 
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    yesterday = datetime.now().date() - timedelta(days=1)

    if (df['timestamp'].dt.date < yesterday).any():
        raise Exception("At least one of the returned songs does not have a yesterday's timestamp")


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=pendulum.datetime(2025, 3, 2),
    schedule="30 20 * * *",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["Spotify_ETl"]
)

def spotify_ETL():

    @task
    def extract_spotify_data():
        oauth_obj = create_spotify_oauth()
        token_info = create_spotify_oauth().get_cached_token()

        if oauth_obj.is_token_expired(token_info):
            token_info = oauth_obj.refresh_access_token()
        
        headers = {
            "Accept" : "application/json",
            "Content-Type" : "application/json",
            "Authorization" : "Bearer {bearer_token}".format(bearer_token = token_info['access_token'])
        }

        yesterday = datetime.now() - timedelta(days=1)
        yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

        r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)
        data = r.json()

        return data

    @task
    def transform_df(data: dict):

        # Transform
        
        song_names = []
        artist_names = []
        played_at_list = []
        timestamps = []

        # Extracting only the relevant bits of data from the json object      
        for song in data["items"]:
            song_names.append(song["track"]["name"])
            artist_names.append(song["track"]["album"]["artists"][0]["name"])
            played_at_list.append(song["played_at"])
            timestamps.append(song["played_at"][0:10])
            
        # Prepare a dictionary in order to turn it into a pandas dataframe below       
        song_dict = {
            "song_name" : song_names,
            "artist_name": artist_names,
            "played_at" : played_at_list,
            "timestamp" : timestamps
        }

        song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

        song_df['timestamp'] = pd.to_datetime(song_df['timestamp'])
        song_df['timestamp'] = song_df['timestamp'].dt.date

        yesterday = datetime.now().date() - timedelta(days=1)

        # Filter song_df for yesterday and today's timestamp
        song_df = song_df[song_df['timestamp'] >= yesterday]

        return song_df

    @task
    def load_to_snowflake(df : pd.DataFrame):

        # Validate

        check_if_valid_data(df)

        connection_parameters = {
            "user" : 'yourUsername',
            "password" : 'yourPassword',
            "account" : "Account-Identifier",
            " warehouse" : "yourWarehouse",
            "database" : "yourDatabase",
            "schema" : "yourSchema"
        }
        
        session = Session.builder.configs(connection_parameters).create()
        snowpark_df = session.create_dataframe(df)
        table_name = "MY_RECENTLY_PLAYED"
        snowpark_df.write.mode("append").save_as_table(table_name)   

    json_data = extract_spotify_data()
    dataframe = transform_df(json_data)
    load_to_snowflake(dataframe)

# Instantiate the DAG
spotify_ETL()
