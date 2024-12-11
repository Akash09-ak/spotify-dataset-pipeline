import os
import pandas as pd
import sqlite3
from kaggle.api.kaggle_api_extended import KaggleApi

# Function to download data from Kaggle (requires authentication)
def download_kaggle_data(dataset, path):
    api = KaggleApi()
    api.authenticate()  # Authenticate with Kaggle API credentials
    api.dataset_download_files(dataset, path=path, unzip=True)  # Download and unzip dataset

# Function to load CSV files from specified paths
def load_csv_files(path):
    albums_path = os.path.join(path, "spotify-albums_data_2023.csv")
    tracks_path = os.path.join(path, "spotify_tracks_data_2023.csv")
    try:
        # Load CSV data into DataFrames
        albums_df = pd.read_csv(albums_path)
        tracks_df = pd.read_csv(tracks_path)
        print(f"Loaded CSV files: {albums_path} and {tracks_path}")
        return albums_df, tracks_df
    except pd.errors.ParserError:
        print(f"Error: Files at {albums_path} and {tracks_path} are not valid CSV formats.")
        exit(1)  # Exit the program if CSV files are invalid

# Function to clean the DataFrames by dropping unnecessary columns and rows with missing values
def clean_data(album_df, track_df):
    # Select required columns for albums
    required_album_columns = ['track_name', 'track_id', 'track_number', 'duration_ms',
                              'album_type', 'total_tracks', 'album_name', 'release_date',
                              'label', 'album_popularity', 'album_id', 'artist_id', 'artist_0']
    album_df = album_df[required_album_columns]
    album_df.dropna(subset=required_album_columns, inplace=True)  # Drop rows with missing values

    # Select required columns for tracks
    required_track_columns = ['id', 'track_popularity', 'explicit']
    track_df = track_df[required_track_columns]
    track_df.dropna(subset=required_track_columns, inplace=True)  # Drop rows with missing values

    # To print cleaned DataFrames
    # print(album_df.head(10))
    # print(track_df.tail(10))

    return album_df, track_df

# Function to transform the data by adding a radio_mix column and filtering tracks
def transform_data(album_df, track_df):
    # Categorize songs based on duration
    album_df['radio_mix'] = album_df['duration_ms'].apply(lambda x: x / 60000 <= 3)

    # Filter tracks to include only non-explicit and popular tracks
    track_df = track_df[(track_df['explicit'] == False) & (track_df['track_popularity'] > 50)]

    # To print transformed DataFrames
    # print(album_df.head(15))
    # print(track_df)

    return album_df, track_df

# Function to load DataFrames into an SQLite database
def load_to_database(df, db_name, table_name):
    # Print the DataFrame before loading just for checking
    #print(df)
    with sqlite3.connect(db_name) as conn:
        df.to_sql(table_name, conn, if_exists='replace', index=False)  # Load DataFrame to table

# Function to execute SQL queries on the database and return results
def query_database(db_name, query):
    with sqlite3.connect(db_name) as conn:
        return pd.read_sql_query(query, conn)

if __name__ == "__main__":
    # Define dataset, file paths, database name, and table names
    kaggle_dataset = "tonygordonjr/spotify-dataset-2023"                  # dataset name from kaggle
    local_path = "./spotify_dataset"                                      # local path
    db_name = "spotify_data.db"                                           # database name
    table_name1 = "transformed_album"                                     # table for transformed album data
    table_name2 = "transformed_track"                                     # table for transformed track data

    # Download data from Kaggle
    download_kaggle_data(kaggle_dataset, local_path)

    # Load data into DataFrames
    albums_data, tracks_data = load_csv_files(local_path)

    # Clean and transform the data
    cleaned_albums, cleaned_tracks = clean_data(albums_data.copy(), tracks_data.copy())              # we ensure that the original DataFrames remain unchanged.
    transformed_albums, transformed_tracks = transform_data(cleaned_albums, cleaned_tracks)         # dataframes are passed as arguments to the function.

    # Load the transformed data into the database
    load_to_database(transformed_albums, db_name, table_name1)
    load_to_database(transformed_tracks, db_name, table_name2)

    # Define and execute SQL queries
    top_labels_query = """
        SELECT ta.label, COUNT(*) AS total_tracks 
        FROM transformed_track tt
        JOIN transformed_album ta ON tt.id = ta.track_id
        GROUP BY ta.label 
        ORDER BY total_tracks DESC 
        LIMIT 20;
    """

    top_tracks_query = """
        SELECT * 
        FROM transformed_album
        WHERE release_date BETWEEN '2020-01-01' AND '2023-01-01'
        ORDER BY album_popularity DESC 
        LIMIT 25;
    """

    top_labels = query_database(db_name, top_labels_query)
    top_tracks = query_database(db_name, top_tracks_query)

    # Printing query results
    print("Top Labels:")
    print(top_labels)

    print("\nTop Tracks:")
    print(top_tracks)