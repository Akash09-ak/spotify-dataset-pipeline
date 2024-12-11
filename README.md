# spotify-dataset-pipeline
This repository contains Python code to extract, clean, transform, and load Spotify dataset into a SQLite database. The code also performs basic data analysis.

Below is the stepwise explanation of what code does-

1. Download Data (if needed):

    The code defines a function download_kaggle_data that uses the kaggle.api.kaggle_api_extended library to download a specified dataset (here, "spotify-dataset-2023") from Kaggle if necessary. This requires authentication with Kaggle API credentials.

2. Load CSV Files:

    The load_csv_files function takes a path and reads two CSV files ("spotify-albums_data_2023.csv" and "spotify-tracks_data_2023.csv") into pandas DataFrames (albums_df and tracks_df).

3. Clean Data :

    The clean_data function takes both DataFrames and performs cleaning steps:
        Selects relevant columns for each DataFrame.
        Removes rows with missing values (dropna).
    You can optionally uncomment the print statements to view the cleaned DataFrames.

4. Transform Data :

    The transform_data function takes the cleaned DataFrames:
        Adds a new column radio_mix to the albums DataFrame based on the duration.
        Filters the tracks DataFrame to include only non-explicit and popular tracks.
    You can optionally uncomment the print statements to view the transformed DataFrames.

5. Load Data into Database:

    The load_to_database function takes a DataFrame, database name, and table name.
    It connects to the SQLite database and loads the DataFrame into a table (replacing existing data if it exists).

6. Execute SQL Queries :

    The code defines two example SQL queries:
        top_labels_query: Finds top labels with the most tracks.
        top_tracks_query: Finds top popular tracks released between 2020-01-01 and 2023-01-01.
    The query_database function executes these queries on the database and returns the results as DataFrames.
