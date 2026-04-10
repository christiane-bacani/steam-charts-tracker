"""
Python module to transform the extracted data of the current top 10 games
(by current players) from a JSON file.
"""
import json
import pandas as pd
from datetime import datetime

from logs.etl_pipeline_logs import provide_logs

def transform_top_10_games(filepath: str) -> None:
    try:
        # Parse the scraped data from a JSON file to perform data transformation
        with open(filepath, "r") as file:
            top_10_games = json.load(file)

        df = pd.DataFrame(top_10_games)

        # Remove '.' and convert the datatype of all values for 'rank' key
        # to integer
        df["rank"] = df["rank"].str.replace(".", "")
        df["rank"] = pd.to_numeric(df["rank"], errors="raise")

        # Convert the datatype of all values for 'app_id' key to integer
        df["app_id"] = pd.to_numeric(df["app_id"], errors="coerce")

        # Remove leading and trailing whitespaces of all values for 'game_name' key
        df["game_name"] = df["game_name"].str.strip()

        # Convert the datatype of all values for 'current_players' key to integer
        df["current_players"] = pd.to_numeric(df["current_players"], errors="coerce")

        # Convert the datatype of all values for 'peak_players' key to integer
        df["peak_players"] = pd.to_numeric(df["peak_players"], errors="coerce")

        # Convert the datatype of all values for 'hours_played' key to integer
        df["hours_played"] = pd.to_numeric(df["hours_played"], errors="coerce")

        # Convert the datatype of all values for 'current_datetime' key
        # to datetime object with specified format
        df["current_datetime"] = pd.to_datetime(
            df["current_datetime"],
            errors="raise",
            format="%Y-%m-%d %H:%M:%S PST%z"
        )

        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the current top 10 games "
            "(by current players) from a JSON file.",
            "SUCCESSFUL",
            None
        )
        # Store the DataFrame object to a CSV file of `data/output` directory
        df.to_csv("data/output/top_10_games.csv", index=False)

    except FileNotFoundError:
        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the current top 10 games "
            "(by current players) from a JSON file.",
            "FAILED",
            f"Filename: '{filepath}' is invalid for parsing the extracted data "
            "of the current top 10 games (by current players) from a JSON file to "
            "perform data transformation."
        )
        raise FileNotFoundError("The given filename for parsing the extracted data "
                                "of the current top 10 games (by current players) "
                                "from a JSON file to perform data transformation "
                                "is invalid!")