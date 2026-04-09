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

        transformed_data = {
            "rank":             [],
            "app_id":           [],
            "game_name":        [],
            "current_players":  [],
            "peak_players":     [],
            "hours_played":     [],
            "current_datetime": []
        }

        for rank in top_10_games["rank"]:
            rank = str(rank).replace(".", "")            
            transformed_data["rank"].append(rank)

        for app_id in top_10_games["app_id"]:
            app_id = int(app_id)
            transformed_data["app_id"].append(app_id)

        for game_name in top_10_games["game_name"]:
            game_name = str(game_name).strip()
            transformed_data["game_name"].append(game_name)

        for current_players in top_10_games["current_players"]:
            current_players = int(current_players)
            transformed_data["current_players"].append(current_players)

        for peak_players in top_10_games["peak_players"]:
            peak_players = int(peak_players)
            transformed_data["peak_players"].append(peak_players)

        for hours_played in top_10_games["hours_played"]:
            hours_played = int(hours_played)
            transformed_data["hours_played"].append(hours_played)

        for current_datetime in top_10_games["current_datetime"]:
            current_datetime = datetime.strptime(
                current_datetime, "%Y-%m-%d %H:%M:%S PST%z"
            )
            transformed_data["current_datetime"].append(current_datetime)

        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the current top 10 games "
            "(by current players) from a JSON file.",
            "SUCCESSFUL",
            None
        )
        df = pd.DataFrame(transformed_data)
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