import json
import pandas as pd
from datetime import datetime

from logs.etl_pipeline_logs import provide_logs

def transform_top_5_trending_games(filepath: str) -> None:
    try:
        # Parse the scraped data from a JSON file to perform data transformation
        with open(filepath, "r") as file:
            top_5_trending_games: dict[str, list] = json.load(file)

        # Remove '.' and convert the datatype of all values for 'rank' key
        # to integer
        for index, rank in enumerate(top_5_trending_games["rank"]):
            rank = int(str(rank).replace(".", ""))
            top_5_trending_games["rank"][index] = rank

        # Convert the datatype of all values for 'app_id' key to integer
        for index, app_id in enumerate(top_5_trending_games["app_id"]):
            app_id = int(app_id)
            top_5_trending_games["app_id"][index] = app_id

        # Remove leading and trailing whitespaces of all values for 'game_name' key
        for index, game_name in enumerate(top_5_trending_games["game_name"]):
            game_name = str(game_name).strip()
            top_5_trending_games["game_name"][index] = game_name

        # Convert the datatype of all values for 'current_players' key to integer
        for index, current_players in enumerate(
            top_5_trending_games["current_players"]
        ):
            current_players = int(current_players)
            top_5_trending_games["current_players"][index] = current_players

        # Convert the datatype of all values for 'current_datetime' key
        # to datetime object with specified format
        for index, current_datetime in enumerate(
            top_5_trending_games["current_datetime"]
        ):
            current_datetime = datetime.strptime(
                current_datetime,
                "%Y-%m-%d %H:%M:%S PST%z"
            )
            top_5_trending_games["current_datetime"][index] = current_datetime

        # Convert the transformed data to a DataFrame object and
        # store the DataFrame to a CSV file from `data/output` directory
        transformed_top_5_trending_games = pd.DataFrame(top_5_trending_games)
        transformed_top_5_trending_games.to_csv(
            "data/output/top_5_trending_games.csv",
            index=False
        )

    except FileNotFoundError:
        raise FileNotFoundError("The given filename for parsing the extracted data "
                                "of the top 5 trending games from a JSON file is "
                                "invalid!")