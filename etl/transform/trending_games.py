"""
Python module to transform the extracted data of the current top 5 trending games
from a JSON file.
"""
import json
import pandas as pd
from datetime import datetime

from logs.etl_pipeline_logs import provide_logs

def transform_top_5_trending_games(filepath: str) -> None:
    """
    Transform the extracted data of the current top 5 trending games from a JSON file.

    Args:
        filepath (str): The filepath of a JSON file
    """
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

        # Remove '%', handle invalid values, and convert the datatype of
        # all values for 'twenty_four_hour_change_pct' key to float
        for index, twenty_four_hour_change_pct in enumerate(
            top_5_trending_games["twenty_four_hour_change_pct"]
        ):
            twenty_four_hour_change_pct = str(twenty_four_hour_change_pct).replace(
                "%", ""
            )
            if twenty_four_hour_change_pct == "-":
                twenty_four_hour_change_pct = 0.0

            else:
                twenty_four_hour_change_pct = float(twenty_four_hour_change_pct)
            top_5_trending_games[
                "twenty_four_hour_change_pct"
            ][index] = twenty_four_hour_change_pct

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

        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the current top 5 trending games "
            "from a JSON file.",
            "SUCCESSFUL",
            None
        )
        # Convert the transformed data to a DataFrame object and
        # store the DataFrame to a CSV file of `data/output` directory
        df = pd.DataFrame(top_5_trending_games)
        df.to_csv(
            "data/output/top_5_trending_games.csv",
            index=False
        )

    except FileNotFoundError:
        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the current top 5 trending games "
            "from a JSON file.",
            "FAILED",
            f"Filename: '{filepath}' is invalid for parsing the extracted data "
            "of the current top 5 trending games from a JSON file to perform data "
            "transformation."
        )
        raise FileNotFoundError("The given filename for parsing the extracted data "
                                "of the current top 5 trending games from a JSON file "
                                "to perform data transformation is invalid!")

def transform_trending_games_stats_overview(
        filepath: str
) -> None:
    """
    Transform the extracted data of the current top 5 trending games' stats overview
    from a JSON file.

    Args:
        filepath (str): The filepath of a JSON file
    """
    try:
        # Parse the scraped data from a JSON file to perform data transformation
        with open(filepath, "r") as file:
            trending_games_stats_overview: dict[str, list] = json.load(file)

        # Convert the datatype of all values for 'app_id' key to integer
        for index, app_id in enumerate(
            trending_games_stats_overview["app_id"]
        ):
            app_id = int(app_id)
            trending_games_stats_overview["app_id"][index] = app_id

        # Convert the datatype of all values for 'twenty_four_hour_peak_players'
        # key to integer
        for index, twenty_four_hour_peak_players in enumerate(
            trending_games_stats_overview["twenty_four_hour_peak_players"]
        ):
            twenty_four_hour_peak_players = int(twenty_four_hour_peak_players)
            trending_games_stats_overview[
                "twenty_four_hour_peak_players"
            ][index] = twenty_four_hour_peak_players

        # Convert the datatype of all values for 'all_time_peak_players' key
        # to integer
        for index, all_time_peak_players in enumerate(
            trending_games_stats_overview["all_time_peak_players"]
        ):
            all_time_peak_players = int(all_time_peak_players)
            trending_games_stats_overview[
                "all_time_peak_players"
            ][index] = all_time_peak_players

        # Convert the datatype of all values for 'current_datetime' key
        # to datetime object with specified format
        for index, current_datetime in enumerate(
            trending_games_stats_overview["current_datetime"]
        ):
            current_datetime = datetime.strptime(
                current_datetime,
                "%Y-%m-%d %H:%M:%S PST%z"
            )
            trending_games_stats_overview["current_datetime"][index] = current_datetime

        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the top 5 trending games' stats "
            "overview from a JSON file.",
            "SUCCESSFUL",
            None
        )
        # Convert the transformed data to a DataFrame object and
        # store the DataFrame to a CSV file of `data/output` directory
        df = pd.DataFrame(
            trending_games_stats_overview
        )
        df.to_csv(
            "data/output/top_5_trending_games_stats_overview.csv",
            index=False
        )

    except FileNotFoundError:
        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the top 5 trending games' stats "
            "overview from a JSON file.",
            "FAILED",
            f"Filename: '{filepath}' is invalid for parsing the extracted data "
            "of the top 5 trending games' stats overview from a JSON file to "
            "perform data transformation."
        )
        raise FileNotFoundError("The given filename for parsing the extracted data "
                                "of the top 5 trending games' stats overview from "
                                "a JSON file to perform data transformation is "
                                "invalid!")

def transform_trending_games_historical_stats(
        filepath: str
) -> None:
    """
    Transform the extracted data of the current top 5 trending games's historical stats
    from a JSON file.

    Args:
        filepath (str): The filepath of a JSON file
    """
    try:
        # Parse the scraped data from a JSON file to perform data transformation
        with open(filepath, "r") as file:
            trending_games_historical_stats: dict[str, dict] = json.load(file)

        # Dictionary to store the transformed data
        data = {
            "app_id":       [],
            "month":        [],
            "avg_players":  [],
            "gain":         [],
            "gain_pct":     [],
            "peak_players": [],
        }

        for app_id, historical_stats in trending_games_historical_stats.items():
            # Convert the datatype of all values for 'app_id' key to integer
            app_id = int(app_id)
            for _ in range(len(historical_stats["month"])):
                data["app_id"].append(app_id)

            # Remove leading and trailing whitespaces of all values for 'month' key and
            # transform the values of that key that contains 'Last 30 Days' to a proper
            # month and year format (e.g. April 2026)
            for month in historical_stats["month"]:
                month = str(month).strip()
                month_mappings = {1 : "January",  2 : "February", 3 : "March",
                                  4 : "April",    5 : "May",      6 : "June",
                                  7 : "July",     8 : "August",   9 : "September",
                                  10: "October",  11: "November", 12: "December"}

                if month == "Last 30 Days":
                    month = month_mappings[datetime.now().month]
                    year = str(datetime.now().year)
                    month = month + " " + year

                data["month"].append(month)

            # Convert the datatype of all values for 'avg_players' key to float
            for avg_players in historical_stats["avg_players"]:
                avg_players = float(avg_players)
                data["avg_players"].append(avg_players)

            # Handle invalid values and convert the datatype of all values for
            # 'gain' key to float
            for gain in historical_stats["gain"]:
                if str(gain) == "-":
                    gain = 0.0

                else:
                    gain = float(gain)
                data["gain"].append(gain)

            # Remove '%', handle invalid values, and convert the datatype of
            # all values for 'gain_pct' key to float
            for gain_pct in historical_stats["gain_pct"]:
                gain_pct = str(gain_pct).replace("%", "")
                if gain_pct == "-":
                    gain_pct = 0.0

                else:
                    gain_pct = float(gain_pct)
                data["gain_pct"].append(gain_pct)

            # Convert the datatype of all values for 'peak_players' key to integer
            for peak_players in historical_stats["peak_players"]:
                peak_players = int(peak_players)
                data["peak_players"].append(peak_players)

        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the top 5 trending games' historical "
            "stats from a JSON file.",
            "SUCCESSFUL",
            None
        )
        # Convert the transformed data to a DataFrame object and
        # store the DataFrame to a CSV file of `data/output` directory
        df = pd.DataFrame(
            data
        )
        df.to_csv(
            "data/output/top_5_trending_games_historical_stats.csv",
            index=False
        )

    except FileNotFoundError:
        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the top 5 trending games' historical "
            "stats from a JSON file.",
            "FAILED",
            f"Filename: '{filepath}' is invalid for parsing the extracted data "
            "of the top 5 trending games' historical stats from a JSON file to "
            "perform data transformation."
        )
        raise FileNotFoundError("The given filename for parsing the extracted data "
                                "of the top 5 trending games' historical stats from "
                                "a JSON file to perform data transformation is "
                                "invalid!")