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

        df = pd.DataFrame(top_5_trending_games)

        # Remove '.' and convert the datatype of all values for 'rank' key
        # to integer
        df["rank"] = df["rank"].str.replace(".", "")
        df["rank"] = pd.to_numeric(df["rank"], errors="raise")

        # Convert the datatype of all values for 'app_id' key to integer
        df["app_id"] = pd.to_numeric(df["app_id"], errors="raise")

        # Remove leading and trailing whitespaces of all values for 'game_name' key
        df["game_name"] = df["game_name"].str.strip()

        # Remove '%' and '+', handle invalid values, and convert the datatype of
        # all values for 'twenty_four_hour_change_pct' key to float
        df["twenty_four_hour_change_pct"] = df[
            "twenty_four_hour_change_pct"
        ].str.replace("%", "").str.replace("+", "")
        df["twenty_four_hour_change_pct"] = pd.to_numeric(
            df["twenty_four_hour_change_pct"],
            errors="coerce",
            downcast="float"
        )

        # Convert the datatype of all values for 'current_players' key to integer
        df["current_players"] = pd.to_numeric(df["current_players"], errors="coerce")

        # Convert the datatype of all values for 'current_datetime' key
        # to datetime object with specified format
        df["current_datetime"] = pd.to_datetime(
            df["current_datetime"],
            errors="raise",
            format="%Y-%m-%d %H:%M:%S PST%z"
        )

        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the current top 5 trending games "
            "from a JSON file.",
            "SUCCESSFUL",
            None
        )
        # Store the DataFrame object to a CSV file of `data/output` directory
        df.to_csv("data/output/top_5_trending_games.csv",index=False)

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

        # Dictionary to store the transformed data
        transformed_data = {
            "app_id":                        [],
            "game_image":                    [],
            "twenty_four_hour_peak_players": [],
            "all_time_peak_players":         [],
            "current_datetime":              []
        }

        # Convert the datatype of all values for 'app_id' key to integer
        for app_id in trending_games_stats_overview["app_id"]:
            app_id = int(app_id)
            transformed_data["app_id"].append(app_id)

        # Maps all values for 'game_name' key to the dictionary
        for game_image in trending_games_stats_overview["game_image"]:
            transformed_data["game_image"].append(game_image)

        # Convert the datatype of all values for 'twenty_four_hour_peak_players'
        # key to integer
        for twenty_four_hour_peak_players in trending_games_stats_overview[
            "twenty_four_hour_peak_players"
        ]:
            twenty_four_hour_peak_players = int(twenty_four_hour_peak_players)
            transformed_data["twenty_four_hour_peak_players"].append(
                twenty_four_hour_peak_players
            )

        # Convert the datatype of all values for 'all_time_peak_players' key
        # to integer
        for all_time_peak_players in trending_games_stats_overview[
            "all_time_peak_players"
        ]:
            all_time_peak_players = int(all_time_peak_players)
            transformed_data["all_time_peak_players"].append(all_time_peak_players)

        # Convert the datatype of all values for 'current_datetime' key
        # to datetime object with specified format
        for current_datetime in trending_games_stats_overview["current_datetime"]:
            current_datetime = datetime.strptime(
                current_datetime,
                "%Y-%m-%d %H:%M:%S PST%z"
            )
            transformed_data["current_datetime"].append(current_datetime)

        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the top 5 trending games' stats "
            "overview from a JSON file.",
            "SUCCESSFUL",
            None
        )
        # Convert the transformed data to a DataFrame object and
        # store the DataFrame to a CSV file of `data/output` directory
        df = pd.DataFrame(transformed_data)
        df.to_csv("data/output/top_5_trending_games_stats_overview.csv",index=False)

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
        transformed_data = {
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
                transformed_data["app_id"].append(app_id)

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

                transformed_data["month"].append(month)

            # Convert the datatype of all values for 'avg_players' key to float
            for avg_players in historical_stats["avg_players"]:
                avg_players = float(avg_players)
                transformed_data["avg_players"].append(avg_players)

            # Handle invalid values and convert the datatype of all values for
            # 'gain' key to float
            for gain in historical_stats["gain"]:
                if str(gain) == "-":
                    gain = 0.0

                else:
                    gain = float(gain)
                transformed_data["gain"].append(gain)

            # Remove '%', handle invalid values, and convert the datatype of
            # all values for 'gain_pct' key to float
            for gain_pct in historical_stats["gain_pct"]:
                gain_pct = str(gain_pct).replace("%", "")
                if gain_pct == "-":
                    gain_pct = 0.0

                else:
                    gain_pct = float(gain_pct)
                transformed_data["gain_pct"].append(gain_pct)

            # Convert the datatype of all values for 'peak_players' key to integer
            for peak_players in historical_stats["peak_players"]:
                peak_players = int(peak_players)
                transformed_data["peak_players"].append(peak_players)

        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the top 5 trending games' historical "
            "stats from a JSON file.",
            "SUCCESSFUL",
            None
        )
        # Convert the transformed data to a DataFrame object and
        # store the DataFrame to a CSV file of `data/output` directory
        df = pd.DataFrame(transformed_data)
        df.to_csv("data/output/top_5_trending_games_historical_stats.csv",index=False)

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