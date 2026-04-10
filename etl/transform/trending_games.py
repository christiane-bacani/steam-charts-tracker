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

def transform_trending_games_stats_overview(filepath: str) -> None:
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

        df = pd.DataFrame(trending_games_stats_overview)

        # Convert the datatype of all values for 'app_id' key to integer
        df["app_id"] = pd.to_numeric(df["app_id"], errors="raise")

        # Convert the datatype of all values for 'twenty_four_hour_peak_players'
        # key to integer
        df["twenty_four_hour_peak_players"] = pd.to_numeric(
            df["twenty_four_hour_peak_players"],
            errors="coerce"
        )

        # Convert the datatype of all values for 'all_time_peak_players' key
        # to integer
        df["all_time_peak_players"] = pd.to_numeric(
            df["all_time_peak_players"],
            errors="coerce"
        )

        # Convert the datatype of all values for 'current_datetime' key
        # to datetime object with specified format
        df["current_datetime"] = pd.to_datetime(
            df["current_datetime"],
            errors="raise",
            format="%Y-%m-%d %H:%M:%S PST%z"
        )

        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the top 5 trending games' stats "
            "overview from a JSON file.",
            "SUCCESSFUL",
            None
        )
        # Store the DataFrame object to a CSV file of `data/output` directory
        df.to_csv("data/output/top_5_trending_games_stats_overview.csv", index=False)

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

def transform_trending_games_historical_stats(filepath: str) -> None:
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

        data = {
            "app_id":       [],
            "month":        [],
            "avg_players":  [],
            "gain":         [],
            "gain_pct":     [],
            "peak_players": []
        }

        # Flattened the nested JSON objects for easier parsing of DataFrame
        for app_id, historical_stats in trending_games_historical_stats.items():
            for month in historical_stats["month"]:
                data["app_id"].append(app_id)
                data["month"].append(month)

            for avg_players in historical_stats["avg_players"]:
                data["avg_players"].append(avg_players)

            for gain in historical_stats["gain"]:
                data["gain"].append(gain)

            for gain_pct in historical_stats["gain_pct"]:
                data["gain_pct"].append(gain_pct)

            for peak_players in historical_stats["peak_players"]:
                data["peak_players"].append(peak_players)

        df = pd.DataFrame(data)

        # Convert the datatype of all values for 'app_id' key to integer
        df["app_id"] = pd.to_numeric(df["app_id"], errors="raise")

        # Remove whitespaces and handle invalid values for 'month' key by
        # providing proper month and year format as a replacement
        month_mappings = {1: "January",  2: "February",  3: "March",
                          4: "April",    5: "May",       6: "June",
                          7: "July",     8: "August",    9: "September",
                          10: "October", 11: "November", 12: "December"}
        current_month = month_mappings[datetime.now().month]
        current_year = datetime.now().year
        df["month"] = df["month"].str.strip()
        df["month"] = df["month"].str.replace(
            "Last 30 Days",
            f"{current_month} {current_year}"
        )

        # Convert the datatype of all values for 'avg_players' key to float
        df["avg_players"] = pd.to_numeric(df["avg_players"], errors="coerce")

        # Handle invalid values and convert the datatype of all values for
        # 'gain' key to float
        df["gain"] = df["gain"].str.replace("+", "")
        df["gain"] = pd.to_numeric(df["gain"], errors="coerce")

        # Remove '%', handle invalid values, and convert the datatype of
        # all values for 'gain_pct' key to float
        df["gain_pct"] = df["gain_pct"].str.replace("%", "").str.replace("+", "")
        df["gain_pct"] = pd.to_numeric(df["gain_pct"], errors="coerce")

        # Convert the datatype of all values for 'peak_players' key to integer
        df["peak_players"] = pd.to_numeric(df["peak_players"], errors="coerce")

        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the top 5 trending games' historical "
            "stats from a JSON file.",
            "SUCCESSFUL",
            None
        )
        # Store the DataFrame object to a CSV file of `data/output` directory
        df.to_csv("data/output/top_5_trending_games_historical_stats.csv", index=False)

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