"""
Python module to transform the extracted data of the current top 10 records
from a JSON file.
"""
import json
import pandas as pd
from datetime import datetime

from logs.etl_pipeline_logs import provide_logs

def transform_top_10_records(filepath: str) -> None:
    """
    Transform the extracted data of the current top 10 records from a JSON file.

    Args:
        filepath (str): The filepath of a JSON file
    """
    try:
        # Parse the scraped data from a JSON file to perform data transformation
        with open(filepath, "r") as file:
            top_10_records = json.load(file)

        df = pd.DataFrame(top_10_records)

        # Remove '.' and convert the datatype of all values for 'rank' key
        # to integer
        df["rank"] = df["rank"].str.replace(".", "")
        df["rank"] = pd.to_numeric(df["rank"], errors="raise")

        # Convert the datatype of all values for 'app_id' key to integer
        df["app_id"] = pd.to_numeric(df["app_id"], errors="coerce")

        # Remove leading and trailing whitespaces of all values for 'game_name' key
        df["game_name"] = df["game_name"].str.strip()

        # Convert the datatype of all values for 'peak_players' key to integer
        df["peak_players"] = pd.to_numeric(df["peak_players"], errors="coerce")

        # Convert the datatype to a datetime object and provide proper month and
        # and year format for representing all values for 'time' key
        month_mappings = {1: "January",  2: "February",  3: "March",
                          4: "April",    5: "May",       6: "June",
                          7: "July",     8: "August",    9: "September",
                          10: "October", 11: "November", 12: "December"}
        df["time"] = pd.to_datetime(
            df["time"],
            errors="raise",
            format="%Y-%m-%dT%H:%M:%SZ"
        )
        year = df["time"].dt.year
        month = month_mappings[df["time"].dt.month]
        df["time"] = df["time"].str.replace(df["time"], f"{month} {year}")

        # Convert the datatype of all values for 'current_datetime' key
        # to datetime object with specified format
        df["current_datetime"] = pd.to_datetime(
            df["current_datetime"],
            errors="raise",
            format="%Y-%m-%d %H:%M:%S PST%z"
        )

        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the current top 10 records "
            "from a JSON file.",
            "FAILED",
            f"Filename: '{filepath}' is invalid for parsing the extracted data "
            "of the current top 10 records from a JSON file to perform data "
            "transformation."
        )
        # Store the DataFrame object to a CSV file of `data/output` directory
        df.to_csv("data/output/top_10_records.csv", index=False)

    except FileNotFoundError:
        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the current top 10 records "
            "from a JSON file.",
            "FAILED",
            f"Filename: '{filepath}' is invalid for parsing the extracted data "
            "of the current top 10 records from a JSON file to perform data "
            "transformation."
        )
        raise FileNotFoundError("The given filename for parsing the extracted data "
                                "of the current top 10 records from a JSON file to "
                                "perform data transformation is invalid!")

def transform_top_records_stats_overview(filepath: str) -> None:
    """
    Transform the extracted data of the current top 10 records' stats overview
    from a JSON file.

    Args:
        filepath (str): The filepath of a JSON file
    """
    try:
        with open(filepath, "r") as file:
            top_records_stats_overview = json.load(file)

        df = pd.DataFrame(top_records_stats_overview)

        # Convert the datatype of all values for 'app_id' key to integer
        df["app_id"] = pd.to_numeric(df["app_id"], errors="raise")

        # Convert the datatype of all values for 'twenty_four_hour_peak_players' key
        # to integer
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
            "Transform the extracted data of the current top 10 records' stats "
            "overview from a JSON file.",
            "SUCCESSFUL",
            None
        )
        # Store the DataFrame object to a CSV file of `data/output` directory
        df.to_csv("data/output/top_10_records_stats_overview.csv", index=False)

    except FileNotFoundError:        
        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the current top 10 records' stats "
            "overview from a JSON file.",
            "FAILED",
            f"Filename: '{filepath}' is invalid for parsing the extracted data "
            "of the current top 10 records' stats overview from a JSON file to "
            "perform data transformation."
        )
        raise FileNotFoundError("The given filename for parsing the extracted data "
                                "of the current top 10 records' stats overview from "
                                "a JSON file to perform data transformation is "
                                "invalid!")

def transform_top_records_historical_stats(filepath: str) -> None:
    """
    Transform the extracted data of the current top 10 records' historical stats from
    a JSON file.

    Args:
        filepath (str): The filepath of a JSON file
    """
    try:
        # Parse the scraped data from a JSON file to perform data transformation
        with open(filepath, "r") as file:
            top_records_historical_stats: dict[str, dict] = json.load(file)

        data = {
            "app_id":       [],
            "month":        [],
            "avg_players":  [],
            "gain":         [],
            "gain_pct":     [],
            "peak_players": [],
        }

        # Flattened the nested JSON objects for easier parsing of DataFrame
        for app_id, historical_stats in top_records_historical_stats:
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

        # Remove leading and trailing whitespaces of all values for 'month' key
        df["month"] = df["month"].str.strip()

        # Convert the datatype of all values for 'avg_players' key to float
        df["avg_players"] = pd.to_numeric(df["avg_players"], errors="coerce")

        # Convert the datatype of all values for 'gain' key to float
        df["gain"] = pd.to_numeric(df["gain"], errors="coerce")

        # Remove '%' and '+' and convert the datatype of all values for 'gain_pct'
        # key to float
        df["gain_pct"] = df["gain_pct"].str.replace("%", "").str.replace("+", "")
        df["gain_pct"] = pd.to_numeric(df["gain_pct"], errors="coerce")

        # Convert the datatype of all values for 'peak_players' key to integer
        df["peak_players"] = pd.to_numeric(df["peak_players"], errors="coerce")

        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the current top 10 records' historical "
            "stats from a JSON file.",
            "SUCCESSFUL",
            None
        )
        # Store the DataFrame object to a CSV file of `data/output` directory
        df.to_csv("data/output/top_records_historical_stats.csv", index=False)

    except FileNotFoundError:
        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the current top 10 records' historical "
            "stats from a JSON file.",
            "FAILED",
            f"Filename: '{filepath}' is invalid for parsing the extracted data "
            "of the current top 10 records' historical stats from a JSON file to "
            "perform data transformation."
        )
        raise FileNotFoundError("The given filename for parsing the extracted data "
                                "of the current top 10 records' historical stats from "
                                "a JSON file to perform data transformation is "
                                "invalid!")