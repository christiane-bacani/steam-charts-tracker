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