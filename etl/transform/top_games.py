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

        # TODO: Add transformation logic here...

    except FileNotFoundError:
        provide_logs(
            "TRANSFORM",
            "Transform the extracted data of the current top 10 games "
            "(by current players) from a JSON file.",
            "FAILED",
            f"Filename: '{filepath}' is invalid for parsing the extracted data "
            "of the top 10 games (by current players) from a JSON file."
        )
        raise FileNotFoundError("The given filename for parsing the extracted data "
                                "of the top 5 trending games from a JSON file is "
                                "invalid!")