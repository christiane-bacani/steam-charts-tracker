"""
Python module to perform data transformation to all extracted data related
to the current top 5 trending games tracked by Steam Charts.
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger

def extract_top10_games_raw() -> pd.DataFrame:
    """
    Extract the data from the table `top10_games_raw` of the raw
    data storage layer.
    """
    logger.info("Extracting data from the table: 'top10_games_raw'.")
    load_dotenv()
    engine = init_connection(os.getenv("HOST"),
                             os.getenv("PORT"),
                             "steam_charts",
                             os.getenv("DB_USERNAME"),
                             os.getenv("DB_PASSWORD"))

    query = "SELECT * FROM raw.top10_games_raw;"
    df = pd.read_sql(query, engine)

    logger.info("Successfully extracted the data from the table: 'top10_games_raw'.")
    return df