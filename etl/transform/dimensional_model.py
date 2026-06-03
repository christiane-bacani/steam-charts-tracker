"""
Python module to perform dimensional modeling to all data tracked by Steam Charts.
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger

def create_dim_table(column_name: str) -> None:
    """
    Create dimension tables using the given columns of different tables from
    `stg` database schema to create a dimension model for reporting and
    dashboarding queries.

    Args:
        column_name (str): The name of the column.
    """
    logger.info("Establishing a connection to PostgreSQL to transform dim columns.")
    load_dotenv()
    engine = init_connection(
        os.getenv("HOST"),
        os.getenv("PORT"),
        "steam_charts",
        os.getenv("DB_USERNAME"),
        os.getenv("DB_PASSWORD")
    )

    logger.info("Transforming the dimension column: `application_id`.")

    trending_games_app_id = pd.read_sql_table("top5_trending_games_stg",
                                              con=engine,
                                              schema="stg",
                                              columns=[column_name])
    
    logger.info("Successfully transformed the dimension column: `application_id`.")