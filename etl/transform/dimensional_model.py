"""
Python module to perform dimensional modeling to all data tracked by Steam Charts.
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger

def transform_dim_column(column_name: str) -> None:
    """
    Transform dimension columns from tables of the silver data layer to
    create dimension tables that has business-ready structure using the given
    dimension columns.

    Args:
        table_name (str): The name of the table.
        column_name (str): The name of the dimension column.
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
    print(trending_games_app_id)

    logger.info("Successfully transformed the dimension column: `application_id`.")