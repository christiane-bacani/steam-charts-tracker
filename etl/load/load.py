"""
Python module to store all scraped, transformed, aggregrated, or modeleted data
from Steam Charts website to their corresponding data layers (bronze/raw, silver/stage,
and gold/mart).
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger

def load_data_to_schema(data: dict | pd.DataFrame,
                        schema_name: str,
                        table_name: str) -> None:
    """
    Load the data to a certain database schema which is an equivalent to a certain data
    layer (bronze/raw, silve/stage, and gold/mart).

    Args:
        data (dict | DataFrame): The data which can be a dictionary or DataFrame.
        schema_name (str): The name of the database schema.
        table_name: str: The name of the SQL Table.
    """
    logger.info("Establishing a connection to PostgreSQL to load the data to a table.")
    load_dotenv()
    engine = init_connection(
        os.getenv("HOST"),
        os.getenv("PORT"),
        "steam_charts",
        os.getenv("DB_USERNAME"),
        os.getenv("DB_PASSWORD")
    )

    if type(data) is dict:
        df = pd.DataFrame(data)

    else:
        df = data

    logger.info(f"Loading new data to SQL Table: '{table_name}'.")
    df.to_sql(
        table_name, con=engine, schema=schema_name, if_exists="append", index=False
    )
    logger.info(f"Successfully loaded new data to SQL table: '{table_name}'.")