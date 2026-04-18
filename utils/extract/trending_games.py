"""
Python module to store all the scraped data related to the current top 5 trending games
tracked by Steam Charts.
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connections import init_connection

from logs import logger

def load_scraped_data_to_raw_layer(scraped_data: dict,
                                   table_name: str) -> None:
    """
    Load the scraped data related to the current top 5 trending games to the data
    storage layer called 'raw'.

    Args:
        scraped_data (dict): The scraped data as a dictionary.
        table_name: str: The name of the SQL Table.
    """
    logger.info("Establishing a connection to PostgreSQL to load the data to a table.")
    load_dotenv()
    conn = init_connection(
        os.getenv("HOST"),
        os.getenv("PORT"),
        "steam_charts",
        os.getenv("DB_USERNAME"),
        os.getenv("DB_PASSWORD")
    )

    df = pd.DataFrame(scraped_data)
    df.to_sql(table_name, conn, schema="raw", if_exists="append", index=False)
    logger.info(f"Successfully loaded new data to SQL table: '{table_name}'.")

    conn.close()