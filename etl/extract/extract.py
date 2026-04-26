"""
Python module to perform data extraction to all data tracked by Steam Charts.
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger

def extract_data_from_sql_table(schema_name: str, table_name: str) -> pd.DataFrame:
    """
    Extract data from different SQL tables from a certain database schema that
    corresponds to a certain data layer (bronze/raw, silver/stage, and gold/mart).

    Args:
        schema_name (str): The name of the database schema.
        table_name (str): The name of the SQL Table.

    Returns:
        DataFrame: The extracted data as a DataFrame.
    """
    logger.info(f"Extracting the data from: '{table_name}'.")
    load_dotenv()
    engine = init_connection(os.getenv("HOST"),
                             os.getenv("PORT"),
                             "steam_charts",
                             os.getenv("DB_USERNAME"),
                             os.getenv("DB_PASSWORD"))

    query = f"SELECT * FROM {schema_name}.{table_name};"
    df = pd.read_sql(query, engine)

    logger.info(f"Successfully extracted the data from: '{table_name}'.")
    return df