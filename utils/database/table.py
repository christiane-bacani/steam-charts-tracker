"""
Python module to create database table from a certain schema.
"""
import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger

def create_table_for_raw_layer(table_name: str) -> None:
    """
    Create new table (if still does not exist) to store all
    data from a certain data storage layer.

    Args:
        table_name (str): The name of the table.
    """
    logger.info("Establishing a connection to PostgreSQL to create new table.")
    load_dotenv()
    conn = init_connection(os.getenv("HOST"),
                           os.getenv("PORT"),
                           "steam_charts",
                           os.getenv("DB_USERNAME"),
                           os.getenv("DB_PASSWORD"))
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("""SELECT 1
                   FROM information_schema.tables
                   WHERE table_schema = %s
                   AND table_name = %s""", ("raw", "top_5_trending_games"))
    exists = cursor.fetchone()

    if not exists:
        logger.info(f"Creating table: 'raw.{table_name}'.")
        cursor.execute("""CREATE TABLE top_5_trending_games (
                    app_id VARCHAR(255),
                    rank VARCHAR(255),
                    name VARCHAR(255),
                    twenty_four_hour_change VARCHAR(255),
                    current_players VARCHAR(255)
                    );""")
        logger.info(f"Successfully created a new table: 'raw.{table_name}'.")

    else:
        logger.info(f"Table: 'raw.{table_name}' was already created.")

    conn.autocommit = False
    cursor.close()
    conn.close()