"""
Python module to create SQL tables from a specific schema that represents
a specific data storage layer.
"""
from sqlalchemy import text

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger

def create_table_for_raw_layer(table_name: str) -> None:
    """
    Create new SQL table (if still does not exist) from 'raw' schema
    to store all data.

    Args:
        table_name (str): The name of the table.
    """
    logger.info("Establishing a connection to PostgreSQL to create new table.")
    load_dotenv()
    engine = init_connection(os.getenv("HOST"),
                           os.getenv("PORT"),
                           "steam_charts",
                           os.getenv("DB_USERNAME"),
                           os.getenv("DB_PASSWORD"))

    if table_name == "top5_trending_games_raw":
        command = """
        CREATE TABLE raw.top5_trending_games_raw (
        id SERIAL PRIMARY KEY,
        app_id TEXT,
        rank TEXT,
        name TEXT,
        twenty_four_hour_change TEXT,
        current_players TEXT,
        timestamp TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'Asia/Manila'));
        """

    elif table_name == "top100_games_raw":
        command = """
        CREATE TABLE raw.top100_games_raw (
        id SERIAL PRIMARY KEY,
        app_id TEXT,
        rank TEXT,
        name TEXT,
        current_players TEXT,
        peak_players TEXT,
        hours_played TEXT,
        timestamp TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'Asia/Manila'));
        """

    elif table_name == "top10_records_raw":
        command = """
        CREATE TABLE raw.top10_records_raw (
        id SERIAL PRIMARY KEY,
        app_id TEXT,
        rank TEXT,
        name TEXT,
        peak_players TEXT,
        time TEXT,
        timestamp TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'Asia/Manila'));
        """

    else:
        raise Exception("Invalid table name!")

    with engine.connect() as connection:
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")

        result = connection.execute(
            text("""
                 SELECT 1
                 FROM information_schema.tables
                 WHERE table_schema =:schema
                 AND table_name =:table;
                 """),
                 {"schema": "raw", "table": table_name})
        exists = result.fetchone()

        if not exists:
            logger.info(f"Creating table: '{table_name}'.")
            connection.execute(text(command))
            logger.info(f"Successfully created a new table: '{table_name}'.")

        else:
            logger.info(f"Table: '{table_name}' was already created.")