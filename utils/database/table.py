"""
Python module to create new SQL Tables from specific Database Schema
of PostgreSQL and Snowflake that represents a specific data layer (bronze/
raw, silver/stage, and gold/mart).
"""
from sqlalchemy import text

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection_to_postgres
from utils.database.connection import init_connection_to_snowflake

from logs import logger

def create_postgres_table_for_raw(table_name: str) -> None:
    """
    Create new PostgreSQL Table (if still does not exists).

    Args:
        table_name (str): The desired name of the database table.
    """
    logger.info("Establishing a connection to PostgreSQL to create new table.")
    load_dotenv()
    engine = init_connection_to_postgres(os.getenv("POSTGRES_DB_USERNAME"),
                                         os.getenv("POSTGRES_DB_PASSWORD"),
                                         os.getenv("HOST"),
                                         os.getenv("PORT"),
                                         "steam_charts")

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
            logger.info(f"Creating PostgreSQL Table: '{table_name}'.")
            connection.execute(text(command))
            logger.info(f"Successfully created a new PostgreSQL table: '{table_name}'.")

        else:
            logger.info(f"Table: '{table_name}' was already created.")

def create_snowflake_table_for_mart(table_name: str) -> None:
    """
    Create new Snowflake Table (if still does not exists).

    Args:
        table_name (str): The desired name of the database table.
    """
    logger.info("Establishing a connection to Snowflake to create new table.")
    load_dotenv()
    conn = init_connection_to_snowflake(os.getenv("SNOWFLAKE_USERNAME"),
                                        os.getenv("SNOWFLAKE_PASSWORD"),
                                        os.getenv("SNOWFLAKE_ACCOUNT_IDENTIFIER"),
                                        "steam_charts_warehouse",
                                        "STEAM_CHARTS",
                                        "MART")

    cursor = conn.cursor()

    cursor.execute(f"""
    SELECT COUNT(*)
    FROM
        INFORMATION_SCHEMA.TABLES
    WHERE
        TABLE_CATALOG = 'MART' AND
        TABLE_NAME = '{table_name}'
    """)
    row = cursor.fetchone()
    exists = row is not None

    if table_name == "DIM_RANK_NUMBER":
        command = """
        CREATE TABLE STEAM_CHARTS.MART.DIM_RANK_NUMBER (
        rank_number INTEGER PRIMARY KEY);
        """

    if exists:
        logger.info(f"Snowflake SQL Table: '{table_name}' was already created.")

    else:
        logger.info(f"Creating Snowflake SQL Table: '{table_name}'.")
        cursor.execute(command)
        logger.info(f"Successfully created a new Snowflake SQL Table: '{table_name}'.")