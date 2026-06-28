"""
Python module to create the PostgreSQL Database and Snowflake
Database to store all the data to different data layers (bronze/raw,)
silver/stage, and gold/mart).
"""
from sqlalchemy import text

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection_to_postgres
from utils.database.connection import init_connection_to_snowflake

from logs import logger

def create_postgres_database(database_name: str) -> None:
    """
    Create new PostgreSQL Database (if still does not exist)
    to store all the data for bronze/raw and silver/stage
    data layer.

    Args:
        database_name (str): The desired name of the database.
    """
    logger.info("Establishing a connection to PostgreSQL to create new database.")
    load_dotenv()
    engine = init_connection_to_postgres(os.getenv("POSTGRES_DB_USERNAME"),
                                         os.getenv("POSTGRES_DB_PASSWORD"),
                                         os.getenv("HOST"),
                                         os.getenv("PORT"),
                                         "postgres")

    with engine.connect() as connection:
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")

        result = connection.execute(
            text("SELECT 1 from pg_database WHERE datname =:database"),
            {"database": database_name}
        )
        exists = result.fetchone()

        if not exists:
            logger.info(f"Creating database: '{database_name}'.")
            connection.execute(text(f"CREATE DATABASE {database_name};"))
            logger.info(f"Successfully created a new database: '{database_name}'.")

        else:
            logger.info(f"Database: '{database_name}' was already created.")

def create_snowflake_database(database_name: str) -> None:
    """
    Create new Snowflake Database (if still does not exist)
    to store all the data for gold/mart data layer.

    Args:
        database_name (str): The desired name of the database.
    """
    logger.info("Establishing a connection to Snowflake to create new database.")
    load_dotenv()
    conn = init_connection_to_snowflake(os.getenv("SNOWFLAKE_USERNAME"),
                                        os.getenv("SNOWFLAKE_PASSWORD"),
                                        os.getenv("SNOWFLAKE_ACCOUNT_IDENTIFIER"),
                                        "steam_charts_warehouse")

    cursor = conn.cursor()