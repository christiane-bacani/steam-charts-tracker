"""
Python module to create PostgreSQL and Snowflake Database Schema
that represents the different data layers (bronze/raw, silver/stage,
gold/mart).
"""
from sqlalchemy import text

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection_to_postgres
from utils.database.connection import init_connection_to_snowflake

from logs import logger

def create_postgres_schema(schema_name: str) -> None:
    """
    Create new PostgreSQL Database Schema (if still does not
    exist) that consist of tables.

    Args:
        schema_name (str): The desired name of the database schema.
    """
    logger.info("Establishing a connection to PostgreSQL to create new schema.")
    load_dotenv()
    engine = init_connection_to_postgres(os.getenv("POSTGRES_DB_USERNAME"),
                                         os.getenv("POSTGRES_DB_PASSWORD"),
                                         os.getenv("HOST"),
                                         os.getenv("PORT"),
                                         "steam_charts")

    with engine.connect() as connection:
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")

        result = connection.execute(
            text("""
                 SELECT 1
                 FROM information_schema.schemata
                 WHERE schema_name =:schema
                 """),
                 {"schema": schema_name})
        exists = result.fetchone()

        if not exists:
            logger.info(f"Creating new schema: '{schema_name}'.")
            connection.execute(text(f"CREATE SCHEMA {schema_name};"))
            logger.info(f"Successfully created a new schema: '{schema_name}'.")

        else:
            logger.info(f"Schema: '{schema_name}' was already created.")

def create_snowflake_schema(schema_name: str) -> None:
    """
    Create new Snowflake Database Schema (if still does not
    exist) that consist of tables.

    Args:
        schema_name (str): The desired name of the database schema.
    """
    logger.info("Establishing a connection to Snowflake to create new schema.")
    load_dotenv()
    conn = init_connection_to_snowflake(os.getenv("SNOWFLAKE_USERNAME"),
                                        os.getenv("SNOWFLAKE_PASSWORD"),
                                        os.getenv("SNOWFLAKE_ACCOUNT_IDENTIFIER"),
                                        "steam_charts_warehouse",
                                        "STEAM_CHARTS")

    cursor = conn.cursor()

    cursor.execute(f"""
    SELECT
        SCHEMA_NAME
    FROM
        INFORMATION_SCHEMA.SCHEMATA
    WHERE
        SCHEMA_NAME = {schema_name}
    """)
    exists = cursor.fetchone()[0]

    if not exists:
        logger.info(f"Creating new Snowflake DB Schema: '{schema_name}'.")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS '{schema_name}.'")
        logger.info(f"Successfully created a new Snowflake DB Schema: '{schema_name}'.")

    else:
        logger.info(f"Snowflake Database Schema: '{schema_name}' was already created.")

    cursor.close()