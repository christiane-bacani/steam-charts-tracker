"""
Python module to create database schema for different data storage layer.
"""
from sqlalchemy import text

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger

def create_schema(schema_name: str) -> None:
    """
    Create new schema (if still does not exist) that consist of SQL tables.

    Args:
        schema_name (str): The name of the schema.
    """
    logger.info("Establishing a connection to PostgreSQL to create new schema.")
    load_dotenv()
    engine = init_connection(os.getenv("HOST"),
                           os.getenv("PORT"),
                           "steam_charts",
                           os.getenv("DB_USERNAME"),
                           os.getenv("DB_PASSWORD"))

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
            logger.info(f"Creating schema: '{schema_name}'.")
            connection.execute(text(f"CREATE SCHEMA {schema_name};"))
            logger.info(f"Successfully created a new schema: '{schema_name}'.")

        else:
            logger.info(f"Schema: '{schema_name}' was already created.")