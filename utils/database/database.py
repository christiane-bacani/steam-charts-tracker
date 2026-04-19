"""
Python module to create the database to store data for different data storage layers.
"""
from sqlalchemy import text

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger

def create_database(database_name: str) -> None:
    """
    Create new database (if still does not exist) to store
    all the data for different data storage layers.

    Args:
        database_name (str): The name of the database.
    """
    logger.info("Establishing a connection to PostgreSQL to create new database.")
    load_dotenv()
    engine = init_connection(os.getenv("HOST"),
                           os.getenv("PORT"),
                           "postgres",
                           os.getenv("DB_USERNAME"),
                           os.getenv("DB_PASSWORD"))

    with engine.connect() as connection:
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")

        result = connection.execute(
            text(f"SELECT 1 from pg_database WHERE datname =:name"),
            {"name": database_name}
        )
        exists = result.fetchone()

        if not exists:
            logger.info(f"Creating database: '{database_name}'.")
            connection.execute(text(f"CREATE DATABASE {database_name};"))
            logger.info(f"Successfully created a new database: '{database_name}'.")

        else:
            logger.info(f"Database: '{database_name}' was already created.")