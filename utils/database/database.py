"""
Python module to create the database to store data for different layers.
"""
import os
from dotenv import load_dotenv

from utils.database.connections import init_connection

from logs import logger

def create_database(database_name: str) -> None:
    """
    Create new database (if still does not exist) to store
    all the data for different layers.

    Args:
        database_name (str): The database name
    """
    load_dotenv()
    conn = init_connection(os.getenv("DB_USERNAME"),
                           os.getenv("HOST"),
                           os.getenv("DB_PASSWORD"))
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute(f"SELECT 1 from pg_database WHERE datname = %s", (database_name, ))
    exists = cursor.fetchone()

    if not exists:
        logger.info(f"Creating database: '{database_name}'.")
        cursor.execute(f"CREATE DATABASE {database_name};")
        logger.info(f"Successfully created a new database: '{database_name}'.")

    conn.autocommit = False
    cursor.close()
    conn.close()