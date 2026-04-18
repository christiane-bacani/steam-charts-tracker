"""
Python module to create database schema that consist of SQL tables per data layer.
"""
import os
from dotenv import load_dotenv

from utils.database.connections import init_connection

from logs import logger

def create_schema(schema_name: str) -> None:
    """
    Create new schema (if still does not exist) to store all
    data data for different layers.

    Args:
        schema_name (str): The name of the schema.
    """
    logger.info("Establishing a connection to PostgreSQL to create new schema.")
    load_dotenv()
    conn = init_connection(os.getenv("HOST"),
                           os.getenv("PORT"),
                           "steam_charts",
                           os.getenv("DB_USERNAME"),
                           os.getenv("DB_PASSWORD"))
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("""SELECT 1
                   FROM information_schema.schemata
                   WHERE schema_name = %s""", (schema_name, ))
    exists = cursor.fetchone()

    if not exists:
        logger.info(f"Creating schema: '{schema_name}'.")
        cursor.execute(f"CREATE SCHEMA {schema_name};")
        logger.info(f"Successfully created a new schema: '{schema_name}'.")

    else:
        logger.info(f"Schema: '{schema_name}' was already created.")

    conn.autocommit = False
    cursor.close()
    conn.close()