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

    with engine.connect() as connection:
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")

        result = connection.execute(
            text(f"""
                 SELECT 1
                 FROM information_schema.tables
                 WHERE table_schema =:schema
                 AND table_name =:table;
            """),
            {"schema": "raw", "table": table_name}
        )
        exists = result.fetchone()

        if not exists:
            logger.info(f"Creating table: '{table_name}'.")
            connection.execute(
                text("""
                    CREATE TABLE raw.top5_trending_games_raw (
                    app_id VARCHAR(255),
                    rank VARCHAR(255),
                    name VARCHAR(255),
                    twenty_four_hour_change VARCHAR(255),
                    currennt_players VARCHAR(255)
                    )
            """)
            )
            logger.info(f"Successfully created a new table: '{table_name}'.")

        else:
            logger.info(f"Table: '{table_name}' was already created.")