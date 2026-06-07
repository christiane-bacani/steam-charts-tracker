"""
Python module to store all scraped, transformed, aggregrated, or modeleted data
from Steam Charts website to their corresponding data layers (bronze/raw, silver/stage,
and gold/mart).
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger

def load_data_to_schema(data: dict | pd.DataFrame,
                        schema_name: str,
                        table_name: str) -> None:
    """
    Load the data to a certain database schema which is an equivalent to a certain data
    layer (bronze/raw, silver/stage, and gold/mart).

    Args:
        data (dict | DataFrame): The data which can be a dictionary or DataFrame.
        schema_name (str): The name of the database schema.
        table_name (str): The name of the SQL Table.
    """
    logger.info("Establishing a connection to PostgreSQL to load the data to a table.")
    load_dotenv()
    engine = init_connection(
        os.getenv("HOST"),
        os.getenv("PORT"),
        "steam_charts",
        os.getenv("DB_USERNAME"),
        os.getenv("DB_PASSWORD")
    )

    if type(data) is dict:
        df = pd.DataFrame(data)

    else:
        df = data

    logger.info(f"Loading new data to SQL Table: '{table_name}'.")

    if schema_name == "raw":    
        df.to_sql(table_name,
                  con=engine,
                  schema=schema_name,
                  if_exists="append",
                  index=False)

    elif schema_name == "stg" or schema_name == "mart":
        df.to_sql(table_name,
                    con=engine,
                    schema=schema_name,
                    if_exists="replace",
                    index=False)

    else:
        raise Exception("Invalid database schema name!")

    from sqlalchemy import text

    if schema_name == "stg" and table_name == "top5_trending_games_stg":
        with engine.begin() as connection:
            connection.execute(text(
                f"""ALTER TABLE stg.top5_trending_games_stg
                    ADD PRIMARY KEY (id);

                    ALTER TABLE stg.top5_trending_games_stg
                    ALTER COLUMN application_id TYPE INTEGER
                    USING application_id::INTEGER;

                    ALTER TABLE stg.top5_trending_games_stg
                    ALTER COLUMN current_rank TYPE INTEGER
                    USING current_rank::INTEGER;

                    ALTER TABLE stg.top5_trending_games_stg
                    ALTER COLUMN game_name TYPE VARCHAR(255);

                    ALTER TABLE stg.top5_trending_games_stg
                    ALTER COLUMN change_pct_within_24hr TYPE DECIMAL(5, 1);

                    ALTER TABLE stg.top5_trending_games_stg
                    ALTER COLUMN no_of_current_players TYPE INTEGER
                    USING no_of_current_players::INTEGER;"""
            ))

    elif schema_name == "stg" and table_name == "top100_games_stg":
        with engine.begin() as connection:
            connection.execute(text(
                f"""ALTER TABLE stg.top100_games_stg
                    ADD PRIMARY KEY (id);

                    ALTER TABLE stg.top100_games_stg
                    ALTER COLUMN application_id TYPE INTEGER
                    USING application_id::INTEGER;

                    ALTER TABLE stg.top100_games_stg
                    ALTER COLUMN current_rank TYPE INTEGER
                    USING current_rank::INTEGER;

                    ALTER TABLE stg.top100_games_stg
                    ALTER COLUMN game_name TYPE VARCHAR(255);

                    ALTER TABLE stg.top100_games_stg
                    ALTER COLUMN no_of_current_players TYPE INTEGER
                    USING no_of_current_players::INTEGER;

                    ALTER TABLE stg.top100_games_stg
                    ALTER COLUMN no_of_peak_players TYPE INTEGER
                    USING no_of_peak_players::INTEGER;

                    ALTER TABLE stg.top100_games_stg
                    ALTER COLUMN no_of_hours_played TYPE INTEGER;"""
            ))

    elif schema_name == "stg" and table_name == "top10_records_stg":
        with engine.begin() as connection:
            connection.execute(text(
                f"""ALTER TABLE stg.top10_records_stg
                    ADD PRIMARY KEY (id);

                    ALTER TABLE stg.top10_records_stg
                    ALTER COLUMN application_id TYPE INTEGER
                    USING application_id::INTEGER;

                    ALTER TABLE stg.top10_records_stg
                    ALTER COLUMN current_rank TYPE INTEGER
                    USING current_rank::INTEGER;

                    ALTER TABLE stg.top10_records_stg
                    ALTER COLUMN game_name TYPE VARCHAR(255);

                    ALTER TABLE stg.top10_records_stg
                    ALTER COLUMN no_of_peak_players TYPE INTEGER
                    USING no_of_peak_players::INTEGER;

                    CREATE TYPE enum_month AS ENUM (
                    'January', 'February', 'March',
                    'April',   'May',      'June',
                    'July',    'August',   'September',
                    'October', 'November', 'December');

                    ALTER TABLE stg.top10_records_stg
                    ALTER COLUMN peak_month TYPE enum_month
                    USING peak_month::enum_month;

                    ALTER TABLE stg.top10_records_stg
                    ALTER COLUMN peak_year TYPE CHAR(4)
                    USING LPAD(peak_year::TEXT, 4, '0');"""
            ))

    logger.info(f"Successfully loaded new data to SQL table: '{table_name}'.")