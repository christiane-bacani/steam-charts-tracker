"""
Python module to store all scraped, transformed, aggregrated, or modeleted data
from Steam Charts website to their corresponding data layers (bronze/raw, silver/stage,
and gold/mart).
"""
import pandas as pd
from sqlalchemy import text

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

    elif schema_name == "stg":
        df.to_sql(table_name,
                    con=engine,
                    schema=schema_name,
                    if_exists="replace",
                    index=False)

    elif schema_name == "mart":
        with engine.begin() as connection:
            connection.execute(text(
                f"""ALTER TABLE mart.fact_trending_games
                    DROP CONSTRAINT IF EXISTS fk_application_id_trending_games;

                    ALTER TABLE mart.fact_trending_games
                    DROP CONSTRAINT IF EXISTS fk_rank_number_id_trending_games;

                    ALTER TABLE mart.fact_trending_games
                    DROP CONSTRAINT IF EXISTS fk_timestamp_id_trending_games;"""
            ))
        df.to_sql(table_name,
                  con=engine,
                  schema=schema_name,
                  if_exists="replace",
                  index=False)

    else:
        raise Exception("Invalid database schema name!")

    if schema_name == "raw":
        pass

    elif schema_name == "stg" and table_name == "top5_trending_games_stg":
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

                    ALTER TABLE stg.top10_records_stg
                    ALTER COLUMN peak_month TYPE VARCHAR(25)
                    USING peak_month::VARCHAR(25);

                    ALTER TABLE stg.top10_records_stg
                    ALTER COLUMN peak_year TYPE INTEGER
                    USING peak_year::INTEGER;"""
            ))

    elif schema_name == "mart" and table_name == "dim_rank_number":
        with engine.begin() as connection:
            connection.execute(text(
                f"""ALTER TABLE mart.dim_rank_number
                    ALTER COLUMN rank_number TYPE INTEGER
                    USING rank_number::INTEGER;

                    ALTER TABLE mart.dim_rank_number
                    ADD PRIMARY KEY (rank_number);"""
            ))

    elif schema_name == "mart" and table_name == "dim_steam_game":
        with engine.begin() as connection:
            connection.execute(text(
                f"""ALTER TABLE mart.dim_steam_game
                    ALTER COLUMN application_id TYPE INTEGER
                    USING application_id::INTEGER;

                    ALTER TABLE mart.dim_steam_game
                    ADD PRIMARY KEY (application_id);

                    ALTER TABLE mart.dim_steam_game
                    ALTER COLUMN game_name TYPE VARCHAR(255);"""
            ))

    elif schema_name == "mart" and table_name == "dim_timestamp":
        with engine.begin() as connection:
            connection.execute(text(
                f"""ALTER TABLE mart.dim_timestamp
                    ALTER COLUMN id TYPE INTEGER
                    USING id::INTEGER;

                    ALTER TABLE mart.dim_timestamp
                    ADD PRIMARY KEY (id);"""
            ))

    elif schema_name == "mart" and table_name == "dim_peak_month":
        with engine.begin() as connection:
            connection.execute(text(
                f"""ALTER TABLE mart.dim_peak_month
                    ALTER COLUMN id TYPE INTEGER
                    USING id::INTEGER;

                    ALTER TABLE mart.dim_peak_month
                    ADD PRIMARY KEY(id);

                    ALTER TABLE mart.dim_peak_month
                    ALTER COLUMN peak_month TYPE VARCHAR(25)
                    USING peak_month::VARCHAR(25);"""
            ))

    elif schema_name == "mart" and table_name == "dim_peak_year":
        with engine.begin() as connection:
            connection.execute(text(
                f"""ALTER TABLE mart.dim_peak_year
                    ALTER COLUMN id TYPE INTEGER
                    USING id::INTEGER;

                    ALTER TABLE mart.dim_peak_year
                    ADD PRIMARY KEY (id);

                    ALTER TABLE mart.dim_peak_year
                    ALTER COLUMN peak_year TYPE INTEGER
                    USING peak_year::INTEGER;"""
            ))

    elif schema_name == "mart" and table_name == "fact_trending_games":
        with engine.begin() as connection:
            connection.execute(text(
                f"""ALTER TABLE mart.fact_trending_games
                    ALTER COLUMN application_id TYPE INTEGER
                    USING application_id::INTEGER;

                    ALTER TABLE mart.fact_trending_games
                    ADD CONSTRAINT fk_application_id_trending_games
                    FOREIGN KEY (application_id)
                    REFERENCES mart.dim_steam_game(application_id)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE;

                    ALTER TABLE mart.fact_trending_games
                    ALTER COLUMN rank_number_id TYPE INTEGER
                    USING rank_number_id::INTEGER;

                    ALTER TABLE mart.fact_trending_games
                    ADD CONSTRAINT fk_rank_number_id_trending_games
                    FOREIGN KEY (rank_number_id)
                    REFERENCES mart.dim_rank_number(rank_number)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE;

                    ALTER TABLE mart.fact_trending_games
                    ALTER COLUMN change_pct_within_24hr TYPE DECIMAL(5, 1);

                    ALTER TABLE mart.fact_trending_games
                    ALTER COLUMN no_of_current_players TYPE INTEGER
                    USING no_of_current_players::INTEGER;

                    ALTER TABLE mart.fact_trending_games
                    ALTER COLUMN timestamp_id TYPE INTEGER
                    USING timestamp_id::INTEGER;

                    ALTER TABLE mart.fact_trending_games
                    ADD CONSTRAINT fk_timestamp_id_trending_games
                    FOREIGN KEY (timestamp_id)
                    REFERENCES mart.dim_timestamp(id)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE;"""
            ))

    elif schema_name == "mart" and table_name == "fact_top_games":
        with engine.begin() as connection:
            connection.execute(text(
                f"""ALTER TABLE mart.fact_top_games
                    ALTER COLUMN application_id TYPE INTEGER
                    USING application_id::INTEGER;

                    ALTER TABLE mart.fact_top_games
                    ADD CONSTRAINT fk_application_id_top_games
                    FOREIGN KEY (application_id)
                    REFERENCES mart.dim_steam_game(application_id)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE;

                    ALTER TABLE mart.fact_top_games
                    ALTER COLUMN rank_number_id TYPE INTEGER
                    USING rank_number_id::INTEGER;

                    ALTER TABLE mart.fact_top_games
                    ADD CONSTRAINT fk_rank_number_id_top_games
                    FOREIGN KEY (rank_number_id)
                    REFERENCES mart.dim_rank_number(rank_number);

                    ALTER TABLE mart.fact_top_games
                    ALTER COLUMN no_of_current_players TYPE INTEGER
                    USING no_of_current_players::INTEGER;"""
            ))

    else:
        raise Exception("Invalid database table name!")

    logger.info(f"Successfully loaded new data to SQL table: '{table_name}'.")