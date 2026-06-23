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
                    DROP CONSTRAINT IF EXISTS fk_timestamp_id_trending_games;

                    ALTER TABLE mart.fact_top_games
                    DROP CONSTRAINT IF EXISTS fk_application_id_top_games;

                    ALTER TABLE mart.fact_top_games
                    DROP CONSTRAINT IF EXISTS fk_rank_number_id_top_games;

                    ALTER TABLE mart.fact_top_games
                    DROP CONSTRAINT IF EXISTS fk_timestamp_id_top_games;

                    ALTER TABLE mart.fact_top_records
                    DROP CONSTRAINT IF EXISTS fk_application_id_top_records;

                    ALTER TABLE mart.fact_top_records
                    DROP CONSTRAINT IF EXISTS fk_rank_number_id_top_records;

                    ALTER TABLE mart.fact_top_records
                    DROP CONSTRAINT IF EXISTS fk_peak_month_id_top_records;

                    ALTER TABLE mart.fact_top_records
                    DROP CONSTRAINT IF EXISTS fk_peak_year_id_top_records;

                    ALTER TABLE mart.fact_top_records
                    DROP CONSTRAINT IF EXISTS fk_timestamp_id_top_records;"""
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
                    REFERENCES mart.dim_rank_number(rank_number)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE;

                    ALTER TABLE mart.fact_top_games
                    ALTER COLUMN no_of_current_players TYPE INTEGER
                    USING no_of_current_players::INTEGER;

                    ALTER TABLE mart.fact_top_games
                    ALTER COLUMN no_of_peak_players TYPE INTEGER
                    USING no_of_peak_players::INTEGER;

                    ALTER TABLE mart.fact_top_games
                    ALTER COLUMN no_of_hours_played TYPE INTEGER
                    USING no_of_hours_played::INTEGER;

                    ALTER TABLE mart.fact_top_games
                    ALTER COLUMN timestamp_id TYPE INTEGER
                    USING timestamp_id::INTEGER;

                    ALTER TABLE mart.fact_top_games
                    ADD CONSTRAINT fk_timestamp_id_top_games
                    FOREIGN KEY (timestamp_id)
                    REFERENCES mart.dim_timestamp(id)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE;"""
            ))

    elif schema_name == "mart" and table_name == "fact_top_records":
        with engine.begin() as connection:
            connection.execute(text(
                f"""ALTER TABLE mart.fact_top_records
                    ALTER COLUMN application_id TYPE INTEGER
                    USING application_id::INTEGER;

                    ALTER TABLE mart.fact_top_records
                    ADD CONSTRAINT fk_application_id_top_records
                    FOREIGN KEY (application_id)
                    REFERENCES mart.dim_steam_game(application_id)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE;

                    ALTER TABLE mart.fact_top_records
                    ALTER COLUMN rank_number_id TYPE INTEGER
                    USING rank_number_id::INTEGER;

                    ALTER TABLE mart.fact_top_records
                    ADD CONSTRAINT fk_rank_number_id_top_records
                    FOREIGN KEY (rank_number_id)
                    REFERENCES mart.dim_rank_number(rank_number)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE;
    
                    ALTER TABLE mart.fact_top_records
                    ALTER COLUMN no_of_peak_players TYPE INTEGER
                    USING no_of_peak_players::INTEGER;

                    ALTER TABLE mart.fact_top_records
                    ALTER COLUMN peak_month_id TYPE INTEGER
                    USING peak_month_id::INTEGER;

                    ALTER TABLE mart.fact_top_records
                    ADD CONSTRAINT fk_peak_month_id_top_records
                    FOREIGN KEY (peak_month_id)
                    REFERENCES mart.dim_peak_month(id);

                    ALTER TABLE mart.fact_top_records
                    ALTER COLUMN peak_year_id TYPE INTEGER
                    USING peak_year_id::INTEGER;

                    ALTER TABLE mart.fact_top_records
                    ADD CONSTRAINT fk_peak_year_id_top_records
                    FOREIGN KEY (peak_year_id)
                    REFERENCES mart.dim_peak_year(id);

                    ALTER TABLE mart.fact_top_records
                    ALTER COLUMN timestamp_id TYPE INTEGER
                    USING timestamp_id::INTEGER;

                    ALTER TABLE mart.fact_top_records
                    ADD CONSTRAINT fk_timestamp_id_top_records
                    FOREIGN KEY (timestamp_id)
                    REFERENCES mart.dim_timestamp(id);"""
            ))

    else:
        raise Exception("Invalid database table name!")

    logger.info(f"Successfully loaded new data to SQL table: '{table_name}'.")

def load_scraped_top5_trending_games(df: pd.DataFrame) -> None:
    """
    Load the ingested data: `top5_trending_games` to the raw data
    layer for further processing.

    Args:
        df (DataFrame): The ingested data as a DataFrame.
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

    logger.info(f"Loading new data to SQL Table: 'top5_trending_games_raw'.")
    df.to_sql("top5_trending_games_raw",
              con=engine,
              schema="raw",
              if_exists="append",
              index=False)
    logger.info(f"Successfully loaded new data to SQL table: 'top5_trending_games_raw'.")

def load_scraped_top100_games(df: pd.DataFrame) -> None:
    """
    Load the ingested data: `top100_games` to the raw data
    layer for further processing.

    Args:
        df (DataFrame): The ingested data as a DataFrame.
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

    logger.info(f"Loading new data to SQL Table: 'top100_games_raw'.")
    df.to_sql("top100_games_raw",
              con=engine,
              schema="raw",
              if_exists="append",
              index=False)
    logger.info(f"Successfully loaded new data to SQL table: 'top100_games_raw'.")

def load_scraped_top10_records(df: pd.DataFrame) -> None:
    """
    Load the ingested data: `top10_records` to the raw data
    layer for further processing.

    Args:
        df (DataFrame): The ingested data as a DataFrame.
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

    logger.info(f"Loading new data to SQL Table: 'top10_records_raw'.")
    df.to_sql("top10_records_raw",
              con=engine,
              schema="raw",
              if_exists="append",
              index=False)
    logger.info(f"Successfully loaded new data to SQL table: 'top10_records_raw'.")

def load_top5_trending_games_raw(df: pd.DataFrame) -> None:
    """
    Load the extracted data: `top5_trending_games_raw` to the stg data
    layer for further processing.

    Args:
        df (DataFrame): The extracted data as a DataFrame.
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

    logger.info(f"Loading new data to SQL Table: 'top5_trending_games_stg'.")

    logger.info("Attempting to acquire connection...")
    with engine.begin() as connection:
        logger.info("Connection acquired.")

        logger.info("Creating table: 'top5_trending_games_new'.")
        connection.execute(text(
            f"""CREATE OR REPLACE stg.top5_trending_games_new (
                    id SERIAL PRIMARY KEY,
                    application_id INTEGER,
                    current_rank INTEGER,
                    game_name VARCHAR(255),
                    change_pct_within_24hr DECIMAL(5, 1),
                    no_of_current_players INTEGER,
                    timestamp TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'Asia/Manila')
                );"""
        ))

        logger.info("Loading new data to the table: 'top5_trending_games_new'.")
        df.to_sql("top5_trending_games_new",
                  con=connection,
                  schema="stg",
                  if_exists="append",
                  index=False,
                  method="multi",
                  chunksize=1000)

        logger.info("Dropping the existing table: 'top5_trending_games_stg'.")
        connection.execute(text(
            f"""DROP TABLE IF EXISTS stg.top5_trending_games_stg;"""
        ))

        logger.info("Renaming the table: 'top5_trending_games_new' to 'top5_trending_games_stg'.")
        connection.execute(text(
           f"""ALTER TABLE stg.top5_trending_games_new
               RENAME TO stg.top5_trending_games_stg;"""
        ))
    logger.info(f"Successfully loaded new data to SQL table: 'top5_trending_games_stg'.")

def load(data: dict | pd.DataFrame) -> pd.DataFrame:
    """
    Load the extracted, transformed, and validated data
    from the raw/stg data layer to the next data layer
    for further processing.

    Args:
        data (dict | DataFrame): The ingested data as a dictionary | 
                                 The extracted, transformed, validated
                                 data as a DataFrame.
    """
    if type(data) is dict:
        columns = list(data.keys())

    elif type(data) is pd.DataFrame:
        columns = list(data.columns)

    else:
        raise Exception("The data consist of invalid datatype!")       

    if columns == ["app_id",
                   "rank",
                   "name",
                   "twenty_four_hour_change",
                   "current_players"]:
        load_scraped_top5_trending_games(pd.DataFrame(data))

    elif columns == ["app_id",
                     "rank",
                     "name",
                     "current_players",
                     "peak_players",
                     "hours_played"]:
        load_scraped_top100_games(pd.DataFrame(data))

    elif columns == ["app_id",
                     "rank",
                     "name",
                     "peak_players",
                     "time"]:
        load_scraped_top10_records(pd.DataFrame(data))

    elif columns == ["id",
                     "application_id",
                     "current_rank",
                     "game_name",
                     "change_pct_within_24hr",
                     "no_of_current_players",
                     "timestamp"]:
        load_top5_trending_games_raw(data)

    else:
        raise Exception("Invalid data to load to the target data layer!")