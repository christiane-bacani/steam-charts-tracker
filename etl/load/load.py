"""
Python module to store all scraped, transformed, aggregrated, or modeleted data
from Steam Charts website to their corresponding data layers (bronze/raw, silver/stage,
and gold/mart).
"""
import pandas as pd
from sqlalchemy import text
from snowflake.connector.pandas_tools import write_pandas

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection_to_postgres
from utils.database.connection import init_connection_to_snowflake

from logs import logger

def load_top5_trending_games_data(df: pd.DataFrame) -> None:
    """
    Load the ingested data: `top5_trending_games` to the raw data
    layer for further processing.

    Args:
        df (DataFrame): The ingested data as a DataFrame.
    """
    logger.info("Establishing a connection to PostgreSQL to load the data to a table.")
    load_dotenv()
    engine = init_connection_to_postgres(os.getenv("POSTGRES_DB_USERNAME"),
                                         os.getenv("POSTGRES_DB_PASSWORD"),
                                         os.getenv("HOST"),
                                         os.getenv("PORT"),
                                         "steam_charts")

    logger.info(f"Loading new data to SQL Table: 'top5_trending_games_raw'.")
    df.to_sql("top5_trending_games_raw",
              con=engine,
              schema="raw",
              if_exists="append",
              index=False)
    logger.info(f"Successfully loaded new data to SQL table: 'top5_trending_games_raw'.")

def load_top100_games_data(df: pd.DataFrame) -> None:
    """
    Load the ingested data: `top100_games` to the raw data
    layer for further processing.

    Args:
        df (DataFrame): The ingested data as a DataFrame.
    """
    logger.info("Establishing a connection to PostgreSQL to load the data to a table.")
    load_dotenv()
    engine = init_connection_to_postgres(os.getenv("POSTGRES_DB_USERNAME"),
                                         os.getenv("POSTGRES_DB_PASSWORD"),
                                         os.getenv("HOST"),
                                         os.getenv("PORT"),
                                         "steam_charts")

    logger.info(f"Loading new data to SQL Table: 'top100_games_raw'.")
    df.to_sql("top100_games_raw",
              con=engine,
              schema="raw",
              if_exists="append",
              index=False)
    logger.info(f"Successfully loaded new data to SQL table: 'top100_games_raw'.")

def load_top10_records_data(df: pd.DataFrame) -> None:
    """
    Load the ingested data: `top10_records` to the raw data
    layer for further processing.

    Args:
        df (DataFrame): The ingested data as a DataFrame.
    """
    logger.info("Establishing a connection to PostgreSQL to load the data to a table.")
    load_dotenv()
    engine = init_connection_to_postgres(os.getenv("POSTGRES_DB_USERNAME"),
                                         os.getenv("POSTGRES_DB_PASSWORD"),
                                         os.getenv("HOST"),
                                         os.getenv("PORT"),
                                         "steam_charts")

    logger.info(f"Loading new data to SQL Table: 'top10_records_raw'.")
    df.to_sql("top10_records_raw",
              con=engine,
              schema="raw",
              if_exists="append",
              index=False)
    logger.info(f"Successfully loaded new data to SQL table: 'top10_records_raw'.")

def load_top5_trending_games_raw(df: pd.DataFrame) -> None:
    """
    Load the extracted data: `top5_trending_games_raw` to the
    stg data layer for further processing.

    Args:
        df (DataFrame): The extracted data as a DataFrame.
    """
    logger.info("Establishing a connection to PostgreSQL to load the data to a table.")
    load_dotenv()
    engine = init_connection_to_postgres(os.getenv("POSTGRES_DB_USERNAME"),
                                         os.getenv("POSTGRES_DB_PASSWORD"),
                                         os.getenv("HOST"),
                                         os.getenv("PORT"),
                                         "steam_charts")

    logger.info(f"Loading new data to SQL Table: 'top5_trending_games_stg'.")

    with engine.begin() as connection:
        connection.execute(text(
            f"""DROP TABLE IF EXISTS stg.top5_trending_games_new;"""
        ))

        connection.execute(text(
            f"""CREATE TABLE stg.top5_trending_games_new (
                    id SERIAL PRIMARY KEY,
                    application_id INTEGER,
                    current_rank INTEGER,
                    game_name VARCHAR(255),
                    change_pct_within_24hr DECIMAL(5, 1),
                    no_of_current_players INTEGER,
                    timestamp TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'Asia/Manila')
                );"""
        ))

        df.to_sql("top5_trending_games_new",
                  con=connection,
                  schema="stg",
                  if_exists="append",
                  index=False,
                  method="multi",
                  chunksize=1000)

        connection.execute(text(
            f"""DROP TABLE IF EXISTS stg.top5_trending_games_stg;"""
        ))

        connection.execute(text(
           f"""ALTER TABLE stg.top5_trending_games_new
               RENAME TO top5_trending_games_stg;"""
        ))
    logger.info(f"Successfully loaded new data to SQL table: 'top5_trending_games_stg'.")

def load_top100_games_raw(df: pd.DataFrame) -> None:
    """
    Load the extracted data: `top100_games_raw` to the
    stg data layer for further processing.

    Args:
        df (DataFrame): The extracted data as a DataFrame.
    """
    logger.info("Establishing a connection to PostgreSQL to load the data to a table.")
    load_dotenv()
    engine = init_connection_to_postgres(os.getenv("POSTGRES_DB_USERNAME"),
                                         os.getenv("POSTGRES_DB_PASSWORD"),
                                         os.getenv("HOST"),
                                         os.getenv("PORT"),
                                         "steam_charts")

    logger.info(f"Loading new data to SQL Table: 'top100_games_stg'.")

    with engine.begin() as connection:
        connection.execute(text(
            f"""DROP TABLE IF EXISTS stg.top100_games_new;"""
        ))

        connection.execute(text(
            f"""CREATE TABLE stg.top100_games_new (
                    id SERIAL PRIMARY KEY,
                    application_id INTEGER,
                    current_rank INTEGER,
                    game_name VARCHAR(255),
                    no_of_current_players INTEGER,
                    no_of_peak_players INTEGER,
                    no_of_hours_played INTEGER,
                    timestamp TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'Asia/Manila')
                );"""
        ))

        df.to_sql("top100_games_new",
                  con=connection,
                  schema="stg",
                  if_exists="append",
                  index=False,
                  method="multi",
                  chunksize=1000)

        connection.execute(text(
            f"""DROP TABLE IF EXISTS stg.top100_games_stg;"""
        ))

        connection.execute(text(
           f"""ALTER TABLE stg.top100_games_new
               RENAME TO top100_games_stg;"""
        ))
    logger.info(f"Successfully loaded new data to SQL table: 'top100_games_stg'.")

def load_top10_records_raw(df: pd.DataFrame) -> None:
    """
    Load the extracted data: `top10_records_raw` to the
    stg data layer for further processing.

    Args:
        df (DataFrame): The extracted data as a DataFrame.
    """
    logger.info("Establishing a connection to PostgreSQL to load the data to a table.")
    load_dotenv()
    engine = init_connection_to_postgres(os.getenv("POSTGRES_DB_USERNAME"),
                                         os.getenv("POSTGRES_DB_PASSWORD"),
                                         os.getenv("HOST"),
                                         os.getenv("PORT"),
                                         "steam_charts")

    logger.info(f"Loading new data to SQL Table: 'top10_records_stg'.")

    with engine.begin() as connection:
        connection.execute(text(
            f"""DROP TABLE IF EXISTS stg.top10_records_new;"""
        ))

        connection.execute(text(
            f"""CREATE TABLE stg.top10_records_new (
                    id SERIAL PRIMARY KEY,
                    application_id INTEGER,
                    current_rank INTEGER,
                    game_name VARCHAR(255),
                    no_of_peak_players INTEGER,
                    peak_month VARCHAR(25),
                    peak_year INTEGER,
                    timestamp TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'Asia/Manila')
                );"""
        ))

        df.to_sql("top10_records_new",
                  con=connection,
                  schema="stg",
                  if_exists="append",
                  index=False,
                  method="multi",
                  chunksize=1000)

        connection.execute(text(
            f"""DROP TABLE IF EXISTS stg.top10_records_stg;"""
        ))

        connection.execute(text(
           f"""ALTER TABLE stg.top10_records_new
               RENAME TO top10_records_stg;"""
        ))
    logger.info(f"Successfully loaded new data to SQL table: 'top10_records_stg'.")

def load_dim_rank_number(df: pd.DataFrame) -> None:
    """
    Load the dimension data: `DIM_RANK_NUMBER` to the
    mart data layer (Snowflake Data Warehouse) to
    perform data analysis.

    Args:
        df (DataFrame): The dimension data as a DataFrame.
    """
    logger.info("Establishing a connection to Snowflake to load the data to a table.")
    load_dotenv()
    conn = init_connection_to_snowflake(os.getenv("SNOWFLAKE_USERNAME"),
                                        os.getenv("SNOWFLAKE_PASSWORD"),
                                        os.getenv("SNOWFLAKE_ACCOUNT_IDENTIFIER"),
                                        "steam_charts_warehouse",
                                        "STEAM_CHARTS",
                                        "MART")

    cursor = conn.cursor()
    
    logger.info(f"Loading new data to SQL Table: 'DIM_RANK_NUMBER'.")

    cursor.execute("""
    CREATE OR REPLACE TABLE STEAM_CHARTS.MART.TEMP_DIM_RANK_NUMBER (
    RANK_NUMBER INTEGER PRIMARY KEY);
    """)
    write_pandas(conn=conn,
                 df=df,
                 warehouse="STEAM_CHARTS_WAREHOUSE",
                 database="STEAM_CHARTS",
                 schema="MART",
                 table_name="TEMP_DIM_RANK_NUMBER",
                 auto_create_table=False,
                 overwrite=True)
    cursor.execute("""
    DROP TABLE IF EXISTS STEAM_CHARTS.MART.DIM_RANK_NUMBER;
    """)
    cursor.execute("""
    ALTER TABLE STEAM_CHARTS.MART.TEMP_DIM_RANK_NUMBER
    RENAME TO STEAM_CHARTS.MART.DIM_RANK_NUMBER;
    """)

    logger.info(f"Successfully loaded new data to SQL table: 'DIM_RANK_NUMBER'.")

def load(data: dict | pd.DataFrame) -> pd.DataFrame:
    """
    Load the ingested, extracted, transformed, and
    validated data from the website source or raw/stg
    data layer to the next data layer for further processing.

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
        load_top5_trending_games_data(pd.DataFrame(data))

    elif columns == ["app_id",
                     "rank",
                     "name",
                     "current_players",
                     "peak_players",
                     "hours_played"]:
        load_top100_games_data(pd.DataFrame(data))

    elif columns == ["app_id",
                     "rank",
                     "name",
                     "peak_players",
                     "time"]:
        load_top10_records_data(pd.DataFrame(data))

    elif columns == ["id",
                     "application_id",
                     "current_rank",
                     "game_name",
                     "change_pct_within_24hr",
                     "no_of_current_players",
                     "timestamp"]:
        load_top5_trending_games_raw(data)

    elif columns == ["id",
                     "application_id",
                     "current_rank",
                     "game_name",
                     "no_of_current_players",
                     "no_of_peak_players",
                     "no_of_hours_played",
                     "timestamp"]:
        return load_top100_games_raw(data)

    elif columns == ['id',
                     'application_id',
                     'current_rank',
                     'game_name',
                     'no_of_peak_players',
                     'peak_month',
                     'peak_year',
                     'timestamp']:
        return load_top10_records_raw(data)

    elif columns == ["RANK_NUMBER"]:
        return load_dim_rank_number(data)

    else:
        raise Exception("Invalid data to load to the target data layer!")