"""
Python module to create dimension tables from silver data layer that consist of all
data tracked by Steam Charts.
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection_to_postgres

from logs import logger

def create_dim_rank_number(top5_trending_games_stg: pd.DataFrame,
                           top100_games_stg: pd.DataFrame,
                           top10_records_stg: pd.DataFrame) -> pd.DataFrame:
    """
    Create the dimension table: `DIM_RANK_NUMBER` using different tables
    of `stg` database schema.

    Args:
        top5_trending_games_stg (DataFrame): The top 5 trending games as a DataFrame.
        top100_games_stg (DataFrame): The top 100 games as a DataFrame.
        top10_records_stg (DataFrame): The top 10 records as a DataFrame.

    Returns:
        DataFrame: The created dimension table: `DIM_RANK_NUMBER`.
    """
    logger.info(f"Creating new dimension table: 'DIM_RANK_NUMBER'.")

def create_dimension_table(dim_column: str) -> pd.DataFrame:
    """
    Create dimension table from different different columns
    or a certain column from a DataFrame object that are located
    from the `stg` database schema.

    Args:
        dim_column (str): The column that should be a dimension table.

    Returns:
        DataFrame: The created dimension table.
    """
    load_dotenv()
    engine = init_connection_to_postgres(os.getenv("POSTGRES_DB_USERNAME"),
                                         os.getenv("POSTGRES_DB_PASSWORD"),
                                         os.getenv("HOST"),
                                         os.getenv("PORT"),
                                         "steam_charts")

    if dim_column == "current_rank":
        logger.info("Creating new dimension table: 'dim_rank_number'.")

        dim_rank_number = pd.DataFrame(columns=["rank_number"])
        dim_rank_number["rank_number"] = range(1, 101)

        logger.info("Successfully created the new dimension table: 'dim_rank_number'.")
        return dim_rank_number

    elif dim_column == "game_name":
        logger.info("Creating new dimension table: 'dim_steam_game'.")

        trending_games = pd.read_sql_table("top5_trending_games_stg",
                                           con=engine,
                                           schema="stg",
                                           columns=["application_id",
                                                    "game_name"])
        top_records = pd.read_sql_table("top10_records_stg",
                                        con=engine,
                                        schema="stg",
                                        columns=["application_id",
                                                 "game_name"])
        top_games  = pd.read_sql_table("top100_games_stg",
                                       con=engine,
                                       schema="stg",
                                       columns=["application_id",
                                                "game_name"])
        dim_steam_game = pd.DataFrame(columns=[
            "application_id", "game_name"
        ])

        dataframes = [trending_games, top_games, top_records]

        for dataframe in dataframes:
            dim_steam_game = pd.concat([dim_steam_game, dataframe], ignore_index=True)

        logger.info("Successfully created the new dimension table: 'dim_steam_game'.")
        return dim_steam_game

    elif dim_column == "timestamp":
        logger.info("Creating new dimension table: 'dim_timestamp'.")

        trending_games_timestamp = pd.read_sql_table("top5_trending_games_stg",
                                                con=engine,
                                                schema="stg",
                                                columns=["timestamp"])
        top_records_timestamp = pd.read_sql_table("top10_records_stg",
                                              con=engine,
                                              schema="stg",
                                              columns=["timestamp"])
        top_games_timestamp  = pd.read_sql_table("top100_games_stg",
                                             con=engine,
                                             schema="stg",
                                             columns=["timestamp"])

        dim_timestamp = pd.DataFrame(columns=["timestamp"])
    
        dataframes = [
            trending_games_timestamp, top_records_timestamp, top_games_timestamp
        ]
    
        for dataframe in dataframes:
            dim_timestamp = pd.concat([dim_timestamp, dataframe], ignore_index=True)

        logger.info("Successfully created the new dimension table: 'dim_timestamp'.")
        return dim_timestamp

    elif dim_column == "peak_month":
        logger.info("Creating new dimension table: 'dim_peak_month'.")

        dim_peak_month = pd.DataFrame({
            "peak_month": [
                "January", "February", "March",
                "April",   "May",      "June",
                "July",    "August",   "September",
                "October", "November", "December"
            ]
        })

        logger.info("Successfully created the new dimension table: 'dim_peak_month'.")
        return dim_peak_month

    elif dim_column == "peak_year":
        logger.info("Creating new dimension table: 'dim_peak_year'.")

        dim_peak_year = pd.read_sql_table("top10_records_stg",
                                          con=engine,
                                          schema="stg",
                                          columns=["peak_year"])

        logger.info("Successfully created the new dimension table: 'dim_peak_year'.")
        return dim_peak_year

    else:
        raise Exception("Invalid dimension column name!")