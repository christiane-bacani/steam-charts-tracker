"""
Python module to create dimension tables from silver data layer that consist of all
data tracked by Steam Charts.
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger

def create_dimension_table(dim_column: str) -> pd.DataFrame:
    """
    Create dimension table from different different columns
    or a certain column from a DataFrame object that are located
    from the `stg` database schema.

    Args:
        dim_column (str): The column that should be a dimension table.

    Returns:
        DataFrame: The created dimensio table.
    """
    load_dotenv()
    engine = init_connection(
        os.getenv("HOST"),
        os.getenv("PORT"),
        "steam_charts",
        os.getenv("DB_USERNAME"),
        os.getenv("DB_PASSWORD")
    )

    if dim_column == "current_rank":
        logger.info("Integrating the data of dim column: `current_rank`.")

        dim_rank_number = pd.DataFrame(columns=["rank_number"])
        dim_rank_number["rank_number"] = range(1, 101)

        logger.info(f"Successfully integrated the data of dim column: `current_rank`.")
        return dim_rank_number

    elif dim_column == "game_name":
        logger.info("Integrating the data of dim column: `game_name`.")

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

        logger.info(f"Successfully integrated the data of dim column: `game_name`.")
        return dim_steam_game

    elif dim_column == "timestamp":
        logger.info("Integrating the data of dim column: `timestamp`.")

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

        logger.info("Successfully integrated the data of dim column: `timestamp`.")
        return dim_timestamp

    elif dim_column == "peak_month":
        logger.info("Integrating the data of dim column: `peak_month`.")

        dim_peak_month = pd.DataFrame({
            "peak_month": [
                "January", "February", "March",
                "April",   "May",      "June",
                "July",    "August",   "September",
                "October", "November", "December"
            ]
        })

        logger.info("Successfully integrated the data of dim column: `peak_month`.")
        return dim_peak_month

    elif dim_column == "peak_year":
        logger.info("Integrating the data of dim column: `peak_year`.")

        dim_peak_year = pd.read_sql_table("top10_records_stg",
                                          con=engine,
                                          schema="stg",
                                          columns=["peak_year"])

        logger.info("Successfully integrated the data of dim column: `peak_year`.")
        return dim_peak_year

    else:
        raise Exception("Invalid dimension column name!")