"""
Python module to perform dimensional modeling to all data tracked by Steam Charts.
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger

def integrate_dimension(dim_column: str) -> pd.DataFrame:
    """
    Integrate all the data of different dimension columns
    from different tables of `stg` database schema to create
    a unified dimension table for reporting and dashboarding
    queries.

    Args:
        dim_column (str): The name of the dimension.
    """
    logger.info("Establishing a connection to PostgreSQL to integrate dim columns..")
    load_dotenv()
    engine = init_connection(
        os.getenv("HOST"),
        os.getenv("PORT"),
        "steam_charts",
        os.getenv("DB_USERNAME"),
        os.getenv("DB_PASSWORD")
    )

    if dim_column == "game_name":
        logger.info("Integrating the data of dim column: `game_name`.")
        top5_trending_games_stg = pd.read_sql_table("top5_trending_games_stg",
                                                con=engine,
                                                schema="stg",
                                                columns=["application_id", "game_name"])
        top10_records_stg = pd.read_sql_table("top10_records_stg",
                                              con=engine,
                                              schema="stg",
                                              columns=["application_id", "game_name"])
        top100_games_stg = pd.read_sql_table("top100_games_stg",
                                             con=engine,
                                             schema="stg",
                                             columns=["application_id", "game_name"])
        dim_steam_game = pd.DataFrame(columns=["application_id", "game_name"])

        dataframes = [top5_trending_games_stg, top10_records_stg, top100_games_stg]

        for dataframe in dataframes:
            dim_steam_game = pd.concat([dim_steam_game, dataframe], ignore_index=True)

        logger.info("Successfully integrated the data of dim column: `game_name`.")
        return dim_steam_game