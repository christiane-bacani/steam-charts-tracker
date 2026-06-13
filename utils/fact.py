"""
Python module to create fact tables from silver data layer that consist of all
data tracked by Steam Charts.
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger

def create_fact_table(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Create fact table from a certain DataFrame object.

    Args:
        df (DataFrame): The DataFrame object.
        table_name (str): The table name of the given DataFrame object.
        
    Returns:
        DataFrame: The created fact table.
    """
    logger.info("Establishing a connection to PostgreSQL to integrate dim columns.")
    load_dotenv()
    engine = init_connection(
        os.getenv("HOST"),
        os.getenv("PORT"),
        "steam_charts",
        os.getenv("DB_USERNAME"),
        os.getenv("DB_PASSWORD")
    )

    if table_name == "top5_trending_games_stg":
        logger.info("Creating new fact table: 'fact_trending_game'.")

        query = """
        SELECT
            mart.dim_steam_game.application_id AS application_id,
            mart.dim_rank_number.rank_number AS rank_number_id,
            stg.top5_trending_games_stg.change_pct_within_24hr AS change_pct_within_24hr,
            stg.top5_trending_games_stg.no_of_current_players AS no_of_current_players,
            mart.dim_timestamp.id AS timestamp_id
        FROM
            stg.top5_trending_games_stg
        INNER JOIN
            mart.dim_steam_game  
        ON
            stg.top5_trending_games_stg.application_id = mart.dim_steam_game.application_id
        INNER JOIN
            mart.dim_rank_number
        ON
            stg.top5_trending_games_stg.current_rank = mart.dim_rank_number.rank_number
        INNER JOIN
            mart.dim_timestamp
        ON
            stg.top5_trending_games_stg.timestamp = mart.dim_timestamp.timestamp
        """        
        fact_trending_games = pd.read_sql(query, engine)

        logger.info("Successfully created the new fact table: 'fact_trending_games'.")
        return fact_trending_games