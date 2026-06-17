"""
Python module to create fact tables from silver data layer that consist of all
data tracked by Steam Charts.
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger

def create_fact_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create fact table from a certain DataFrame object.

    Args:
        df (DataFrame): The DataFrame object.
        
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

    columns = list(df.columns)

    if columns == ["id",
                   "application_id",
                   "current_rank",
                   "game_name",
                   "change_pct_within_24hr",
                   "no_of_current_players","timestamp"]:
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

    elif columns == ["id",
                     "application_id",
                     "current_rank",
                     "game_name",
                     "no_of_current_players",
                     "no_of_peak_players",
                     "no_of_hours_played",
                     "timestamp"]:
        logger.info("Creating new fact table: 'fact_top_games'.")

        query = """
        SELECT
            stg.top100_games_stg.id AS id,
            mart.dim_steam_game.application_id AS application_id,
            mart.dim_rank_number.rank_number AS rank_number_id,
            stg.top100_games_stg.no_of_current_players AS no_of_current_players,
            stg.top100_games_stg.no_of_peak_players AS no_of_peak_players,
            stg.top100_games_stg.no_of_hours_played AS no_of_hours_played,
            mart.dim_timestamp.id AS timestamp_id
        FROM
            stg.top100_games_stg
        INNER JOIN
            mart.dim_steam_game
        ON
            stg.top100_games_stg.application_id = mart.dim_steam_game.application_id
        INNER JOIN
            mart.dim_rank_number
        ON
            stg.top100_games_stg.current_rank = mart.dim_rank_number.rank_number
        INNER JOIN
            mart.dim_timestamp
        ON
            stg.top100_games_stg.timestamp = mart.dim_timestamp.timestamp
        """
        fact_top_games = pd.read_sql(query, engine)

        # For unknown reason, I can't get the natural result order of rows without
        # using the PK: 'id' that's why I query it above compared to other SQL commands
        # we've made and sort it to ascending order before removing it totally for our
        # fact table.
        fact_top_games.sort_values(by="id", inplace=True)
        fact_top_games = fact_top_games[[
            "application_id",     "rank_number_id",     "no_of_current_players",
            "no_of_peak_players", "no_of_hours_played", "timestamp_id"
        ]]

        logger.info("Successfully created the new fact table: 'fact_top_games'.")
        return fact_top_games

    elif columns == ["id",
                     "application_id",
                     "current_rank",
                     "game_name",
                     "no_of_peak_players",
                     "peak_month",
                     "peak_year",
                     "timestamp"]:
        logger.info("Creating new fact table: 'fact_top_records'.")

        query = """
        SELECT
            stg.top10_records_stg.id AS id,
            mart.dim_steam_game.application_id AS application_id,
            mart.dim_rank_number.rank_number AS rank_number_id,
            stg.top10_records_stg.no_of_peak_players AS no_of_peak_players,
            mart.dim_peak_month.id AS peak_month_id,
            mart.dim_peak_year.id AS peak_year_id,
            mart.dim_timestamp.id AS timestamp_id
        FROM
            stg.top10_records_stg
        INNER JOIN
            mart.dim_steam_game
        ON
            stg.top10_records_stg.application_id = mart.dim_steam_game.application_id
        INNER JOIN
            mart.dim_rank_number
        ON
            stg.top10_records_stg.current_rank = mart.dim_rank_number.rank_number
        INNER JOIN
            mart.dim_peak_month
        ON
            stg.top10_records_stg.peak_month = mart.dim_peak_month.peak_month
        INNER JOIN
            mart.dim_peak_year
        ON
            stg.top10_records_stg.peak_year = mart.dim_peak_year.peak_year
        INNER JOIN
            mart.dim_timestamp
        ON
            stg.top10_records_stg.timestamp = mart.dim_timestamp.timestamp;
        """
        fact_top_records = pd.read_sql(query, engine)

        # For unknown reason, I can't get the natural result order of rows without
        # using the PK: 'id' that's why I query it above compared to other SQL commands
        # we've made and sort it to ascending order before removing it totally for our
        # fact table.
        fact_top_records.sort_values(by="id", inplace=True)
        fact_top_records = fact_top_records[[
            "application_id", "rank_number_id", "no_of_peak_players",
            "peak_month_id",  "peak_year_id",   "timestamp_id"
        ]]

        logger.info("Successfully created the new fact table: 'fact_top_records'.")
        return fact_top_records

    else:
        raise Exception("Invalid table to use for creating fact table!")