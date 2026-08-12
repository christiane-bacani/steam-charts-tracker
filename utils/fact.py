"""
Python module to create fact tables from silver data layer that consist of all
data tracked by Steam Charts.
"""
import pandas as pd

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection_to_postgres
from utils.database.connection import init_connection_to_snowflake

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
    engine = init_connection_to_postgres(os.getenv("POSTGRES_DB_USERNAME"),
                                         os.getenv("POSTGRES_DB_PASSWORD"),
                                         os.getenv("HOST"),
                                         os.getenv("PORT"),
                                         "steam_charts")

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
            stg.top5_trending_games_stg.timestamp = mart.dim_timestamp.timestamp;
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

def create_fact_trending_games() -> pd.DataFrame:
    """
    Create the fact table: `FACT_TRENDING_GAMES` using the table
    `top5_trending_games_stg` of `stg` database schema and reference
    the dimension data using the dimension tables.

    Returns:
        DataFrame: The created dimension table: `FACT_TRENDING_GAMES`.
    """
    logger.info("Creating new fact table: `FACT_TRENDING_GAMES`.")

    load_dotenv()

    logger.info("Establishing a connection to PostgreSQL to create a new fact table.")
    engine = init_connection_to_postgres(os.getenv("POSTGRES_DB_USERNAME"),
                                         os.getenv("POSTGRES_DB_PASSWORD"),
                                         os.getenv("HOST"),
                                         os.getenv("PORT"),
                                         "steam_charts")

    logger.info("Establishing a connection to Snowflake to create a new fact table.")
    conn = init_connection_to_snowflake(os.getenv("SNOWFLAKE_USERNAME"),
                                        os.getenv("SNOWFLAKE_PASSWORD"),
                                        os.getenv("SNOWFLAKE_ACCOUNT_IDENTIFIER"),
                                        "steam_charts_warehouse",
                                        "STEAM_CHARTS",
                                        "MART")
    cursor = conn.cursor()

    query = """
    SELECT
        stg.top5_trending_games_stg.id,
        stg.top5_trending_games_stg.application_id,
        stg.top5_trending_games_stg.current_rank,
        stg.top5_trending_games_stg.change_pct_within_24hr,
        stg.top5_trending_games_stg.no_of_current_players,
        stg.top5_trending_games_stg.timestamp
    FROM
        stg.top5_trending_games_stg
    """
    top5_trending_games_stg = pd.read_sql(query, engine)

    cursor.execute("""
    SELECT
        APPLICATION_ID,
        GAME_NAME
    FROM
        STEAM_CHARTS.MART.DIM_STEAM_GAME;
    """)
    dim_steam_game = cursor.fetch_pandas_all()

    cursor.execute("""
    SELECT
        RANK_NUMBER
    FROM
        STEAM_CHARTS.MART.DIM_RANK_NUMBER;
    """)
    dim_rank_number = cursor.fetch_pandas_all()

    cursor.execute("""
    SELECT
        ID,
        TIMESTAMP
    FROM
        STEAM_CHARTS.MART.DIM_TIMESTAMP;
    """)
    dim_timestamp = cursor.fetch_pandas_all()

    # Normalize column names to lowercase so the merges line up cleanly,
    # since Snowflake returns uppercase column names by default
    dim_steam_game.columns = [column.lower() for column in dim_steam_game.columns]
    dim_rank_number.columns = [column.lower() for column in dim_rank_number.columns]
    dim_timestamp.columns = [column.lower() for column in dim_timestamp.columns]

    dim_timestamp = dim_timestamp.rename(columns={"id": "timestamp_id"})

    fact_trending_games = top5_trending_games_stg.merge(
        dim_steam_game,
        on="application_id",
        how="inner"
    )

    fact_trending_games = fact_trending_games.merge(
        dim_rank_number,
        left_on="current_rank",
        right_on="rank_number",
        how="inner"
    )

    fact_trending_games = fact_trending_games.merge(
        dim_timestamp,
        on="timestamp",
        how="inner"
    )

    fact_trending_games = fact_trending_games.rename(columns={"rank_number": "rank_number_id"})

    fact_trending_games = fact_trending_games[[
        "application_id",
        "rank_number_id",
        "change_pct_within_24hr",
        "no_of_current_players",
        "timestamp_id"
    ]]

    print(fact_trending_games)
    """
    query =
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
        stg.top5_trending_games_stg.timestamp = mart.dim_timestamp.timestamp;            
    """