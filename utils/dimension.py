"""
Python module to create dimension tables from silver data layer that consist of all
data tracked by Steam Charts.
"""
import pandas as pd

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
    logger.info("Creating new dimension table: 'DIM_RANK_NUMBER'.")
    
    dim_rank_number = pd.DataFrame(columns=["current_rank"])
    dataframes = [top5_trending_games_stg, top100_games_stg, top10_records_stg]

    for dataframe in dataframes:
        dim_rank_number = pd.concat([dim_rank_number,
                                     pd.DataFrame({"current_rank": dataframe["current_rank"]})
                                     ],
                                     ignore_index=True)

    logger.info("Successfully created a new dimension table: 'DIM_RANK_NUMBER'.")
    return dim_rank_number

def create_dim_steam_game(top5_trending_games_stg: pd.DataFrame,
                          top100_games_stg: pd.DataFrame,
                          top10_records_stg: pd.DataFrame) -> pd.DataFrame:
    """
    Create the dimension table: `DIM_STEAM_GAME` using different tables
    of `stg` database schema.

    Args:
        top5_trending_games_stg (DataFrame): The top 5 trending games as a DataFrame.
        top100_games_stg (DataFrame): The top 100 games as a DataFrame.
        top10_records_stg (DataFrame): The top 10 records as a DataFrame.

    Returns:
        DataFrame: The created dimension table: `DIM_STEAM_GAME`.
    """
    logger.info("Creating new dimension table: 'DIM_STEAM_GAME'.")

    dim_steam_game = pd.DataFrame(columns=["application_id", "game_name"])
    dataframes = [top5_trending_games_stg, top100_games_stg, top10_records_stg]

    for dataframe in dataframes:
        dim_steam_game = pd.concat([dim_steam_game,
                                    dataframe[["application_id", "game_name"]]
                                    ],
                                    ignore_index=True)

    logger.info("Successfully created a new dimension table: 'DIM_STEAM_GAME'.")
    return dim_steam_game

def create_dim_timestamp(top5_trending_games_stg: pd.DataFrame,
                         top100_games_stg: pd.DataFrame,
                         top10_records_stg: pd.DataFrame) -> pd.DataFrame:
    """
    Create the dimension table: `DIM_TIMESTAMP` using different tables
    of `stg` database schema.

    Args:
        top5_trending_games_stg (DataFrame): The top 5 trending games as a DataFrame.
        top100_games_stg (DataFrame): The top 100 games as a DataFrame.
        top10_records_stg (DataFrame): The top 10 records as a DataFrame.

    Returns:
        DataFrame: The created dimension table: `DIM_TIMESTAMP`.
    """
    logger.info("Creating new dimension table: 'DIM_TIMESTAMP'.")

    dim_timestamp = pd.DataFrame(columns=["timestamp"])
    dataframes = [top5_trending_games_stg, top100_games_stg, top10_records_stg]

    for dataframe in dataframes:
        dim_timestamp = pd.concat([dim_timestamp,
                                   dataframe[["timestamp"]]
                                   ],
                                   ignore_index=True)

    logger.info("Successfully created a new dimension table: 'DIM_TIMESTAMP'.")
    return dim_timestamp