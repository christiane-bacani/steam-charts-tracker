"""
Python module to perform data validation to all transformed data related
to the current top 5 trending games tracked by Steam Charts.
"""
import pandas as pd

from logs import logger

def validate_top5_trending_games_stg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the data from the DataFrame object 'top5_trending_games_stg'
    before loading to the stage data storage layer.

    Args:
        df (DataFrame): The transformed data as a DataFrame.

    Returns:
        DataFrame: The validated and transformed data as a DataFrame.
    """
    logger.info("Validating the data from: 'top5_trending_games_stg'.")

    if not pd.api.types.is_numeric_dtype(df["id"]):
        logger.info("Column: 'id' consist of wrong datatype!")
        df["id"] = pd.to_numeric(df["id"], errors="raise")
        logger.info("Type-casted the values of 'id' column.")

    total_no_of_rows = df.shape[0]

    if min(df["id"]) not in range(1, total_no_of_rows + 1):
        raise Exception("Invalid range of values from the 'id' column!")

    elif max(df["id"]) not in range(1, total_no_of_rows + 1):
        raise Exception("Invalid range of values from the 'id' column!")

    if df["id"].isnull().sum() > 0:
        logger.info("Column: 'id' consist of null values!")
        df.dropna(subset=["id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'id' with missing values are removed.")

    id_column_has_duplicates = df["id"].duplicated().any()

    if id_column_has_duplicates:
        logger.info("Column: 'id' consist of duplicate values!")
        df["id"].drop_duplicates(keep="first", inplace=True)
        df.reset_index()
        logger.info("Successfully removed duplicate values for 'id' column.")

    if not pd.api.types.is_numeric_dtype(df["application_id"]):
        logger.info("Column: 'application_id' consist of wrong datatype!")
        df["application_id"] = pd.to_numeric(df["application_id"], errors="coerce")
        logger.info("Type-casted the values of 'application_id' column.")

    if df["application_id"].isnull().sum() > 0:
        logger.info("Column: 'application_id' consist of null values!")
        df.dropna(subset=["application_id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'application_id' with missing values are removed.")

    if not pd.api.types.is_numeric_dtype(df["current_rank"]):
        logger.info("Column: 'current_rank' consist of wrong datatype!")
        df["current_rank"] = pd.to_numeric(df["current_rank"], errors="coerce")
        logger.info("Type-casted the values of 'current_rank' column.")

    if df["current_rank"].isnull().sum() > 0:
        logger.info("Column: 'current_rank' column consist of null values!")
        df.dropna(subset=["current_rank"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'current_rank' with missing values are removed.")

    if min(df["current_rank"]) not in range(1, 5 + 1):
        raise Exception("Invalid range of values from the 'current_rank' column!")

    if max(df["current_rank"]) not in range(1, 5 + 1):
        raise Exception("Invalid range of values from the 'current_rank' column!")

    if not pd.api.types.is_string_dtype(df["game_name"]):
        logger.info("Column: 'game_name' consist of wrong datatype!")
        df["game_name"] = df["game_name"].astype(str)
        logger.info("Type-casted the values of 'game_name' column.")

    if df["game_name"].str.len() > 255:
        raise Exception("Column: 'game_name' consist of moe than 255 characters!")

    if df["game_name"].isnull().sum() > 0:
        logger.info("Column: 'game_name' consist of null values!")
        df.dropna(subset=["game_name"], inplace=True)
        df.reset_index()
        logger.info("Column: 'game_name' with missing values are removed.")

    if not pd.api.types.is_numeric_dtype(df["change_pct_within_24hr"]):
        logger.info("Column: 'change_pct_within_24hr' consist of wrong datatype!")
        df["change_pct_within_24hr"] = pd.to_numeric(
            df["change_pct_within_24hr"], errors="coerce"
        )
        logger.info("Type-casted the values of 'change_pct_within_24hr' column.")

    if df["change_pct_within_24hr"].isnull().sum() > 0:
        logger.info("Column: 'change_pct_within_24hr' consist of null values!")
        df.dropna(subset=["change_pct_within_24hr"], inplace=True)
        df.reset_index()
        logger.info("Column: 'change_pct_within_24hr' with missing values are removed.")