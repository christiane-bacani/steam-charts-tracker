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
        logger.info("Values of 'id' column are in the wrong datatype!")
        df["id"] = pd.to_numeric(df["id"], errors="raise")
        logger.info("Successfully type-casted the values of 'id' column.")

    total_no_of_rows = df.shape[0]

    if min(df["id"]) not in range(1, total_no_of_rows + 1):
        raise Exception("Invalid range of values from the 'id' column!")

    elif max(df["id"]) not in range(1, total_no_of_rows + 1):
        raise Exception("Invalid range of values from the 'id' column!")

    if df["id"].isnull().sum() > 0:
        logger.info("'id' column consist of null values!")
        df.dropna(subset=["id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Removed rows with missing values of 'id' column.")

    id_column_has_duplicates = df["id"].duplicated().any()

    if id_column_has_duplicates:
        logger.info("'id' column consist of duplicate values!")
        df["id"].drop_duplicates(keep="first", inplace=True)
        df.reset_index()
        logger.info("Successfully removed duplicate values for 'id' column.")

    if not pd.api.types.is_numeric_dtype(df["application_id"]):
        logger.info("Values of 'application_id' column are in the wrong datatype!")
        df["application_id"] = pd.to_numeric(df["application_id"], errors="coerce")
        logger.info("Successfully type-casted the values of 'application_id' column.")

    if df["application_id"].isnull().sum() > 0:
        logger.info("'application_id' column consist of null values!")
        df.dropna(subset=["application_id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Removed rows with missing values of 'application_id' column.")

    if not pd.api.types.is_numeric_dtype(df["current_rank"]):
        logger.info("Values of 'current_rank' column are in the wrong dataype!")
        df["current_rank"] = pd.to_numeric(df["current_rank"], errors="coerce")
        logger.info("Successfully type-casted the values of 'current_rank' column.")

    if df["current_rank"].isnull().sum() > 0:
        # TODO: Check if it can be fix
        raise Exception("'current_rank' column consist of null values!")

    if min(df["current_rank"]) not in range(1, 5 + 1):
        raise Exception("Invalid range of values from the 'current_rank' column!")

    if max(df["current_rank"]) not in range(1, 5 + 1):
        raise Exception("Invalid range of values from the 'current_rank' column!")

    if not pd.api.types.is_string_dtype(df["game_name"]):
        logger.info("Values of 'game_name' column are in the wrong dataype!")
        df["game_name"] = df["game_name"].astype(str)
        logger.info("Successfully type-casted the values of 'game_name' column.")

    if df["game_name"].str.len() > 255:
        raise Exception("'game_name' column consist of more than 255 characters!")

    if df["game_name"].isnull().sum() > 0:
        logger.info("'game_name' column consist of null values!")
        df.dropna(subset=["game_name"], inplace=True)
        df.reset_index()
        logger.info("Removed rows with missing values of 'game_name' column.")