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
        df["id"] = pd.to_numeric(df["id"], errors="raise")

    total_no_of_rows = len(df)

    if min(df["id"]) not in range(1, total_no_of_rows + 1):
        raise Exception("Invalid range of values from the 'id' column!")

    elif max(df["id"]) not in range(1, total_no_of_rows + 1):
        raise Exception("Invalid range of values from the 'id' column!")

    if df["id"].isnull().sum() > 0:
        raise Exception("'id' column consist of null values!")

    id_column_has_duplicates = df["id"].duplicated().any()

    if id_column_has_duplicates:
        raise Exception("'id' column consist of duplicate values!")