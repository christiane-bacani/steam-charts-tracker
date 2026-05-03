"""
Python module to perform data validation to all transformed data related
to the current top 5 trending games tracked by Steam Charts.
"""
import pandas as pd

from datetime import datetime

from logs import logger

def validate_top5_trending_games_stg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the data from the DataFrame object 'top5_trending_games_stg'
    before loading to the stage data layer.

    Args:
        df (DataFrame): The transformed data as a DataFrame.

    Returns:
        DataFrame: The validated and transformed data as a DataFrame.
    """
    logger.info("Validating the data from: 'top5_trending_games_stg'.")

    # Perform validation checks to 'application_id' column
    if not pd.api.types.is_numeric_dtype(df["application_id"]):
        logger.info("Column: 'application_id' consist of wrong datatype!")
        df["application_id"] = pd.to_numeric(df["application_id"], errors="coerce")
        logger.info("Type-casted the values of 'application_id' column.")

    if df["application_id"].isnull().sum() > 0:
        logger.info("Column: 'application_id' consist of null values!")
        df.dropna(subset=["application_id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'application_id' with missing values are removed.")

    # Perform validation checks to 'current_rank' column
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

    # Perform validation checks to 'game_name' column
    if not pd.api.types.is_string_dtype(df["game_name"]):
        logger.info("Column: 'game_name' consist of wrong datatype!")
        df["game_name"] = df["game_name"].astype(str)
        logger.info("Type-casted the values of 'game_name' column.")

    if (df["game_name"].str.len() > 255).any():
        raise Exception("Column: 'game_name' consist of moe than 255 characters!")

    if df["game_name"].isnull().sum() > 0:
        logger.info("Column: 'game_name' consist of null values!")
        df.dropna(subset=["game_name"], inplace=True)
        df.reset_index()
        logger.info("Column: 'game_name' with missing values are removed.")

    # Perform validation checks to 'change_pct_within_24hr' column
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

    # Perform validation checks to 'no_of_current_players' column
    if not pd.api.types.is_numeric_dtype(df["no_of_current_players"]):
        logger.info("Column: 'no_of_current_players' consist of wrong datatype!")
        df["no_of_current_players"] = pd.to_numeric(
            df["no_of_current_players"], errors="coerce"
        )
        logger.info("Type-casted the values of 'no_of_current_players' column.")

    if df["no_of_current_players"].isnull().sum() > 0:
        logger.info("Column: 'no_of_current_players' consist of null values!")
        df.dropna(subset=["no_of_current_players"], inplace=True)
        df.reset_index()
        logger.info("Column: 'no_of_current_players' with missing values are removed.")

    # Perform validation check to the whole dataset
    columns = list(df.columns)
    correct_order_of_columns = [
        "id",
        "application_id",
        "current_rank",
        "game_name",
        "change_pct_within_24hr",
        "no_of_current_players",
        "timestamp"
    ]

    if columns != correct_order_of_columns:
        raise Exception("Columns of the table: 'top5_trending_games_stg' are inaccurate!")

    logger.info("Successfully validated the data from: 'top5_trending_games_stg'.")
    return df

def validate_top100_games_stg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the data from the DataFrame object 'top100_game_stg' before
    loading to the stage data layer.

    Args:
        df (DataFrame): The transformed data as a DataFrame.

    Returns:
        DataFrame: The validated and transformed data as a DataFrame.
    """
    logger.info("Validating the data from: 'top5_trending_games_stg'.")

    # Perform validation checks to 'application_id' column
    if not pd.api.types.is_numeric_dtype(df["application_id"]):
        logger.info("Column: 'application_id' consist of wrong datatype!")
        df["application_id"] = pd.to_numeric(df["application_id"], errors="coerce")
        logger.info("Type-casted the values of 'application_id' column.")

    if df["application_id"].isnull().sum() > 0:
        logger.info("Column: 'application_id' consist of null values!")
        df.dropna(subset=["application_id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'application_id' with missing values are removed.")

    # Perform validation checks to 'current_rank' column
    if not pd.api.types.is_numeric_dtype(df["current_rank"]):
        logger.info("Column: 'current_rank' consist of wrong datatype!")
        df["current_rank"] = pd.to_numeric(df["current_rank"], errors="coerce")
        logger.info("Type-casted the values of 'current_rank' column.")

    if df["current_rank"].isnull().sum() > 0:
        logger.info("Column: 'current_rank' column consist of null values!")
        df.dropna(subset=["current_rank"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'current_rank' with missing values are removed.")

    if min(df["current_rank"]) not in range(1, 100 + 1):
        raise Exception("Invalid range of values from the 'current_rank' column!")

    if max(df["current_rank"]) not in range(1, 100 + 1):
        raise Exception("Invalid range of values from the 'current_rank' column!")

    # Perform validation checks to 'game_name' column
    if not pd.api.types.is_string_dtype(df["game_name"]):
        logger.info("Column: 'game_name' consist of wrong datatype!")
        df["game_name"] = df["game_name"].astype(str)
        logger.info("Type-casted the values of 'game_name' column.")

    if (df["game_name"].str.len() > 255).any():
        raise Exception("Column: 'game_name' consist of moe than 255 characters!")

    if df["game_name"].isnull().sum() > 0:
        logger.info("Column: 'game_name' consist of null values!")
        df.dropna(subset=["game_name"], inplace=True)
        df.reset_index()
        logger.info("Column: 'game_name' with missing values are removed.")

    # Perform validation checks to 'no_of_current_players' column
    if not pd.api.types.is_numeric_dtype(df["no_of_current_players"]):
        logger.info("Column: 'no_of_current_players' consist of wrong datatype!")
        df["no_of_current_players"] = pd.to_numeric(
            df["no_of_current_players"], errors="coerce"
        )
        logger.info("Type-casted the values of 'no_of_current_players' column.")

    if df["no_of_current_players"].isnull().sum() > 0:
        logger.info("Column: 'no_of_current_players' consist of null values!")
        df.dropna(subset=["no_of_current_players"], inplace=True)
        df.reset_index()
        logger.info("Column: 'no_of_current_players' with missing values are removed.")

    # Perform validation checks to 'no_of_peak_players' column
    if not pd.api.types.is_numeric_dtype(df["no_of_peak_players"]):
        logger.info("Column: 'no_of_peak_players' consist of wrong datatype!")
        df["no_of_peak_players"] = pd.to_numeric(
            df["no_of_peak_players"], errors="coerce"
        )
        logger.info("Type-casted the values of 'no_of_peak_players' column.")

    if df["no_of_peak_players"].isnull().sum() > 0:
        logger.info("Column: 'no_of_peak_players' consist of null values!")
        df.dropna(subset=["no_of_peak_players"], inplace=True)
        df.reset_index()
        logger.info("Column: 'no_of_peak_players' with missing values are removed.")

    # Perform validation checks to 'no_of_hours_played' column
    if not pd.api.types.is_numeric_dtype(df["no_of_hours_played"]):
        logger.info("Column: 'no_of_hours_played' consist of wrong datatype!")
        df["no_of_hours_played"] = pd.to_numeric(
            df["no_of_hours_played"], errors="coerce"
        )
        logger.info("Type-casted the values of 'no_of_hours_played' column.")

    if df["no_of_hours_played"].isnull().sum() > 0:
        logger.info("Column: 'no_of_hours_played' consist of null values!")
        df.dropna(subset=["no_of_hours_played"], inplace=True)
        df.reset_index()
        logger.info("Column: 'no_of_hours_played' with missing values are removed.")

    # Perform validation check to the whole dataset
    columns = list(df.columns)
    correct_order_of_columns = [
        "id",
        "application_id",
        "current_rank",
        "game_name",
        "no_of_current_players",
        "no_of_peak_players",
        "no_of_hours_played",
        "timestamp"
    ]

    if columns != correct_order_of_columns:
        raise Exception("Columns of the table: 'top100_games_stg' are inaccurate!")

    logger.info("Successfully validated the data from: 'top100_games_stg'.")
    return df

def validate_top10_records_stg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the data from the DataFrame object 'top10_records_stg' before
    loading to the stage data layer.

    Args:
        df (DataFrame): The transformed data as a DataFrame.

    Returns:
        DataFrame: The validated and transformed data as a DataFrame.
    """
    logger.info("Validating the data from: 'top5_trending_games_stg'.")

    # Perform validation checks to 'application_id' column
    if not pd.api.types.is_numeric_dtype(df["application_id"]):
        logger.info("Column: 'application_id' consist of wrong datatype!")
        df["application_id"] = pd.to_numeric(df["application_id"], errors="coerce")
        logger.info("Type-casted the values of 'application_id' column.")

    if df["application_id"].isnull().sum() > 0:
        logger.info("Column: 'application_id' consist of null values!")
        df.dropna(subset=["application_id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'application_id' with missing values are removed.")

    # Perform validation checks to 'current_rank' column
    if not pd.api.types.is_numeric_dtype(df["current_rank"]):
        logger.info("Column: 'current_rank' consist of wrong datatype!")
        df["current_rank"] = pd.to_numeric(df["current_rank"], errors="coerce")
        logger.info("Type-casted the values of 'current_rank' column.")

    if df["current_rank"].isnull().sum() > 0:
        logger.info("Column: 'current_rank' column consist of null values!")
        df.dropna(subset=["current_rank"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'current_rank' with missing values are removed.")

    if min(df["current_rank"]) not in range(1, 100 + 1):
        raise Exception("Invalid range of values from the 'current_rank' column!")

    if max(df["current_rank"]) not in range(1, 100 + 1):
        raise Exception("Invalid range of values from the 'current_rank' column!")

    # Perform validation checks to 'game_name' column
    if not pd.api.types.is_string_dtype(df["game_name"]):
        logger.info("Column: 'game_name' consist of wrong datatype!")
        df["game_name"] = df["game_name"].astype(str)
        logger.info("Type-casted the values of 'game_name' column.")

    if (df["game_name"].str.len() > 255).any():
        raise Exception("Column: 'game_name' consist of moe than 255 characters!")

    if df["game_name"].isnull().sum() > 0:
        logger.info("Column: 'game_name' consist of null values!")
        df.dropna(subset=["game_name"], inplace=True)
        df.reset_index()
        logger.info("Column: 'game_name' with missing values are removed.")

    # Perform validation checks to 'no_of_peak_players' column
    if not pd.api.types.is_numeric_dtype(df["no_of_peak_players"]):
        logger.info("Column: 'no_of_peak_players' consist of wrong datatype!")
        df["no_of_peak_players"] = df["no_of_peak_players"].astype(str)
        logger.info("Type-casted the values of 'no_of_peak_players' column.")

    if df["no_of_peak_players"].isnull().sum() > 0:
        logger.info("Column: 'no_of_peak_players' consist of null values!")
        df.dropna(subset=["no_of_peak_players"], inplace=True)
        df.reset_index()
        logger.info("Column: 'no_of_peak_players' with missing values are removed.")

    # Perform validation checks to 'peak_month'
    if not pd.api.types.is_string_dtype(df["peak_month"]):
        logger.info("Column: 'peak_month' consist of wrong datatype!")
        df["peak_month"] = df["peak_month"].astype(str)
        logger.info("Type-casted the values of 'peak_month' column.")

    if df["peak_month"].isnull().sum() > 0:
        logger.info("Column: 'peak_month' consist of null values!")
        df.dropna(subset=["peak_month"], inplace=True)
        df.reset_index()
        logger.info("Column: 'peak_month' with missing values are removed.")

    month = ["January", "February", "March",
             "April",   "May",      "June",
             "July",    "August",   "September",
             "October", "November", "December"]

    no_of_invalid_peak_months = ~df["peak_month"].isin(month).sum()

    if no_of_invalid_peak_months > 0:
        logger.info("Column: 'peak_month' consist of invalid values!")
        df = df[df["peak_month"].isin(month)]
        df.reset_index()
        logger.info("Column: 'peak_month' with invalid values are removed.")

    # Perform validation checks to 'peak_year'
    if not pd.api.types(df["peak_year"]):
        logger.info("Column: 'peak_year' consist of wrong datatype!")
        df["peak_year"] = df["peak_year"].astype(str)
        logger.info("Type-casted the values of 'peak_year' column.")

    if df["peak_year"].isnull().sum() > 0:
        logger.info("Column: 'peak_year' consist of null values!")
        df.dropna(subset=["peak_year"], inplace=True)
        df.reset_index()
        logger.info("Column: 'peak_year' with missing values are removed.")

    no_of_outdated_peak_years = (df["peak_year"] < 2010).sum()
    no_of_futuristic_peak_years  = (df["peak_year"] > datetime.now().year).sum()
    no_of_invalid_peak_years =  no_of_outdated_peak_years + no_of_futuristic_peak_years

    if no_of_invalid_peak_years > 0:
        logger.info("Column: 'peak_year' consist of invalid values!")
        df = df[df["peak_year"] <= datetime.now().year]
        df.reset_index()
        logger.info("Column: 'peak_year' with invalid values are removed.")

    # Perform validation check to the whole dataset
    columns = list(df.columns)
    correct_order_of_columns = [
        "id",
        "application_id",
        "current_rank",
        "game_name",
        "no_of_peak_players",
        "peak_month",
        "peak_year",
        "timestamp"
    ]

    if columns != correct_order_of_columns:
        raise Exception("Columns of the table: 'top10_records_stg' are inaccurate!")

    logger.info("Successfully validated the data from: 'top10_records_stg'.")
    return df