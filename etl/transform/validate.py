"""
Python module to perform data validation to all transformed data related
to the current top 5 trending games tracked by Steam Charts.
"""
import pandas as pd

from datetime import datetime

from logs import logger

def validate_top5_trending_games_raw(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the data from the DataFrame object 'top5_trending_games_raw'
    before loading to the stage data layer.

    Args:
        df (DataFrame): The transformed data as a DataFrame.

    Returns:
        DataFrame: The validated and transformed data as a DataFrame.
    """
    logger.info("Validating the data from: 'top5_trending_games_raw'.")

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

    if (df["application_id"] < 1).any():
        logger.info("'application_id' column consist of off-range values!")
        df["application_id"] = df[df["application_id"] > 0]
        df.reset_index(inplace=True)
        logger.info("'application_id' column with off-range values are removed.")

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
        raise Exception("Column: 'game_name' consist of more than 255 characters!")

    if df["game_name"].isnull().sum() > 0:
        logger.info("Column: 'game_name' consist of null values!")
        df.dropna(subset=["game_name"], inplace=True)
        df.reset_index(inplace=True)
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
        df.reset_index(inplace=True)
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
        df.reset_index(inplace=True)
        logger.info("Column: 'no_of_current_players' with missing values are removed.")

    if (df["no_of_current_players"] < 0).any():
        logger.info("'no_of_current_players' column consist of off-range values!")
        df["no_of_current_players"] = df[df["no_of_current_players"] >= 0]
        df.reset_index(inplace=True)
        logger.info("'no_of_current_players' column with off-range values are removed.")

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
        raise Exception("Columns of the table: 'top5_trending_games_raw' are inaccurate!")

    logger.info("Successfully validated the data from: 'top5_trending_games_raw'.")
    return df

def validate_top100_games_raw(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the data from the DataFrame object 'top100_games_raw'
    before loading to the stage data layer.

    Args:
        df (DataFrame): The transformed data as a DataFrame.

    Returns:
        DataFrame: The validated and transformed data as a DataFrame.
    """
    logger.info("Validating the data from: 'top5_trending_games_raw'.")

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

    if (df["application_id"] < 1).any():
        logger.info("'application_id' column consist of off-range values!")
        df["application_id"] = df[df["application_id"] > 0]
        df.reset_index(inplace=True)
        logger.info("'application_id' column with off-range values are removed.")

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
        raise Exception("Column: 'game_name' consist of more than 255 characters!")

    if df["game_name"].isnull().sum() > 0:
        logger.info("Column: 'game_name' consist of null values!")
        df.dropna(subset=["game_name"], inplace=True)
        df.reset_index(inplace=True)
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
        df.reset_index(inplace=True)
        logger.info("Column: 'no_of_current_players' with missing values are removed.")

    if (df["no_of_current_players"] < 0).any():
        logger.info("'no_of_current_players' column consist of off-range values!")
        df["no_of_current_players"] = df[df["no_of_current_players"] >= 0]
        df.reset_index(inplace=True)
        logger.info("'no_of_current_players' column with off-range values are removed.")

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
        df.reset_index(inplace=True)
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
        df.reset_index(inplace=True)
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
        raise Exception("Columns of the table: 'top100_games_raw' are inaccurate!")

    logger.info("Successfully validated the data from: 'top100_games_raw'.")
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

    if (df["application_id"] < 1).any():
        logger.info("'application_id' column consist of off-range values!")
        df["application_id"] = df[df["application_id"] > 0]
        df.reset_index(inplace=True)
        logger.info("'application_id' column with off-range values are removed.")

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
        raise Exception("Column: 'game_name' consist of more than 255 characters!")

    if df["game_name"].isnull().sum() > 0:
        logger.info("Column: 'game_name' consist of null values!")
        df.dropna(subset=["game_name"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'game_name' with missing values are removed.")

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
        df.reset_index(inplace=True)
        logger.info("Column: 'no_of_peak_players' with missing values are removed.")

    # Perform validation checks to 'peak_month'
    if not pd.api.types.is_string_dtype(df["peak_month"]):
        logger.info("Column: 'peak_month' consist of wrong datatype!")
        df["peak_month"] = df["peak_month"].astype(str)
        logger.info("Type-casted the values of 'peak_month' column.")

    if df["peak_month"].isnull().sum() > 0:
        logger.info("Column: 'peak_month' consist of null values!")
        df.dropna(subset=["peak_month"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'peak_month' with missing values are removed.")

    month = ["January", "February", "March",
             "April",   "May",      "June",
             "July",    "August",   "September",
             "October", "November", "December"]

    no_of_invalid_peak_months = ~df["peak_month"].isin(month).sum()

    if no_of_invalid_peak_months > 0:
        logger.info("Column: 'peak_month' consist of invalid values!")
        df = df[df["peak_month"].isin(month)]
        df.reset_index(inplace=True)
        logger.info("Column: 'peak_month' with invalid values are removed.")

    # Perform validation checks to 'peak_year'
    if not pd.api.types.is_numeric_dtype(df["peak_year"]):
        logger.info("Column: 'peak_year' consist of wrong datatype!")
        df["peak_year"] = pd.to_numeric(df["peak_year"], errors="coerce")
        logger.info("Type-casted the values of 'peak_year' column.")

    if df["peak_year"].isnull().sum() > 0:
        logger.info("Column: 'peak_year' consist of null values!")
        df.dropna(subset=["peak_year"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'peak_year' with missing values are removed.")

    no_of_outdated_peak_years = (df["peak_year"] < 2010).sum()
    no_of_futuristic_peak_years  = (df["peak_year"] > datetime.now().year).sum()
    no_of_invalid_peak_years =  no_of_outdated_peak_years + no_of_futuristic_peak_years

    if no_of_invalid_peak_years > 0:
        logger.info("Column: 'peak_year' consist of invalid values!")
        df = df[df["peak_year"] <= datetime.now().year]
        df.reset_index(inplace=True)
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

def validate_dim_rank_number(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the data from the DataFrame object 'dim_rank_number'
    before loading to the stage data layer.

    Args:
        df (DataFrame): The transformed data as a DataFrame.

    Returns:
        DataFrame: The validated and transformed data as a DataFrame.
    """
    logger.info("Validating the data from: 'dim_rank_number'.")

    # Perform validation checks to 'rank_number' column
    if not pd.api.types.is_numeric_dtype(df["rank_number"]):
        logger.info("Column: 'rank_number' consist of wrong datatype!")
        df["rank_number"] = pd.to_numeric(df["rank_number"], errors="coerce")
        logger.info("Type-casted the value pf 'rank_number' column.")

    if df["rank_number"].isnull().sum() > 0:
        logger.info("Column: 'rank_number' consist of null values!")
        df.dropna(subset=["rank_number"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'rank_number' with missing values are removed.")

    if df["rank_number"].duplicated().sum() > 0:
        logger.info("Column: 'rank_number' consist of duplicate values!")
        df.drop_duplicates(subset=["rank_number"], keep="first", inplace=True)
        df.sort_values(by="rank_number", inplace=True)
        logger.info("Column: 'rank_number' with duplicate values are removed.")

    # Perform validation check to the whole dataset
    columns = list(df.columns)

    if columns != ["rank_number"]:
        raise Exception("Columns of the table: 'dim_rank_number' are inaccurate!")

    logger.info("Successfully validated the data from: 'dim_rank_number'.")
    return df

def validate_dim_steam_game(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the data from the DataFrame object 'dim_steam_game'
    before loading to the stage data layer.

    Args:
        df (DataFrame): The transformed data as a DataFrame.

    Returns:
        DataFrame: The validated and transformed data as a DataFrame.
    """
    logger.info("Validating the data from: 'dim_steam_game'.")

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

    if (df["application_id"] < 1).any():
        logger.info("'application_id' column consist of off-range values!")
        df["application_id"] = df[df["application_id"] > 0]
        df.reset_index(inplace=True)
        logger.info("'application_id' column with off-range values are removed.")

    # Perform validation checks to 'game_name' column
    if not pd.api.types.is_string_dtype(df["game_name"]):
        logger.info("Column: 'game_name' consist of wrong datatype!")
        df["game_name"] = df["game_name"].astype(str)
        logger.info("Type-casted the values of 'game_name' column.")

    if (df["game_name"].str.len() > 255).any():
        raise Exception("Column: 'game_name' consist of more than 255 characters!")

    if df["game_name"].isnull().sum() > 0:
        logger.info("Column: 'game_name' consist of null values!")
        df.dropna(subset=["game_name"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'game_name' with missing values are removed.")

    # Perform validation check to the whole dataset
    columns = list(df.columns)
    correct_order_of_columns = [
        "application_id",
        "game_name"
    ]

    if columns != correct_order_of_columns:
        raise Exception("Columns of the table: 'dim_steam_game' are inaccurate!")

    logger.info("Successfully validated the data from: 'dim_steam_game'.")
    return df

def validate_dim_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the data from the DataFrame object 'dim_timestamp'
    before loading to the stage data layer.

    Args:
        df (DataFrame): The transformed data as a DataFrame.

    Returns:
        DataFrame: The validated and transformed data as a DataFrame.
    """
    logger.info("Validating the data from: 'dim_steam_game'.")

    # Perform validation checks to 'id' column
    if not pd.api.types.is_numeric_dtype(df["id"]):
        logger.info("Column: 'id' consist of wrong datatype!")
        df["id"] = pd.to_numeric(df["id"], errors="coerce")
        logger.info("Type-casted the values of 'id' column.")

    if df["id"].isnull().sum() > 0:
        logger.info("Column: 'id' consist of null values!")
        df.dropna(subset=["id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'id' with missing values are removed.")

    if df["id"].duplicated().sum() > 0:
        logger.info("Column: 'id' consist of duplicate values!")
        df.drop_duplicates(subset=["id"], keep="first", inplace=True)
        df.sort_values(by="id", inplace=True)
        logger.info("Column: 'id' with duplicate values are removed.")

    # Perform validation checks to 'timestamp' column
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        logger.info("Column: 'timestamp' consist of wrong datatype!")
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="raise", utc=True)
        logger.info("Column: 'timestamp' with wrong datatype are fixed.")

    if df["timestamp"].isnull().sum() > 0:
        logger.info("Column: 'timestamp' consist of null values!")
        df.dropna(subset=["timestamp"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'timestamp' with missing values are removed.")

    if df["timestamp"].duplicated().sum() > 0:
        logger.info("Column: 'timestamp' consist of duplicate values!")
        df.drop_duplicates(subset=["timestamp"], keep="last", inplace=True)
        df.sort_values(by="timestamp", inplace=True)
        logger.info("Column: 'timestamp' with duplicate values are removed.")

    # Perform validation check to the whole dataset
    columns = list(df.columns)
    correct_order_of_columns = [
        "id",
        "timestamp"
    ]

    if columns != correct_order_of_columns:
        raise Exception("Columns of the table: 'dim_timestamp' are inaccurate!")

    logger.info("Successfully validated the data from: 'dim_timestamp'.")
    return df

def validate_dim_peak_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the data from the DataFrame object 'dim_peak_month'
    before loading to the stage data layer.

    Args:
        df (DataFrame): The transformed data as a DataFrame.

    Returns:
        DataFrame: The validated and transformed data as a DataFrame.
    """
    logger.info("Validating the data from: 'dim_peak_month'.")

    # Perform validation checks to 'id' column
    if not pd.api.types.is_numeric_dtype(df["id"]):
        logger.info("Column: 'id' consist of wrong datatype!")
        df["id"] = pd.to_numeric(df["id"], errors="coerce")
        logger.info("Type-casted the values of 'id' column.")

    if df["id"].isnull().sum() > 0:
        logger.info("Column: 'id' consist of null values!")
        df.dropna(subset=["id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'id' with missing values are removed.")

    if df["id"].duplicated().sum() > 0:
        logger.info("Column: 'id' consist of duplicate values!")
        df.drop_duplicates(subset=["id"], keep="first", inplace=True)
        df.sort_values(by="id", inplace=True)
        logger.info("Column: 'id' with duplicate values are removed.")

    # Perform validation checks to 'peak_month' column
    if not pd.api.types.is_string_dtype(df["peak_month"]):
        logger.info("Column: 'peak_month' consist of wrong datatype!")
        df["peak_month"] = df["peak_month"].astype(str)
        logger.info("Type-casted the values of 'peak_month' column.")

    if (df["peak_month"].str.len() > 255).any():
        raise Exception("Column: 'peak_month' consist of more than 255 characters!")

    if df["peak_month"].isnull().sum() > 0:
        logger.info("Column: 'peak_month' consist of null values!")
        df.dropna(subset=["peak_month"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'peak_month' with missing values are removed.")

    if df["peak_month"].duplicated().sum() > 0:
        logger.info("Column: 'peak_month' consist of duplicate values!")
        df.drop_duplicates(subset=["peak_month"], keep="first", inplace=True)
        df["peak_month"] = pd.Categorical(
            df["peak_month"],
            categories=[
                "January", "February", "March",
                "April",   "May",      "June",
                "July",    "August",   "September",
                "October", "November", "December"
            ]
        )
        df.sort_values(by="peak_month", inplace=True)
        logger.info("Column: 'peak_month' with duplicate values are removed.")

    # Perform validation check to the whole dataset
    columns = list(df.columns)
    correct_order_of_columns = [
        "id",
        "peak_month"
    ]

    if columns != correct_order_of_columns:
        raise Exception("Columns of the table: 'dim_peak_month' are inaccurate!")

    logger.info("Successfully validated the data from: 'dim_peak_month'.")
    return df

def validate_dim_peak_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the data from the DataFrame object 'dim_peak_year'
    before loading to the stage data layer.

    Args:
        df (DataFrame): The transformed data as a DataFrame.

    Returns:
        DataFrame: The validated and transformed data as a DataFrame.    
    """
    logger.info("Validating the data from: 'dim_peak_year'.")

    # Perform validation checks to 'id' column
    if not pd.api.types.is_numeric_dtype(df["id"]):
        logger.info("Column: 'id' consist of wrong datatype!")
        df["id"] = pd.to_numeric(df["id"], errors="coerce")
        logger.info("Type-casted the values of 'id' column.")

    if df["id"].isnull().sum() > 0:
        logger.info("Column: 'id' consist of null values!")
        df.dropna(subset=["id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'id' with missing values are removed.")

    if df["id"].duplicated().sum() > 0:
        logger.info("Column: 'id' consist of duplicate values!")
        df.drop_duplicates(subset=["id"], keep="first", inplace=True)
        df.sort_values(by="id", inplace=True)
        logger.info("Column: 'id' with duplicate values are removed.")

    # Perform validation checks to 'peak_year' column
    if not pd.api.types.is_numeric_dtype(df["peak_year"]):
        logger.info("Column: 'peak_year' consist of wrong datatype!")
        df["peak_year"] = pd.to_numeric(df["peak_year"], errors="coerce")
        logger.info("Type-casted the values of 'peak_year' column.")

    if df["peak_year"].isnull().sum() > 0:
        logger.info("Column: 'peak_year' consist of null values!")
        df.dropna(subset=["peak_year"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'peak_year' with missing values are removed.")

    if df["peak_year"].duplicated().sum() > 0:
        logger.info("Column: 'peak_year' consist of duplicate values!")
        df.drop_duplicates(subset=["peak_year"], keep="first", inplace=True)
        df.sort_values(by="peak_year", inplace=True)
        logger.info("Column: 'peak_year' with duplicate values are removed.")

    from datetime import datetime

    if (df["peak_year"] > datetime.now().year).any():
        logger.info("'peak_year' column consist of off-range values!")
        df["peak_year"] = df[df["peak_year"] <= datetime.now().year]
        df["id"] = range(1, len(df) + 1)
        logger.info("'peak_year' column with off-range values are removed.")

    # Perform validation check to the whole dataset
    columns = list(df.columns)
    correct_order_of_columns = [
        "id",
        "peak_year"
    ]

    if columns != correct_order_of_columns:
        raise Exception("Columns of the table: 'dim_peak_year' are inaccurate!")

    logger.info("Successfully validated the data from: 'dim_peak_year'.")
    return df

def validate_fact_trending_games(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the data from the DataFrame object 'fact_trending_games'
    before loading to the stage data layer.

    Args:
        df (DataFrame): The transformed data as a DataFrame.

    Returns:
        DataFrame: The validated and transformed data as a DataFrame.    
    """
    logger.info("Validating the data from: 'fact_trending_games'.")

    # Perform validation checks to 'application_id' column
    if not pd.api.types.is_numeric_dtype(df["application_id"]):
        logger.info("Column: 'application_id' consist of wrong datatype!")
        df["application_id"] = pd.to_numeric(df["application_id"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'application_id' column.")

    if df["application_id"].isnull().sum() > 0:
        logger.info("Column: 'application_id' consist of null values!")
        df.dropna(subset=["application_id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'application_id' with missing values are removed.")

    if (df["application_id"] < 1).any():
        logger.info("'application_id' column consist of off-range values!")
        df["application_id"] = df[df["application_id"] > 0]
        df.reset_index(inplace=True)
        logger.info("'application_id' column with off-range values are removed.")

    # Perform validation checks to 'rank_number_id' column
    if not pd.api.types.is_numeric_dtype(df["rank_number_id"]):
        logger.info("Column: 'rank_number_id' consist of wrong datatype!")
        df["rank_number_id"] = pd.to_numeric(df["rank_number_id"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'rank_number_id' column.")

    if df["rank_number_id"].isnull().sum() > 0:
        logger.info("Column: 'rank_number_id' consist of null values!")
        df.dropna(subset=["rank_number_id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'rank_number_id' with missing values are removed.")

    if (df["rank_number_id"] > 100).any() or (df["rank_number_id"] < 1).any():
        logger.info("'rank_number_id' column consist of off-range values!")
        df["rank_number"] = df[(df["rank_number"] >= 1) & (df["rank_number" <= 100])]
        df.reset_index(inplace=True)
        logger.info("'rank_number_id' column with off-range values are removed.")

    # Perform validation checks to 'change_pct_within_24hr' column
    if not pd.api.types.is_numeric_dtype(df["change_pct_within_24hr"]):
        logger.info("Column: 'change_pct_within_24hr' consist of wrong datatype!")
        df["change_pct_within_24hr"] = pd.to_numeric(df["change_pct_within_24hr"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'change_pct_within_24hr' column.")

    if df["change_pct_within_24hr"].isnull().sum() > 0:
        logger.info("Column: 'change_pct_within_24hr' consist of null values!")
        df.dropna(subset=["change_pct_within_24hr"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'change_pct_within_24hr' with missing values are removed.")

    # Perform validation checks to 'no_of_current_players' column
    if not pd.api.types.is_numeric_dtype(df["no_of_current_players"]):
        logger.info("Column: 'no_of_current_players' consist of wrong datatype!")
        df["no_of_current_players"] = pd.to_numeric(df["no_of_current_players"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'no_of_current_players' column.")

    if df["no_of_current_players"].isnull().sum() > 0:
        logger.info("Column: 'no_of_current_players' consist of null values!")
        df.dropna(subset=["no_of_current_players"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'no_of_current_players' with missing values are removed.")

    if (df["no_of_current_players"] < 0).any():
        logger.info("'no_of_current_players' column consist of off-range values!")
        df["no_of_current_players"] = df[df["no_of_current_players"] >= 0]
        df.reset_index(inplace=True)
        logger.info("'no_of_current_players' column with off-range values are removed.")

    # Perform validation checks to 'timestamp_id' column
    if not pd.api.types.is_numeric_dtype(df["timestamp_id"]):
        logger.info("Column: 'timestamp_id' consist of wrong datatype!")
        df["timestamp_id"] = pd.to_numeric(df["timestamp_id"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'timestamp_id' column.")

    if df["timestamp_id"].isnull().sum() > 0:
        logger.info("Column: 'timestamp_id' consist of null values!")
        df.dropna(subset=["timestamp_id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'timestamp_id' with missing values are removed.")

    if (df["timestamp_id"] < 1).any():
        logger.info("'timestamp_id' column consist of off-range values!")
        df["timestamp_id"] = df[df["timestamp_id"] > 0]
        df.reset_index(inplace=True)
        logger.info("'timestaml_id' column with off-range values are removed.")

    # Perform validation check to the whole dataset
    columns = list(df.columns)
    correct_order_of_columns = [
        "application_id",
        "rank_number_id",
        "change_pct_within_24hr",
        "no_of_current_players",
        "timestamp_id"
    ]

    if columns != correct_order_of_columns:
        raise Exception("Columns of the table: 'fact_trending_games' are inaccurate!")

    logger.info("Successfully validated the data from: 'fact_trending_games'.")
    return df

def validate_fact_top_games(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the data from the DataFrame object 'fact_top_games'
    before loading to the stage data layer.

    Args:
        df (DataFrame): The transformed data as a DataFrame.

    Returns:
        DataFrame: The validated and transformed data as a DataFrame.    
    """
    logger.info("Validating the data from: 'fact_top_games'.")

    # Perform validation checks to 'application_id' column
    if not pd.api.types.is_numeric_dtype(df["application_id"]):
        logger.info("Column: 'application_id' consist of wrong datatype!")
        df["application_id"] = pd.to_numeric(df["application_id"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'application_id' column.")

    if df["application_id"].isnull().sum() > 0:
        logger.info("Column: 'application_id' consist of null values!")
        df.dropna(subset=["application_id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'application_id' with missing values are removed.")

    if (df["application_id"] < 1).any():
        logger.info("'application_id' column consist of off-range values!")
        df["application_id"] = df[df["application_id"] > 0]
        df.reset_index(inplace=True)
        logger.info("'application_id' column with off-range values are removed.")

    # Perform validation checks to 'rank_number_id' column
    if not pd.api.types.is_numeric_dtype(df["rank_number_id"]):
        logger.info("Column: 'rank_number_id' consist of wrong datatype!")
        df["rank_number_id"] = pd.to_numeric(df["rank_number_id"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'rank_number_id' column.")

    if df["rank_number_id"].isnull().sum() > 0:
        logger.info("Column: 'rank_number_id' consist of null values!")
        df.dropna(subset=["rank_number_id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'rank_number_id' with missing values are removed.")

    if (df["rank_number_id"] > 100).any() or (df["rank_number_id"] < 1).any():
        logger.info("'rank_number_id' column consist of off-range values!")
        df["rank_number"] = df[(df["rank_number"] >= 1) & (df["rank_number" <= 100])]
        df.reset_index(inplace=True)
        logger.info("'rank_number_id' column with off-range values are removed.")

    # Perform validation checks to 'no_of_current_players' column
    if not pd.api.types.is_numeric_dtype(df["no_of_current_players"]):
        logger.info("Column: 'no_of_current_players' consist of wrong datatype!")
        df["no_of_current_players"] = pd.to_numeric(df["no_of_current_players"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'no_of_current_players' column.")

    if df["no_of_current_players"].isnull().sum() > 0:
        logger.info("Column: 'no_of_current_players' consist of null values!")
        df.dropna(subset=["no_of_current_players"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'no_of_current_players' with missing values are removed.")

    if (df["no_of_current_players"] < 0).any():
        logger.info("'no_of_current_players' column consist of off-range values!")
        df["no_of_current_players"] = df[df["no_of_current_players"] >= 0]
        df.reset_index(inplace=True)
        logger.info("'no_of_current_players' column with off-range values are removed.")

    # Perform validation checks to 'no_of_peak_players' column
    if not pd.api.types.is_numeric_dtype(df["no_of_peak_players"]):
        logger.info("Column: 'no_of_peak_players' consist of wrong datatype!")
        df["no_of_peak_players"] = pd.to_numeric(df["no_of_peak_players"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'no_of_peak_players' column.")

    if df["no_of_peak_players"].isnull().sum() > 0:
        logger.info("Column: 'no_of_peak_players' consist of null values!")
        df.dropna(subset=["no_of_peak_players"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'no_of_peak_players' with missing values are removed.")

    if (df["no_of_peak_players"] < 0).any():
        logger.info("'no_of_peak_players' column consist of off-range values!")
        df["no_of_peak_players"] = df[df["no_of_peak_players"] >= 0]
        df.reset_index(inplace=True)
        logger.info("'no_of_peak_players' column with off-range values are removed.")

    # Perform validation checks to 'no_of_hours_played' column
    if not pd.api.types.is_numeric_dtype(df["no_of_hours_played"]):
        logger.info("Column: 'no_of_hours_played' consist of wrong datatype!")
        df["no_of_hours_played"] = pd.to_numeric(df["no_of_hours_played"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'no_of_hours_played' column.")

    if df["no_of_hours_played"].isnull().sum() > 0:
        logger.info("Column: 'no_of_hours_played' consist of null values!")
        df.dropna(subset=["no_of_hours_played"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'no_of_hours_played' with missing values are removed.")

    if (df["no_of_hours_played"] < 0).any():
        logger.info("'no_of_hours_played' column consist of off-range values!")
        df["no_of_hours_played"] = df[df["no_of_hours_played"] >= 0]
        df.reset_index(inplace=True)
        logger.info("'no_of_hours_played' column with off-range values are removed.")

    # Perform validation checks to 'timestamp_id' column
    if not pd.api.types.is_numeric_dtype(df["timestamp_id"]):
        logger.info("Column: 'timestamp_id' consist of wrong datatype!")
        df["timestamp_id"] = pd.to_numeric(df["timestamp_id"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'timestamp_id' column.")

    if df["timestamp_id"].isnull().sum() > 0:
        logger.info("Column: 'timestamp_id' consist of null values!")
        df.dropna(subset=["timestamp_id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'timestamp_id' with missing values are removed.")

    if (df["timestamp_id"] < 1).any():
        logger.info("'timestamp_id' column consist of off-range values!")
        df["timestamp_id"] = df[df["timestamp_id"] > 0]
        df.reset_index(inplace=True)
        logger.info("'timestamp_id' column with off-range values are removed.")

    # Perform validation check to the whole dataset
    columns = list(df.columns)
    correct_order_of_columns = [
        "application_id",
        "rank_number_id",
        "no_of_current_players",
        "no_of_peak_players",
        "no_of_hours_played",
        "timestamp_id"
    ]

    if columns != correct_order_of_columns:
        raise Exception("Columns of the table: 'fact_top_games' are inaccurate!")

    logger.info("Successfully validated the data from: 'fact_top_games'.")
    return df

def validate_fact_top_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the data from the DataFrame object 'fact_top_games'
    before loading to the stage data layer.

    Args:
        df (DataFrame): The transformed data as a DataFrame.

    Returns:
        DataFrame: The validated and transformed data as a DataFrame.    
    """
    logger.info("Validating the data from: 'fact_top_records'.")

    # Perform validation checks to 'application_id' column
    if not pd.api.types.is_numeric_dtype(df["application_id"]):
        logger.info("Column: 'application_id' consist of wrong datatype!")
        df["application_id"] = pd.to_numeric(df["application_id"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'application_id' column.")

    if df["application_id"].isnull().sum() > 0:
        logger.info("Column: 'application_id' consist of null values!")
        df.dropna(subset=["application_id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'application_id' with missing values are removed.")

    if (df["application_id"] < 1).any():
        logger.info("'application_id' column consist of off-range values!")
        df["application_id"] = df[df["application_id"] > 0]
        df.reset_index(inplace=True)
        logger.info("'application_id' column with off-range values are removed.")

    # Perform validation checks to 'rank_number_id' column
    if not pd.api.types.is_numeric_dtype(df["rank_number_id"]):
        logger.info("Column: 'rank_number_id' consist of wrong datatype!")
        df["rank_number_id"] = pd.to_numeric(df["rank_number_id"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'rank_number_id' column.")

    if df["rank_number_id"].isnull().sum() > 0:
        logger.info("Column: 'rank_number_id' consist of null values!")
        df.dropna(subset=["rank_number_id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'rank_number_id' with missing values are removed.")

    if (df["rank_number_id"] > 100).any() or (df["rank_number_id"] < 1).any():
        logger.info("'rank_number_id' column consist of off-range values!")
        df["rank_number"] = df[(df["rank_number"] >= 1) & (df["rank_number" <= 100])]
        df.reset_index(inplace=True)
        logger.info("'rank_number_id' column with off-range values are removed.")

    # Perform validation checks to 'no_of_peak_players' column
    if not pd.api.types.is_numeric_dtype(df["no_of_peak_players"]):
        logger.info("Column: 'no_of_peak_players' consist of wrong datatype!")
        df["no_of_peak_players"] = pd.to_numeric(df["no_of_peak_players"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'no_of_peak_players' column.")

    if df["no_of_peak_players"].isnull().sum() > 0:
        logger.info("Column: 'no_of_peak_players' consist of null values!")
        df.dropna(subset=["no_of_peak_players"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'no_of_peak_players' with missing values are removed.")

    if (df["no_of_peak_players"] < 0).any():
        logger.info("'no_of_peak_players' column consist of off-range values!")
        df["no_of_peak_players"] = df[df["no_of_peak_players"] >= 0]
        df.reset_index(inplace=True)
        logger.info("'no_of_peak_players' column with off-range values are removed.")

    # Perform validation checks to 'peak_month_id' column
    if not pd.api.types.is_numeric_dtype(df["peak_month_id"]):
        logger.info("Column: 'peak_month_id' consist of wrong datatype!")
        df["peak_month_id"] = pd.to_numeric(df["peak_month_id"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'peak_month_id' column.")

    if df["peak_month_id"].isnull().sum() > 0:
        logger.info("Column: 'peak_month_id' consist of null values!")
        df.dropna(subset=["peak_month_id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'peak_month_id' with missing values are removed.")

    if (df["peak_month_id"] < 1).any() or (df["peak_month_id"] > 12).any():
        logger.info("'peak_month_id' column consist of off-range values!")
        df["peak_month_id"] = df[(df["peak_month_id"] >= 1) &
                                 df["peak_month_id"] <= 12]
        df.reset_index(inplace=True)
        logger.info("'peak_month_id' column with off-range values are removed.")

    # Perform validation checks to 'peak_year_id'
    if not pd.api.types.is_numeric_dtype(df["peak_year_id"]):
        logger.info("Column: 'peak_year_id' consist of wrong datatype!")
        df["peak_year_id"] = pd.to_numeric(df["peak_year_id"],
                                                  errors="coerce")
        logger.info("Type-casted the values of 'peak_year_id' column.")

    if df["peak_year_id"].isnull().sum() > 0:
        logger.info("Column: 'peak_year_id' consist of null values!")
        df.dropna(subset=["peak_year_id"], inplace=True)
        df.reset_index(inplace=True)
        logger.info("Column: 'peak_year_id' with missing values are removed.")

    if (df["peak_year_id"] < 1).any():
        logger.info("'peak_year_id' column consist of off-range values!")
        df["peak_year_id"] = df[df["peak_year_id"] > 0]
        df.reset_index(inplace=True)
        logger.info("'peak_year_id' column with off-range values are removed.")

    # Perform validation check to the whole dataset
    columns = list(df.columns)
    correct_order_of_columns = [
        "application_id",
        "rank_number_id",
        "no_of_peak_players",
        "peak_month_id",
        "peak_year_id",
        "timestamp_id"
    ]

    if columns != correct_order_of_columns:
        raise Exception("Columns of the table: 'fact_top_records' are inaccurate!")

    logger.info("Successfully validated the data from: 'fact_top_records'.")
    return df

def validate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the extracted and transformed data
    from the raw/stg data layer before loading it
    to the next data layer.

    Args:
        df (DataFrame): The extracted and transformed
                        data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.        
    """
    columns = list(df.columns)

    if columns == ["id", 
                   "application_id", 
                   "current_rank", 
                   "game_name", 
                   "change_pct_within_24hr", 
                   "no_of_current_players", 
                   "timestamp"]:
        return validate_top5_trending_games_raw(df)

    elif columns == ["id",
                     "app_id",
                     "rank",
                     "name",
                     "current_players",
                     "peak_players",
                     "hours_played",
                     "timestamp"]:
        return validate_top100_games_raw(df)

    else:
        raise Exception("Invalid data to validate!")