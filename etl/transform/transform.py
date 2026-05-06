"""
Python module to perform data transformation to all data tracked by Steam Charts.
"""
import pandas as pd

from logs import logger

def transform_top5_trending_games_raw(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the extracted data from the table `top5_trending_games_raw` of the raw
    data layer.

    Args:
        df (DataFrame): The extracted data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transforming the data from: 'top5_trending_games_raw'.")

    # Perform data transformation per column
    df["app_id"] = df["app_id"].str.replace("/app/", "")
    df["app_id"] = pd.to_numeric(df["app_id"], errors="coerce")

    df["rank"].str.strip()
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")

    df["name"] = df["name"].str.strip()

    df["twenty_four_hour_change"] = df["twenty_four_hour_change"].str.replace("+", "")
    df["twenty_four_hour_change"] = df["twenty_four_hour_change"].str.replace("%", "")
    df["twenty_four_hour_change"] = pd.to_numeric(
        df["twenty_four_hour_change"], errors="coerce", downcast="float"
    )

    df["current_players"] = pd.to_numeric(df["current_players"], errors="coerce")

    # Rename the column names
    df.rename(columns={
        "app_id":                  "application_id",
        "rank":                    "current_rank",
        "name":                    "game_name",
        "twenty_four_hour_change": "change_pct_within_24hr",
        "current_players":         "no_of_current_players"
    }, inplace=True)

    logger.info("Successfully transformed the data from: 'top5_trending_games_raw'.")
    return df

def transform_top100_games_raw(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the extracted data from the table `top100_games_raw` of the raw data
    layer.

    Args:
        df (DataFrame): The extracted data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transforming the data from: 'top100_games_raw'.")

    # Perform data transformation per column
    df["app_id"] = df["app_id"].str.replace("/app/", "")
    df["app_id"] = pd.to_numeric(df["app_id"], errors="coerce")

    df["rank"] = df["rank"].str.replace(".", "").str.strip()
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")

    df["name"] = df["name"].str.strip()

    df["current_players"] = pd.to_numeric(df["current_players"], errors="coerce")

    df["peak_players"] = pd.to_numeric(df["peak_players"], errors="coerce")

    df["hours_played"] = pd.to_numeric(df["hours_played"], errors="coerce")

    # Rename the column names
    df.rename(columns={
        "app_id":          "application_id",
        "rank":            "current_rank",
        "name":            "game_name",
        "current_players": "no_of_current_players",
        "peak_players":    "no_of_peak_players",
        "hours_played":    "no_of_hours_played",
    }, inplace=True)

    logger.info("Successfully transformed the data from: 'top100_games_raw'.")
    return df

def transform_top10_records_raw(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the extracted data from the table `top10_records_raw` of the raw data
    layer.

    Args:
        df (DataFrame): The extracted data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transforming the data from: 'top10_records_raw'.")

    # Perform data transformation per column
    df["app_id"] = df["app_id"].str.replace("/app/", "")
    df["app_id"] = pd.to_numeric(df["app_id"], errors="coerce")

    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")

    df["name"] = df["name"].str.strip()

    df["peak_players"] = pd.to_numeric(df["peak_players"], errors="coerce")

    df["time"] = pd.to_datetime(df["time"], errors="raise")

    df["peak_month"] = df["time"].dt.month_name()
    df["peak_year"] = df["time"].dt.year

    # Remove unnecessary column
    df.drop(columns=["time"], inplace=True)

    # Reorder the structure of the columns
    df = df[[
        "id",
        "app_id",
        "rank",
        "name",
        "peak_players",
        "peak_month",
        "peak_year",
        "timestamp"
    ]]

    # Rename the column names
    df.rename(columns={
        "app_id": "application_id",
        "rank":   "current_rank",
        "name":   "game_name",
        "peak_players": "no_of_peak_players"
    }, inplace=True)

    logger.info("Successfully transformed the data from: 'top10_records_raw'.")
    return df