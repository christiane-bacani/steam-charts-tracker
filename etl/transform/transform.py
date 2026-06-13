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

def transform_dim_rank_number(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the dimension table: `dim_rank_number` from the mart data layer.

    Args:
        df (DataFrame): The extracted data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transforming the data from: 'dim_rank_number'.")

    # Type-cast the column 'rank_number'
    df["rank_number"] = pd.to_numeric(df["rank_number"], errors="coerce")

    logger.info("Successfully transformed the data from: 'dim_rank_number'.")
    return df

def transform_dim_steam_game(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the dimension table: `dim_steam_game` from the mart data layer.

    Args:
        df (DataFrame): The extracted data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transforming the data from: 'dim_steam_game'.")

    # Data deduplication
    df.drop_duplicates(keep="first", inplace=True)

    # Type-cast the column 'application_id'
    df["application_id"] = pd.to_numeric(df["application_id"], errors="raise")

    # Sort the dataframe based on the application ID
    df.sort_values(by="application_id", inplace=True)

    logger.info("Successfully transformed the data from: `dim_steam_game`.")
    return df

def transform_dim_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the dimension table: `dim_timestamp` from the mart data layer.

    Args:
        df (DataFrame): The extracted data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transforming the data from: 'dim_timestamp'.")

    # Data deduplication
    df.drop_duplicates(keep="first", inplace=True)

    # Sort the dataframe based on the earliest timestamp
    df.sort_values(by="timestamp", inplace=True)

    # Create the primary key
    df["id"] = range(1, len(df) + 1)

    # Reorder the structure of columns
    df = df[["id", "timestamp"]]

    logger.info("Successfully transformed the data from: `dim_timestamp`.")
    return df

def transform_dim_peak_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the dimension table: `dim_peak_month` from the mart data layer.

    Args:
        df (DataFrame): The extracted data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transforming the data from: 'dim_peak_month'.")

    # Create the primary key
    df["id"] = range(1, len(df) + 1)

    # Reorder the structure of columns
    df = df[["id", "peak_month"]]

    logger.info("Successfully transformed the data from: `dim_peak_month`.")
    return df

def transform_dim_peak_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the dimension table: `dim_peak_year` from the mart data layer.
    
    Args:
        df (DataFrame): The extracted data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transform the data from: 'dim_peak_year'.")

    # Data deduplication
    df.drop_duplicates(keep="first", inplace=True)

    # Type-cast the column 'peak_year'
    df["peak_year"] = pd.to_numeric(df["peak_year"], errors="raise")

    # Sort the dataframe based on the earliest year
    df.sort_values(by="peak_year", inplace=True)

    # Create the primary key
    df["id"] = range(1, len(df) + 1)

    # Reorder the structure of columns
    df = df[["id", "peak_year"]]

    logger.info("Successfully transformed the data from: `dim_peak_year`.")
    return df

def transform_fact_trending_games(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the dimension table: `dim_peak_year` from the mart data layer.
    Transform the fact table: `fact_trending_games` from the mart data layer.

    Args:
        df (DataFrame): The extracted data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transform the data from: `fact_trending_games`.")

    # Remove rows with any missing values from any columns
    df.dropna(how="all", inplace=True, ignore_index=True)

    # Type-cast the column 'application_id'
    df["application_id"] = pd.to_numeric(df["application_id"], errors="coerce")

    # Type-cast the column 'rank_number_id'
    df["rank_number_id"] = pd.to_numeric(df["rank_number_id"], errors="coerce")

    # Type-cast the column 'change_pct_within_24hr'
    df["change_pct_within_24hr"] = pd.to_numeric(df["change_pct_within_24hr"],
                                                  errors="coerce")
    
    # Type-cast the column 'no_of_current_players'
    df["no_of_current_players"] = pd.to_numeric(df["no_of_current_players"],
                                                errors="coerce")

    # Type-cast the column 'timestamp_id'
    df["timestamp_id"] = pd.to_numeric(df["timestamp_id"],
                                       errors="coerce")

    logger.info("Successfully transformed the data from: `fact_trending_games`.")
    return df