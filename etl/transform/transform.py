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
    logger.info("Transforming the data: 'top5_trending_games_raw'.")

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

    logger.info("Successfully transformed the data: 'top5_trending_games_raw'.")
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
    logger.info("Transforming the data: 'top100_games_raw'.")

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

    logger.info("Successfully transformed the data: 'top100_games_raw'.")
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
    logger.info("Transforming the data: 'top10_records_raw'.")

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

    logger.info("Successfully transformed the data: 'top10_records_raw'.")
    return df

def transform_dim_rank_number(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the dimension table: `DIM_RANK_NUMBER`.

    Args:
        df (DataFrame): The extracted dimension data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transforming the data: 'DIM_RANK_NUMBER'.")

    # Rename the column
    df = df.rename(columns={"current_rank": "RANK_NUMBER"})
    
    # Type-cast the column 'RANK_NUMBER'
    df["RANK_NUMBER"] = pd.to_numeric(df["RANK_NUMBER"], errors="coerce")

    # Remove duplicate rows
    df = df.drop_duplicates(keep="first")

    # Sort the dataframe based on the primary key
    df = df.sort_values(by="RANK_NUMBER", ascending=True)

    # Reset the index of the dataframe
    df = df.reset_index(drop=True)

    logger.info("Successfully transformed the data: 'DIM_RANK_NUMBER'.")
    return df

def transform_dim_steam_game(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the dimension table: `DIM_STEAM_GAME`.

    Args:
        df (DataFrame): The extracted dimension data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transforming the data: 'DIM_STEAM_GAME'.")

    # Rename the column
    df = df.rename(columns={"application_id": "APPLICATION_ID",
                            "game_name": "GAME_NAME"})

    # Type-cast the column 'APPLICATION_ID'
    df["APPLICATION_ID"] = pd.to_numeric(df["APPLICATION_ID"], errors="raise")

    # Remove duplicate rows
    df = df.drop_duplicates(keep="first")

    # Sort the dataframe based on the primary key
    df = df.sort_values(by="APPLICATION_ID", ascending=True)

    # Reset the index of the dataframe
    df = df.reset_index(drop=True)

    logger.info("Successfully transformed the data: `DIM_STEAM_GAME`.")
    return df

def transform_dim_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the dimension table: `DIM_TIMESTAMP`.

    Args:
        df (DataFrame): The extracted dimension data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transforming the data: 'DIM_TIMESTAMP'.")

    # Rename the column
    df = df.rename(columns={"timestamp": "TIMESTAMP"})

    # Type-cast the column 'TIMESTAMP' safely and let pandas infer if formats vary
    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"],
                                     format="%Y-%m-%d %H:%M:%S.%f%z",
                                     errors="coerce")

    # Remove duplicate rows
    df = df.drop_duplicates(keep="first")

    # Sort the dataframe based on the 'TIMESTAMP' column
    df = df.sort_values(by="TIMESTAMP", ascending=True)

    # Create the primary key of the dataframe
    df["ID"] = range(1, len(df) + 1)

    # Reorder the column structure of the dataframe
    new_order_of_columns = ["ID", "TIMESTAMP"]
    df = df[new_order_of_columns]

    # Reset the index of the dataframe
    df = df.reset_index(drop=True)

    logger.info("Successfully transformed the data: `DIM_TIMESTAMP`.")
    return df

def transform_dim_peak_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the dimension table: `DIM_PEAK_MONTH`.

    Args:
        df (DataFrame): The extracted dimension data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transforming the data: 'DIM_PEAK_MONTH'.")

    # Rename the column
    df = df.rename(columns={"peak_month": "PEAK_MONTH"})

    # Type-cast the column 'PEAK_MONTH'
    df["PEAK_MONTH"] = df["PEAK_MONTH"].astype(str)

    # Remove duplicate rows
    df = df.drop_duplicates(keep="first")

    # Define the custom order on the 'PEAK_MONTH' column
    custom_order = ["January", "February", "March",
                    "April",   "May",      "June",
                    "July",    "August",   "September",
                    "October", "November", "December"]
    df["PEAK_MONTH"] = pd.Categorical(df["PEAK_MONTH"],
                                      categories=custom_order,
                                      ordered=True)
    # Actually apply the sort based on that custom order
    df = df.sort_values(by="PEAK_MONTH")

    # Create the primary key of the dataframe
    df["ID"] = range(1, len(df) + 1)

    # Reoder the column structure of the dataframe
    new_order_of_columns = ["ID", "PEAK_MONTH"]
    df = df[new_order_of_columns]

    # Reset the index of the dataframe
    df = df.reset_index(drop=True)

    logger.info("Successfully transformed the data: `DIM_PEAK_MONTH`.")
    return df

def transform_dim_peak_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the dimension table: `dim_peak_year`.

    Args:
        df (DataFrame): The extracted dimension data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transforming the data: 'dim_peak_year'.")

    # Rename the column
    df = df.rename(columns={"peak_year": "PEAK_YEAR"})

    # Type-cast the column 'PEAK_YEAR'
    df["PEAK_YEAR"] = pd.to_numeric(df["PEAK_YEAR"], errors="coerce")

    # Remove duplicate rows
    df = df.drop_duplicates(keep="first")

    # Sort the dataframe based on the 'PEAK_YEAR' column
    df = df.sort_values(by="PEAK_YEAR", ascending=True)

    # Create the primary key of the dataframe
    df["ID"] = range(1, len(df) + 1)

    # Reoder the column structure of the dataframe
    new_order_of_columns = ["ID", "PEAK_YEAR"]
    df = df[new_order_of_columns]

    # Reset the index of the dataframe    
    df = df.reset_index(drop=True)
    
    logger.info("Successfully transformed the data: `dim_peak_year`.")
    return df

def transform_fact_trending_games(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the fact table: `fact_trending_games`.

    Args:
        df (DataFrame): The extracted fact data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transforming the data: `fact_trending_games`.")

    # Remove rows with any missing values from any columns
    df.dropna(how="all", inplace=True, ignore_index=True)

    # Type-cast the column 'application_id'
    df["application_id"] = pd.to_numeric(df["application_id"], errors="raise")

    # Type-cast the column 'rank_number_id'
    df["rank_number_id"] = pd.to_numeric(df["rank_number_id"], errors="raise")

    # Type-cast the column 'change_pct_within_24hr'
    df["change_pct_within_24hr"] = pd.to_numeric(df["change_pct_within_24hr"],
                                                  errors="coerce")
    
    # Type-cast the column 'no_of_current_players'
    df["no_of_current_players"] = pd.to_numeric(df["no_of_current_players"],
                                                errors="coerce")

    # Type-cast the column 'timestamp_id'
    df["timestamp_id"] = pd.to_numeric(df["timestamp_id"],
                                       errors="raise")

    logger.info("Successfully transformed the data: `fact_trending_games`.")
    return df

def transform_fact_top_games(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the fact table: `fact_top_games`.

    Args:
        df (DataFrame): The extracted fact data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transforming the data: `fact_top_games`.")

    # Remove rows with any missing values from any columns
    df.dropna(how="all", inplace=True, ignore_index=True)

    # Type-cast the column 'application_id'
    df["application_id"] = pd.to_numeric(df["application_id"], errors="raise")

    # Type-cast the column 'rank_number_id'
    df["rank_number_id"] = pd.to_numeric(df["rank_number_id"], errors="raise")

    # Type-cast the column 'no_of_current_players'
    df["no_of_current_players"] = pd.to_numeric(df["no_of_current_players"],
                                                "coerce")

    # Type-cast the column 'no_of_peak_players'
    df["no_of_peak_players"] = pd.to_numeric(df["no_of_peak_players"],
                                             "coerce")

    # Type-cast the column 'no_of_hours_played'
    df["no_of_hours_played"] = pd.to_numeric(df["no_of_hours_played"],
                                             "coerce")

    # Type-cast the column 'no_of_hours_played'
    df["timestamp_id"] = pd.to_numeric(df["timestamp_id"], errors="raise")

    logger.info("Successfully transformed the data: `fact_top_games`.")
    return df

def transform_fact_top_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the fact table: `fact_top_records`.

    Args:
        df (DataFrame): The extracted fact data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.
    """
    logger.info("Transforming the data: `fact_top_records`.")

    # Type-cast the column 'application_id'
    df["application_id"] = pd.to_numeric(df["application_id"], errors="raise")

    # Type-cast the column 'rank_number_id'
    df["rank_number_id"] = pd.to_numeric(df["rank_number_id"], errors="raise")

    # Type-cast the column 'no_of_peak_players'
    df["no_of_peak_players"] = pd.to_numeric(df["no_of_peak_players"], errors="coerce")

    # Type-cast the column 'peak_month_id'
    df["peak_month_id"] = pd.to_numeric(df["peak_month_id"], errors="raise")

    # Type-cast the column 'peak_year_id'
    df["peak_year_id"] = pd.to_numeric(df["peak_year_id"], errors="raise")

    # Type-cast the column 'timestamp_id'
    df["timestamp_id"] = pd.to_numeric(df["timestamp_id"], errors="raise")

    logger.info("Successfully transformed the data: `fact_top_records`.")
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the extracted data from the raw/stg data layer by
    checking the column structure of the extracted data then run
    the correct transformation function.

    Args:
        df (DataFrame): The extracted data as a DataFrame.

    Returns:
        DataFrame: The transformed data as a DataFrame.        
    """
    columns = list(df.columns)

    if columns == ["id", 
                   "app_id",
                   "rank",
                   "name",
                   "twenty_four_hour_change",
                   "current_players",
                   "timestamp"]:
        return transform_top5_trending_games_raw(df)

    elif columns == ["id",
                     "app_id",
                     "rank",
                     "name",
                     "current_players",
                     "peak_players",
                     "hours_played",
                     "timestamp"]:
        return transform_top100_games_raw(df)

    elif columns == ["id",
                     "app_id",
                     "rank",
                     "name",
                     "peak_players",
                     "time",
                     "timestamp"]:
        return transform_top10_records_raw(df)

    elif columns == ["current_rank"]:
        return transform_dim_rank_number(df)

    elif columns == ["application_id",
                     "game_name"]:
        return transform_dim_steam_game(df)

    elif columns == ["timestamp"]:
        return transform_dim_timestamp(df)

    elif columns == ["peak_month"]:
        return transform_dim_peak_month(df)

    elif columns == ["peak_year"]:
        return transform_dim_peak_year(df)

    elif columns == ["application_id",
                     "rank_number_id",
                     "change_pct_within_24hr",
                     "no_of_current_players",
                     "timestamp_id"]:
        return transform_fact_trending_games(df)

    else:
        raise Exception("Invalid data to transform!")