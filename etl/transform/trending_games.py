"""
Python module to perform data transformation to all extracted data related
to the current top 5 trending games tracked by Steam Charts.
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