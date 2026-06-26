"""
Python module to perform data extraction to all data tracked by Steam Charts.
"""
import pandas as pd
from bs4 import BeautifulSoup

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection_to_postgres

from logs import logger

def ingest_top5_trending_games(soup: BeautifulSoup) -> dict[str, list]:
    """
    Ingest the data of the current top 5 trending games on Steam Charts website.

    Args:
        soup (bs4.BeautifulSoup): The parsed BeautifulSoup object.

    Returns:
        dict[str, list]: The ingested data as a dictionary.
    """
    logger.info("Ingesting the current data of the top 5 trending games.")
    body = soup.find("body")
    content_wrapper = body.find("div", attrs={"id": "content-wrapper"})
    content_class = content_wrapper.find(
        "div", attrs={"class": "content"}
    )

    table = content_class.find("table")
    tbody = table.find("tbody")
    table_rows = tbody.find_all("tr")

    ingested_data = {
        "app_id":                  [],
        "rank":                    [],
        "name":                    [],
        "twenty_four_hour_change": [],
        "current_players":         []
    }

    for rank, table_row in enumerate(table_rows):
        table_data = table_row.find_all("td")

        # Ingest application ID
        app_id = table_data[0].find("a")["href"]
        ingested_data["app_id"].append(app_id)

        # Ingest rank number
        ingested_data["rank"].append(rank + 1)

        # Ingest the game name
        name = table_data[0].get_text()
        ingested_data["name"].append(name)

        # Ingest 24-hour change percentage
        twenty_four_hour_change = table_data[1].get_text()
        ingested_data["twenty_four_hour_change"].append(twenty_four_hour_change)

        # Ingest the no. of current players
        current_players = table_data[3].get_text()
        ingested_data["current_players"].append(current_players)

    logger.info("Successfully ingested the current data of the top 5 trending games.")
    return ingested_data

def ingest_top100_games(soup: BeautifulSoup,
                        ingested_data: dict[str, list]) -> dict[str, list]:
    """
    Ingest the data of the current top 100 games (by current players) on Steam
    Charts website.

    Args:
        soup (bs4.BeautifulSoup): The parsed BeautifulSoup object.
        ingested_data (dict): The existing ingested data as a dictionary.

    Returns:
        dict[str, list]: The ingested data as a dictionary.
    """
    if len(ingested_data["rank"]) == 0:
        n = "1-25"

    elif len(ingested_data["rank"]) == 25:
        n = "26-50"

    elif len(ingested_data["rank"]) == 50:
        n = "51-75"

    else:
        n = "76-100"

    logger.info(f"Ingesting the current data of the top {n} games.")
    body = soup.find("body")
    content_wrapper = body.find("div", attrs={"id": "content-wrapper"})
    content_class = content_wrapper.find(
        "div", attrs={"class": "content"}
    )

    table = content_class.find("table")
    tbody = table.find("tbody")
    table_rows = tbody.find_all("tr")

    for table_row in table_rows:
        table_data = table_row.find_all("td")

        # Ingest application ID
        app_id = table_data[1].find("a")["href"]
        ingested_data["app_id"].append(app_id)

        # Ingest rank number
        rank = table_data[0].get_text()
        ingested_data["rank"].append(rank)

        # Ingest the game name
        name = table_data[1].get_text()
        ingested_data["name"].append(name)

        # Ingest the no. of current players
        current_players = table_data[2].get_text()
        ingested_data["current_players"].append(current_players)

        # Ingest the no. of peak players
        peak_players = table_data[4].get_text()
        ingested_data["peak_players"].append(peak_players)

        # Ingest the total no. of hours played
        hours_played = table_data[5].get_text()
        ingested_data["hours_played"].append(hours_played)

    logger.info(f"Successfully ingested the current data of the top {n} games.")
    return ingested_data

def ingest_top10_records(soup: BeautifulSoup) -> dict[str, list]:
    """
    Ingest the data of the current top 10 records on Steam Charts website.

    Args:
        soup (bs4.BeautifulSoup): The parsed BeautifulSoup object.

    Returns:
        dict[str, list]: The ingested data as a dictionary.
    """
    logger.info("Ingesting the current data of the top 10 records.")
    body = soup.find("body")
    content_wrapper = body.find("div", attrs={"id": "content-wrapper"})
    content_class = content_wrapper.find_all(
        "div", attrs={"class": "content"}
    )[2]

    table = content_class.find("table")
    tbody = table.find("tbody")
    table_rows = tbody.find_all("tr")

    ingested_data = {
        "app_id":       [],
        "rank":         [],
        "name":         [],
        "peak_players": [],
        "time":         []
    }

    for rank, table_row in enumerate(table_rows):
        table_data = table_row.find_all("td")

        # Ingest application ID
        app_id = table_data[0].find("a")["href"]
        ingested_data["app_id"].append(app_id)

        # Ingest rank number
        ingested_data["rank"].append(rank + 1)

        # Ingest the game name
        name = table_data[0].get_text()
        ingested_data["name"].append(name)

        # Ingest the no. of peak players
        peak_players = table_data[1].get_text()
        ingested_data["peak_players"].append(peak_players)

        # Ingest the time
        time = table_data[2].get_text()
        ingested_data["time"].append(time)

    logger.info("Successfully ingested the current data of the top 10 records.")
    return ingested_data

def extract(table_name: str) -> pd.DataFrame:
    """
    Extract the data from the raw/stg data layer by
    checking the name of the table then extract it
    for further processing.

    Args:
        table_name (str): The name of the table from raw/stg
                          data layer.

    Returns:
        DataFrame: The extracted data as a DataFrame.
    """
    load_dotenv()
    engine = init_connection_to_postgres(os.getenv("HOST"),
                             os.getenv("PORT"),
                             "steam_charts",
                             os.getenv("DB_USERNAME"),
                             os.getenv("DB_PASSWORD"))

    if table_name == "top5_trending_games_raw":
        logger.info("Extracting the data: 'top5_trending_game_raw'.")
        query = "SELECT * FROM raw.top5_trending_games_raw;"
        logger.info("Successfully extracted the data: 'top5_trending_games_raw'.")
        return pd.read_sql(query, engine)

    elif table_name == "top100_games_raw":
        logger.info("Extracting the data: 'top100_games_raw'.")
        query = "SELECT * FROM raw.top100_games_raw;"
        logger.info("Successfully extracted the data: 'top100_games_raw'.")
        return pd.read_sql(query, engine)

    elif table_name == "top10_records_raw":
        logger.info("Extracting the data: 'top10_records_raw'.")
        query = "SELECT * FROM raw.top10_records_raw;"
        logger.info("Successfully extracted the data: 'top10_records_raw'.")
        return pd.read_sql(query, engine)

    else:
        raise Exception("Invalid table name to extract!")