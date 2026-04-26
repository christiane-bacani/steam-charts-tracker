"""
Python module to perform data extraction to all data tracked by Steam Charts.
"""
import pandas as pd
from bs4 import BeautifulSoup

import os
from dotenv import load_dotenv

from utils.database.connection import init_connection

from logs import logger

def scrape_top5_trending_games(soup: BeautifulSoup) -> dict[str, list]:
    """
    Web-scrape the data of the current top 5 trending games on Steam Charts website.

    Args:
        soup (bs4.BeautifulSoup): The parsed BeautifulSoup object for web-scraping.

    Returns:
        dict[str, list]: The scraped data as a dictionary.
    """
    logger.info("Scraping the current data of the top 5 trending games.")
    body = soup.find("body")
    content_wrapper = body.find("div", attrs={"id": "content-wrapper"})
    content_class = content_wrapper.find(
        "div", attrs={"class": "content"}
    )

    table = content_class.find("table")
    tbody = table.find("tbody")
    table_rows = tbody.find_all("tr")

    scraped_data = {
        "app_id":                  [],
        "rank":                    [],
        "name":                    [],
        "twenty_four_hour_change": [],
        "current_players":         []
    }

    for rank, table_row in enumerate(table_rows):
        table_data = table_row.find_all("td")

        # Extract application ID
        app_id = table_data[0].find("a")["href"]
        scraped_data["app_id"].append(app_id)

        # Extract rank number
        scraped_data["rank"].append(rank + 1)

        # Extract the game name
        name = table_data[0].get_text()
        scraped_data["name"].append(name)

        # Extract 24-hour change percentage
        twenty_four_hour_change = table_data[1].get_text()
        scraped_data["twenty_four_hour_change"].append(twenty_four_hour_change)

        # Extract the no. of current players
        current_players = table_data[3].get_text()
        scraped_data["current_players"].append(current_players)

    logger.info("Successfully scraped the current data of the top 5 trending games.")
    return scraped_data

def extract_data_from_sql_table(schema_name: str, table_name: str) -> pd.DataFrame:
    """
    Extract data from different SQL tables from a certain database schema that
    corresponds to a certain data layer (bronze/raw, silver/stage, and gold/mart).

    Args:
        schema_name (str): The name of the database schema.
        table_name (str): The name of the SQL Table.

    Returns:
        DataFrame: The extracted data as a DataFrame.
    """
    logger.info(f"Extracting the data from: '{table_name}'.")
    load_dotenv()
    engine = init_connection(os.getenv("HOST"),
                             os.getenv("PORT"),
                             "steam_charts",
                             os.getenv("DB_USERNAME"),
                             os.getenv("DB_PASSWORD"))

    query = f"SELECT * FROM {schema_name}.{table_name};"
    df = pd.read_sql(query, engine)

    logger.info(f"Successfully extracted the data from: '{table_name}'.")
    return df