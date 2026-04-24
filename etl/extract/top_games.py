"""
Python module to perform web-scraping to extract all the data related to
the current top 100 games (by current players) tracked by Steam Charts.
"""
from bs4 import BeautifulSoup

from logs import logger

def scrap_top_100_games(soup: BeautifulSoup) -> dict[str, list]:
    """
    Web-scrape the data of the current top 100 games (by current players) on Steam
    Charts website.

    Args:
        soup (bs4.BeautifulSoup): The parsed BeautifulSoup object for web-scraping.

    Returns:
        dict[str, list]: The scraped data as a dictionary.
    """