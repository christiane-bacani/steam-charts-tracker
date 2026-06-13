"""
Python module to parse objects related to extraction phase of the ETL Pipeline.
"""
import requests
from bs4 import BeautifulSoup

from logs import logger

def parse_soup(url: str) -> BeautifulSoup:
    """
    Fetch a URL and return a parsed BeautifulSoup object.

    Args:
        url (str): URL of the website to scrape.

    Returns:
        BeautifulSoup: Parsed object ready for scraping.
    """
    logger.info(f"Fetching URL: '{url}'.")
    response = requests.get(url, timeout=10)

    if response.status_code != 200:
        logger.info(f"Unsuccessfully fetched URL: {url}")
        return response.raise_for_status()

    else:
        logger.info(f"Successfully fetched URL: '{url}'.")
        return BeautifulSoup(response.text, "html.parser")