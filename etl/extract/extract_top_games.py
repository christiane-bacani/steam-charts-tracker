"""
Extract top games by current players from the Steam Charts.
"""
import requests
from bs4 import BeautifulSoup
from logs.etl_pipeline_logs import etl_pipeline_logs

def extract_and_parse_soup(url: str) -> BeautifulSoup | None:
    """
    Extract and parse BeautifulSoup from the Steam Charts website.

    :param url: Website URL to be extracted and parsed as a BeautifulSoup
        object
    :type url: str

    :return: BeautifulSoup object representing the web-page from the url, NoneType if
        non-existent
    :rtype: BeautifulSoup | None
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
    }
    response = requests.get(url=url, headers=headers)

    if response.status_code != 200:
        etl_pipeline_logs(
            "EXTRACT",
            "Extract and parse BeautifulSoup object",
            "FAILED",
            None
        )
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    etl_pipeline_logs(
            "EXTRACT",
            "Extract and parse BeautifulSoup object",
            "SUCCESSFUL",
            None
    )
    return soup

def extract_top_games_table(soup: BeautifulSoup | None) -> dict[str, list]:
    """
    Extract top games by current players table from the Steam Charts website.

    :param soup: BeautifulSoup object representing the web-page from the url, NoneType
        if non-existent
    :type soup: BeautifulSoup | None

    :return: Top games by current players dictionary
    :rtype: dict[str, list]
    """
    result = {
        "name": [],
        "current_players": [],
        "peak_concurrent_players_30d": [],
        "total_hours_played_30d": []
    }

    if soup is None:
        etl_pipeline_logs(
            "EXTRACT",
            "Extract top games data by current players",
            "FAILED",
            None
        )
        return result