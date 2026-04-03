""""
Python module from utility package to parse objects that is useful
during the extraction phase of the pipeline.
"""
import requests
import json
from bs4 import BeautifulSoup

from logs.etl_pipeline_logs import provide_logs

def parse_soup(url: str, description: str) -> BeautifulSoup | None:
    """
    Parse BeautifulSoup object using the URL of the target website.

    Args:
        url (str): The URL of the target website.
        description (str): The whole description for parsing the BeautifulSoup object.

    Returns:
        BeautifulSoup | None: The parsed BeautifulSoup object, if the
            request is not successful, return NoneType.
    """
    # User-Agent header for scraping
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 "
        "Edg/144.0.0.0"
    }
    response = requests.get(url, headers=headers)

    # Check the response if the website allows scraping
    if response.status_code != 200:
        provide_logs(
            "EXTRACT",
            description,
            "FAILED",
            f"Status code: {response.status_code}"
        )
        return None

    # If yes, return the parsed BeautifulSoup object
    soup = BeautifulSoup(response.text, 'html.parser')

    provide_logs(
        "EXTRACT",
        description,
        "SUCCESSFUL",
        None
    )
    return soup

def parse_top_5_trending_games(filepath: str) -> dict:
    """
    Parse the top 5 current trending games data from a JSON file.

    Args:
        filepath (str): The filepath of a JSON file

    Returns:
        dict: The parsed data as a dictionary
    """
    try:
        # Parse the scraped data from a JSON file
        with open(filepath, "r") as file:
            scraped_data = json.load(file)

        provide_logs(
            "EXTRACT",
            "Parse the extracted top 5 trending games data from a JSON file."
            "SUCCESSFUL",
            None
        )
        return scraped_data

    except FileNotFoundError:
        provide_logs(
            "EXTRACT",
            "Parse the extracted top 5 trending games data from a JSON file.",
            "FAILED",
            f"Filename: '{filepath}' is not existing for parsing the extracted data "
            "of the top 5 trending games from a JSON file."
        )
        raise FileNotFoundError("The given filename for parsing the extracted data "
                                "of the top 5 trending games from a JSON file is "
                                "not existing!")