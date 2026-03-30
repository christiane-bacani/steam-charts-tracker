""""
Python module to parse BeautifulSoup for website navigation and manipulation.
"""
import requests
from bs4 import BeautifulSoup

from logs.etl_pipeline_logs import provide_logs

def parse_soup(url: str) -> BeautifulSoup | int:
    """
    Parse BeautifulSoup object using the URL of the target website.

    Args:
        url (str): The URL of the target website.

    Returns:
        BeautifulSoup | int: The parsed BeautifulSoup object, if the
        request is not successful, it returns the status code
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
            "Initialization",
            "Initialize BeautifulSoup object for navigating the elements inside the website.",
            "Unsuccessful",
            f"Status code: {response.status_code}"
        )
        return response.status_code

    # If yes, return the parsed BeautifulSoup object
    soup = BeautifulSoup(response.text, 'html.parser')
    provide_logs(
        "Initialization",
        "Initialize BeautifulSoup object for navigating the elements inside the website.",
        "Successful"
    )
    return soup