"""
Python module to parse objects related to extraction phase of the ETL Pipeline.
"""
import requests
from bs4 import BeautifulSoup

def parse_soup(url: str) -> BeautifulSoup:
    """
    Parse the BeautifulSoup object to perform web-scraping.

    Args:
        url (str): URL of the website.

    Returns:
        bs4.BeautifulSoup: The parsed BeautifulSoup object for web-scraping.
    """
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception("The request has been unsuccessfully processed by the server.")

    else:
        soup = BeautifulSoup(response.text, "html.parser")
        return soup