""""
Python module from utility package to parse objects that is useful
during the extraction phase of the pipeline.
"""
import requests
import json
from bs4 import BeautifulSoup

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
        return response.status_code

    # If yes, return the parsed BeautifulSoup object
    soup = BeautifulSoup(response.text, 'html.parser')

    return soup

def parse_scraped_data(filepath: str) -> dict:
    """
    Parse the data that was already extracted from previous extraction phase
    (e.g. Top 5 trending games).

    Args:
        filepath (str): The filepath of a JSON file

    Returns:
        dict: The parsed data as a dictionary
    """
    try:
        # Parse the scraped data from a JSON file
        with open(filepath, "r") as file:
            scraped_data = json.load(file)

        return scraped_data

    except FileNotFoundError:
        """
        Raise 'FileNotFoundError' if the filepath is not existing instead of
        handling the error to prevent misbehavior throughout the pipeline
        """
        raise FileNotFoundError(f"File: {filepath} is not existing!")