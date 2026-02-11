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