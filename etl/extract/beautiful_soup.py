"""
This module provides functionality to parse HTML content from Steam Charts web pages
using BeautifulSoup. It includes methods to fetch a web page, handle HTTP requests,
and log extraction results for ETL pipelines. Intended for extracting structured data
from Steam Charts for further processing.
"""
import requests
from bs4 import BeautifulSoup
from logs.etl_pipeline_logs import etl_pipeline_logs

def parse_soup(url: str) -> BeautifulSoup | None:
    """
    Parse BeautifulSoup object from a given Steam Charts web-page
    that consist of HTML content.

    :param url: URL of a specific Steam Charts page
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
            "Extract and parse BeautifulSoup object.",
            "FAILED",
            None
        )
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    etl_pipeline_logs(
        "EXTRACT",
        "Extract and parse BeautifulSoup object.",
        "SUCCESSFUL",
        None
    )
    return soup