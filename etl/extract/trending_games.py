import requests
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
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 "
        "Edg/144.0.0.0"
    }
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        return response.status_code

    soup = BeautifulSoup(response.text, 'html.parser')
    return soup