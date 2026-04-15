"""
"""
from bs4 import BeautifulSoup

def extract_top_5_trending_games(soup: BeautifulSoup) -> dict[str, list]:
    body = soup.find("body")
    content_wrapper = body.find("div", attrs={"id": "content-wrapper"})
    content_class = content_wrapper.find(
        "div", attrs={"class": "content"}
    )

    table = content_class.find("table")
    tbody = table.find("tbody")