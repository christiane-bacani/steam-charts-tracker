"""
"""
from bs4 import BeautifulSoup

def extract_top_5_trending_games(soup: BeautifulSoup) -> dict[str, list]:
    div_id_with_content_wrapper = soup.find("div", attrs={"id": "content-wrapper"})
    div_tag_with_content_class = div_id_with_content_wrapper.find(
        "div", attrs={"class": "content"}
    )
    table_tag = div_tag_with_content_class.find("table")