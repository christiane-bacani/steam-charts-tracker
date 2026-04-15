"""
"""
from bs4 import BeautifulSoup

def extract_top_5_trending_games(soup: BeautifulSoup) -> dict[str, list]:
    