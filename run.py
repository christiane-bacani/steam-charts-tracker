"""
Python module to run the ETL Pipeline.
"""
from bs4 import BeautifulSoup

from etl.extract.beautiful_soup import parse_soup
from etl.extract.trending_games import extract_top_5_trending_games

# URL of the website to scrape
url = "https://steamcharts.com/"

soup = parse_soup(url)
top_5_trending_games = extract_top_5_trending_games(soup)