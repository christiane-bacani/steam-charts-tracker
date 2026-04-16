"""
Python module to run the ETL Pipeline.
"""
from etl.extract.trending_games import scrape_top_5_trending_games
from utils.extract.parse import parse_soup

url = "https://steamcharts.com/"
soup = parse_soup(url)

top_5_trending_games = scrape_top_5_trending_games(soup)