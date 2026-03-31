"""
Python module to run the ETL Pipeline.
"""
from etl.extract.beautiful_soup import parse_soup
import etl.extract.trending_games as trending_games

# URL of the website to scrape
url = "https://steamcharts.com/"

# Scrape the top 5 trending games
soup = parse_soup(url)
trending_games.extract_top_5_trending_games(soup)