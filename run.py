"""
Python module to run the ETL Pipeline.
"""
from etl.extract.trending_games import parse_soup

# URL of the website to scrape
url = "https://steamcharts.com/"

soup = parse_soup(url)