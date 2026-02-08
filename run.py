"""
This module orchestrates and executes different functions from their
corresponding modules of the following packages: `etl/extract`,
`etl/transform/`, and `etl/load`.
"""
from etl.extract.extract_trending_games import extract_and_parse_soup
from etl.extract.extract_trending_games import extract_trending_games_table
from etl.extract.extract_trending_games import extract_player_concurrency_data
from etl.extract.extract_trending_games import extract_historical_player_stats
from logs.etl_pipeline_logs import etl_pipeline_logs

base_url = "https://steamcharts.com/"
soup = extract_and_parse_soup(base_url)

# Top 5 current trending games
trending_games_dictionary = extract_trending_games_table(soup)