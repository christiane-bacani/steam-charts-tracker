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

trending_games_player_concurrency = []
trending_games_player_stats = []

player_concurrency_dict = {}
historical_player_stats_dict = {}

# Get the player concurrency data and historical player stats of every trending game
for number in range(5):
    path = trending_games_dictionary["app_id"][number]
    path = str(path).replace("/app", "app")
    url = base_url + path

    app_name = trending_games_dictionary["app_name"][number]
    app_name = str(app_name)

    soup = extract_and_parse_soup(url)

    player_concurrency_data = extract_player_concurrency_data(soup, number)
    player_concurrency_dict[app_name] = player_concurrency_data

    historical_player_stats = extract_historical_player_stats(soup, number)
    historical_player_stats_dict[app_name] = historical_player_stats