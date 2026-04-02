"""
Python module to run the ETL Pipeline.
"""
import json

from utils.extract.parse import parse_soup
from utils.extract.parse import parse_scraped_data
import etl.extract.trending_games as trending_games

url = "https://steamcharts.com/"

soup = parse_soup(url)
trending_games.extract_top_5_trending_games(soup)

# Parse the scraped data of the current top 5 trending games from a JSON file
top_5_trending_games = parse_scraped_data("data/input/top_5_trending_games.json")

trending_games_stats_overview = {
    "app_id":                        [],
    "game_image":                    [],
    "twenty_four_hour_peak_players": [],
    "all_time_peak_players":         []
}
trending_games_historical_stats = {}

"""
Scrape the stats overview and historical stats of every current trending game
using the parsed data earlier by extracting the 'app_id' because it can be use
as a URL path and combine it to the base url of the website to allow the pipeline
to scrape all the necessary data
"""
for number, app_id in enumerate(top_5_trending_games["app_id"]):
    soup = parse_soup(url + "app/" + app_id)

    # Scrape stats overview of all current trending game
    stats_overview = trending_games.extract_trending_games_stats_overview(
        soup,
        app_id
    )
    for key, value in stats_overview.items():
        trending_games_stats_overview[key].append(value)

    # Scrape historical stats of all current trending game
    trending_games_historical_stats = trending_games.extract_trending_games_historical_stats(
        soup,
        app_id,
        trending_games_historical_stats
    )