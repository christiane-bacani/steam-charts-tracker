"""
Python module to run the ETL Pipeline.
"""
import json

from etl.extract.beautiful_soup import parse_soup
import etl.extract.trending_games as trending_games

# URL of the website to scrape
url = "https://steamcharts.com/"

# Scrape the top 5 trending games from the URL
soup = parse_soup(url)
trending_games.extract_top_5_trending_games(soup)

# Get the scraped data of the top 5 trending games
# to perform another scraping method to get their
# stats overview and historical stats per game 
with open("data/input/top_5_trending_games.json", "r") as file:
    top_5_trending_games = json.load(file)

trending_games_stats_overview = {
    "app_id": [],
    "game_image": [],
    "twenty_four_hour_peak_players": [],
    "all_time_peak_players": []
}

for app_id in top_5_trending_games["app_id"]:
    # Scrape the stats overview and historical stats of every current trending game
    soup = parse_soup(url + "app/" + app_id)

    stats_overview = trending_games.extract_trending_games_stats_overview(
        soup,
        app_id
    )

    for key, value in stats_overview.items():
        trending_games_stats_overview[key].append(value)