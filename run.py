"""
Python module to run the ETL Pipeline.
"""
from etl.extract.beautiful_soup import parse_soup
import etl.extract.trending_games as trending_games

# URL of the website to scrape
url = "https://steamcharts.com/"

# Scrape the top 5 trending games
soup = parse_soup(url)
top_5_trending_games = trending_games.extract_top_5_trending_games(soup)

# Dictionaries to store the scraped data of stats overview and historical stats of
# top 5 trending game
trending_games_stats_overview = {
    "app_id": [],
    "game_image": [],
    "twenty_four_hour_peak_players": [],
    "all_time_peak_players": []
}
trending_games_historical_stats = {}

for app_id in top_5_trending_games["app_id"]:
    # Scrape all the stats overview and historical stats of every game listed as a
    # top 5 trending game
    soup = parse_soup(url + "app/" + app_id)

    trending_games_stats_overview = trending_games.extract_game_stats_overview(
        soup,
        app_id,
        trending_games_stats_overview
    )

    trending_games_historical_stats = trending_games.extract_historical_stats(
        soup,
        app_id,
        trending_games_historical_stats
    )