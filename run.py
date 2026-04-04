"""
Python module to run the ETL Pipeline.
"""
from utils.extract.parse import parse_soup
from utils.extract.parse import parse_top_5_trending_games

from etl.extract.trending_games import extract_top_5_trending_games
from etl.extract.trending_games import extract_trending_games_stats_overview
from etl.extract.trending_games import save_trending_games_stats_overview_to_json
from etl.extract.trending_games import extract_trending_games_historical_stats

url = "https://steamcharts.com/"

soup = parse_soup(
    url,
    "Parse the BeautifulSoup object from Steam Charts website to extract the data of "
    "the current top 5 trending games."
)
extract_top_5_trending_games(soup)

trending_games_stats_overview = {
    "app_id":                        [],
    "game_image":                    [],
    "twenty_four_hour_peak_players": [],
    "all_time_peak_players":         []
}
trending_games_historical_stats = {}

# Parse the scraped data of the current top 5 trending games from a JSON file
top_5_trending_games = parse_top_5_trending_games(
    "data/input/top_5_trending_games.json"
)

for number, app_id in enumerate(top_5_trending_games["app_id"]):
    soup = parse_soup(
        url + "app/" + app_id,
        f"Parse the BeautifulSoup object of the current number {number + 1} trending "
        "game from Steam Charts website to extract the data of its stats overview and "
        "historical stats.",
    )

    # Scrape stats overview of all current trending games
    stats_overview = extract_trending_games_stats_overview(
        soup,
        app_id,
        f"Extract the stats overview data of the current number {number + 1} "
        "trending game from Steam Charts."
    )
    for key, value in stats_overview.items():
        trending_games_stats_overview[key].append(value)

    # Scrape historical stats of all current trending games
    historical_stats = extract_trending_games_historical_stats(
        soup,
        app_id,
        f"Extract the historical stats data of the current number {number + 1} "
        "trending game from Steam Charts.",
    )
    trending_games_historical_stats[app_id] = historical_stats[app_id]

# Save the scraped data of stats overview and historical stats of all current trending games
save_trending_games_stats_overview_to_json(
    trending_games_stats_overview,
    "data/input/top_5_trending_games_stats_overview.json"
)