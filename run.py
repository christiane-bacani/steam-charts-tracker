"""
Orchestrate and executes workflow of the Steam Charts ETL Pipeline.
"""
from etl.extract.beautiful_soup import parse_soup
from etl.extract.trending_games import extract_trending_games_table
from etl.extract.stats_overview import extract_game_stats_overview
from etl.extract.trending_games import extract_historical_player_stats_table
from etl.extract.top_games import extract_top_games_table

base_url = "https://steamcharts.com/"

# Top 5 current trending games
soup = parse_soup(base_url)
trending_apps_dict = extract_trending_games_table(soup)

trending_games_stats_overview_dict = {}
trending_games_historical_player_stats_dict = {}

# Get the player concurrency data and historical player stats of every trending game
for number in range(5):
    path = trending_apps_dict["app_id"][number]
    url = base_url + path

    app_name = trending_apps_dict["app_name"][number]
    app_name = str(app_name)

    soup = parse_soup(url)

    game_stats_overview_dict = extract_game_stats_overview(soup, number, "Trending")
    trending_games_stats_overview_dict[app_name] = game_stats_overview_dict

    historical_player_stats_dict = extract_historical_player_stats_table(soup, number)
    trending_games_historical_player_stats_dict[
        app_name
    ] = historical_player_stats_dict

# Top games by current players
url = base_url + "top"
soup = parse_soup(url)
top_games_dict = extract_top_games_table(soup)

top_games_stats_overview_dict = {}
top_games_historical_player_stats_dict = {}

# Get the stats overview and historical player stats of every top game
for number in range(25):
    path = top_games_dict["app_id"][number]
    url = base_url + path

    app_name = top_games_dict["app_name"][number]
    app_name = str(app_name)
    soup = parse_soup(url)

    game_stats_overview_dict = extract_game_stats_overview(soup, number, "Top")
    top_games_stats_overview_dict[app_name] = game_stats_overview_dict