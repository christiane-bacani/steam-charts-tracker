"""
Python module to extract data about the trending games from the Steam Charts website.
"""
import json
from bs4 import BeautifulSoup

from logs.etl_pipeline_logs import provide_logs

def extract_top_5_trending_games(soup: BeautifulSoup):
    """
    Scrape the top 5 trending games from the Steam Charts website.

    Args:
        soup (BeautifulSoup): The parsed BeautifulSoup object
    """
    # Navigate the web-page to get the exact HTML elements for accurate scraping
    body_tag = soup.find("body")
    div_tag_with_content_class = body_tag.find("div", attrs={"class": "content"})

    table_tag = div_tag_with_content_class.find("table", attrs={
        "id": "trending-recent"
    })
    tbody_tag = table_tag.find("tbody")
    table_row_tags = tbody_tag.find_all("tr")

    # Dictionary to store the scraped data
    data = {
        "app_id": [],
        "game_name": [],
        "twenty_four_hour_change_pct": [],
        "current_players": []
    }

    for table_row_tag in table_row_tags:
        table_data_tags = table_row_tag.find_all("td")

        # Extract the application ID
        app_id = table_data_tags[0].find("a")["href"]
        app_id = app_id.replace("/app/", "")
        data["app_id"].append(app_id)

        # Extract the game game
        game_name = table_data_tags[0].get_text()
        data["game_name"].append(game_name)

        # Extract the percentage of 24-hour change
        twenty_four_hour_change_pct = table_data_tags[1].get_text()
        data["twenty_four_hour_change_pct"].append(twenty_four_hour_change_pct)

        # Extract the number of current players
        current_players = table_data_tags[3].get_text()
        data["current_players"].append(current_players)

    # Store the scraped data to a JSON file from `data/input` directory
    with open("data/input/top_5_trending_games.json", "w") as file:
        json.dump(data, file, indent=4)

    provide_logs(
        "EXTRACT",
        "Extract top 5 trending games from https://steamcharts.com/.",
        "Successful"
    )

def extract_game_stats_overview(
        soup: BeautifulSoup,
        app_id: str,
        trending_games_stats_overview: dict[str, list]
) -> dict[str, str]:
    """
    Scrape the game statistics overview of a trending game from the Steam
    Charts website.

    Args:
        soup (BeautifulSoup): The parsed BeautifulSoup object
        app_id (str): The application ID of a trending game
        trending_games_stats_overview (dict): The dictionary to store the scraped
            data
    Returns:
        dict: The scraped game statistics overview of a trending game
    """
    # Navigate the web-page to get the exact HTML elements for accurate scraping
    body_tag = soup.find("body")
    div_tag_with_content_wrapper_id = body_tag.find("div", attrs={
        "id": "content-wrapper"
    })

    div_tag_with_app_heading_id = div_tag_with_content_wrapper_id.find("div", attrs={
        "id": "app-heading"
    })

    # Extract the application ID of game
    trending_games_stats_overview["app_id"].append(app_id)

    # Extract the image of the game as a URL path
    game_image_url_path = div_tag_with_app_heading_id.find("img")["src"]
    game_image_url_path = game_image_url_path.replace("/assets", "assets")
    game_image = "https://steamcharts.com/" + game_image_url_path
    trending_games_stats_overview["game_image"].append(game_image)

    div_tag_with_app_stat_classes = div_tag_with_app_heading_id.find_all(
        "div",
        attrs={
            "class": "app-stat"
        }
    )

    # Extract the peak no. of players in the span of 24-hour period
    second_div_tag_with_app_class = div_tag_with_app_stat_classes[1]
    span_tag = second_div_tag_with_app_class.find("span")
    twenty_four_hour_peak_players = span_tag.get_text()
    trending_games_stats_overview["twenty_four_hour_peak_players"].append(twenty_four_hour_peak_players)

    # Extract the all-time peak no. of players
    third_div_tag_with_app_class = div_tag_with_app_stat_classes[2]
    span_tag = third_div_tag_with_app_class.find("span")
    all_time_peak_players = span_tag.get_text()
    trending_games_stats_overview["all_time_peak_players"].append(all_time_peak_players)

    return trending_games_stats_overview

def extract_historical_stats(
        soup: BeautifulSoup,
        app_id: str,
        historical_stats: dict[str, dict]
) -> dict[str, dict]:
    """
    Scrape the historical statistics of a trending game from the Steam Charts website.

    Args:
        soup (BeautifulSoup): The parsed BeautifulSoup object
        app_id (str): The application ID of a trending game
        historical_stats (dict): The dictionary to store the scraped data
    """
    # Navigate the web-page to get the exact HTML elements for accurate scraping
    body_tag = soup.find("body")
    div_tag_with_content_wrapper_id = body_tag.find("div", attrs={
        "id": "content-wrapper"
    })

    third_div_tag_with_content_classes = div_tag_with_content_wrapper_id.find_all(
        "div",
        attrs={
            "class": "content"
        }
    )[2]
    table_tag = third_div_tag_with_content_classes.find("table", attrs={
        "class": "common-table"
    })

    tbody_tag = table_tag.find("tbody")
    table_row_tags = tbody_tag.find_all("tr")

    # Structure the dictionary to store the scraped data
    historical_stats[app_id] = {
        "month": [],
        "avg_players": [],
        "gain": [],
        "gain_pct": [],
        "peak_players": []
    }

    for table_row_tag in table_row_tags:
        table_data_tags = table_row_tag.find_all("td")

        # Extract the month
        month = table_data_tags[0].get_text()
        historical_stats[app_id]["month"].append(month)

        # Extract the average players of the month
        avg_players = table_data_tags[1].get_text()
        historical_stats[app_id]["avg_players"].append(avg_players)

        # Extract the number of gains of average players from previous month
        gain = table_data_tags[2].get_text()
        historical_stats[app_id]["gain"].append(gain)

        # Extract the gain percentage of average players from previous month
        gain_pct = table_data_tags[3].get_text()
        historical_stats[app_id]["gain_pct"].append(gain_pct)

        # Extract the no. of peak players of the month
        peak_players = table_data_tags[4].get_text()
        historical_stats[app_id]["peak_players"].append(peak_players)

    return historical_stats