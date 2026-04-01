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

def extract_trending_games_stats_overview(
    soup: BeautifulSoup,
    app_id: str
) -> dict[str, str]:
    """
    Scrape the stats overview of a trending game.

    Args:
        soup (BeautifulSoup): The parsed BeautifulSoup object
        app_id (str): The application ID of a trending game
    
    Returns:
        dict: The scraped data as a dictionary
    """
    # Navigate the web-page to get the exact HTML elements for accurate scraping
    body_tag = soup.find("body")
    div_tag_with_content_wrapper_id = body_tag.find("div", attrs={
        "id": "content-wrapper"
    })

    div_tag_with_app_heading_id = div_tag_with_content_wrapper_id.find("div", attrs={
        "id": "app-heading"
    })

    # Dictionary to store the scraped data
    result = {}

    # Extract the applicatiion ID
    result["app_id"] = app_id

    # Extract the game image as a URL
    game_image_url_path = div_tag_with_app_heading_id.find("img")["src"]
    game_image_url = "https://steamcharts.com" + game_image_url_path
    result["game_image"] = game_image_url

    div_tag_with_app_stat_classes = div_tag_with_app_heading_id.find_all("div", attrs={
        "class": "app-stat"
    })

    # Extract the number of peak players with a span of 24-hour period
    span_tag = div_tag_with_app_stat_classes[1].find("span")
    twenty_four_hour_peak_players = span_tag.get_text()
    result["twenty_four_hour_peak_players"] = twenty_four_hour_peak_players

    # Extract the number of peak players of all time
    span_tag = div_tag_with_app_stat_classes[2].find("span")
    all_time_peak_players = span_tag.get_text()
    result["all_time_peak_players"] = all_time_peak_players

    return result