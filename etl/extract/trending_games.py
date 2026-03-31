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