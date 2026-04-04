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

    provide_logs(
        "EXTRACT",
        "Extract the data of the current top 5 trending games from Steam Charts.",
        "SUCCESSFUL",
        None
    )
    # Store the scraped data to a JSON file from `data/input` directory
    with open("data/input/top_5_trending_games.json", "w") as file:
        json.dump(data, file, indent=4)

def extract_trending_games_stats_overview(
    soup: BeautifulSoup,
    app_id: str,
    description: str
) -> dict[str, str]:
    """
    Scrape the stats overview of a current trending game.

    Args:
        soup (BeautifulSoup): The parsed BeautifulSoup object
        app_id (str): The application ID of a current trending game
        description (str): The whole deescription for extracting the data
            of a current trending game's stats overview

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
    data = {}

    # Extract the application ID
    data["app_id"] = app_id

    # Extract the game image as a URL
    game_image_url_path = div_tag_with_app_heading_id.find("img")["src"]
    game_image_url = "https://steamcharts.com" + game_image_url_path
    data["game_image"] = game_image_url

    div_tag_with_app_stat_classes = div_tag_with_app_heading_id.find_all("div", attrs={
        "class": "app-stat"
    })

    # Extract the number of peak players with a span of 24-hour period
    span_tag = div_tag_with_app_stat_classes[1].find("span")
    twenty_four_hour_peak_players = span_tag.get_text()
    data["twenty_four_hour_peak_players"] = twenty_four_hour_peak_players

    # Extract the number of peak players of all time
    span_tag = div_tag_with_app_stat_classes[2].find("span")
    all_time_peak_players = span_tag.get_text()
    data["all_time_peak_players"] = all_time_peak_players

    provide_logs(
        "EXTRACT",
        description,
        "SUCCESSFUL",
        None
    )
    return data

def save_trending_games_stats_overview_to_json(
        trending_games_stats_overview: dict[str, list],
        filepath: str
):
    """
    Save the extracted data of the current top 5 trending games' stats overview.

    Args:
        trending_games_stats_overview (dict): The scraped data as a dictionary
        filepath (str): The target filepath as a JSON file to store the scraped data
    """
    with open(filepath, "w") as file:
        json.dump(trending_games_stats_overview, file, indent=4)

    provide_logs(
        "EXTRACT",
        "Save the extracted data of the top 5 trending games' stats "
        "overview to a JSON file.",
        "SUCCESSFUL",
        None
    )

def extract_trending_games_historical_stats(
        soup: BeautifulSoup,
        app_id: str,
        description: str
) -> dict[str, dict]:
    """
    Scrape the historical stats of a current trending game.

    Args:
        soup (BeautifulSoup): The parsed BeautifulSoup object
        app_id (str): The application ID of a current trending game
        description (str): The whole description for extracting the data
            of a current trending game's historical stats

    Returns:
        dict: The scraped data as a dictionary
    """
    # Navigate the web-page to get the exact HTML elements for accurate scraping
    body_tag = soup.find("body")
    div_tag_with_content_wrapper_id = body_tag.find("div", attrs={
        "id": "content-wrapper"
    })

    div_tag_with_content_class = div_tag_with_content_wrapper_id.find_all(
        "div", attrs={"class": "content"}
    )[2]

    table_tag = div_tag_with_content_class.find("table", attrs={
        "class": "common-table"
    })
    tbody_tag = table_tag.find("tbody")
    table_row_tags = tbody_tag.find_all("tr")

    # Dictionary to store the scraped data
    data = {
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
        data["month"].append(month)

        # Extract the average no. of players from that month
        avg_players = table_data_tags[1].get_text()
        data["avg_players"].append(avg_players)

        # Extract the gain of average no. of players from previous month
        gain = table_data_tags[2].get_text()
        data["gain"].append(gain)

        # Extract the gain pct of average no. of players from previous month
        gain_pct = table_data_tags[3].get_text()
        data["gain_pct"].append(gain_pct)

        # Extract the no. of peak players from all month
        peak_players = table_data_tags[4].get_text()
        data["peak_players"].append(peak_players)

    provide_logs(
        "EXTRACT",
        description,
        "SUCCESSFUL",
        None
    )
    return {app_id: data}