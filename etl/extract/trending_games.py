"""
Python module to extract data about the trending games from the Steam Charts website.
"""
from bs4 import BeautifulSoup

from logs.etl_pipeline_logs import provide_logs

def extract_top_5_trending_games(soup: BeautifulSoup) -> dict[str, dict]:
    """
    Scrape the top 5 trending games from the Steam Charts website.

    Args:
        soup (BeautifulSoup): The parsed BeautifulSoup object

    Returns:
        dict: The scraped data of the top 5 trending games
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
    result = {
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
        result["app_id"].append(app_id)

        # Extract the game game
        game_name = table_data_tags[0].get_text()
        result["game_name"].append(game_name)

        # Extract the percentage of 24-hour change
        twenty_four_hour_change_pct = table_data_tags[1].get_text()
        result["twenty_four_hour_change_pct"].append(twenty_four_hour_change_pct)

        # Extract the number of current players
        current_players = table_data_tags[3].get_text()
        result["current_players"].append(current_players)

    provide_logs(
        "EXTRACT",
        "Extract top 5 trending games from https://steamcharts.com/.",
        "Successful"
    )
    return result

def extract_game_stats_overview(
        soup: BeautifulSoup,
        trending_games_stats_overview: dict[str, list]
) -> dict[str, str]:
    """
    Scrape the game statistics overview of a trending game from the Steam
    Charts website.

    Args:
        soup (BeautifulSoup): The parsed BeautifulSoup object
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