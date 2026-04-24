"""
Python module to perform web-scraping to extract all the data related to
the current top 100 games (by current players) tracked by Steam Charts.
"""
from bs4 import BeautifulSoup

from logs import logger

def scrape_top100_games(soup: BeautifulSoup,
                         scraped_data: dict[str, list]) -> dict[str, list]:
    """
    Web-scrape the data of the current top 100 games (by current players) on Steam
    Charts website.

    Args:
        soup (bs4.BeautifulSoup): The parsed BeautifulSoup object for web-scraping.

    Returns:
        dict[str, list]: The scraped data as a dictionary.
    """
    if len(scraped_data["rank"]) == 0:
        n = "1-25"

    elif len(scraped_data["rank"]) == 25:
        n = "26-50"

    elif len(scraped_data["rank"]) == 50:
        n = "51-75"

    else:
        n = "76-100"

    logger.info(f"Scraping the current data of the top {n} games.")
    body = soup.find("body")
    content_wrapper = body.find("div", attrs={"id": "content-wrapper"})
    content_class = content_wrapper.find(
        "div", attrs={"class": "content"}
    )

    table = content_class.find("table")
    tbody = table.find("tbody")
    table_rows = tbody.find_all("tr")

    for table_row in table_rows:
        table_data = table_row.find_all("td")

        # Extract application ID
        app_id = table_data[1].find("a")["href"]
        app_id = str(app_id).replace("/app/", "")
        scraped_data["app_id"].append(app_id)

        # Extract rank number
        rank = table_data[0].get_text()
        scraped_data["rank"].append(rank)

        # Extract the game name
        name = table_data[1].get_text()
        scraped_data["name"].append(name)

        # Extract the no. of current players
        current_players = table_data[2].get_text()
        scraped_data["current_players"].append(current_players)

        # Extract the no. of peak players
        peak_players = table_data[4].get_text()
        scraped_data["peak_players"].append(peak_players)

        # Extract the total no. of hours played
        hours_played = table_data[5].get_text()
        scraped_data["hours_played"].append(hours_played)

    logger.info(f"Successfully scraped the current data of the top {n} games.")
    return scraped_data