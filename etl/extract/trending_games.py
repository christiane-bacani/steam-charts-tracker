"""
Python module to perform web-scraping to extract all the data related to
the current trending games tracked by Steam Charts.
"""
from bs4 import BeautifulSoup

from logs import logger

def scrape_top_5_trending_games(soup: BeautifulSoup) -> dict[str, list]:
    """
    Web-scrape the data of the current top 5 trending games on Steam Charts website.

    Args:
        soup (bs4.BeautifulSoup): The parsed BeautifulSoup object for web-scraping.

    Returns:
        dict[str, list]: The scraped data as a dictionary.
    """
    logger.info(f"Scraping the current data of the top 5 trending games.")
    body = soup.find("body")
    content_wrapper = body.find("div", attrs={"id": "content-wrapper"})
    content_class = content_wrapper.find(
        "div", attrs={"class": "content"}
    )

    table = content_class.find("table")
    tbody = table.find("tbody")
    table_rows = tbody.find_all("tr")

    scraped_data = {
        "app_id":                  [],
        "rank":                    [],
        "name":                    [],
        "twenty_four_hour_change": [],
        "current_players":         []
    }

    for rank, table_row in enumerate(table_rows):
        table_data = table_row.find_all("td")

        # Extract application ID
        app_id = table_data[0].find("a")["href"]
        app_id = str(app_id).replace("/app/", "")
        scraped_data["app_id"].append(app_id)

        # Extract rank number
        scraped_data["rank"].append(rank)

        name = table_data[0].get_text()
        scraped_data["name"].append(name)

        # Extract 24-hour change percentage
        twenty_four_hour_change = table_data[1].get_text()
        scraped_data["twenty_four_hour_change"].append(twenty_four_hour_change)

        # Extract the no. of current players
        current_players = table_data[3].get_text()
        scraped_data["current_players"].append(current_players)

    logger.info("Successfully scraped the current data of the top 5 trending games")
    return scraped_data