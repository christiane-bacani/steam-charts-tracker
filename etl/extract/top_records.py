"""
Python module to perform web-scraping to extract all the data related to
the current top 10 records tracked by Steam Charts.
"""
from bs4 import BeautifulSoup

from logs import logger

def scrape_top10_records(soup: BeautifulSoup) -> dict[str, list]:
    """
    Web-scrape the data of the current top 10 records on Steam Charts Website.

    Args:
        soup (bs4.BeautifulSoup): The parsed BeautifulSoup object for web-scraping.

    Returns:
        dict[str, list]: The scraped data as a dictionary.
    """
    logger.info("Scraping the current data of the top 10 records.")
    body = soup.find("body")
    content_wrapper = body.find("div", attrs={"id": "content-wrapper"})
    content_class = content_wrapper.find_all(
        "div", attrs={"class": "content"}
    )[2]

    table = content_class.find("table")
    tbody = table.find("tbody")
    table_rows = tbody.find_all("tr")

    scraped_data = {
        "app_id":       [],
        "rank":         [],
        "name":         [],
        "peak_players": [],
        "time":         []
    }

    for rank, table_row in enumerate(table_rows):
        table_data = table_row.find_all("td")

        # Extract application ID
        app_id = table_data[0].find("a")["href"]
        scraped_data["app_id"].append(app_id)

        # Extract rank number
        scraped_data["rank"].append(rank + 1)

        # Extract the game name
        name = table_data[0].get_text()
        scraped_data["name"].append(name)

        # Extract the no. of peak players
        peak_players = table_data[1].get_text()
        scraped_data["peak_players"].append(peak_players)

        # Extract the time
        time = table_data[2].get_text()
        scraped_data["time"].append(time)

    logger.info("Successfully scraped the current data of the top 10 records.")
    return scraped_data