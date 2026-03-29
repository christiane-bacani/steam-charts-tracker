# Steam Charts Tracker

Steam Charts Tracker is a a data processing system that extracts, transforms, and loads data from Steam Charts Website to track the current trending games, top games, top records, player concurrency data, and lastly historical player data using Power BI or Python for Data Visualization. 

## Table of contents
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)
- [Todo](#Todo)

# Todo
- Add app_id and game_name to the data to scrape for the function "extract_game_stats_overview" of the module trending_games.py
- After successfully scraping all the required data in the module trending_games.py, refactor the location of the data from dictionary to local JSON file stored in the local machine.