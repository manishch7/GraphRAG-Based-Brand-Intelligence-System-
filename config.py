# config.py
"""
I use this module to define my configuration and constants.
It includes settings for the Twitter scraper such as the search query, CSV file name,
delay ranges, and logging setup.
"""

import logging
from datetime import datetime, timedelta, timezone

# === Twitter Scraper Configuration ===
BRAND = "(to:@Nike OR to:@adidas OR to:@UnderArmour) -filter:retweets lang:en"  # Brand search query
MINIMUM_TWEETS = 50              # Total tweets to fetch
CSV_FILE = "niketweets.csv"      # CSV file where tweets will be stored
DEFAULT_WAIT_TIME = 60           # Default wait (in seconds) for unknown rate limits

# === Delay Settings (in seconds) ===
SHORT_DELAY_RANGE = (4, 8)       # Delay between processing individual tweets
LONG_DELAY_RANGE = (12, 24)      # Delay between consecutive API requests

# === Logging Configuration ===
logging.basicConfig(
    filename="scraper_errors.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_date_range_query(brand: str) -> str:
    """
    I use this function to construct a search query that fetches tweets from the past 5 days.
    It excludes retweets and ensures tweets are in English.
    """
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=5)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    return f"{brand} since:{start_str} until:{end_str} -filter:retweets lang:en"

# I build the search query dynamically using my brand constant.
QUERY = get_date_range_query(BRAND)
