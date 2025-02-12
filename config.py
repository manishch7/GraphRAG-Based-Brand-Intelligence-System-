# config.py
"""
This module defines the configuration settings and constants for the Twitter scraper.
It includes settings for the search query, tweet limits, CSV file details, delay ranges,
and logging configuration.
"""

import logging
from datetime import datetime, timedelta, timezone

# === Twitter Scraper Configuration ===
BRAND = "(-ad (@nike OR @adidas OR @underarmour) -filter:retweets -filter:replies lang:en)"   # Brand search query

MINIMUM_TWEETS = 50        # Total tweets to fetch

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
    start_date = datetime.now(timezone.utc) - timedelta(days=5)  # 5 days ago
    start_str = start_date.strftime("%Y-%m-%d")

    return f"{brand} since:{start_str}"

# I build the search query dynamically using my brand constant.
QUERY = get_date_range_query(BRAND)
