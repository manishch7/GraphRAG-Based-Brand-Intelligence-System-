# config.py
"""
This module defines the configuration settings and constants for the Twitter scraper.
It includes settings for the search query, tweet limits, CSV file details, delay ranges,
and logging configuration.
"""
import logging
from datetime import datetime, timedelta, timezone
import configparser

config = configparser.ConfigParser()
config.read("config.ini")

# === Twitter Scraper Configuration ===
QUERY = "(-ad (@nike OR @nikestore OR @adidasfootball OR @nikefootball OR @adidas OR @adidasoriginals OR @puma OR @pumafootball OR @pumasportstyle OR #Nike OR #Adidas OR #Puma OR #NikeOriginals OR #AdidasOriginals OR #PumaSport OR #PumaFootball) -filter:retweets -filter:replies lang:en)"
   # Brand search query

MINIMUM_TWEETS = 50  # Total tweets to fetch

DEFAULT_WAIT_TIME = 60           # Default wait (in seconds) for unknown rate limits
SHORT_DELAY_RANGE = (5, 15)       # Delay between processing individual tweets
LONG_DELAY_RANGE = (30, 60)      # Delay between consecutive API requests

# === Snowflake Configuration ===
SNOWFLAKE_STAGE_TABLE = "STAGING_TWEETS"  # New line
SNOWFLAKE_USER = config.get("snowflake", "user")
SNOWFLAKE_PASSWORD = config.get("snowflake", "password")
SNOWFLAKE_ACCOUNT = config.get("snowflake", "account")
SNOWFLAKE_DATABASE = config.get("snowflake", "database")
SNOWFLAKE_SCHEMA = config.get("snowflake", "schema")
SNOWFLAKE_WAREHOUSE = config.get("snowflake", "warehouse")
SNOWFLAKE_ROLE = config.get("snowflake", "role")

# === Twikit Authentication Configuration ===
X_USERNAME = config.get("X", "username")
X_EMAIL = config.get("X", "email")
X_PASSWORD = config.get("X", "password", raw=True)

# === Logging ===
logging.basicConfig(
    filename="scraper_errors.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

