# utils.py
"""
This module provides utility functions for the Twitter scraper.
It includes functions for applying delays, initializing the CSV file, 
loading existing tweet IDs, extracting data from text (hashtags, mentions, URLs),
processing tweet objects, and logging errors.
"""

import asyncio
import random
import os
import re
from datetime import datetime, timezone
from config import *
from snowflake_connector import get_connection
import logging

async def apply_delay(delay_range: tuple):
    """
    Applies a random delay within the specified range.
    This helps prevent hitting rate limits during API requests.
    """
    delay = random.randint(*delay_range)
    print(f"⏳ Waiting for {delay} seconds...")
    await asyncio.sleep(delay)

def load_existing_tweet_ids() -> set:
    """Fetch existing tweet IDs from Snowflake"""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT TWEET_ID 
                    FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE_TABLE}
                """)
                return {str(row[0]) for row in cur.fetchall()}
    except Exception as e:
        logging.error(f"Failed to load tweet IDs: {str(e)}")
        return set()

def extract_hashtags(text: str) -> str:
    """Extract hashtags as comma-separated string"""
    return ', '.join(re.findall(r'#\w+', text))

def extract_mentions(text: str) -> str:
    """Extract mentions as comma-separated string"""
    return ', '.join(re.findall(r'@\w+', text))

def extract_urls(text: str) -> str:
    """Extract URLs as comma-separated string"""
    return ', '.join(re.findall(r'https?://\S+', text))

def process_tweet(tweet) -> list:
    """Process tweet data for Snowflake insertion"""
    return [
        str(tweet.id),
        str(tweet.created_at),
        tweet.full_text,
        str(tweet.user.id),
        tweet.user.screen_name,
        tweet.user.name,
        str(tweet.user.statuses_count),
        str(tweet.user.followers_count),
        str(tweet.retweet_count),
        str(tweet.favorite_count),
        extract_hashtags(tweet.full_text),
        extract_mentions(tweet.full_text),
        extract_urls(tweet.full_text),
        tweet.user.location
    ]

def handle_rate_limit(exception) -> float:
    """
    Determines the wait time when a rate limit error occurs.
    Attempts to extract the reset time from the exception and calculates the wait time;
    otherwise, returns a default wait time.
    """
    reset_time = extract_rate_limit_reset_time(exception)
    if reset_time:
        wait_seconds = max(0, (reset_time - datetime.now(timezone.utc)).total_seconds())
        print(f"🚨 Rate limit reached. Waiting for {round(wait_seconds, 2)} seconds until {reset_time}.")
        log_error("handle_rate_limit", f"Rate limit reached. Waiting until {reset_time}.")
        return wait_seconds
    else:
        print(f"🚨 Rate limit reached. Defaulting wait time to {DEFAULT_WAIT_TIME} seconds.")
        log_error("handle_rate_limit", "Rate limit reached with unknown reset time.")
        return DEFAULT_WAIT_TIME

def extract_rate_limit_reset_time(exception) -> datetime:
    """
    Attempts to extract the rate limit reset time from the exception headers.
    """
    try:
        if hasattr(exception, "headers") and "x-rate-limit-reset" in exception.headers:
            reset_timestamp = int(exception.headers["x-rate-limit-reset"])
            return datetime.fromtimestamp(reset_timestamp, tz=timezone.utc)
    except Exception as e:
        log_error("extract_rate_limit_reset_time", e)
    return None

def log_error(function_name: str, error):
    """
    Logs errors with details to both a log file and the console.
    This aids in tracking and debugging issues.
    """
    error_message = f"❌ Error in {function_name}: {error}"
    logging.error(error_message)
    print(error_message)
