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
import pytz
from dateutil import parser

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
    hashtags = re.findall(r'#\w+', text)
    return ', '.join(hashtags) if hashtags else 'NoHashtags'

def extract_mentions(text: str) -> str:
    """Extract mentions as comma-separated string"""
    mentions = re.findall(r'@\w+', text)
    return ', '.join(mentions) if mentions else 'NoMentions'

def extract_urls(text: str) -> str:
    """Extract URLs as comma-separated string"""
    urls = re.findall(r'https?://\S+', text)
    return ', '.join(urls) if urls else 'NoURLs'

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
        tweet.user.location if tweet.user.location else 'NoLocation'
    ]

def log_error(function_name: str, error):
    """
    Logs errors with details to both a log file and the console.
    This aids in tracking and debugging issues.
    """
    error_message = f"❌ Error in {function_name}: {error}"
    logging.error(error_message)
    print(error_message)
