# utils.py
"""
I use this module to define utility functions that help with various tasks
like handling delays, CSV operations, data extraction from tweets, and error logging.
"""

import asyncio
import random
import csv
import os
import re
from datetime import datetime, timezone
from config import CSV_FILE, SHORT_DELAY_RANGE, LONG_DELAY_RANGE, DEFAULT_WAIT_TIME
import logging

async def apply_delay(delay_range: tuple):
    """
    I use this function to apply a random delay within a specified range.
    This helps me avoid hitting rate limits when making API requests.
    """
    delay = random.randint(*delay_range)
    print(f"⏳ Waiting for {delay} seconds...")
    await asyncio.sleep(delay)

def initialize_csv():
    """
    I create the CSV file with appropriate headers if it doesn't already exist.
    This file will store all the tweet data I scrape.
    """
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "tweet_id", "created_at", "text", "user_id", "screen_name", "verified",
                "followers_count", "retweet_count", "like_count",
                "hashtags", "mentions", "urls", "language"
            ])

def load_existing_tweet_ids() -> set:
    """
    I load tweet IDs from the CSV file into a set so that I can quickly check for duplicates.
    """
    tweet_ids = set()
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header row
            for row in reader:
                if row:
                    tweet_ids.add(row[0])
    return tweet_ids

def extract_hashtags(text: str):
    """I extract and return a list of hashtags from the provided text."""
    return re.findall(r'#\w+', text)

def extract_mentions(text: str):
    """I extract and return a list of user mentions from the provided text."""
    return re.findall(r'@\w+', text)

def extract_urls(text: str):
    """I extract and return a list of URLs from the provided text."""
    return re.findall(r'https?://\S+', text)

def process_tweet(tweet) -> list:
    """
    I process a tweet object by extracting its properties and relevant data.
    The processed data is returned as a list which will be stored in the CSV.
    """
    hashtags = extract_hashtags(tweet.full_text)
    mentions = extract_mentions(tweet.full_text)
    urls = extract_urls(tweet.full_text)
    
    return [
        tweet.id,
        tweet.created_at,
        tweet.full_text,
        tweet.user.id,
        tweet.user.screen_name,
        tweet.user.verified,
        tweet.user.followers_count,
        tweet.retweet_count,
        tweet.favorite_count,
        hashtags,
        mentions,
        urls,
        tweet.lang
    ]

def handle_rate_limit(exception) -> float:
    """
    When I hit a rate limit error, I use this function to calculate how long to wait.
    I try to extract the reset time from the exception, and if that fails, I use a default wait time.
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
    I attempt to extract the rate limit reset time from the exception headers.
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
    I log errors both to the console and to a file. This helps me keep track of issues.
    """
    error_message = f"❌ Error in {function_name}: {error}"
    logging.error(error_message)
    print(error_message)
