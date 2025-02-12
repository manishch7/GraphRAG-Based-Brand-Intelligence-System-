# twitter_client.py
"""
This module handles Twitter API interactions using Twikit.
It includes functions for authentication, fetching tweets based on a search query,
and scraping tweets using cursor-based pagination.
"""

import asyncio
from datetime import datetime, timezone
from twikit import Client, TooManyRequests, BadRequest, Unauthorized, Forbidden, NotFound
from config import QUERY, MINIMUM_TWEETS
from utils import log_error, apply_delay, load_existing_tweet_ids, process_tweet, handle_rate_limit, SHORT_DELAY_RANGE, LONG_DELAY_RANGE, CSV_FILE
import csv
import os

async def authenticate() -> Client:
    """
    Authenticates to Twitter using stored cookies.
    If cookies are not available, performs a manual login and saves the cookies.
    """
    try:
        client = Client(language="en-US")
        if os.path.exists("cookies.json"):
            client.load_cookies("cookies.json")
            print("✅ Cookies loaded; authentication assumed successful.")
        else:
            print("⚠️ No cookies found; performing manual login...")
            # Replace 'your_username' and 'your_password' with actual credentials
            client.login("your_username", "your_password")
            client.dump_cookies("cookies.json")
            print("✅ Login successful; cookies saved.")
        return client
    except Exception as e:
        log_error("authenticate", e)
        return None

async def fetch_tweets(client: Client):
    """
    Fetches tweets from Twitter based on the configured search query.
    Utilizes Twikit's cursor mechanism for pagination.
    """
    print(f"{datetime.now(timezone.utc)} - Fetching tweets")
    tweets_result = await client.search_tweet(QUERY, product="Latest")  # Ensures latest tweets are fetched
    return tweets_result

async def scrape_tweets(client: Client):
    """
    Scrapes tweets until at least MINIMUM_TWEETS are collected.
    Checks for duplicate tweets and uses the cursor to paginate through results.
    """
    tweet_count = 0
    existing_ids = load_existing_tweet_ids()

    # Open the CSV file in append mode to store tweet data.
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        tweets_result = await fetch_tweets(client)

        while tweet_count < MINIMUM_TWEETS:
            try:
                if not tweets_result or len(tweets_result) == 0:
                    print(f"{datetime.now(timezone.utc)} - No more tweets found. Stopping.")
                    break

                # Process each tweet in the current batch.
                for tweet in tweets_result:
                    if str(tweet.id) in existing_ids:
                        print(f"⚠️ Skipping duplicate tweet: {tweet.id}")
                        continue

                    writer.writerow(process_tweet(tweet))
                    existing_ids.add(str(tweet.id))
                    print(f"✅ Saved Tweet ID: {tweet.id}")
                    tweet_count += 1

                    if tweet_count >= MINIMUM_TWEETS:
                        break

                    await apply_delay(SHORT_DELAY_RANGE)

                print(f"📊 Total tweets scraped: {tweet_count}")
                await apply_delay(LONG_DELAY_RANGE)

                # Use the next cursor to fetch additional tweets, if available.
                if tweets_result.next_cursor:
                    try:
                        tweets_result = await tweets_result.next()
                    except Exception as e:
                        log_error("scrape_tweets (cursor fetch)", e)
                        break
                else:
                    print(f"{datetime.now(timezone.utc)} - No further tweets available. Stopping.")
                    break

            except TooManyRequests as e:
                wait_time = handle_rate_limit(e)
                await asyncio.sleep(wait_time)
            except (BadRequest, Unauthorized, Forbidden, NotFound) as e:
                log_error("scrape_tweets (API error)", e)
                break
            except Exception as e:
                log_error("scrape_tweets (General error)", e)
                break

    print("🎉 Scraping complete! Data saved to CSV.")
