# twitter_client.py
"""
This module handles Twitter API interactions using Twikit.
It includes functions for authentication, fetching tweets based on a search query,
and scraping tweets using cursor-based pagination.
"""

import asyncio
import os
from datetime import datetime, timezone
from twikit import Client, TooManyRequests, BadRequest, Unauthorized, Forbidden, NotFound
from config import QUERY, MINIMUM_TWEETS
from utils import log_error, apply_delay, load_existing_tweet_ids, process_tweet, handle_rate_limit
from snowflake_connector import get_connection
import snowflake.connector
from config import *

async def authenticate() -> Client:
    """
    Authenticates to Twitter using Twikit.
    If cookies are not available, performs a manual login and saves the cookies.
    """
    client = Client(language="en-US")
    cookies_path = "cookies.json"

    try:
        if os.path.exists(cookies_path):
            client.load_cookies(cookies_path)
            print("✅ Cookies loaded; authentication assumed successful.")
        else:
            print("🔄 No cookies found; performing manual login...")
            await client.login(
                auth_info_1=X_USERNAME,
                auth_info_2=X_EMAIL,
                password=X_PASSWORD,
                cookies_file=cookies_path
            )
            print("✅ Login successful; cookies saved.")
        return client
    except Exception as e:
        log_error("authenticate", e)
        print(f"❌ Authentication failed: {e}")
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
    """Modified version for Snowflake inserts"""
    tweet_count = 0
    existing_ids = load_existing_tweet_ids()

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                tweets_result = await fetch_tweets(client)

                while tweet_count < MINIMUM_TWEETS:
                    if not tweets_result:
                        break

                    for tweet in tweets_result:
                        if str(tweet.id) in existing_ids:
                            print(f"⚠️ Skipping duplicate: {tweet.id}")
                            continue
                            
                        data = process_tweet(tweet)
                        try:
                            cur.execute(
                                f"""
                                INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STAGING_TWEETS
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                """, 
                                data
                            )
                            conn.commit()
                            tweet_count += 1
                            print(f"✅ Inserted Tweet ID: {tweet.id}")
                            
                        except snowflake.connector.errors.ProgrammingError as e:
                            print(f"❌ Insert failed for {tweet.id}: {e.msg}")
                            
                        await apply_delay(SHORT_DELAY_RANGE)

                    # Pagination logic
                    await apply_delay(LONG_DELAY_RANGE)
                    if tweets_result.next_cursor:
                        tweets_result = await tweets_result.next()
                    else:
                        break

    except Exception as e:
        print(f"❌ Snowflake error: {str(e)}")
    finally:
        print(f"🎉 Scraping complete! Inserted {tweet_count} new tweets.")
