# twitter_client.py
"""
This module handles Twitter API interactions using Twikit.
It includes functions for authentication, fetching tweets based on a search query,
and scraping tweets using cursor-based pagination.
"""

import asyncio
import os
from datetime import datetime, timezone
from twikit import Client
from utils import log_error, apply_delay, load_existing_tweet_ids, process_tweet
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
    try:
        tweets_result = await client.search_tweet(QUERY, product="Latest")  # Ensures latest tweets are fetched
        return tweets_result
    except Exception as e:
        log_error("fetch_tweets", e)
        print(f"❌ Error fetching tweets: {e}")
        return []

async def scrape_tweets(client: Client):
    """Modified version for Snowflake inserts"""
    tweet_count = 0
    existing_ids = load_existing_tweet_ids()

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                tweets_result = await fetch_tweets(client)

                while tweet_count < MINIMUM_TWEETS:
                    if not tweets_result or len(tweets_result) == 0:
                        print("❌ No more tweets found. Stopping.")
                        break

                    batch_size = 0

                    for tweet in tweets_result:
                        if str(tweet.id) in existing_ids:
                            print(f"⚠️ Skipping duplicate: {tweet.id}")
                            continue
                        
                        try:
                            print(f"Processing tweet ID: {tweet.id}")
                            data = process_tweet(tweet)
                            cur.execute(
                                f"""
                                INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.STAGING_TWEETS
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                """, 
                                data
                            )
                            conn.commit()
                            tweet_count += 1
                            batch_size += 1
                            print(f"✅ Inserted Tweet ID: {tweet.id}")
                            
                        except snowflake.connector.errors.ProgrammingError as e:
                            print(f"❌ Insert failed for {tweet.id}: {e.msg}")
                        except Exception as e:
                            print(f"❌ Error processing tweet ID {tweet.id}: {str(e)}")

                        if tweet_count >= MINIMUM_TWEETS:
                            print(f"🎯 Reached MINIMUM_TWEETS ({MINIMUM_TWEETS}). Stopping extraction.")
                            return  # Exit function
                            
                        await apply_delay(SHORT_DELAY_RANGE)
                    print(f"📦 Batch complete. Inserted {batch_size} tweets.")

                    if batch_size  >  0:
                        print(f"⏳ Applying long delay before fetching next batch...")
                        await apply_delay(LONG_DELAY_RANGE)

                    # Pagination logic
                    if tweets_result.next_cursor:
                        try:
                            tweets_result = await tweets_result.next()

                        except Exception as e:
                        
                            log_error("scrape_tweets (pagination)", e)
                            print(f"❌ Pagination failed. Applying default wait time of {DEFAULT_WAIT_TIME} seconds...")
                            await asyncio.sleep(DEFAULT_WAIT_TIME)
                            break  # Stop if pagination fails
                    else:
                        print("❌ No further tweets available. Stopping pagination.")
                        break

    except Exception as e:
        log_error("scrape_tweets", e)
        print(f"❌ Snowflake error: {str(e)}")
        print(f"⏳ Applying default wait time of {DEFAULT_WAIT_TIME} seconds before retrying...")
        await asyncio.sleep(DEFAULT_WAIT_TIME)
    finally:
        print(f"🎉 Scraping complete! Inserted {tweet_count} new tweets.")