# twitter_client.py
"""
I use this module to handle all Twitter client related operations.
This includes authentication, fetching tweets from Twitter, and scraping them using Twikit.
"""

import asyncio
from datetime import datetime, timezone
from twikit import Client, TooManyRequests, BadRequest, Unauthorized, Forbidden, NotFound
from config import QUERY
from utils import log_error, apply_delay, load_existing_tweet_ids, process_tweet, handle_rate_limit, SHORT_DELAY_RANGE, LONG_DELAY_RANGE, CSV_FILE
import csv
import os

async def authenticate() -> Client:
    """
    I authenticate to Twitter using stored cookies.
    If cookies are not available, I perform a manual login and save the cookies for future use.
    """
    try:
        client = Client(language="en-US")
        if os.path.exists("cookies.json"):
            client.load_cookies("cookies.json")
            print("✅ Cookies loaded, authentication assumed successful.")
        else:
            print("⚠️ No cookies found. Logging in manually...")
            # Replace 'your_username' and 'your_password' with your actual credentials
            client.login("your_username", "your_password")
            client.dump_cookies("cookies.json")
            print("✅ Login successful, cookies saved.")
        return client
    except Exception as e:
        log_error("authenticate", e)
        return None

async def fetch_tweets(client: Client):
    """
    I fetch tweets from Twitter based on my search query.
    I use Twikit’s cursor mechanism to handle pagination.
    """
    print(f"{datetime.now(timezone.utc)} - Fetching tweets")
    tweets_result = await client.search_tweet(QUERY, product="Latest")
    return tweets_result

async def scrape_tweets(client: Client):
    """
    I scrape tweets until I have collected at least MINIMUM_TWEETS.
    I check for duplicate tweets and use the cursor to paginate through results.
    """
    tweet_count = 0
    existing_ids = load_existing_tweet_ids()

    # I open the CSV file in append mode to store tweet data.
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        tweets_result = await fetch_tweets(client)

        while tweet_count < int(QUERY and QUERY):  # Fallback in case tweet_count is zero.
            try:
                if not tweets_result or len(tweets_result) == 0:
                    print(f"{datetime.now(timezone.utc)} - No more tweets found. Stopping.")
                    break

                # I process each tweet in the current batch.
                for tweet in tweets_result:
                    if str(tweet.id) in existing_ids:
                        print(f"⚠️ Skipping duplicate tweet: {tweet.id}")
                        continue

                    writer.writerow(process_tweet(tweet))
                    existing_ids.add(str(tweet.id))
                    print(f"✅ Saved Tweet ID: {tweet.id}")
                    tweet_count += 1

                    if tweet_count >= int(QUERY and QUERY):  # Check against my MINIMUM_TWEETS.
                        break

                    await apply_delay(SHORT_DELAY_RANGE)

                print(f"📊 Total tweets scraped: {tweet_count}")
                await apply_delay(LONG_DELAY_RANGE)

                # I check if there is a next cursor to fetch more tweets.
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
