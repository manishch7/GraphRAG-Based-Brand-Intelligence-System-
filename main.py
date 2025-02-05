
import asyncio
import random
import csv
import os
import logging
from datetime import datetime, timedelta, timezone
from twikit import Client, TooManyRequests, BadRequest, Unauthorized, Forbidden, NotFound
import re

# ============================
# Configuration & Constants
# ============================
BRAND = "Nike"                   # Brand to search for
MINIMUM_TWEETS = 25              # Total tweets to fetch
CSV_FILE = "niketweets.csv"      # CSV file to store tweets
DEFAULT_WAIT_TIME = 60           # Default wait (in seconds) for unknown rate limits


# Delay settings (in seconds)
SHORT_DELAY_RANGE = (4, 8)       # Delay between processing individual tweets
LONG_DELAY_RANGE = (12, 24)      # Delay between consecutive API requests


# Set up logging to record errors
logging.basicConfig(
    filename="scraper_errors.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_date_range_query(brand: str) -> str:
    """
    Constructs a query string to fetch tweets from the past 30 days.
    Uses Twitter's advanced search operators:
      - 'since:' and 'until:' to define the date range
      - '-filter:retweets' to exclude retweets
      - 'lang:en' to limit tweets to English (adjust as needed)
    """
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=5)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    return f"{brand} since:{start_str} until:{end_str} -filter:retweets lang:en"


# Build the query dynamically.
QUERY = get_date_range_query(BRAND)

async def apply_delay(delay_range: tuple):
    """
    Asynchronously applies a random delay within the specified range.
    """
    delay = random.randint(*delay_range)
    print(f"⏳ Waiting for {delay} seconds...")
    await asyncio.sleep(delay)

def initialize_csv():
    """
    Creates the CSV file with headers if it doesn't already exist.
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
    Loads tweet IDs from the CSV file into a set for quick duplicate checks.
    """
    tweet_ids = set()
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            for row in reader:
                if row:
                    tweet_ids.add(row[0])
    return tweet_ids

def extract_hashtags(text):
    """Return a list of hashtags found in the text."""
    # This will match words preceded by a '#'
    return re.findall(r'#\w+', text)

def extract_mentions(text):
    """Return a list of user mentions found in the text."""
    # This matches words preceded by an '@'
    return re.findall(r'@\w+', text)

def extract_urls(text):
    """Return a list of URLs found in the text."""
    # This is a basic URL matching regex
    return re.findall(r'https?://\S+', text)

def process_tweet(tweet) -> list:
    """
    Processes a tweet object and returns a list of its properties,
    which are then saved to the CSV.
    """
    # Extract hashtags, mentions, and URLs from the tweet text using regex.
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
    Determines how long to wait when a rate limit error occurs.
    Returns the wait time in seconds.
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
    Logs errors with details to the log file and prints them.
    """
    error_message = f"❌ Error in {function_name}: {error}"
    logging.error(error_message)
    print(error_message)

# ============================
# Twitter Scraping Functions
# ============================
async def authenticate() -> Client:
    """
    Authenticates to Twitter using stored cookies and returns the client.
    If cookies.json doesn't exist, prompts for login, saves cookies, and then continues.
    """
    try:
        client = Client(language="en-US")

        if os.path.exists("cookies.json"):
            client.load_cookies("cookies.json")
            print("✅ Cookies loaded, authentication assumed successful.")
        else:
            print("⚠️ No cookies found. Logging in manually...")
            client.login("your_username", "your_password")  # Login manually
            client.dump_cookies("cookies.json")  # Save cookies for future use
            print("✅ Login successful, cookies saved.")

        return client

    except Exception as e:
        log_error("authenticate", e)  # Ensure log_error is defined.
        return None

async def fetch_tweets(client: Client, max_id: int = None ):

    if max_id:
        print(f"{datetime.now(timezone.utc)} - Fetching tweets with max_id: {max_id}")
        tweets = await client.search_tweet(QUERY, product="Latest", max_id=max_id)
    else:
        print(f"{datetime.now(timezone.utc)} - Fetching tweets with no max_id")
        tweets = await client.search_tweet(QUERY, product="Latest")
    return tweets

async def scrape_tweets(client: Client):
    """
    Scrapes tweets until at least MINIMUM_TWEETS have been collected.
    Checks for duplicates using an in-memory set.
    """
    tweet_count = 0 # Initialize the tweet counter
    existing_ids = load_existing_tweet_ids() # Load existing tweet IDs
    max_id = None 

    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        while tweet_count < MINIMUM_TWEETS:
            try:
                tweets = await fetch_tweets(client, max_id )
                if not tweets:
                    print(f"{datetime.now(timezone.utc)} - No more tweets found. Stopping.")
                    break

                for tweet in tweets:
                    # Skip duplicate tweets
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

                # Update max_id to fetch tweets older than the ones just received.
                # We assume tweets are returned in descending order (newest first).
                # Using the smallest tweet.id minus one ensures we don't get the same tweet again.
                max_id = min(int(tweet.id) for tweet in tweets) - 1

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

# ============================
# Main Entry Point
# ============================
async def main():
    """
    Main function: authenticates the client, initializes the CSV, and starts scraping.
    """
    client = await authenticate()
    if not client:
        print("❌ Authentication failed. Exiting.")
        return

    initialize_csv()
    await scrape_tweets(client)

if __name__ == "__main__":
    asyncio.run(main())