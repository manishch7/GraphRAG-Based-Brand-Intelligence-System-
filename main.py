# main.py
"""
This is the main entry point for the Twitter scraper.
It initializes the CSV file, authenticates to Twitter, and starts scraping tweets.
"""

import asyncio
from utils import initialize_csv
from twitter_client import authenticate, scrape_tweets

async def main():
    """
    Authenticates the Twitter client, initializes the CSV file, and starts scraping tweets.
    """
    client = await authenticate()
    if not client:
        print("❌ Authentication failed. Exiting.")
        return

    initialize_csv()
    await scrape_tweets(client)

if __name__ == "__main__":
    asyncio.run(main())
