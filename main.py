# main.py
"""
I use this module as the main entry point for my Twitter scraper.
I authenticate to Twitter, initialize my CSV file, and then start scraping tweets.
"""

import asyncio
from utils import initialize_csv
from twitter_client import authenticate, scrape_tweets

async def main():
    """
    I begin by authenticating to Twitter.
    If authentication is successful, I initialize the CSV and then start scraping tweets.
    """
    client = await authenticate()
    if not client:
        print("❌ Authentication failed. Exiting.")
        return

    initialize_csv()
    await scrape_tweets(client)

if __name__ == "__main__":
    asyncio.run(main())

