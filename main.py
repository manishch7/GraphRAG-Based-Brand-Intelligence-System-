import asyncio
from twitter_client import authenticate, scrape_tweets
from utils import log_error  # Added for error handling

async def main():
    """Main entry point"""
    try:
        client = await authenticate()
        if client:
            await scrape_tweets(client)
    except Exception as e:
        log_error("main", e)

if __name__ == "__main__":
    asyncio.run(main())