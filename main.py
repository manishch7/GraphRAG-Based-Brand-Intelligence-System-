import asyncio
from data_pipeline.twitter_client import authenticate, scrape_tweets
from data_pipeline.utils import log_error  # Added for error handling
from data_pipeline.enriched_tweets import process_tweets
from data_pipeline.data_loading_neo4j import load_tweets_data_into_neo4j

async def main():
    """Main entry point"""
    try:
        client = await authenticate()
        if client:
            await scrape_tweets(client)
            process_tweets()
            load_tweets_data_into_neo4j()
    except Exception as e:
        log_error("main", e)

if __name__ == "__main__":
    asyncio.run(main())