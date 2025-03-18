import asyncio
from twitter_client import authenticate, scrape_tweets
from utils import log_error  # Added for error handling
from Enriched_Tweets import process_tweets
from Data_Loading_Neo4j import load_tweets_data_into_neo4j

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