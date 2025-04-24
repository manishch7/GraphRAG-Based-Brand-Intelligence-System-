# add_tweet_embeddings.py
import time
import asyncio
import configparser
import openai
from connectors.neo4j_connector import get_driver
from config import NEO4J_DATABASE
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper_errors.log"),
        logging.StreamHandler()  # This will output to console as well
    ]
)

# Read config file
config = configparser.ConfigParser()
config.read("config.ini")

# Set OpenAI API key
openai.api_key = config.get("openai", "api_key")
print(f"OpenAI API key loaded: {openai.api_key[:5]}...")

async def create_vector_index():
    """Create vector index in Neo4j for tweet embeddings"""
    print("Creating vector index...")
    driver = get_driver()
    with driver.session(database=NEO4J_DATABASE) as session:
        # Check if index already exists
        result = session.run("""
        SHOW INDEXES
        YIELD name, type
        WHERE name = 'tweet_embeddings' AND type = 'VECTOR'
        RETURN count(*) as count
        """)
        
        if result.single()["count"] > 0:
            print("Vector index 'tweet_embeddings' already exists")
            logging.info("Vector index 'tweet_embeddings' already exists")
            return
        
        # Create the index
        print("Creating new vector index...")
        session.run("""
        CREATE VECTOR INDEX tweet_embeddings
        FOR (t:Tweet)
        ON (t.embedding)
        OPTIONS {
            indexConfig: {
                `vector.dimensions`: 1536,
                `vector.similarity_function`: 'cosine'
            }
        }
        """)
        print("Vector index 'tweet_embeddings' created successfully")
        logging.info("Vector index 'tweet_embeddings' created successfully")

async def get_total_tweets_without_embeddings():
    """Get the total count of tweets without embeddings"""
    print("Checking for tweets without embeddings...")
    driver = get_driver()
    with driver.session(database=NEO4J_DATABASE) as session:
        result = session.run("""
        MATCH (t:Tweet)
        WHERE t.embedding IS NULL
        RETURN count(t) as total
        """)
        total = result.single()["total"]
        print(f"Found {total} tweets without embeddings")
        return total

async def generate_embeddings(texts):
    """Generate embeddings using OpenAI API"""
    print(f"Generating embeddings for {len(texts)} texts...")
    try:
        # For older OpenAI package
        response = openai.Embedding.create(
            input=texts,
            model="text-embedding-3-small"
        )
        print(f"Successfully generated {len(response['data'])} embeddings")
        return [data["embedding"] for data in response["data"]]
    except Exception as e:
        print(f"ERROR generating embeddings: {str(e)}")
        logging.error(f"Error generating embeddings: {str(e)}")
        # Add delay in case of rate limiting
        await asyncio.sleep(5)
        return None

async def update_tweet_embeddings(tweet_id, embedding):
    """Update a tweet node with its embedding"""
    print(f"Updating tweet {tweet_id} with embedding...")
    driver = get_driver()
    with driver.session(database=NEO4J_DATABASE) as session:
        try:
            session.run("""
            MATCH (t:Tweet {tweet_id: $id})
            CALL db.create.setNodeVectorProperty(t, 'embedding', $embedding)
            RETURN t
            """, id=tweet_id, embedding=embedding)
            print(f"Successfully updated tweet {tweet_id}")
            return True
        except Exception as e:
            print(f"ERROR updating tweet {tweet_id}: {str(e)}")
            logging.error(f"Error updating tweet {tweet_id}: {str(e)}")
            return False

async def process_in_batches(batch_size=10):
    """Process tweets in batches to add embeddings"""
    total = await get_total_tweets_without_embeddings()
    if total == 0:
        print("No tweets found without embeddings. All tweets are already processed.")
        logging.info("No tweets found without embeddings. All tweets are already processed.")
        return
        
    print(f"Found {total} tweets without embeddings. Processing in batches of {batch_size}...")
    logging.info(f"Found {total} tweets without embeddings. Processing in batches of {batch_size}...")
    
    processed = 0
    while processed < total:
        # Get a batch of tweets
        print(f"Fetching batch of tweets ({processed}/{total} processed so far)...")
        driver = get_driver()
        with driver.session(database=NEO4J_DATABASE) as session:
            result = session.run("""
            MATCH (t:Tweet)
            WHERE t.embedding IS NULL
            RETURN t.tweet_id AS id, t.text AS text
            LIMIT $batch_size
            """, batch_size=batch_size)
            
            tweets = [(record["id"], record["text"]) for record in result]
        
        if not tweets:
            print("No more tweets to process, exiting loop")
            break
            
        # Generate embeddings
        print(f"Processing batch of {len(tweets)} tweets...")
        texts = [text for _, text in tweets]
        embeddings = await generate_embeddings(texts)
        
        if not embeddings:
            print("Failed to generate embeddings. Retrying after delay...")
            logging.warning("Failed to generate embeddings. Retrying after delay...")
            await asyncio.sleep(10)
            continue
        
        # Update tweets with embeddings
        success_count = 0
        for i, (tweet_id, _) in enumerate(tweets):
            success = await update_tweet_embeddings(tweet_id, embeddings[i])
            if success:
                success_count += 1
                processed += 1
        
        print(f"Batch complete: {success_count}/{len(tweets)} successful. Total progress: {processed}/{total}")
        
        # Add a delay to respect API rate limits
        await asyncio.sleep(1)
    
    print(f"Embedding migration complete! Added embeddings to {processed} tweets.")
    logging.info(f"Embedding migration complete! Added embeddings to {processed} tweets.")

async def main():
    """Main entry point for the script"""
    start_time = time.time()
    print("=" * 50)
    print("Starting embedding migration process...")
    logging.info("Starting embedding migration process...")
    
    # Create vector index first
    await create_vector_index()
    
    # Process tweets in batches
    await process_in_batches(batch_size=10)
    
    elapsed_time = time.time() - start_time
    print(f"Embedding migration completed in {elapsed_time:.2f} seconds")
    print("=" * 50)
    logging.info(f"Embedding migration completed in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    print("Script started!")
    asyncio.run(main())