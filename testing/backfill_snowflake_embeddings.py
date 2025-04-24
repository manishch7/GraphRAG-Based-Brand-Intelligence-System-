#!/usr/bin/env python3
# backfill_embeddings.py
import asyncio
import json
from connectors.neo4j_connector import get_driver
from connectors.snowflake_connector import get_connection
from config import NEO4J_DATABASE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("backfill_embeddings.log"),
        logging.StreamHandler()  # This will output to console as well
    ]
)

async def extract_embeddings_from_neo4j():
    """Extract tweet embeddings from Neo4j"""
    print("Extracting tweet embeddings from Neo4j...")
    driver = get_driver()
    embeddings_data = []
    
    with driver.session(database=NEO4J_DATABASE) as session:
        # Only fetch tweets that have embeddings
        result = session.run("""
        MATCH (t:Tweet)
        WHERE t.embedding IS NOT NULL
        RETURN t.tweet_id AS tweet_id, t.embedding AS embedding
        """)
        
        for record in result:
            tweet_id = record["tweet_id"]
            embedding = record["embedding"]
            embeddings_data.append((tweet_id, embedding))
    
    print(f"Extracted embeddings for {len(embeddings_data)} tweets from Neo4j")
    return embeddings_data

def update_snowflake_embeddings(embeddings_data):
    """Update Snowflake Final_Tweets table with embeddings"""
    print(f"Updating Snowflake with {len(embeddings_data)} tweet embeddings...")
    
    # Connect to Snowflake
    conn = get_connection()
    cursor = conn.cursor()
    
    # First, verify if EMBEDDING column exists and create it if not
    try:
        cursor.execute(f"DESC TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FINAL_TWEETS")
        columns = [row[0].upper() for row in cursor.fetchall()]
        
        if "EMBEDDING" not in columns:
            print("EMBEDDING column does not exist in FINAL_TWEETS table. Creating it now...")
            cursor.execute(f"""
            ALTER TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FINAL_TWEETS 
            ADD COLUMN EMBEDDING VARIANT
            """)
            print("EMBEDDING column created.")
    except Exception as e:
        print(f"Error checking/creating table schema: {str(e)}")
        return False
    
    # Prepare for batch updates
    batch_size = 100
    total_batches = (len(embeddings_data) + batch_size - 1) // batch_size
    successful_updates = 0
    
    for i in range(0, len(embeddings_data), batch_size):
        batch = embeddings_data[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{total_batches} ({len(batch)} tweets)")
        
        for tweet_id, embedding in batch:
            try:
                # Convert the embedding to a JSON string
                embedding_json = json.dumps(embedding)
                
                # Update the tweet row with the embedding
                cursor.execute(f"""
                UPDATE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FINAL_TWEETS
                SET EMBEDDING = PARSE_JSON('{embedding_json}')
                WHERE TWEET_ID = '{tweet_id}'
                """)
                
                successful_updates += 1
                
                if successful_updates % 10 == 0:
                    print(f"Progress: {successful_updates}/{len(embeddings_data)} tweets updated")
                
            except Exception as e:
                print(f"Error updating tweet {tweet_id}: {str(e)}")
                logging.error(f"Error updating tweet {tweet_id}: {str(e)}")
                
                # Try an alternative approach for this tweet
                try:
                    # Use named parameters instead of string interpolation
                    cursor.execute(f"""
                    UPDATE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FINAL_TWEETS
                    SET EMBEDDING = PARSE_JSON(%s)
                    WHERE TWEET_ID = %s
                    """, (embedding_json, tweet_id))
                    
                    successful_updates += 1
                    print(f"Alternative approach succeeded for tweet {tweet_id}")
                except Exception as alt_e:
                    print(f"Alternative approach also failed for tweet {tweet_id}: {str(alt_e)}")
                    logging.error(f"Alternative approach also failed for tweet {tweet_id}: {str(alt_e)}")
    
    # Commit all updates
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Successfully updated {successful_updates} out of {len(embeddings_data)} tweets in Snowflake")
    return successful_updates > 0

async def main():
    """Main entry point for the backfill script"""
    print("=" * 50)
    print("Starting embedding backfill process from Neo4j to Snowflake...")
    
    # Extract embeddings from Neo4j
    embeddings_data = await extract_embeddings_from_neo4j()
    
    if not embeddings_data:
        print("No embeddings found in Neo4j. Exiting.")
        return
    
    # Update Snowflake with the embeddings
    success = update_snowflake_embeddings(embeddings_data)
    
    if success:
        print("Embedding backfill completed successfully!")
    else:
        print("Embedding backfill completed with errors. Check the logs for details.")
    
    print("=" * 50)

if __name__ == "__main__":
    print("Backfill script started!")
    asyncio.run(main())