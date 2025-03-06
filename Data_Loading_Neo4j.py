#!/usr/bin/env python3
import traceback
import snowflake.connector
from neo4j.exceptions import ServiceUnavailable, Neo4jError

# Import connection functions from your connector files
from snowflake_connector import get_connection as get_snowflake_connection
from neo4j_connector import get_driver as get_neo4j_driver
from config import NEO4J_DATABASE

def load_tweets_data_into_neo4j():
    try:
        # Establish connections using your configured connectors
        snowflake_connection = get_snowflake_connection()
        neo4j_driver = get_neo4j_driver()
        
        # Query data from the Final_Tweets table in Snowflake
        snowflake_cursor = snowflake_connection.cursor(snowflake.connector.DictCursor)
        snowflake_cursor.execute("SELECT * FROM Final_Tweets")
        tweet_rows = snowflake_cursor.fetchall()
        print(f"Fetched {len(tweet_rows)} rows from the Final_Tweets table in Snowflake.")

        # -- Early Duplicate Check: Fetch existing tweet IDs from Neo4j --
        with neo4j_driver.session(database=NEO4J_DATABASE) as neo4j_session:
            result = neo4j_session.run("MATCH (t:Tweet) RETURN t.tweet_id AS tweet_id")
            existing_tweet_ids = {record["tweet_id"] for record in result}
        
        original_count = len(tweet_rows)
        tweet_rows = [row for row in tweet_rows if row["TWEET_ID"] not in existing_tweet_ids]
        filtered_count = len(tweet_rows)
        print(f"Filtered out {original_count - filtered_count} duplicate tweet(s) already in Neo4j.")
        
        if not tweet_rows:
            print("No new tweets to load into Neo4j. Exiting.")
            return
        
        def merge_tweet_data(tx, tweet_row):
            # Convert comma-separated string fields into lists, if not 'NO' values
            hashtag_list = []
            if tweet_row['HASHTAGS'] and tweet_row['HASHTAGS'].upper() != 'NOHASHTAGS':
                hashtag_list = [tag.strip() for tag in tweet_row['HASHTAGS'].split(',') if tag.strip()]
            
            url_list = []
            if tweet_row['URLS'] and tweet_row['URLS'].upper() != 'NOURLS':
                url_list = [url.strip() for url in tweet_row['URLS'].split(',') if url.strip()]
            
            mention_list = []
            if tweet_row['MENTIONS'] and tweet_row['MENTIONS'].upper() != 'NOMENTIONS':
                mention_list = [mention.strip() for mention in tweet_row['MENTIONS'].split(',') if mention.strip()]
            
            cypher_query = """
            // Merge User node (uniquely identified by user_id)
            MERGE (user:User {user_id: $user_id})
              ON CREATE SET user.screen_name = $user_screen_name,
                            user.name = $user_name,
                            user.tweets_count = $user_tweets_count,
                            user.followers_count = $user_followers_count
            
            // Merge Tweet node (uniquely identified by tweet_id)
            MERGE (tweet:Tweet {tweet_id: $tweet_id})
              ON CREATE SET tweet.text = $tweet_text,
                            tweet.created_at = $tweet_created_at,
                            tweet.day = $tweet_day,
                            tweet.date = $tweet_date,
                            tweet.time = $tweet_time,
                            tweet.retweet_count = $tweet_retweet_count,
                            tweet.like_count = $tweet_like_count
            
            // Create relationship between User and Tweet
            MERGE (user)-[:POSTED]->(tweet)
            
            WITH tweet, $hashtag_list AS hashtags, $url_list AS urls, $tweet_location AS location, 
                 $tweet_sentiment AS sentiment, $tweet_topic AS topic, $mention_list AS mentions
            
            // Process Hashtags
            FOREACH (hashtag IN hashtags |
                MERGE (h:Hashtag {tag: hashtag})
                MERGE (tweet)-[:CONTAINS_HASHTAG]->(h)
            )
            
            // Process URLs
            FOREACH (url IN urls |
                MERGE (u:URL {url: url})
                MERGE (tweet)-[:CONTAINS_URL]->(u)
            )
            
            // Process Location
            MERGE (loc:Location {location: location})
            MERGE (tweet)-[:ORIGINATES_FROM]->(loc)
            
            // Process Sentiment
            MERGE (s:Sentiment {label: sentiment})
            MERGE (tweet)-[:HAS_SENTIMENT]->(s)
            
            // Process Topic
            MERGE (tpc:Topic {name: topic})
            MERGE (tweet)-[:BELONGS_TO_TOPIC]->(tpc)
            
            // Process Mentions
            FOREACH (m IN mentions |
                MERGE (mnt:Mention {mention: m})
                MERGE (tweet)-[:MENTIONS]->(mnt)
            )
            """
            
            tx.run(cypher_query,
                   user_id=tweet_row['USER_ID'],
                   user_screen_name=tweet_row['SCREEN_NAME'],
                   user_name=tweet_row['NAME'],
                   user_tweets_count=tweet_row['TWEETS_COUNT'],
                   user_followers_count=tweet_row['FOLLOWERS_COUNT'],
                   tweet_id=tweet_row['TWEET_ID'],
                   tweet_text=tweet_row['TEXT'],
                   tweet_created_at=str(tweet_row['CREATED_AT']),
                   tweet_day=tweet_row['DAY'],
                   tweet_date=str(tweet_row['DATE']),
                   tweet_time=tweet_row['TIME'],
                   tweet_retweet_count=tweet_row['RETWEET_COUNT'],
                   tweet_like_count=tweet_row['LIKE_COUNT'],
                   hashtag_list=hashtag_list,
                   url_list=url_list,
                   tweet_location=tweet_row['LOCATION'],
                   tweet_sentiment=tweet_row['SENTIMENT'],
                   tweet_topic=tweet_row['TOPIC'],
                   mention_list=mention_list)
        
        # Write each tweet row into Neo4j using a session
        with neo4j_driver.session(database=NEO4J_DATABASE) as neo4j_session:
            for tweet_row in tweet_rows:
                neo4j_session.execute_write(merge_tweet_data, tweet_row)
            print(f"Loaded {len(tweet_rows)} tweets into Neo4j.")
        
        print("Data loading complete. Re-running this script will not create duplicates.")
        
    except Exception as ex:
        print("An error occurred during data loading:")
        print(ex)
        traceback.print_exc()
    finally:
        try:
            snowflake_cursor.close()
            snowflake_connection.close()
            neo4j_driver.close()
        except Exception as close_ex:
            print("Error closing connections:", close_ex)

if __name__ == "__main__":
    load_tweets_data_into_neo4j()
