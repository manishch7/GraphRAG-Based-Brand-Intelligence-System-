import pandas as pd
import torch
import re
import openai
import configparser
import json
from tqdm import tqdm
from snowflake.connector import connect
from snowflake.connector.cursor import DictCursor
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline
from snowflake_connector import get_connection

# Read config file - add this where you inialize other components
config = configparser.ConfigParser()
config.read("config.ini")

# Set OpenAI API key - read from config.ini
openai.api_key = config.get("openai", "api_key")

def process_tweets():
    """Fetch, clean, analyze, and store tweets in Snowflake."""
    conn = get_connection()
    query = "SELECT * FROM CLEAN_TWEETS ORDER BY CREATED_AT DESC "
    cursor = conn.cursor()
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

    # -- Early duplicate check: Filter out tweets already in FINAL_TWEETS --
    check_query = """ SELECT TWEET_ID FROM FINAL_TWEETS WHERE CREATED_AT >= DATEADD(day, -3, CURRENT_TIMESTAMP()); """
    cursor_existing = conn.cursor()
    cursor_existing.execute(check_query)
    existing_ids = set(row[0] for row in cursor_existing.fetchall())
    cursor_existing.close()

    initial_count = len(df)
    df = df[pd.to_datetime(df["CREATED_AT"]) >= (pd.Timestamp.now() - pd.Timedelta(days=3))]
    filtered_count = len(df)
    print(f"Keeping {filtered_count} tweets from the last 3 days (filtered out {initial_count - filtered_count} older tweets).")
    
    df = df[~df["TWEET_ID"].isin(existing_ids)]
    after_dedup_count = len(df)
    print(f"Filtered out {filtered_count - after_dedup_count} duplicate tweet(s) from the last 3 days.")

    if df.empty:
        print("No new tweets to process. Exiting.")
        conn.close()
        exit()

    # **2️⃣ Set Up GPU (MPS) for Apple Silicon**
    device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
    print(f"Using device: {device}")

    # **3️⃣ Clean Text Data (Remove URLs)**
    def remove_urls(text):
        if isinstance(text, str):
            return re.sub(r"http\S+|www\S+|bit.ly\S+", "", text).strip()
        return text

    df["TEXT"] = df["CLEANED_TEXT"].apply(remove_urls)

    # **4️⃣ Convert CREATED_AT to Proper Timestamp Format**
    df["CREATED_AT"] = pd.to_datetime(df["CREATED_AT"])
    df["DAY"] = df["CREATED_AT"].dt.day_name()
    df["DATE"] = df["CREATED_AT"].dt.date
    df["TIME"] = df["CREATED_AT"].dt.strftime('%H:%M:%S')

    # **5️⃣ Load RoBERTa Sentiment Analysis Model**
    roberta_model_name = "cardiffnlp/twitter-roberta-base-sentiment"
    roberta_tokenizer = AutoTokenizer.from_pretrained(roberta_model_name)
    roberta_model = AutoModelForSequenceClassification.from_pretrained(roberta_model_name).to(device)

    SENTIMENT_LABELS = ["Negative", "Neutral", "Positive"]

    def get_twitter_roberta_sentiment(text):
        if not isinstance(text, str) or text.strip() == "":
            return "Neutral"
        tokens = roberta_tokenizer(text, return_tensors="pt", truncation=True, padding=True, max_length=128).to(device)
        with torch.no_grad():
            output = roberta_model(**tokens)
        scores = output.logits.softmax(dim=-1).tolist()[0]
        sentiment = SENTIMENT_LABELS[scores.index(max(scores))]
        return sentiment

    tqdm.pandas(desc="Applying Sentiment Analysis")
    df["SENTIMENT"] = df["TEXT"].progress_apply(get_twitter_roberta_sentiment)

    # **7️⃣ Load Zero-Shot Classification Model**
    classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli", device=0 if device.type == "mps" else -1)

    brand_topics = [
        "Brand Mentions & Engagement",
        "Sentiment & Customer Feedback",
        "Marketing & Influencer Impact",
        "Competitor Analysis",
        "Consumer Trends & Hype"
    ]

    def zero_shot_classification(text):
        if not isinstance(text, str) or text.strip() == "":
            return "Unknown"
        result = classifier(text, brand_topics)
        return result["labels"][0]

    tqdm.pandas(desc="Classifying Topics")
    df["TOPIC"] = df["TEXT"].progress_apply(zero_shot_classification)

    # **8️⃣ Generate Embeddings (store in memory, update later)**
    def get_embedding(text):
        try:
            if not isinstance(text, str) or text.strip() == "":
                return []
            response = openai.Embedding.create(input=text, model="text-embedding-3-small")
            return response['data'][0]['embedding']
        except Exception as e:
            print(f"Embedding error: {str(e)}")
            return []

    tqdm.pandas(desc="Generating Embeddings")
    df["EMBEDDING"] = df["TEXT"].progress_apply(get_embedding)

    # **9️⃣ Format Data for Insertion**
    df["CREATED_AT"] = df["CREATED_AT"].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
    df["TWEETS_COUNT"] = df["TWEETS_COUNT"].astype(int)
    df["FOLLOWERS_COUNT"] = df["FOLLOWERS_COUNT"].astype(int)
    df["RETWEET_COUNT"] = df["RETWEET_COUNT"].astype(int)
    df["LIKE_COUNT"] = df["LIKE_COUNT"].astype(int)
    df["HASHTAGS"] = df["HASHTAGS"].fillna("")
    df["MENTIONS"] = df["MENTIONS"].fillna("")
    df["URLS"] = df["URLS"].fillna("")

    # 🔁 Insert into FINAL_TWEETS (EXCLUDE EMBEDDING)
    insert_query = """ 
    INSERT INTO FINAL_TWEETS (
        TWEET_ID, CREATED_AT, DAY, DATE, TIME, TEXT, USER_ID, SCREEN_NAME, NAME,
        TWEETS_COUNT, FOLLOWERS_COUNT, RETWEET_COUNT, LIKE_COUNT, HASHTAGS, MENTIONS, URLS,
        LOCATION, SENTIMENT, TOPIC
    ) VALUES (
        %(TWEET_ID)s, %(CREATED_AT)s, %(DAY)s, %(DATE)s, %(TIME)s, %(TEXT)s, %(USER_ID)s, %(SCREEN_NAME)s, %(NAME)s,
        %(TWEETS_COUNT)s, %(FOLLOWERS_COUNT)s, %(RETWEET_COUNT)s, %(LIKE_COUNT)s, %(HASHTAGS)s, %(MENTIONS)s, %(URLS)s,
        %(LOCATION)s, %(SENTIMENT)s, %(TOPIC)s
    )
    """

    columns = [
        "TWEET_ID", "CREATED_AT", "DAY", "DATE", "TIME", "TEXT", "USER_ID", "SCREEN_NAME", "NAME",
        "TWEETS_COUNT", "FOLLOWERS_COUNT", "RETWEET_COUNT", "LIKE_COUNT", "HASHTAGS", "MENTIONS", "URLS",
        "LOCATION", "SENTIMENT", "TOPIC"
    ]

    data_to_insert = df[columns].to_dict(orient="records")

    cursor = conn.cursor(DictCursor)
    for i, row in enumerate(data_to_insert):
        cursor.execute(insert_query, row)
        if i % 100 == 0:
            conn.commit()
    conn.commit()
    print("✅ Data inserted without embeddings.")

    # ✨ NEW: Update embeddings separately
    update_embeddings_variant(df, conn)

    cursor.close()
    conn.close()

#  NEW FUNCTION: Update embeddings separately

def update_embeddings_variant(df, conn):
    print("🔁 Updating EMBEDDING column in FINAL_TWEETS...")
    cursor = conn.cursor()
    for i, row in df.iterrows():
        tweet_id = row["TWEET_ID"]
        embedding = row["EMBEDDING"]
        if not embedding:
            continue
        try:
            cursor.execute(
                """
                UPDATE FINAL_TWEETS
                SET EMBEDDING = PARSE_JSON(%s)
                WHERE TWEET_ID = %s AND EMBEDDING IS NULL
                """,
                (json.dumps(embedding), tweet_id)
            )
            if i % 100 == 0:
                conn.commit()
        except Exception as e:
            print(f"❌ Failed to update tweet {tweet_id}: {str(e)}")
    conn.commit()
    cursor.close()
    print("✅ EMBEDDING updates complete.")
