import pandas as pd
import torch
import re
from tqdm import tqdm
from snowflake.connector import connect
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline
from snowflake_connector import get_connection

# **1️⃣ Fetch Data & Check for Duplicates**
# Establish Snowflake connection || Fetch Data from Snowflake (CLEAN_TWEETS)**
# then query FINAL_TWEETS for existing TWEET_IDs and filter out duplicates.

conn = get_connection()
query = "SELECT * FROM CLEAN_TWEETS ORDER BY CREATED_AT DESC "
cursor = conn.cursor()
cursor.execute(query)
df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

# -- Early duplicate check: Filter out tweets already in FINAL_TWEETS --
check_query = "SELECT TWEET_ID FROM FINAL_TWEETS"
cursor_existing = conn.cursor()
cursor_existing.execute(check_query)
existing_ids = set(row[0] for row in cursor_existing.fetchall())
cursor_existing.close()

initial_count = len(df)
df = df[~df["TWEET_ID"].isin(existing_ids)]
filtered_count = len(df)
print(f"Filtered out {initial_count - filtered_count} duplicate tweet(s).")

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

df["TEXT"] = df["CLEANED_TEXT"].apply(remove_urls)  # Rename column to match Final Tweets schema

# **4️⃣ Convert CREATED_AT to Proper Timestamp Format**
df["CREATED_AT"] = pd.to_datetime(df["CREATED_AT"])  # Ensure datetime format
df["DAY"] = df["CREATED_AT"].dt.day_name()  # Extract Day
df["DATE"] = df["CREATED_AT"].dt.date  # Extract Date
df["TIME"] = df["CREATED_AT"].dt.strftime('%H:%M:%S')  # Extract Time

# **5️⃣ Load RoBERTa Sentiment Analysis Model on GPU**
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

# **6️⃣ Apply Sentiment Analysis on GPU**
tqdm.pandas(desc="Applying Sentiment Analysis on MPS")
df["SENTIMENT"] = df["TEXT"].progress_apply(get_twitter_roberta_sentiment)

# **7️⃣ Load Zero-Shot Classification Model on GPU**
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli", device=0 if device.type == "mps" else -1)

brand_topics = [
    "Brand Mentions & Engagement",
    "Sentiment & Customer Feedback",
    "Marketing & Influencer Impact",
    "Competitor Analysis",
    "Consumer Trends & Hype"
]

# **8️⃣ Apply Zero-Shot Classification on GPU**
def zero_shot_classification(text):
    if not isinstance(text, str) or text.strip() == "":
        return "Unknown"
    result = classifier(text, brand_topics)
    return result["labels"][0]  # Get the top predicted topic

tqdm.pandas(desc="Applying Zero-Shot Classification on MPS")
df["TOPIC"] = df["TEXT"].progress_apply(zero_shot_classification)

# **9️⃣ Format Data for Insertion into FINAL_TWEETS**
df["CREATED_AT"] = df["CREATED_AT"].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
df["TWEETS_COUNT"] = df["TWEETS_COUNT"].astype(int)
df["FOLLOWERS_COUNT"] = df["FOLLOWERS_COUNT"].astype(int)
df["RETWEET_COUNT"] = df["RETWEET_COUNT"].astype(int)
df["LIKE_COUNT"] = df["LIKE_COUNT"].astype(int)
df["HASHTAGS"] = df["HASHTAGS"].fillna("")
df["MENTIONS"] = df["MENTIONS"].fillna("")
df["URLS"] = df["URLS"].fillna("")

# **🔟 Insert Data into FINAL_TWEETS**
insert_query = """ 
INSERT INTO FINAL_TWEETS (
    TWEET_ID, CREATED_AT, DAY, DATE, TIME, TEXT, USER_ID, SCREEN_NAME, NAME,
    TWEETS_COUNT, FOLLOWERS_COUNT, RETWEET_COUNT, LIKE_COUNT, HASHTAGS, MENTIONS, URLS,
    LOCATION, SENTIMENT, TOPIC
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
data_to_insert = df[[   # Convert DataFrame to List of Tuples       
    "TWEET_ID", "CREATED_AT", "DAY", "DATE", "TIME", "TEXT", "USER_ID", "SCREEN_NAME", "NAME",
    "TWEETS_COUNT", "FOLLOWERS_COUNT", "RETWEET_COUNT", "LIKE_COUNT", "HASHTAGS", "MENTIONS", "URLS",
    "LOCATION", "SENTIMENT", "TOPIC"
]].values.tolist()

cursor = conn.cursor()  # Execute insert query (without batch processing)
cursor.executemany(insert_query, data_to_insert)
conn.commit()
print("✅ Data successfully inserted into ENRICHED_TWEETS!")

# **1️⃣1️⃣ Close Snowflake Connection**
cursor.close()
conn.close()
