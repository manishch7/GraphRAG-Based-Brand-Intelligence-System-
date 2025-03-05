import pandas as pd
import torch
import re
from tqdm import tqdm
from snowflake.connector import connect
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline
from snowflake_connector import get_connection

# **1️⃣ Set Up GPU (MPS) for Apple Silicon**
device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
print(f"Using device: {device}")

# **2️⃣ Establish Snowflake Connection**
conn = get_connection()

# **3️⃣ Fetch Data from Snowflake (CLEAN_TWEETS)**
query = "SELECT * FROM CLEAN_TWEETS ORDER BY CREATED_AT DESC "
cursor = conn.cursor()
cursor.execute(query)
df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

# **4️⃣ Clean Text (Remove URLs)**
def remove_urls(text):
    if isinstance(text, str):
        return re.sub(r"http\S+|www\S+|bit.ly\S+", "", text).strip()
    return text

df["TEXT"] = df["CLEANED_TEXT"].apply(remove_urls)  # Rename column to match Final Tweets schema

# **5️⃣ Convert CREATED_AT to Proper Timestamp Format**
df["CREATED_AT"] = pd.to_datetime(df["CREATED_AT"])  # Ensure datetime format
df["DAY"] = df["CREATED_AT"].dt.day_name()  # Extract Day
df["DATE"] = df["CREATED_AT"].dt.date  # Extract Date
df["TIME"] = df["CREATED_AT"].dt.strftime('%H:%M:%S')  # Extract Time

# **6️⃣ Load RoBERTa Sentiment Analysis Model on GPU**
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

# **7️⃣ Apply Sentiment Analysis on GPU**
tqdm.pandas(desc="Applying Sentiment Analysis on MPS")
df["SENTIMENT"] = df["TEXT"].progress_apply(get_twitter_roberta_sentiment)

# **8️⃣ Load Zero-Shot Classification Model on GPU**
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli", device=0 if device.type == "mps" else -1)

# **9️⃣ Define Topic Categories**
brand_topics = [
    "Brand Mentions & Engagement",
    "Sentiment & Customer Feedback",
    "Marketing & Influencer Impact",
    "Competitor Analysis",
    "Consumer Trends & Hype"
]

# **🔟 Perform Zero-Shot Classification on GPU**
def zero_shot_classification(text):
    if not isinstance(text, str) or text.strip() == "":
        return "Unknown"
    
    result = classifier(text, brand_topics)
    return result["labels"][0]  # Get the top predicted topic

tqdm.pandas(desc="Applying Zero-Shot Classification on MPS")
df["TOPIC"] = df["TEXT"].progress_apply(zero_shot_classification)


# Ensure CREATED_AT is a string in Snowflake-compatible format
df["CREATED_AT"] = df["CREATED_AT"].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]

# Convert numeric values explicitly
df["TWEETS_COUNT"] = df["TWEETS_COUNT"].astype(int)
df["FOLLOWERS_COUNT"] = df["FOLLOWERS_COUNT"].astype(int)
df["RETWEET_COUNT"] = df["RETWEET_COUNT"].astype(int)
df["LIKE_COUNT"] = df["LIKE_COUNT"].astype(int)

# Ensure HASHTAGS, MENTIONS, and URLS remain as strings (avoid NULL values)
df["HASHTAGS"] = df["HASHTAGS"].fillna("")
df["MENTIONS"] = df["MENTIONS"].fillna("")
df["URLS"] = df["URLS"].fillna("")

# **1️⃣3️⃣ Define SQL Query to Insert Data into Snowflake**
insert_query = """
INSERT INTO FINAL_TWEETS (
    TWEET_ID, CREATED_AT, DAY, DATE, TIME, TEXT, USER_ID, SCREEN_NAME, NAME,
    TWEETS_COUNT, FOLLOWERS_COUNT, RETWEET_COUNT, LIKE_COUNT, HASHTAGS, MENTIONS, URLS,
    LOCATION, SENTIMENT, TOPIC
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Convert DataFrame to List of Tuples
data_to_insert = df[[
    "TWEET_ID", "CREATED_AT", "DAY", "DATE", "TIME", "TEXT", "USER_ID", "SCREEN_NAME", "NAME",
    "TWEETS_COUNT", "FOLLOWERS_COUNT", "RETWEET_COUNT", "LIKE_COUNT", "HASHTAGS", "MENTIONS", "URLS",
    "LOCATION", "SENTIMENT", "TOPIC"
]].values.tolist()

# Execute insert query (without batch processing)
cursor = conn.cursor()
cursor.executemany(insert_query, data_to_insert)
conn.commit()

print("✅ Data successfully inserted into ENRICHED_TWEETS!")

# **1️⃣6️⃣ Close Connection**
cursor.close()
conn.close()
