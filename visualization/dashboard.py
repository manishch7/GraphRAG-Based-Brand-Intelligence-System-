import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from connectors.snowflake_connector import get_connection
from connectors.neo4j_connector import get_driver

# Page setup
st.set_page_config(page_title="Brand Analytics Dashboard", layout="wide")
st.title("Sportswear Brand Analytics Dashboard")

# Sidebar filters
st.sidebar.header("Filters")
date_range = st.sidebar.selectbox(
    "Time Period",
    ["Last 7 days", "Last 30 days", "Last 90 days", "All time"],
    index=2  # Default to last 90 days
)

brands = st.sidebar.multiselect(
    "Select Brands",
    ["Nike", "Adidas", "Puma"],
    default=["Nike", "Adidas", "Puma"]
)

# Add view selector
views = ["Brand Overview", "Sentiment Analysis", "Topic Analysis"]
selected_view = st.sidebar.radio("Select View", views)

# Connect to databases
@st.cache_resource
def init_connections():
    snowflake_conn = get_connection()
    neo4j_driver = get_driver()
    return snowflake_conn, neo4j_driver

snowflake_conn, neo4j_driver = init_connections()

# Helper function to create where clause for queries
def get_where_clause():
    # Date filter
    if date_range == "Last 7 days":
        date_filter = "DATE >= DATEADD(day, -7, CURRENT_DATE())"
    elif date_range == "Last 30 days":
        date_filter = "DATE >= DATEADD(day, -30, CURRENT_DATE())"
    elif date_range == "Last 90 days":
        date_filter = "DATE >= DATEADD(day, -90, CURRENT_DATE())"
    else:
        date_filter = "1=1"  # All time
    
    # Brand filter
    brand_conditions = []
    for brand in brands:
        brand_conditions.append(f"TEXT ILIKE '%{brand}%'")
    
    if brand_conditions:
        brand_filter = "(" + " OR ".join(brand_conditions) + ")"
    else:
        brand_filter = "1=1"
    
    return f"WHERE {date_filter} AND {brand_filter}"

# Data loading functions
@st.cache_data(ttl=300)
def get_brand_sentiment():
    where_clause = get_where_clause()
    
    query = f"""
    SELECT 
        CASE 
            WHEN TEXT ILIKE '%Nike%' THEN 'Nike'
            WHEN TEXT ILIKE '%Adidas%' THEN 'Adidas'
            WHEN TEXT ILIKE '%Puma%' THEN 'Puma'
            ELSE 'Other'
        END as BRAND,
        SENTIMENT,
        COUNT(*) as COUNT
    FROM FINAL_TWEETS
    {where_clause}
    GROUP BY BRAND, SENTIMENT
    ORDER BY BRAND, SENTIMENT
    """
    
    try:
        cursor = snowflake_conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        
        return pd.DataFrame(results, columns=["Brand", "Sentiment", "Count"])
    except Exception as e:
        st.error(f"Error fetching brand sentiment: {str(e)}")
        return pd.DataFrame(columns=["Brand", "Sentiment", "Count"])

@st.cache_data(ttl=300)
def get_topic_distribution():
    where_clause = get_where_clause()
    
    query = f"""
    SELECT 
        TOPIC,
        COUNT(*) as COUNT
    FROM FINAL_TWEETS
    {where_clause}
    GROUP BY TOPIC
    ORDER BY COUNT DESC
    """
    
    try:
        cursor = snowflake_conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        
        return pd.DataFrame(results, columns=["Topic", "Count"])
    except Exception as e:
        st.error(f"Error fetching topic distribution: {str(e)}")
        return pd.DataFrame(columns=["Topic", "Count"])

@st.cache_data(ttl=300)
def get_top_tweets():
    where_clause = get_where_clause()
    
    query = f"""
    SELECT 
        TEXT,
        SCREEN_NAME,
        CREATED_AT,
        SENTIMENT,
        LIKE_COUNT,
        RETWEET_COUNT
    FROM FINAL_TWEETS
    {where_clause}
    ORDER BY (LIKE_COUNT + RETWEET_COUNT) DESC
    LIMIT 10
    """
    
    try:
        cursor = snowflake_conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        
        return pd.DataFrame(results, columns=["Text", "User", "Date", "Sentiment", "Likes", "Retweets"])
    except Exception as e:
        st.error(f"Error fetching top tweets: {str(e)}")
        return pd.DataFrame(columns=["Text", "User", "Date", "Sentiment", "Likes", "Retweets"])

# Display based on selected view
if selected_view == "Brand Overview":
    # Load data
    brand_sentiment = get_brand_sentiment()
    top_tweets = get_top_tweets()
    
    # Calculate brand share of voice
    if not brand_sentiment.empty:
        share_of_voice = brand_sentiment.groupby('Brand')['Count'].sum().reset_index()
        total_mentions = share_of_voice['Count'].sum()
        share_of_voice['Percentage'] = (share_of_voice['Count'] / total_mentions * 100).round(1)
        
        # Display metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Brand Mentions", f"{total_mentions:,}")
        
        with col2:
            positive_count = brand_sentiment[brand_sentiment['Sentiment'] == 'Positive']['Count'].sum()
            positive_pct = (positive_count / total_mentions * 100).round(1) if total_mentions > 0 else 0
            st.metric("Positive Sentiment", f"{positive_pct}%")
        
        with col3:
            top_brand = share_of_voice.iloc[0]['Brand']
            top_pct = share_of_voice.iloc[0]['Percentage']
            st.metric("Leading Brand", f"{top_brand} ({top_pct}%)")
        
        # Share of voice chart
        st.subheader("Brand Share of Voice")
        fig = px.pie(
            share_of_voice, 
            values="Count", 
            names="Brand",
            color="Brand",
            color_discrete_map={
                "Nike": "#FF9800",
                "Adidas": "#2196F3",
                "Puma": "#4CAF50",
                "Other": "#9E9E9E"
            }
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Sentiment by brand
        st.subheader("Sentiment by Brand")
        fig = px.bar(
            brand_sentiment,
            x="Brand",
            y="Count",
            color="Sentiment",
            color_discrete_map={
                "Positive": "#4CAF50",
                "Neutral": "#2196F3",
                "Negative": "#F44336"
            },
            barmode="group"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No brand data available for selected filters.")
    
    # Display top tweets
    st.subheader("Top Tweets")
    if not top_tweets.empty:
        for _, tweet in top_tweets.iterrows():
            sentiment_color = {
                "Positive": "#4CAF50",
                "Neutral": "#2196F3",
                "Negative": "#F44336"
            }.get(tweet["Sentiment"], "#2196F3")
            
            st.markdown(f"""
            <div style="padding: 15px; border-left: 5px solid {sentiment_color}; margin-bottom: 15px; background-color: #f8f9fa;">
                <p><strong>@{tweet['User']}</strong> • {tweet['Date']}</p>
                <p>{tweet['Text']}</p>
                <p>❤️ {tweet['Likes']} | 🔄 {tweet['Retweets']} | {tweet['Sentiment']}</p>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No tweets available for selected filters.")

elif selected_view == "Sentiment Analysis":
    # Load sentiment data
    brand_sentiment = get_brand_sentiment()
    
    if not brand_sentiment.empty:
        # Calculate totals by sentiment
        total_sentiment = brand_sentiment.groupby('Sentiment')['Count'].sum().reset_index()
        total_count = total_sentiment['Count'].sum()
        
        # Display metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            positive_count = total_sentiment[total_sentiment['Sentiment'] == 'Positive']['Count'].sum()
            positive_pct = (positive_count / total_count * 100).round(1) if total_count > 0 else 0
            st.metric("Positive", f"{positive_pct}%")
        
        with col2:
            neutral_count = total_sentiment[total_sentiment['Sentiment'] == 'Neutral']['Count'].sum()
            neutral_pct = (neutral_count / total_count * 100).round(1) if total_count > 0 else 0
            st.metric("Neutral", f"{neutral_pct}%")
        
        with col3:
            negative_count = total_sentiment[total_sentiment['Sentiment'] == 'Negative']['Count'].sum()
            negative_pct = (negative_count / total_count * 100).round(1) if total_count > 0 else 0
            st.metric("Negative", f"{negative_pct}%")
        
        # Overall sentiment pie chart
        st.subheader("Overall Sentiment Distribution")
        fig = px.pie(
            total_sentiment,
            values="Count",
            names="Sentiment",
            color="Sentiment",
            color_discrete_map={
                "Positive": "#4CAF50",
                "Neutral": "#2196F3",
                "Negative": "#F44336"
            },
            hole=0.4
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Sentiment by brand
        st.subheader("Sentiment by Brand")
        fig = px.bar(
            brand_sentiment,
            x="Brand",
            y="Count",
            color="Sentiment",
            color_discrete_map={
                "Positive": "#4CAF50",
                "Neutral": "#2196F3",
                "Negative": "#F44336"
            },
            barmode="group"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No sentiment data available for selected filters.")

elif selected_view == "Topic Analysis":
    # Load topic data
    topic_data = get_topic_distribution()
    
    if not topic_data.empty:
        # Display topic distribution
        st.subheader("Topic Distribution")
        fig = px.bar(
            topic_data,
            x="Topic",
            y="Count",
            color="Count",
            color_continuous_scale="Viridis"
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        
        # Topic pie chart
        st.subheader("Topic Breakdown")
        fig = px.pie(
            topic_data,
            values="Count",
            names="Topic",
            hole=0.4
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No topic data available for selected filters.")

    # Neo4j query for hashtags
    try:
        # Define date filter in Neo4j format
        if date_range == "Last 7 days":
            date_clause = "date(t.date) >= date() - duration('P7D')"
        elif date_range == "Last 30 days":
            date_clause = "date(t.date) >= date() - duration('P30D')"
        elif date_range == "Last 90 days":
            date_clause = "date(t.date) >= date() - duration('P90D')"
        else:
            date_clause = "1=1"  # All time
        
        # Simple query to get hashtag counts
        query = f"""
        MATCH (t:Tweet)-[:CONTAINS_HASHTAG]->(h:Hashtag)
        WHERE {date_clause}
        RETURN h.tag AS hashtag, COUNT(t) AS count
        ORDER BY count DESC
        LIMIT 15
        """
        
        with neo4j_driver.session() as session:
            result = session.run(query)
            records = [dict(record) for record in result]
            
            if records:
                hashtag_data = pd.DataFrame(records)
                
                st.subheader("Top Hashtags")
                fig = px.bar(
                    hashtag_data,
                    x="hashtag",
                    y="count",
                    color="count",
                    color_continuous_scale="Viridis"
                )
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No hashtag data available")
    except Exception as e:
        st.error(f"Error fetching hashtag data: {str(e)}")

# Show timestamp
st.sidebar.markdown(f"Last updated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")