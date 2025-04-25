import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os
import atexit
import logging
import configparser
import openai
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add the project root directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# IMPORTANT: This must be the first Streamlit command
st.set_page_config(
    page_title="Brand Analytics Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load config
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.ini")
if os.path.exists(config_path):
    config.read(config_path)
    try:
        openai.api_key = config.get("openai", "api_key")
        # If you need to get Neo4j config, add it here
        NEO4J_URI = config.get("neo4j", "uri", fallback="bolt://localhost:7687")
        NEO4J_USER = config.get("neo4j", "user", fallback="neo4j")
        NEO4J_PASSWORD = config.get("neo4j", "password", fallback="password")
        NEO4J_DATABASE = config.get("neo4j", "database", fallback="neo4j")
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        st.error(f"Configuration error: {e}")
        st.stop()
else:
    logger.error(f"Config file not found at {config_path}")
    st.error("Configuration file not found. Please check your installation.")
    st.stop()

# Import from the root-level connectors
try:
    from connectors.snowflake_connector import get_connection
    from connectors.neo4j_connector import get_driver
    from neo4j import GraphDatabase
except ImportError as e:
    logger.error(f"Import error: {e}")
    st.error(f"Error importing required modules: {e}")
    st.stop()

# Add custom CSS for better styling
st.markdown("""
<style>
    .main {
        background-color: #1e1e1e;
        color: #ffffff;
    }
    h1 {
        color: #ff9900;
        font-weight: 700;
    }
    h2 {
        color: #ff9900;
        font-weight: 600;
    }
    h3 {
        color: #5ac8fa;
        font-weight: 600;
        margin-top: 20px;
    }
    .stMetric {
        background-color: #2d2d2d;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
        color: white;
    }
    .metric-card {
        background-color: #2d2d2d;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
        margin-bottom: 20px;
    }
    .tweet-card {
        padding: 15px;
        border-left: 5px solid #4ade80;
        margin-bottom: 15px;
        background-color: #2d2d2d;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
    }
    .stSidebar {
        background-color: #1a1a1a;
        color: #ffffff;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #333333;
        border-radius: 4px 4px 0px 0px;
        padding: 10px 20px;
        border: none;
        color: #ffffff;
    }
    .stTabs [aria-selected="true"] {
        background-color: #ff9900 !important;
        color: black !important;
        font-weight: bold;
    }
    /* Navigation styling */
    .nav-button {
        background-color: #333333;
        color: white;
        padding: 10px 20px;
        border-radius: 5px;
        margin-right: 10px;
        text-align: center;
        cursor: pointer;
        transition: background-color 0.3s;
    }
    .nav-button-active {
        background-color: #ff9900 !important;
        color: black !important;
        font-weight: bold;
    }
    .refresh-btn {
        border: none;
        color: black;
        background-color: #ff9900;
        border-radius: 5px;
        padding: 10px 24px;
        font-weight: bold;
    }
    .refresh-time {
        font-size: 12px;
        color: #bbbbbb;
        font-style: italic;
    }
    /* Style the sidebar selections */
    .stSelectbox label, .stMultiSelect label {
        color: #ff9900 !important;
        font-weight: bold !important;
    }
    /* Style the Streamlit links in sidebar */
    .stSidebar a {
        color: #5ac8fa !important;
        text-decoration: none !important;
    }
    .stSidebar a:hover {
        text-decoration: underline !important;
    }
    /* Hide the default footer */
    footer {
        visibility: hidden;
    }
    /* Text color for paragraphs */
    p {
        color: #ffffff;
    }
    /* Make the refresh button stand out */
    button[data-testid="baseButton-secondary"] {
        background-color: #ff9900 !important;
        color: black !important;
        font-weight: bold !important;
    }
    /* Style the metric labels */
    [data-testid="stMetricLabel"] {
        color: #5ac8fa !important;
    }
    /* Style the metric values */
    [data-testid="stMetricValue"] {
        color: white !important;
        font-weight: bold !important;
    }
    /* Style for expanders */
    .streamlit-expanderHeader {
        background-color: #333333 !important;
        color: white !important;
        border-radius: 5px !important;
    }
    /* Style for multiselect pills */
    .stMultiSelect span[data-baseweb="tag"] {
        background-color: #ff9900 !important;
        color: black !important;
    }
    /* Style for selectbox */
    .stSelectbox div[data-baseweb="select"] div {
        background-color: #333333 !important;
        color: white !important;
    }
    /* QA section styling */
    .question-box {
        background-color: #2d2d2d;
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 20px;
        border-left: 5px solid #ff9900;
    }
    .answer-box {
        background-color: #2d2d2d;
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 20px;
        border-left: 5px solid #5ac8fa;
    }
</style>
""", unsafe_allow_html=True)

# Create a direct Neo4j driver getter instead of importing
def get_neo4j_driver():
    """Create a Neo4j driver instance"""
    try:
        driver = GraphDatabase.driver(
            NEO4J_URI, 
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
        # Test the connection
        with driver.session(database=NEO4J_DATABASE) as session:
            session.run("RETURN 1")
        return driver
    except Exception as e:
        logger.error(f"Neo4j connection error: {e}")
        st.error(f"Failed to connect to Neo4j: {e}")
        st.stop()

# Connect to databases with better error handling
@st.cache_resource
def init_connections():
    try:
        snowflake_conn = get_connection()
        neo4j_driver = get_neo4j_driver()
        return snowflake_conn, neo4j_driver, None
    except Exception as e:
        return None, None, str(e)

snowflake_conn, neo4j_driver, connection_error = init_connections()

# If there's a connection error, show the error but continue as we might use sample data
if connection_error:
    st.error(f"Failed to connect to databases: {connection_error}")
    st.warning("Displaying sample data instead")

# Create a simplified QA System that works standalone
class SimpleQASystem:
    def __init__(self):
        # Connect to Neo4j
        self.neo4j_driver = neo4j_driver
        # Ensure vector index exists
        self.ensure_vector_index_exists()
    
    def ensure_vector_index_exists(self):
        """Make sure the vector index exists before running queries"""
        try:
            with self.neo4j_driver.session(database=NEO4J_DATABASE) as session:
                # Check if index exists
                result = session.run("""
                SHOW INDEXES
                YIELD name, type
                WHERE name = 'tweet_embeddings' AND type = 'VECTOR'
                RETURN count(*) as count
                """)
                
                if result.single()["count"] == 0:
                    # Create the index if it doesn't exist
                    print("Creating vector index 'tweet_embeddings'...")
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
                    print("Vector index created successfully")
                else:
                    print("Vector index 'tweet_embeddings' already exists")
            return True
        except Exception as e:
            logger.error(f"Error creating vector index: {e}")
            print(f"Error creating vector index: {e}")
            return False
    
    def generate_embeddings(self, text):
        """Generate embeddings for the input text"""
        try:
            # Using older OpenAI API format
            res = openai.Embedding.create(
                input=text,
                model="text-embedding-3-small"
            )
            # Handle different response formats based on OpenAI version
            if hasattr(res, 'data'):
                return res.data[0].embedding
            else:
                return res["data"][0]["embedding"]
        except Exception as e:
            logger.error(f"Embedding error: {e}")
            return []
    
    def extract_keywords(self, text):
        """Extract simple keywords from the question"""
        # Simple keyword extraction without spaCy
        words = text.lower().split()
        stopwords = {'a', 'an', 'the', 'and', 'or', 'but', 'if', 'because', 'as', 'what', 
                    'when', 'where', 'how', 'why', 'which', 'who', 'whom', 'this', 'that', 
                    'these', 'those', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 
                    'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'to', 
                    'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 
                    'through', 'during', 'before', 'after', 'above', 'below', 'on', 'off'}
        return [w for w in words if w.isalpha() and w not in stopwords]
    
    def query_knowledge_graph(self, question, embedding):
        """Query Neo4j with both vector search and keyword matching"""
        # Return empty results if embedding generation failed
        if not embedding:
            return []
        
        # Extract keywords for keyword matching
        keywords = self.extract_keywords(question)
        
        # Build hybrid search query
        query = """
        // Vector search for semantic similarity
        CALL db.index.vector.queryNodes('tweet_embeddings', $topK, $embedding)
        YIELD node, score
        WITH node AS t, score AS semanticScore
        
        // Add keyword matching as additional signal
        WITH t, semanticScore,
            reduce(s=0, k IN $keywords | 
                s + CASE WHEN toLower(t.text) CONTAINS k THEN 1 ELSE 0 END
            ) AS keywordScore
            
        // Get related information
        OPTIONAL MATCH (t)<-[:POSTED]-(u:User)
        OPTIONAL MATCH (t)-[:HAS_SENTIMENT]->(s:Sentiment)
        OPTIONAL MATCH (t)-[:BELONGS_TO_TOPIC]->(topic:Topic)
        
        // Return results with combined relevance score
        RETURN 
            t.text AS tweet, 
            t.created_at AS created, 
            u.screen_name AS user,
            t.retweet_count AS retweet_count,
            t.like_count AS like_count,
            s.label AS sentiment,
            topic.name AS topic,
            t.location AS location,
            semanticScore, 
            keywordScore, 
            (semanticScore * 3 + keywordScore) AS relevance
            
        ORDER BY relevance DESC 
        LIMIT 50
        """
        
        try:
            with self.neo4j_driver.session(database=NEO4J_DATABASE) as session:
                result = session.run(query, {
                    "embedding": embedding,
                    "keywords": keywords,
                    "topK": 100
                })
                return result.data()
        except Exception as e:
            logger.error(f"Neo4j query error: {e}")
            return []
    
    def generate_answer(self, question, results):
        """Generate an answer using the LLM with context from tweets"""
        if not results:
            return "I couldn't find relevant information about that topic.", []
        
        # Create context from top relevant tweets
        context = "\n".join([
            f"- @{r.get('user', 'Anonymous')}: {r.get('tweet', '')}" 
            for r in results[:15]  # Use top 15 results
        ])
        
        try:
            # Support both older and newer OpenAI API formats
            try:
                # Newer format
                client = openai.OpenAI(api_key=openai.api_key)
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You're a sportswear brand analyst answering questions based on tweet data."},
                        {"role": "user", "content": f"Question: {question}\n\nTweets:\n{context}"}
                    ],
                    temperature=0.0
                )
                return response.choices[0].message.content.strip(), results
            except (AttributeError, TypeError):
                # Older format
                res = openai.ChatCompletion.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You're a sportswear brand analyst answering questions based on tweet data."},
                        {"role": "user", "content": f"Question: {question}\n\nTweets:\n{context}"}
                    ],
                    temperature=0.0
                )
                return res.choices[0].message.content.strip(), results
        except Exception as e:
            logger.error(f"LLM answer error: {e}")
            return f"Error generating answer: {str(e)}", results
    
    def generate_followup_questions(self, question, answer):
        """Generate follow-up questions based on the current question and answer"""
        try:
            # Support both older and newer OpenAI API formats
            prompt = f"""
            Based on this question and answer about sportswear brands, suggest 3 natural follow-up questions that someone might ask next.
            Keep the questions short, focused, and directly related to sportswear brands.
            
            Question: {question}
            Answer: {answer}
            
            Format each question on its own line, without numbering or bullets.
            """
            
            try:
                # Newer format
                client = openai.OpenAI(api_key=openai.api_key)
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You generate relevant follow-up questions about sportswear brands."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.7
                )
                questions_text = response.choices[0].message.content.strip()
            except (AttributeError, TypeError):
                # Older format
                res = openai.ChatCompletion.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You generate relevant follow-up questions about sportswear brands."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.7
                )
                questions_text = res.choices[0].message.content.strip()
            
            # Parse questions from the response
            questions = [q.strip() for q in questions_text.split('\n') if q.strip()]
            return questions[:3]  # Ensure we only return up to 3 questions
        except Exception as e:
            logger.error(f"Follow-up questions error: {e}")
            return [
                "How does this brand compare to its competitors?",
                "What are the trending products from this brand?",
                "What marketing strategies is this brand using effectively?"
            ]
    
    def process_question(self, question):
        """Process a question and return answer with sources"""
        # 1. Generate vector embedding
        embedding = self.generate_embeddings(question)
        
        # 2. Perform hybrid search (vector + keyword)
        results = self.query_knowledge_graph(question, embedding)
        logger.info(f"Found {len(results)} relevant tweets")
        
        # 3. Generate answer using LLM
        answer, sources = self.generate_answer(question, results)
        
        # 4. Generate follow-up questions
        followup_questions = self.generate_followup_questions(question, answer)
        
        return {
            "question": question,
            "answer": answer,
            "sources": sources,
            "followup_questions": followup_questions
        }
    
    def close(self):
        """Close the Neo4j connection"""
        if self.neo4j_driver:
            self.neo4j_driver.close()

# IMPROVED COLOR SCHEME FOR CHARTS
BRAND_COLORS = {
    "Nike": "#FF9900",      # Orange
    "Adidas": "#5AC8FA",    # Blue
    "Puma": "#4ADE80",      # Green
    "Under Armour": "#BF5AF2", # Purple
    "New Balance": "#FFD60A",  # Yellow
    "Other": "#8E8E93"      # Gray
}

SENTIMENT_COLORS = {
    "Positive": "#4ADE80",  # Green
    "Neutral": "#5AC8FA",   # Blue
    "Negative": "#FF453A"   # Red
}

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

# Data loading functions with progress indicators and error handling
@st.cache_data(ttl=300)
def get_brand_sentiment():
    where_clause = get_where_clause()
    
    query = f"""
    SELECT 
        CASE 
            WHEN TEXT ILIKE '%Nike%' THEN 'Nike'
            WHEN TEXT ILIKE '%Adidas%' THEN 'Adidas'
            WHEN TEXT ILIKE '%Puma%' THEN 'Puma'
            WHEN TEXT ILIKE '%Under Armour%' THEN 'Under Armour'
            WHEN TEXT ILIKE '%New Balance%' THEN 'New Balance'
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
        
        return pd.DataFrame(results, columns=["Brand", "Sentiment", "Count"]), None
    except Exception as e:
        return pd.DataFrame(columns=["Brand", "Sentiment", "Count"]), str(e)

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
        
        return pd.DataFrame(results, columns=["Topic", "Count"]), None
    except Exception as e:
        return pd.DataFrame(columns=["Topic", "Count"]), str(e)

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
        
        return pd.DataFrame(results, columns=["Text", "User", "Date", "Sentiment", "Likes", "Retweets"]), None
    except Exception as e:
        return pd.DataFrame(columns=["Text", "User", "Date", "Sentiment", "Likes", "Retweets"]), str(e)

# Initialize QA system
@st.cache_resource
def get_qa_system():
    return SimpleQASystem()

try:
    qa = get_qa_system()
except Exception as e:
    st.error(f"Error initializing QA system: {e}")
    qa = None

# Initialize session state for navigation and QA components
if "app_view" not in st.session_state:
    st.session_state.app_view = "ask"  # Default to QA view

if "messages" not in st.session_state:
    st.session_state.messages = []

if "need_rerun" not in st.session_state:
    st.session_state.need_rerun = False

if "question_input" not in st.session_state:
    st.session_state.question_input = ""

# Sidebar setup
with st.sidebar:
    st.title("Brand Analytics")
    st.markdown("---")
    
    # Navigation buttons - stacked vertically
    st.header("Navigation")
    
    if st.button("Ask", key="nav_ask", 
                help="Ask questions about sportswear brands",
                use_container_width=True):
        st.session_state.app_view = "ask"
        st.rerun()
    
    if st.button("Dashboard", key="nav_dashboard", 
                help="View brand analytics dashboard",
                use_container_width=True):
        st.session_state.app_view = "dashboard"
        st.rerun()
    
    st.markdown("---")
    
    # Filter section (only shown when in dashboard view)
    if st.session_state.app_view == "dashboard":
        st.header("📌 Filters")
        
        date_range = st.selectbox(
            "⏱️ Time Period",
            ["Last 7 days", "Last 30 days", "Last 90 days", "All time"],
            index=2  # Default to last 90 days
        )
        
        st.markdown("---")
        
        brands = st.multiselect(
            "🏷️ Select Brands",
            ["Nike", "Adidas", "Puma", "Under Armour", "New Balance"],
            default=["Nike", "Adidas", "Puma"]
        )
    
    # Footer links
    st.markdown("---")
    st.markdown("[📚 User Guide](https://example.com)")
    st.markdown("[💬 Report Issues](https://example.com/issues)")
    
    # Add time stamp
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.markdown(f"<p class='refresh-time'>Last updated: {current_time}</p>", unsafe_allow_html=True)

# Check if we need to rerun (set by a callback)
if st.session_state.need_rerun:
    st.session_state.need_rerun = False
    st.rerun()

# Function to handle question submission and clear the input
def handle_submit():
    if st.session_state.question_input and qa:
        # Get the question from the input widget's state
        question = st.session_state.question_input
        
        with st.spinner("Analyzing tweets..."):
            try:
                # Process the question
                result = qa.process_question(question)
                
                # Add to chat history
                st.session_state.messages.append({"role": "user", "content": question})
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": result["answer"],
                    "sources": result["sources"][:10],
                    "followup_questions": result["followup_questions"]
                })
                
                # Clear the input box after processing
                st.session_state.question_input = ""
                
                # Set a flag to indicate we need to rerun after the callback completes
                st.session_state.need_rerun = True
            except Exception as e:
                st.error(f"Error processing question: {e}")

# Main app layout based on selected view
if st.session_state.app_view == "ask":
    # QA interface
    st.title("Sportswear Brand Analytics Q&A")
    st.write("Ask questions about Nike, Adidas, Puma and other sportswear brands")
    
    # Clear history button
    col1, col2 = st.columns([6, 1])
    with col2:
        if st.button("Clear History"):
            st.session_state.messages = []
            st.rerun()
    
    # Display chat history
    for message in st.session_state.messages:
        if message["role"] == "user":
            st.markdown(f"<div class='question-box'><h3>Question</h3>{message['content']}</div>", unsafe_allow_html=True)
        else:
            st.markdown(f"<div class='answer-box'><h3>Answer</h3>{message['content']}</div>", unsafe_allow_html=True)
            
            # If this is an assistant message with sources, add an expander for them
            if "sources" in message:
                with st.expander(f"View Sources ({len(message['sources'])} tweets)"):
                    for i, source in enumerate(message["sources"], 1):
                        # Tweet container
                        st.markdown(f"""
                        **{i}. @{source.get('user', 'Anonymous')}**
                        
                        {source.get('tweet', '')}
                        
                        *Sentiment: {source.get('sentiment', 'Unknown')} | 
                        Likes: {source.get('like_count', 0)} | 
                        Retweets: {source.get('retweet_count', 0)}*
                        
                        ---
                        """)
            
            # If this message has suggested questions, display them at the bottom
            if "followup_questions" in message:
                st.markdown("### Suggested Questions")
                cols = st.columns(len(message["followup_questions"]))
                for i, question in enumerate(message["followup_questions"]):
                    # Create a truly unique key using message index and question index
                    message_idx = st.session_state.messages.index(message)
                    unique_key = f"suggestion_{message_idx}_{i}"
                    if cols[i].button(question, key=unique_key):
                        try:
                            # Set this as the new question and process it
                            st.session_state.question_input = question
                            handle_submit()
                        except Exception as e:
                            st.error(f"Error processing suggested question: {e}")
    
    # Input section at the bottom
    st.markdown("---")
    
    # Text input with callback
    st.text_input(
        "Your question:",
        key="question_input",
        on_change=handle_submit
    )
    
    # Add Ask button
    st.button("Ask", on_click=handle_submit)

else:
    # Dashboard interface
    st.title("Sportswear Brand Analytics Dashboard")
    st.write("Real-time Twitter analysis of leading sportswear brands")
    
    # Add refresh button
    col1, col2 = st.columns([5, 1])
    with col2:
        if st.button("🔄 Refresh Data", help="Refresh the dashboard data"):
            st.cache_data.clear()
            st.rerun()
    
    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["📊 Brand Overview", "😊 Sentiment Analysis", "🔍 Topic Analysis"])
    
    with tab1:
        # Brand Overview Tab
        # Load data with progress indicator
        with st.spinner("Loading brand data..."):
            brand_sentiment, bs_error = get_brand_sentiment()
            top_tweets, tt_error = get_top_tweets()
            
        if bs_error:
            st.error(f"Error fetching brand sentiment: {bs_error}")
        
        if tt_error:
            st.error(f"Error fetching top tweets: {tt_error}")
        
        # Calculate brand share of voice
        if not brand_sentiment.empty:
            share_of_voice = brand_sentiment.groupby('Brand')['Count'].sum().reset_index()
            total_mentions = share_of_voice['Count'].sum()
            share_of_voice['Percentage'] = (share_of_voice['Count'] / total_mentions * 100).round(1)
            
            # Display metrics in cards with better styling
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Brand Mentions", f"{total_mentions:,}", delta=None, delta_color="normal")
            
            with col2:
                positive_count = brand_sentiment[brand_sentiment['Sentiment'] == 'Positive']['Count'].sum()
                positive_pct = (positive_count / total_mentions * 100).round(1) if total_mentions > 0 else 0
                st.metric("Positive Sentiment", f"{positive_pct}%", delta=None, delta_color="normal")
            
            with col3:
                top_brand = share_of_voice.iloc[0]['Brand'] if not share_of_voice.empty else "N/A"
                top_pct = share_of_voice.iloc[0]['Percentage'] if not share_of_voice.empty else 0
                st.metric("Leading Brand", f"{top_brand} ({top_pct}%)", delta=None, delta_color="normal")
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            # Create two columns for charts
            col1, col2 = st.columns(2)
            
            with col1:
                # Share of voice chart
                st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
                st.subheader("Brand Share of Voice")
                fig = px.pie(
                    share_of_voice, 
                    values="Count", 
                    names="Brand",
                    color="Brand",
                    color_discrete_map=BRAND_COLORS,
                    hole=0.4
                )
                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
                    margin=dict(t=30, b=10, l=10, r=10)
                )
                st.plotly_chart(fig, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
            
            with col2:
                # Sentiment by brand
                st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
                st.subheader("Sentiment by Brand")
                fig = px.bar(
                    brand_sentiment,
                    x="Brand",
                    y="Count",
                    color="Sentiment",
                    color_discrete_map=SENTIMENT_COLORS,
                    barmode="group"
                )
                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    xaxis_title="",
                    yaxis_title="Number of Mentions",
                    legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
                    margin=dict(t=30, b=10, l=10, r=10)
                )
                st.plotly_chart(fig, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
        
        else:
            st.info("No brand data available for selected filters.")
        
        # Display top tweets with better styling
        st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
        st.subheader("Top Tweets")
        
        if not top_tweets.empty:
            for _, tweet in top_tweets.iterrows():
                sentiment_color = {
                    "Positive": "#4ADE80",
                    "Neutral": "#5AC8FA",
                    "Negative": "#FF453A"
                }.get(tweet["Sentiment"], "#5AC8FA")
                
                st.markdown(f"""
                <div class="tweet-card" style="border-left: 5px solid {sentiment_color};">
                    <p><strong>@{tweet['User']}</strong> • {pd.to_datetime(tweet['Date']).strftime('%Y-%m-%d %H:%M')}</p>
                    <p>{tweet['Text']}</p>
                    <p>❤️ {tweet['Likes']} | 🔄 {tweet['Retweets']} | {tweet['Sentiment']}</p>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No tweets available for selected filters.")
        
        st.markdown("</div>", unsafe_allow_html=True)
    
    with tab2:
        # Sentiment Analysis Tab
        with st.spinner("Loading sentiment data..."):
            brand_sentiment, bs_error = get_brand_sentiment()
        
        if bs_error:
            st.error(f"Error fetching sentiment data: {bs_error}")
        
        if not brand_sentiment.empty:
            # Calculate totals by sentiment
            total_sentiment = brand_sentiment.groupby('Sentiment')['Count'].sum().reset_index()
            total_count = total_sentiment['Count'].sum()
            
            # Display metrics
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            col1, col2, col3 = st.columns(3)
            
            sentiment_icons = {
                "Positive": "😊",
                "Neutral": "😐",
                "Negative": "😟"
            }
            
            with col1:
                positive_count = total_sentiment[total_sentiment['Sentiment'] == 'Positive']['Count'].sum()
                positive_pct = (positive_count / total_count * 100).round(1) if total_count > 0 else 0
                st.metric(f"{sentiment_icons.get('Positive', '')} Positive", f"{positive_pct}%", f"{positive_count} tweets")
            
            with col2:
                neutral_count = total_sentiment[total_sentiment['Sentiment'] == 'Neutral']['Count'].sum()
                neutral_pct = (neutral_count / total_count * 100).round(1) if total_count > 0 else 0
                st.metric(f"{sentiment_icons.get('Neutral', '')} Neutral", f"{neutral_pct}%", f"{neutral_count} tweets")
            
            with col3:
                negative_count = total_sentiment[total_sentiment['Sentiment'] == 'Negative']['Count'].sum()
                negative_pct = (negative_count / total_count * 100).round(1) if total_count > 0 else 0
                st.metric(f"{sentiment_icons.get('Negative', '')} Negative", f"{negative_pct}%", f"{negative_count} tweets")
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            # Create two columns for charts
            col1, col2 = st.columns(2)
            
            with col1:
                # Overall sentiment pie chart
                st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
                st.subheader("Overall Sentiment Distribution")
                
                fig = px.pie(
                    total_sentiment,
                    values="Count",
                    names="Sentiment",
                    color="Sentiment",
                    color_discrete_map=SENTIMENT_COLORS,
                    hole=0.4
                )
                
                # Add emojis to the sentiment labels
                new_labels = [f"{sentiment_icons.get(label, '')} {label}" for label in fig.data[0].labels]
                fig.update_traces(labels=new_labels)
                
                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
                    margin=dict(t=30, b=10, l=10, r=10)
                )
                
                st.plotly_chart(fig, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
            
            with col2:
                # Sentiment by brand
                st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
                st.subheader("Sentiment by Brand")
                
                # Calculate sentiment percentages by brand for 100% stacked bar chart
                brand_totals = brand_sentiment.groupby('Brand')['Count'].sum().reset_index()
                brand_sentiment_pct = pd.merge(brand_sentiment, brand_totals, on='Brand')
                brand_sentiment_pct['Percentage'] = (brand_sentiment_pct['Count_x'] / brand_sentiment_pct['Count_y'] * 100).round(1)
                
                fig = px.bar(
                    brand_sentiment_pct,
                    x="Brand",
                    y="Percentage",
                    color="Sentiment",
                    color_discrete_map=SENTIMENT_COLORS,
                    barmode="stack"
                )
                
                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    xaxis_title="",
                    yaxis_title="Sentiment Distribution (%)",
                    yaxis=dict(range=[0, 100]),
                    legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
                    margin=dict(t=30, b=10, l=10, r=10)
                )
                
                st.plotly_chart(fig, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
            
            # Sentiment trends insights
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            st.subheader("Sentiment Analysis Insights")
            
            # Calculate which brand has the most positive sentiment
            brand_positive = brand_sentiment[brand_sentiment['Sentiment'] == 'Positive'].copy()
            if not brand_positive.empty:
                total_by_brand = brand_sentiment.groupby('Brand')['Count'].sum().reset_index()
                brand_positive = pd.merge(brand_positive, total_by_brand, on='Brand')
                brand_positive['Percent'] = (brand_positive['Count_x'] / brand_positive['Count_y'] * 100).round(1)
                most_positive_brand = brand_positive.sort_values('Percent', ascending=False).iloc[0]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown(f"""
                    ### Top Positive Brand
                    **{most_positive_brand['Brand']}** has the highest positive sentiment at **{most_positive_brand['Percent']}%**
                    
                    This indicates strong brand perception and customer satisfaction.
                    """)
                
                with col2:
                    # Find brand with most negative sentiment for comparison
                    brand_negative = brand_sentiment[brand_sentiment['Sentiment'] == 'Negative'].copy()
                    if not brand_negative.empty:
                        brand_negative = pd.merge(brand_negative, total_by_brand, on='Brand')
                        brand_negative['Percent'] = (brand_negative['Count_x'] / brand_negative['Count_y'] * 100).round(1)
                        most_negative_brand = brand_negative.sort_values('Percent', ascending=False).iloc[0]
                        
                        st.markdown(f"""
                        ### Improvement Opportunity
                        **{most_negative_brand['Brand']}** has the highest negative sentiment at **{most_negative_brand['Percent']}%**
                        
                        This suggests potential areas for brand improvement.
                        """)
            
            st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info("No sentiment data available for selected filters.")

    with tab3:
        # Topic Analysis Tab
        with st.spinner("Loading topic data..."):
            topic_data, topic_error = get_topic_distribution()
        
        if topic_error:
            st.error(f"Error fetching topic data: {topic_error}")
        
        if not topic_data.empty:
            st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
            
            # Show summary metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                total_topics = len(topic_data)
                st.metric("Total Topics", total_topics)
            
            with col2:
                top_topic = topic_data.iloc[0]['Topic'] if not topic_data.empty else "N/A"
                top_count = topic_data.iloc[0]['Count'] if not topic_data.empty else 0
                st.metric("Most Popular Topic", top_topic, f"{top_count} mentions")
            
            with col3:
                total_mentions = topic_data['Count'].sum()
                avg_mentions = int(total_mentions / total_topics) if total_topics > 0 else 0
                st.metric("Average Mentions per Topic", avg_mentions)
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            # Create two columns for charts
            col1, col2 = st.columns(2)
            
            with col1:
                # Topic distribution bar chart
                st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
                st.subheader("Topic Distribution")
                
                # Limit to top 10 topics for better visualization
                top_topics = topic_data.head(10).copy()
                
                fig = px.bar(
                    top_topics,
                    y="Topic",
                    x="Count",
                    color="Count",
                    color_continuous_scale="Turbo",
                    orientation='h'
                )
                
                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    xaxis_title="Number of Mentions",
                    yaxis_title="",
                    yaxis=dict(autorange="reversed"),  # Display highest count at top
                    margin=dict(t=30, b=10, l=10, r=10)
                )
                
                # Correct way to update colorbar properties
                fig.update_coloraxes(
                    colorbar=dict(
                        title="Count",
                        tickfont=dict(color="white")
                    )
                )
                
                st.plotly_chart(fig, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
            
            with col2:
                # Topic pie chart
                st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
                st.subheader("Topic Breakdown")
                
                # Combine smaller topics into "Other" for cleaner pie chart
                threshold = topic_data['Count'].sum() * 0.05  # 5% threshold
                pie_data = topic_data.copy()
                small_topics = pie_data[pie_data['Count'] < threshold]
                if not small_topics.empty:
                    other_count = small_topics['Count'].sum()
                    pie_data = pie_data[pie_data['Count'] >= threshold]
                    pie_data = pd.concat([pie_data, pd.DataFrame([{'Topic': 'Other', 'Count': other_count}])])
                
                colors = px.colors.sequential.Turbo[:len(pie_data)]
                
                fig = px.pie(
                    pie_data,
                    values="Count",
                    names="Topic",
                    hole=0.4,
                    color_discrete_sequence=colors
                )
                
                fig.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
                    margin=dict(t=30, b=10, l=10, r=10)
                )
                
                st.plotly_chart(fig, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
        
        else:
            st.info("No topic data available for selected filters.")
        
        # Hashtag analysis from Neo4j
        st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
        st.subheader("Top Hashtags")
        
        try:
            with st.spinner("Loading hashtag data..."):
                # Define date filter in Neo4j format
                if date_range == "Last 7 days":
                    date_clause = "date(t.date) >= date() - duration('P7D')"
                elif date_range == "Last 30 days":
                    date_clause = "date(t.date) >= date() - duration('P30D')"
                elif date_range == "Last 90 days":
                    date_clause = "date(t.date) >= date() - duration('P90D')"
                else:
                    date_clause = "1=1"  # All time
                
                # Build brand filter for Neo4j
                brand_clauses = []
                for brand in brands:
                    brand_clauses.append(f"toLower(t.text) CONTAINS toLower('{brand}')")
                
                brand_clause = " OR ".join(brand_clauses) if brand_clauses else "1=1"
                
                # Query with both filters
                query = f"""
                MATCH (t:Tweet)-[:CONTAINS_HASHTAG]->(h:Hashtag)
                WHERE ({date_clause}) AND ({brand_clause})
                RETURN h.tag AS hashtag, COUNT(t) AS count
                ORDER BY count DESC
                LIMIT 15
                """
                
                with neo4j_driver.session() as session:
                    result = session.run(query)
                    records = [dict(record) for record in result]
                    
                    if records:
                        hashtag_data = pd.DataFrame(records)
                        
                        # Create hashtag visualization
                        fig = px.bar(
                            hashtag_data,
                            y="hashtag",
                            x="count",
                            color="count",
                            color_continuous_scale="Turbo",
                            orientation='h'
                        )
                        
                        fig.update_layout(
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='rgba(0,0,0,0)',
                            font=dict(color='white'),
                            xaxis_title="Number of Tweets",
                            yaxis_title="",
                            yaxis=dict(autorange="reversed"),  # Display highest count at top
                            margin=dict(t=30, b=10, l=10, r=10)
                        )
                        
                        # Correct way to update colorbar properties
                        fig.update_coloraxes(
                            colorbar=dict(
                                title="Count",
                                tickfont=dict(color="white")
                            )
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No hashtag data available for selected filters")
        except Exception as e:
            st.error(f"Error fetching hashtag data: {str(e)}")
        
        st.markdown("</div>", unsafe_allow_html=True)



# Add cleanup when app closes
def on_shutdown():
    try:
        if qa:
            qa.close()
    except:
        pass

# Register shutdown function
atexit.register(on_shutdown)