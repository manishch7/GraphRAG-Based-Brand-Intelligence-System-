import streamlit as st
import sys
import os
import atexit
import logging
import configparser
import openai
import importlib.util

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

# Try to import neo4j, and if not available, prompt to install
try:
    from neo4j import GraphDatabase
except ImportError:
    st.error("Neo4j driver not installed. Please run 'pip install neo4j'")
    st.stop()

# Create a direct Neo4j driver getter instead of importing
def get_driver():
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

# Create a simplified QA System that works standalone
class SimpleQASystem:
    def __init__(self):
        # Connect to Neo4j
        self.neo4j_driver = get_driver()
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

# Page setup
st.set_page_config(page_title="Brand Analytics Q&A", layout="wide")
st.title("Sportswear Brand Analytics Q&A")
st.write("Ask questions about Nike, Adidas, Puma and other sportswear brands")

# Add a small Clear History button at the top right
col1, col2 = st.columns([5, 1])
with col2:
    if st.button("Clear History"):
        st.session_state.messages = []
        st.rerun()

# Initialize QA system
@st.cache_resource
def get_qa_system():
    return SimpleQASystem()

try:
    qa = get_qa_system()
except Exception as e:
    st.error(f"Error initializing QA system: {e}")
    st.stop()

# Initialize chat history in session state if it doesn't exist
if "messages" not in st.session_state:
    st.session_state.messages = []

# Initialize the textbox value in session state if it doesn't exist
if "input_value" not in st.session_state:
    st.session_state.input_value = ""

# Function to handle question submission and clear the input
def handle_submit():
    if st.session_state.question_input:
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

# Initialize the rerun flag if it doesn't exist
if "need_rerun" not in st.session_state:
    st.session_state.need_rerun = False

# Check if we need to rerun (set by a callback)
if st.session_state.need_rerun:
    st.session_state.need_rerun = False
    st.rerun()

# Display chat history on the main screen
for message in st.session_state.messages:
    if message["role"] == "user":
        st.markdown(f"### Question\n{message['content']}")
    else:
        st.markdown(f"### Answer\n{message['content']}")
        
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

# Question input at the bottom of the page
st.markdown("---")

# Use the callback approach for text input
st.text_input(
    "Your question:", 
    key="question_input", 
    on_change=handle_submit
)

# Add a button for submission as well
st.button("Ask", on_click=handle_submit)

# Add cleanup when app closes
def on_shutdown():
    try:
        qa.close()
    except:
        pass

# Register shutdown function
atexit.register(on_shutdown)