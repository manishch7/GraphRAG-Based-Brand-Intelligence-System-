import streamlit as st
import sys
import os
import logging
import configparser
from neo4j import GraphDatabase
import atexit

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load config
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.ini")
if os.path.exists(config_path):
    config.read(config_path)
    try:
        # Get Neo4j config
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
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "password"
    NEO4J_DATABASE = "neo4j"
    st.warning(f"Config file not found at {config_path}, using default values.")

# Page setup
st.set_page_config(page_title="Neo4j Database Explorer", layout="wide")
st.title("Neo4j Database Explorer")

# Allow manual configuration
with st.sidebar:
    st.header("Database Connection")
    custom_uri = st.text_input("Neo4j URI", value=NEO4J_URI)
    custom_user = st.text_input("Username", value=NEO4J_USER)
    custom_password = st.text_input("Password", value=NEO4J_PASSWORD, type="password")
    custom_database = st.text_input("Database", value=NEO4J_DATABASE)
    
    if st.button("Connect"):
        NEO4J_URI = custom_uri
        NEO4J_USER = custom_user
        NEO4J_PASSWORD = custom_password
        NEO4J_DATABASE = custom_database

# Connect to Neo4j
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
        return None

try:
    # Main app
    driver = get_driver()
    if not driver:
        st.stop()
    
    # Show database info
    st.subheader("Database Information")
    
    # Exploration options
    exploration_option = st.selectbox(
        "What would you like to explore?",
        ["Node Labels", "Relationship Types", "Indexes", "Constraints", "Sample Nodes", "Custom Query"]
    )
    
    if exploration_option == "Node Labels":
        with st.spinner("Fetching node labels..."):
            with driver.session(database=NEO4J_DATABASE) as session:
                result = session.run("CALL db.labels()")
                labels = [record["label"] for record in result]
                
                st.write(f"Found {len(labels)} node labels:")
                st.write(", ".join(labels))
                
                # For each label, show count
                if labels:
                    st.subheader("Node Counts by Label")
                    for label in labels:
                        count_query = f"MATCH (n:{label}) RETURN count(n) AS count"
                        count_result = session.run(count_query)
                        count = count_result.single()["count"]
                        st.write(f"{label}: {count} nodes")
    
    elif exploration_option == "Relationship Types":
        with st.spinner("Fetching relationship types..."):
            with driver.session(database=NEO4J_DATABASE) as session:
                result = session.run("CALL db.relationshipTypes()")
                rel_types = [record["relationshipType"] for record in result]
                
                st.write(f"Found {len(rel_types)} relationship types:")
                st.write(", ".join(rel_types))
                
                # For each relationship, show count
                if rel_types:
                    st.subheader("Relationship Counts by Type")
                    for rel_type in rel_types:
                        count_query = f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS count"
                        count_result = session.run(count_query)
                        count = count_result.single()["count"]
                        st.write(f"{rel_type}: {count} relationships")
    
    elif exploration_option == "Indexes":
        with st.spinner("Fetching indexes..."):
            with driver.session(database=NEO4J_DATABASE) as session:
                # Try different commands for different Neo4j versions
                try:
                    # Neo4j 4.x+
                    result = session.run("SHOW INDEXES")
                except Exception:
                    try:
                        # Neo4j 3.x
                        result = session.run("CALL db.indexes()")
                    except Exception as e:
                        st.error(f"Error fetching indexes: {e}")
                        result = []
                
                indexes = list(result)
                
                if indexes:
                    st.write(f"Found {len(indexes)} indexes:")
                    for idx, record in enumerate(indexes):
                        st.write(f"**Index {idx+1}**")
                        for key, value in record.items():
                            st.write(f"- {key}: {value}")
                        st.write("---")
                else:
                    st.write("No indexes found.")
                
                # Check specifically for vector indexes
                st.subheader("Vector Indexes")
                try:
                    # This will only work on Neo4j 5.11+ with vector plugin installed
                    vector_result = session.run("SHOW VECTOR INDEXES")
                    vector_indexes = list(vector_result)
                    
                    if vector_indexes:
                        st.write(f"Found {len(vector_indexes)} vector indexes:")
                        for idx, record in enumerate(vector_indexes):
                            st.write(f"**Vector Index {idx+1}**")
                            for key, value in record.items():
                                st.write(f"- {key}: {value}")
                            st.write("---")
                    else:
                        st.write("No vector indexes found.")
                except Exception as e:
                    st.write(f"Vector index query failed: {e}")
                    st.write("Your Neo4j instance may not support vector indexes or may need the vector plugin installed.")
    
    elif exploration_option == "Constraints":
        with st.spinner("Fetching constraints..."):
            with driver.session(database=NEO4J_DATABASE) as session:
                # Try different commands for different Neo4j versions
                try:
                    # Neo4j 4.x+
                    result = session.run("SHOW CONSTRAINTS")
                except Exception:
                    try:
                        # Neo4j 3.x
                        result = session.run("CALL db.constraints()")
                    except Exception as e:
                        st.error(f"Error fetching constraints: {e}")
                        result = []
                
                constraints = list(result)
                
                if constraints:
                    st.write(f"Found {len(constraints)} constraints:")
                    for idx, record in enumerate(constraints):
                        st.write(f"**Constraint {idx+1}**")
                        for key, value in record.items():
                            st.write(f"- {key}: {value}")
                        st.write("---")
                else:
                    st.write("No constraints found.")
    
    elif exploration_option == "Sample Nodes":
        with driver.session(database=NEO4J_DATABASE) as session:
            result = session.run("CALL db.labels()")
            labels = [record["label"] for record in result]
        
        selected_label = st.selectbox("Select node label to sample", labels)
        sample_count = st.slider("Number of nodes to sample", 1, 50, 5)
        
        if st.button("Fetch Sample"):
            with st.spinner(f"Fetching {sample_count} sample nodes with label {selected_label}..."):
                with driver.session(database=NEO4J_DATABASE) as session:
                    query = f"MATCH (n:{selected_label}) RETURN n LIMIT {sample_count}"
                    result = session.run(query)
                    
                    nodes = list(result)
                    if nodes:
                        for idx, record in enumerate(nodes):
                            node = record["n"]
                            st.write(f"**Node {idx+1}**")
                            for key, value in node.items():
                                st.write(f"- {key}: {str(value)[:100]}{'...' if len(str(value)) > 100 else ''}")
                            st.write("---")
                    else:
                        st.write(f"No nodes found with label {selected_label}.")
    
    elif exploration_option == "Custom Query":
        query = st.text_area("Enter Cypher query", "MATCH (n) RETURN n LIMIT 5")
        
        if st.button("Execute Query"):
            with st.spinner("Executing query..."):
                with driver.session(database=NEO4J_DATABASE) as session:
                    try:
                        result = session.run(query)
                        records = list(result)
                        
                        if records:
                            st.write(f"Query returned {len(records)} records:")
                            
                            # Get column names from the first record
                            if records:
                                columns = records[0].keys()
                                
                                # Create a dataframe from the results if possible
                                try:
                                    import pandas as pd
                                    data = []
                                    for record in records:
                                        row = {}
                                        for col in columns:
                                            # Convert Neo4j node/relationship objects to string representations
                                            if hasattr(record[col], 'items'):
                                                row[col] = str(dict(record[col].items()))
                                            else:
                                                row[col] = record[col]
                                        data.append(row)
                                    
                                    df = pd.DataFrame(data)
                                    st.dataframe(df)
                                except Exception as e:
                                    # Fall back to manual display if pandas fails
                                    for idx, record in enumerate(records):
                                        st.write(f"**Record {idx+1}**")
                                        for key, value in record.items():
                                            if hasattr(value, 'items'):
                                                st.write(f"- {key}: {dict(value.items())}")
                                            else:
                                                st.write(f"- {key}: {value}")
                                        st.write("---")
                        else:
                            st.write("Query returned no results.")
                    except Exception as e:
                        st.error(f"Error executing query: {e}")
    
    # Helper code to create tweet_embeddings index if needed
    st.subheader("Create Vector Index")
    with st.expander("Create tweet_embeddings vector index"):
        st.write("If you need to create a vector index for tweets, you can use this utility.")
        
        # First get the available node labels
        with driver.session(database=NEO4J_DATABASE) as session:
            result = session.run("CALL db.labels()")
            labels = [record["label"] for record in result]
        
        node_label = st.selectbox("Select node label for tweets", 
                                 labels, 
                                 index=labels.index("Tweet") if "Tweet" in labels else 0)
        embedding_property = st.text_input("Embedding property name", "embedding")
        dimension = st.number_input("Embedding dimension", min_value=1, value=1536)
        similarity_function = st.selectbox("Similarity function", ["cosine", "euclidean", "dot"])
        
        if st.button("Create Vector Index"):
            with st.spinner("Creating vector index..."):
                try:
                    with driver.session(database=NEO4J_DATABASE) as session:
                        # Check Neo4j version to determine the correct syntax
                        version_query = "CALL dbms.components() YIELD versions RETURN versions[0] as version"
                        version_result = session.run(version_query)
                        version = version_result.single()["version"]
                        
                        if version.startswith("5"):
                            # Neo4j 5.x syntax
                            index_query = f"""
                            CREATE VECTOR INDEX tweet_embeddings IF NOT EXISTS
                            FOR (t:{node_label})
                            ON t.{embedding_property}
                            OPTIONS {{indexConfig: {{
                              `vector.dimensions`: {dimension},
                              `vector.similarity_function`: '{similarity_function}'
                            }}}}
                            """
                        else:
                            # Neo4j 4.x syntax with vector plugin
                            index_query = f"""
                            CALL db.index.vector.createNodeIndex(
                              'tweet_embeddings',
                              '{node_label}',
                              '{embedding_property}',
                              {dimension},
                              '{similarity_function}'
                            )
                            """
                        
                        session.run(index_query)
                        st.success("Vector index 'tweet_embeddings' created successfully!")
                except Exception as e:
                    st.error(f"Error creating vector index: {e}")
                    st.write("This could be because:")
                    st.write("1. Your Neo4j instance doesn't have vector search capabilities")
                    st.write("2. Your Neo4j version uses a different syntax for creating vector indexes")
                    st.write("3. The index already exists with different parameters")

    # Create a fallback search without vector search
    st.subheader("Alternative Search Query")
    with st.expander("Generate alternative search query without vector search"):
        st.write("If vector search isn't working, you can use this to generate a query that uses only keyword matching.")
        
        with driver.session(database=NEO4J_DATABASE) as session:
            result = session.run("CALL db.labels()")
            labels = [record["label"] for record in result]
        
        tweet_label = st.selectbox("Select label for tweets", 
                                  labels, 
                                  index=labels.index("Tweet") if "Tweet" in labels else 0,
                                  key="alt_label")
        text_property = st.text_input("Text property name", "text")
        sample_query = st.text_input("Sample search query", "Nike running shoes")
        
        if st.button("Generate Alternative Query"):
            keywords = [word.lower() for word in sample_query.split() if len(word) > 2]
            
            # Generate Cypher for keyword search
            cypher_query = f"""
            // Keyword search only
            MATCH (t:{tweet_label})
            WHERE {' AND '.join([f'toLower(t.{text_property}) CONTAINS "{keyword}"' for keyword in keywords])}
            
            // Get related information
            OPTIONAL MATCH (t)<-[:POSTED]-(u:User)
            OPTIONAL MATCH (t)-[:HAS_SENTIMENT]->(s:Sentiment)
            OPTIONAL MATCH (t)-[:BELONGS_TO_TOPIC]->(topic:Topic)
            
            // Return results
            RETURN 
                t.{text_property} AS tweet, 
                t.created_at AS created, 
                u.screen_name AS user,
                t.retweet_count AS retweet_count,
                t.like_count AS like_count,
                s.label AS sentiment,
                topic.name AS topic,
                t.location AS location
                
            LIMIT 50
            """
            
            st.code(cypher_query, language="cypher")
            st.write("You can use this query in your application as a temporary solution until the vector search is working.")
            
            # Update the qa_app.py code without vector search
            qa_app_code = """
import streamlit as st
import sys
import os
import atexit
import logging
import configparser
import openai
from neo4j import GraphDatabase

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load config
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.ini")
if os.path.exists(config_path):
    config.read(config_path)
    openai.api_key = config.get("openai", "api_key")
    NEO4J_URI = config.get("neo4j", "uri", fallback="bolt://localhost:7687")
    NEO4J_USER = config.get("neo4j", "user", fallback="neo4j")
    NEO4J_PASSWORD = config.get("neo4j", "password", fallback="password")
    NEO4J_DATABASE = config.get("neo4j", "database", fallback="neo4j")
else:
    logger.error(f"Config file not found at {config_path}")
    st.error("Configuration file not found. Please check your installation.")
    st.stop()

# Create a simplified QA System with keyword search
class SimpleQASystem:
    def __init__(self):
        # Connect to Neo4j
        self.neo4j_driver = GraphDatabase.driver(
            NEO4J_URI, 
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
    
    def extract_keywords(self, text):
        """Extract simple keywords from the question"""
        # Simple keyword extraction
        words = text.lower().split()
        stopwords = {'a', 'an', 'the', 'and', 'or', 'but', 'if', 'because', 'as', 'what', 
                    'when', 'where', 'how', 'why', 'which', 'who', 'whom', 'this', 'that', 
                    'these', 'those', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 
                    'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'to', 
                    'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 
                    'through', 'during', 'before', 'after', 'above', 'below', 'on', 'off'}
        return [w for w in words if w.isalpha() and w not in stopwords and len(w) > 2]
    
    def query_knowledge_graph(self, question):
        """Query Neo4j with keyword matching only"""
        # Extract keywords for keyword matching
        keywords = self.extract_keywords(question)
        if not keywords:
            return []
        
        # Build keyword search query
        where_clauses = [f'toLower(t.text) CONTAINS "{keyword}"' for keyword in keywords]
        query = f"""
        // Keyword search
        MATCH (t:Tweet)
        WHERE {' AND '.join(where_clauses)}
            
        // Get related information
        OPTIONAL MATCH (t)<-[:POSTED]-(u:User)
        OPTIONAL MATCH (t)-[:HAS_SENTIMENT]->(s:Sentiment)
        OPTIONAL MATCH (t)-[:BELONGS_TO_TOPIC]->(topic:Topic)
            
        // Return results
        RETURN 
            t.text AS tweet, 
            t.created_at AS created, 
            u.screen_name AS user,
            t.retweet_count AS retweet_count,
            t.like_count AS like_count,
            s.label AS sentiment,
            topic.name AS topic,
            t.location AS location
                
        LIMIT 50
        """
        
        try:
            with self.neo4j_driver.session(database=NEO4J_DATABASE) as session:
                result = session.run(query)
                return result.data()
        except Exception as e:
            logger.error(f"Neo4j query error: {e}")
            return []
    
    def generate_answer(self, question, results):
        """Generate an answer using the LLM with context from tweets"""
        if not results:
            return "I couldn't find relevant information about that topic.", []
        
        # Create context from top relevant tweets
        context = "\\n".join([
            f"- @{r.get('user', 'Anonymous')}: {r.get('tweet', '')}" 
            for r in results[:15]  # Use top 15 results
        ])
        
        try:
            # Using older OpenAI API format
            res = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You're a sportswear brand analyst answering questions based on tweet data."},
                    {"role": "user", "content": f"Question: {question}\\n\\nTweets:\\n{context}"}
                ],
                temperature=0.0
            )
            return res.choices[0].message.content.strip(), results
        except Exception as e:
            logger.error(f"LLM answer error: {e}")
            return "Error generating answer.", results
    
    def process_question(self, question):
        """Process a question and return answer with sources"""
        # Perform keyword search instead of vector search
        results = self.query_knowledge_graph(question)
        logger.info(f"Found {len(results)} relevant tweets")
        
        # Generate answer using LLM
        answer, sources = self.generate_answer(question, results)
        
        return {
            "question": question,
            "answer": answer,
            "sources": sources
        }
    
    def close(self):
        """Close the Neo4j connection"""
        if self.neo4j_driver:
            self.neo4j_driver.close()

# Page setup
# [Rest of the Streamlit UI code remains the same]
"""
            st.subheader("Alternative QA App Code")
            st.code(qa_app_code, language="python")
            st.write("This is a simplified version of qa_app.py that uses only keyword search instead of vector search.")

    # Close the driver when app shuts down
    def cleanup():
        if driver:
            driver.close()
    
    atexit.register(cleanup)

except Exception as e:
    st.error(f"An error occurred: {e}")