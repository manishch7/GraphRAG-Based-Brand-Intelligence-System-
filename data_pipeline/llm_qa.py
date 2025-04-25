import spacy
import os
from openai import OpenAI
import configparser
import logging
from data_pipeline.connectors.neo4j_connector import get_driver
from data_pipeline.config import NEO4J_DATABASE

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load config
config = configparser.ConfigParser()
config.read("config.ini")
openai_api_key = config.get("openai", "api_key")

class QASystem:
    def __init__(self):
        # Load language model for keyword extraction
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            logger.info("Downloading spaCy model...")
            import subprocess
            subprocess.check_call([
                "python", "-m", "spacy", "download", "en_core_web_sm"
            ])
            self.nlp = spacy.load("en_core_web_sm")
        
        # Initialize OpenAI client
        self.openai_client = OpenAI(api_key=openai_api_key)
        
        # Connect to Neo4j
        self.neo4j_driver = get_driver()
    
    def generate_embeddings(self, text):
        """Generate embeddings for the input text"""
        try:
            response = self.openai_client.embeddings.create(
                input=text,
                model="text-embedding-3-small"
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Embedding error: {e}")
            return []
    
    def extract_keywords(self, text):
        """Extract keywords from the question"""
        doc = self.nlp(text)
        # Keep only content words (not stopwords) that are alphabetic
        keywords = [t.text.lower() for t in doc if t.is_alpha and not t.is_stop]
        return keywords
    
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
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You're a sportswear brand analyst answering questions based on tweet data."},
                    {"role": "user", "content": f"Question: {question}\n\nTweets:\n{context}"}
                ],
                temperature=0.0
            )
            return response.choices[0].message.content.strip(), results
        except Exception as e:
            logger.error(f"LLM answer error: {e}")
            return "Error generating answer.", results
    
    def process_question(self, question):
        """Process a question and return answer with sources"""
        # 1. Generate vector embedding
        embedding = self.generate_embeddings(question)
        
        # 2. Perform hybrid search (vector + keyword)
        results = self.query_knowledge_graph(question, embedding)
        logger.info(f"Found {len(results)} relevant tweets")
        
        # 3. Generate answer using LLM
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

if __name__ == "__main__":
    # Simple command-line interface for testing
    qa = QASystem()
    try:
        print("Sportswear Brand Q&A (type 'exit' to quit):")
        while True:
            q = input("\nQuestion: ")
            if q.lower() in ["exit", "quit"]:
                break
            result = qa.process_question(q)
            print("\nAnswer:", result["answer"])
            print("Tweets found:", len(result["sources"]))
    finally:
        qa.close()