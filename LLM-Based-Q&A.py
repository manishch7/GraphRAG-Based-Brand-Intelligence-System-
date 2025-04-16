import spacy
import openai
import configparser
import logging
# Import the connector functions instead of creating a new connection
from neo4j_connector import get_driver, get_session
from config import NEO4J_DATABASE

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load config
config = configparser.ConfigParser()
config.read("config.ini")
openai.api_key = config.get("openai", "api_key")

class QASystem:
    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")
        # Use the get_driver function from neo4j_connector instead of creating a new one
        self.neo4j_driver = get_driver()
        

    def correct_question(self, question):
        try:
            res = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Correct brand-related spelling, return only corrected question."},
                    {"role": "user", "content": question}
                ],
                temperature=0.0
            )
            return res.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"LLM correction error: {e}")
            return question

    def generate_embeddings(self, text):
        try:
            res = openai.Embedding.create(
                input=text,
                model="text-embedding-3-small"
            )
            return res.data[0].embedding
        except Exception as e:
            logger.error(f"Embedding error: {e}")
            return []

    def query_knowledge_graph(self, question, embedding):
        if not embedding or len(embedding) != 1536:
            logger.error("Invalid embedding length")
            return []

        doc = self.nlp(question)
        keywords = [t.text.lower() for t in doc if t.is_alpha and not t.is_stop]

        query = """
        CALL db.index.vector.queryNodes('tweet_embeddings', $topK, $embedding)
        YIELD node, score
        WITH node AS t, score AS semanticScore
        WITH t, semanticScore,
            reduce(s=0, k IN $keywords | s + CASE WHEN toLower(t.text) CONTAINS k THEN 1 ELSE 0 END) AS keywordScore
        OPTIONAL MATCH (t)<-[:POSTED]-(u:User)
        RETURN t.text AS tweet, t.created_at AS created, u.screen_name AS user,
               semanticScore, keywordScore, (semanticScore * 3 + keywordScore) AS relevance
        ORDER BY relevance DESC """
        
        # Use the database name from config when creating a session
        with self.neo4j_driver.session(database=NEO4J_DATABASE) as session:
            return session.run(query, {
                "embedding": embedding,
                "keywords": keywords,
                "topK": 150
            }).data()

    def generate_answer(self, question, results):
        if not results:
            return "No relevant info found.", []

        context = "\n".join(
            [f"- {r['user'] or 'Unknown'}: {r['tweet']}" for r in results]
        )

        try:
            res = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You're an Expert answering based on tweets about sports brands."},
                    {"role": "user", "content": f"Q: {question}\nTweets:\n{context}"}
                ],
                temperature=0.0,
                max_tokens=3000
            )
            return res.choices[0].message.content.strip(), results
        except Exception as e:
            logger.error(f"LLM answer error: {e}")
            return "Error generating answer.", results

    def process_question(self, question):
        corrected = self.correct_question(question)
        embedding = self.generate_embeddings(corrected)
        results = self.query_knowledge_graph(corrected, embedding)
        logger.info(f"Results found: {len(results)}")
        answer, sources = self.generate_answer(corrected, results)
        return {
            "original": question,
            "corrected": corrected,
            "answer": answer,
            "sources": sources
        }

    def close(self):
        self.neo4j_driver.close()

if __name__ == "__main__":
    qa = QASystem()
    try:
        print("Let me be helpful to get what you need (type 'exit' to quit):")
        while True:
            q = input("\nQuestion: ")
            if q.lower() in ["exit", "quit"]:
                break
            res = qa.process_question(q)
            print("\nCorrected:", res["corrected"])
            print("Answer:", res["answer"])
            print("Tweets used:", len(res["sources"]))
    finally:
        qa.close()
        print("Goodbye!")