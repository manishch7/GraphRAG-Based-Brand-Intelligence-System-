# neo4j_connector.py
from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_DATABASE

def get_driver():
    """
    Create and return a Neo4j driver instance using credentials from the config.
    """
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
    return driver

# Optional: if you want a helper to create a session for a specific database
def get_session():
    """
    Return a Neo4j session connected to the configured database.
    """
    driver = get_driver()
    session = driver.session(database=NEO4J_DATABASE)
    return session