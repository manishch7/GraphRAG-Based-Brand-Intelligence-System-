
#!/usr/bin/env python3
import traceback
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, Neo4jError

def test_connection(uri, user, password, database="tweets"):
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        
        with driver.session(database=database) as session:
            result = session.run("MATCH (n) RETURN count(n) as count")
            count = result.single()["count"]
            print(f"Successfully connected to the local Neo4j database!")
            print(f"Number of nodes in the database: {count}")
        
    except ServiceUnavailable as se:
        print("ServiceUnavailable error: Unable to connect to the Neo4j database.")
        print(f"Error details: {se}")
        traceback.print_exc()
    except Neo4jError as ne:
        print("Neo4jError occurred while connecting.")
        print(f"Error details: {ne}")
        traceback.print_exc()
    except Exception as e:
        print("An unexpected error occurred while trying to connect.")
        print(f"Error details: {e}")
        traceback.print_exc()
    finally:
        try:
            driver.close()
        except Exception:
            pass

if __name__ == "__main__":
    # Local Neo4j connection details
    uri = "bolt://localhost:7687"           # Default Bolt port for Neo4j
    user = "neo4j"                          # Default username
    password = "cyvgaq-Sadnoj-jagbe9"       # Replace with your actual password
    database = "tweets"                     # Default database name, change if needed

    test_connection(uri, user, password, database)
