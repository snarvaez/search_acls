import openai
from pymongo import MongoClient
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load credentials from environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ATLAS_CONNECTION_STRING = os.getenv("ATLAS_CONNECTION_STRING")

# Validate that required environment variables are set
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is not set")
if not ATLAS_CONNECTION_STRING:
    raise ValueError("ATLAS_CONNECTION_STRING environment variable is not set")

# Connect to MongoDB Atlas
client = MongoClient(ATLAS_CONNECTION_STRING)
db = client["sample_mflix"]
collection = db["embedded_movies"]

# Initialize OpenAI client
openai.api_key = OPENAI_API_KEY

# Configuration
VECTOR_INDEX_NAME = "vector_ACLs"
SEARCH_INDEX_NAME = "search_ACLs"
EMBEDDING_FIELD = "plot_embedding"
SEARCH_FIELD = "fullplot"

# Query text
query_text = "Power struggle in Chicago crime syndicate; ambitious hitman overthrows boss, ignites gang war, and faces downfall due to forbidden romance and overprotectiveness of his sister."

# ACL filters
vector_filter = {
    "$and": [
        {"ACL1": {"$in": [17]}},
        {"ACL1": {"$in": [556]}},
        {"ACL2": {"$in": [83]}},
        {"ACL2": {"$in": [358]}}
    ]
}

print(f"Using query: {query_text}")
print(f"Using ACL filters: {vector_filter}")

# Function to generate embeddings using OpenAI API
def get_embedding(text, model="text-embedding-ada-002"):
    """Generate embedding for given text using OpenAI API"""
    try:
        response = openai.embeddings.create(
            input=text,
            model=model
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"Error generating embedding: {e}")
        return None

# Check collection contents
print("\nChecking collection...")
sample_doc = collection.find_one()
if sample_doc:
    print(f"Sample document keys: {list(sample_doc.keys())}")
    print(f"Total documents in collection: {collection.count_documents({})}")
else:
    print("No documents found in collection!")
    exit(1)

# Generate embedding for query
print("\nGenerating embedding for query...")
query_embedding = get_embedding(query_text)
if query_embedding:
    print(f"✓ Successfully generated embedding with {len(query_embedding)} dimensions")
else:
    print("✗ Failed to generate embedding")
    exit(1)

# Perform rank fusion search combining vector search and text search
print("\nPerforming rank fusion search...")
try:
    pipeline = [
        {
            "$rankFusion": {
                "input": {
                    "pipelines": {
                        "vectorSearch": [
                            {
                                "$vectorSearch": {
                                    "index": VECTOR_INDEX_NAME,
                                    "path": EMBEDDING_FIELD,
                                    "queryVector": query_embedding,
                                    "numCandidates": 100,
                                    "limit": 20,
                                    "filter": vector_filter
                                }
                            }
                        ],
                        "textSearch": [
                            {
                                "$search": {
                                    "index": SEARCH_INDEX_NAME,
                                    "compound": {
                                        "must": [
                                            {
                                                "in": {
                                                    "value": 17,
                                                    "path": "ACL1"
                                                }
                                            },
                                            {
                                                "in": {
                                                    "value": 556,
                                                    "path": "ACL1"
                                                }
                                            },
                                            {
                                                "in": {
                                                    "value": 83,
                                                    "path": "ACL2"
                                                }
                                            },
                                            {
                                                "in": {
                                                    "value": 358,
                                                    "path": "ACL2"
                                                }
                                            }
                                        ],
                                        "should": [
                                            {
                                                "text": {
                                                    "query": query_text,
                                                    "path": SEARCH_FIELD,
                                                    "fuzzy": {
                                                        "maxEdits": 2,
                                                        "prefixLength": 3,
                                                        "maxExpansions": 50
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                }
                            },
                            {
                                "$limit": 20
                            }
                        ]
                    }
                }
            }
        },
        {
            "$limit": 5
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    print(f"Found {len(results)} results using rank fusion")
    
    # Print results
    print("\nRank Fusion Results:")
    for i, doc in enumerate(results, 1):
        print(f"{i}. Title: {doc.get('title', 'N/A')}")
        print(f"   Plot: {doc.get('plot', 'N/A')[:200]}...")
        print(f"   Full Plot: {doc.get('fullplot', 'N/A')[:300]}...")
        print(f"   Year: {doc.get('year', 'N/A')}")
        print(f"   Genres: {doc.get('genres', 'N/A')}")
        print(f"   ACL1: {doc.get('ACL1', 'N/A')}")
        print(f"   ACL2: {doc.get('ACL2', 'N/A')}")
        print(f"   Rank Fusion Score: {doc.get('rank_fusion_score', 'N/A')}")
        print("---")

except Exception as e:
    print(f"Rank fusion search failed: {e}")
    print("This might be because $rankFusion is not available in your MongoDB version.")
    print("Please upgrade to MongoDB 8.1+ for native $rankFusion support.")

# Close MongoDB connection
client.close()
