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


# Initialize OpenAI client
openai.api_key = OPENAI_API_KEY

# Verify configuration
print(f"Using model: text-embedding-ada-002")
print(f"API key configured: {'Yes' if OPENAI_API_KEY else 'No'}")
print(f"API key starts with: {OPENAI_API_KEY[:10]}..." if OPENAI_API_KEY else "No API key")

# Connect to MongoDB Atlas
client = MongoClient(ATLAS_CONNECTION_STRING)
db = client["sample_mflix"]
collection = db["embedded_movies"]

# Vector search configuration
VECTOR_INDEX_NAME = "vector_ACLs"
EMBEDDING_FIELD = "plot_embedding"

# Input text to embed and search
#query_text = input("Enter your search query text: ")
query_text = "Power struggle in Chicago crime syndicate; ambitious hitman overthrows boss, ignites gang war, and faces downfall due to forbidden romance and overprotectiveness of his sister."
#query_text = "man dressed as bat fights clown"


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

# Test embedding generation and get embedding for query text
print("Testing embedding generation...")
query_embedding = get_embedding(query_text)
if query_embedding:
    print(f"✓ Successfully generated embedding with {len(query_embedding)} dimensions")
else:
    print("✗ Failed to generate embedding")
    exit(1)

# Define the filter
vector_filter = {
    "$and": [
        {"ACL1": {"$in": [17]}},
        {"ACL1": {"$in": [556]}},
        {"ACL2": {"$in": [83]}},
        {"ACL2": {"$in": [358]}}
    ]
}

# Check collection contents first
print("Checking collection...")
sample_doc = collection.find_one()
if sample_doc:
    print(f"Sample document keys: {list(sample_doc.keys())}")
    if 'plot_embedding' in sample_doc:
        embedding_type = type(sample_doc['plot_embedding'])
        print(f"Embedding field type: {embedding_type}")
        if hasattr(sample_doc['plot_embedding'], '__len__'):
            print(f"Embedding length: {len(sample_doc['plot_embedding'])}")
else:
    print("No documents found in collection!")

print(f"Total documents in collection: {collection.count_documents({})}")

# Perform direct MongoDB vector search
print("Performing vector search...")
try:
    pipeline = [
        {
            "$vectorSearch": {
                "index": VECTOR_INDEX_NAME,
                "path": EMBEDDING_FIELD,
                "queryVector": query_embedding,
                "numCandidates": 100,
                "limit": 5,
                "filter": vector_filter
            }
        },
        {
            "$project": {
                "title": 1,
                "plot": 1,
                "year": 1,
                "genres": 1,
                "ACL1": 1,
                "ACL2": 1,
                "ACL3": 1,
                "score": {"$meta": "vectorSearchScore"}
            }
        }
    ]

    results = list(collection.aggregate(pipeline))
    print(f"Found {len(results)} results")

    # Print results
    print("\nVector Search Results:")
    for i, doc in enumerate(results, 1):
        print(f"{i}. Title: {doc.get('title', 'N/A')}")
        print(f"   Plot: {doc.get('plot', 'N/A')[:200]}...")
        print(f"   Year: {doc.get('year', 'N/A')}")
        print(f"   Genres: {doc.get('genres', 'N/A')}")
        print(f"   ACL1: {doc.get('ACL1', 'N/A')}")
        print(f"   ACL2: {doc.get('ACL2', 'N/A')}")
        print(f"   Similarity Score: {doc.get('score', 'N/A')}")
        print("---")

except Exception as e:
    print(f"Vector search failed: {e}")

    # Fallback: try without filter
    print("Trying search without filter...")
    try:
        pipeline_no_filter = [
            {
                "$vectorSearch": {
                    "index": VECTOR_INDEX_NAME,
                    "path": EMBEDDING_FIELD,
                    "queryVector": query_embedding,
                    "numCandidates": 100,
                    "limit": 5
                }
            },
            {
                "$project": {
                    "title": 1,
                    "plot": 1,
                    "year": 1,
                    "genres": 1,
                    "ACL1": 1,
                    "ACL2": 1,
                    "ACL3": 1,
                    "score": {"$meta": "vectorSearchScore"}
                }
            }
        ]

        fallback_results = list(collection.aggregate(pipeline_no_filter))
        print(f"Fallback search found {len(fallback_results)} results")

        if fallback_results:
            print("\nFallback Search Results (without filter):")
            for i, doc in enumerate(fallback_results, 1):
                print(f"{i}. Title: {doc.get('title', 'N/A')}")
                print(f"   Plot: {doc.get('plot', 'N/A')[:200]}...")
                print(f"   Year: {doc.get('year', 'N/A')}")
                print(f"   Similarity Score: {doc.get('score', 'N/A')}")
                print("---")
    except Exception as fallback_error:
        print(f"Fallback search also failed: {fallback_error}")

# Close MongoDB connection
client.close()