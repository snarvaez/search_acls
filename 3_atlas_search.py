from pymongo import MongoClient
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load credentials from environment variables
ATLAS_CONNECTION_STRING = os.getenv("ATLAS_CONNECTION_STRING")

# Validate that required environment variables are set
if not ATLAS_CONNECTION_STRING:
    raise ValueError("ATLAS_CONNECTION_STRING environment variable is not set")

# Connect to MongoDB Atlas
client = MongoClient(ATLAS_CONNECTION_STRING)
db = client["sample_mflix"]
collection = db["embedded_movies"]

# Atlas Search configuration
SEARCH_INDEX_NAME = "search_ACLs"
SEARCH_FIELD = "fullplot"

# Input text to search
query_text = "Power struggle in Chicago crime syndicate; ambitious hitman overthrows boss, ignites gang war, and faces downfall due to forbidden romance and overprotectiveness of his sister."

# Check collection contents first
print("Checking collection...")
sample_doc = collection.find_one()
if sample_doc:
    print(f"Sample document keys: {list(sample_doc.keys())}")
    if 'fullplot' in sample_doc:
        print(f"Sample fullplot: {sample_doc['fullplot'][:200]}...")
    else:
        print("No 'fullplot' field found in sample document")
else:
    print("No documents found in collection!")

print(f"Total documents in collection: {collection.count_documents({})}")

# Perform Atlas Search with ACL filters
print("Performing Atlas Search...")
try:
    # Atlas Search pipeline using $search with fuzzy text search
    pipeline = [
        {
            "$search": {
                "index": "search_ACLs",
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
                                "path": "fullplot",
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
            "$project": {
                "title": 1,
                "plot": 1,
                "fullplot": 1,
                "year": 1,
                "genres": 1,
                "ACL1": 1,
                "ACL2": 1,
                "ACL3": 1,
                "score": {"$meta": "searchScore"}
            }
        },
        {
            "$limit": 5
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    print(f"Found {len(results)} results")
    
    # Print results
    print("\nAtlas Search Results:")
    for i, doc in enumerate(results, 1):
        print(f"{i}. Title: {doc.get('title', 'N/A')}")
        print(f"   Plot: {doc.get('plot', 'N/A')[:200]}...")
        print(f"   Full Plot: {doc.get('fullplot', 'N/A')[:300]}...")
        print(f"   Year: {doc.get('year', 'N/A')}")
        print(f"   Genres: {doc.get('genres', 'N/A')}")
        print(f"   ACL1: {doc.get('ACL1', 'N/A')}")
        print(f"   ACL2: {doc.get('ACL2', 'N/A')}")
        print(f"   Search Score: {doc.get('score', 'N/A')}")
        print("---")

except Exception as e:
    print(f"Atlas Search failed: {e}")
    
    # Fallback: try without ACL filters
    print("Trying search without ACL filters...")
    try:
        pipeline_no_filter = [
            {
                "$search": {
                    "index": SEARCH_INDEX_NAME,
                    "text": {
                        "query": query_text,
                        "path": SEARCH_FIELD
                    }
                }
            },
            {
                "$project": {
                    "title": 1,
                    "plot": 1,
                    "fullplot": 1,
                    "year": 1,
                    "genres": 1,
                    "ACL1": 1,
                    "ACL2": 1,
                    "ACL3": 1,
                    "score": {"$meta": "searchScore"}
                }
            },
            {
                "$limit": 5
            }
        ]
        
        fallback_results = list(collection.aggregate(pipeline_no_filter))
        print(f"Fallback search found {len(fallback_results)} results")
        
        if fallback_results:
            print("\nFallback Search Results (without ACL filters):")
            for i, doc in enumerate(fallback_results, 1):
                print(f"{i}. Title: {doc.get('title', 'N/A')}")
                print(f"   Plot: {doc.get('plot', 'N/A')[:200]}...")
                print(f"   Year: {doc.get('year', 'N/A')}")
                print(f"   Search Score: {doc.get('score', 'N/A')}")
                print("---")
    except Exception as fallback_error:
        print(f"Fallback search also failed: {fallback_error}")

# Close MongoDB connection
client.close()
