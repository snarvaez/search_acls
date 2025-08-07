import os
import time
from pymongo import MongoClient
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

# Load credentials from environment variables
ATLAS_CONNECTION_STRING = os.getenv("ATLAS_CONNECTION_STRING")

# Validate that required environment variables are set
if not ATLAS_CONNECTION_STRING:
    raise ValueError("ATLAS_CONNECTION_STRING environment variable is not set")

def create_search_index(collection):
    """Create Atlas Search index for ACL filtering"""

    search_index_name = "search_ACLs"
    search_index_definition = {
        "mappings": {
            "dynamic": True,
            "fields": {
                "ACL1": {
                    "type": "number"
                },
                "ACL2": {
                    "type": "number"
                },
                "ACL3": {
                    "type": "number"
                }
            }
        }
    }

    print(f"Creating Atlas Search index: {search_index_name}")
    print(f"Definition: {json.dumps(search_index_definition, indent=2)}")

    # Check if index already exists
    try:
        existing_indexes = list(collection.list_search_indexes())
        for idx in existing_indexes:
            if idx.get('name') == search_index_name and idx.get('type') == 'search':
                print(f"⚠️  Search index '{search_index_name}' already exists and is {idx.get('status', 'unknown')}")
                return True
    except Exception as e:
        print(f"Warning: Could not check existing indexes: {e}")

    try:
        # Try different methods for creating search index
        try:
            # Method 1: Using SearchIndexModel (newer PyMongo versions)
            from pymongo.operations import SearchIndexModel
            search_index_model = SearchIndexModel(
                definition=search_index_definition,
                name=search_index_name
            )
            result = collection.create_search_indexes([search_index_model])
            print(f"✅ Search index creation initiated. Result: {result}")
            return True
        except ImportError:
            # Method 2: Direct method call (older versions)
            result = collection.create_search_index(
                model={
                    "definition": search_index_definition,
                    "name": search_index_name
                }
            )
            print(f"✅ Search index creation initiated. Index name: {result}")
            return True
    except Exception as e:
        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
            print(f"⚠️  Search index '{search_index_name}' already exists")
            return True
        else:
            print(f"❌ Error creating search index: {e}")
            return False

def create_vector_index(collection):
    """Create Atlas Vector Search index for embeddings and ACL filtering"""

    vector_index_name = "vector_ACLs"
    vector_index_definition = {
        "fields": [
            {
                "numDimensions": 1536,
                "path": "plot_embedding",
                "similarity": "cosine",
                "type": "vector"
            },
            {
                "numDimensions": 2048,
                "path": "plot_embedding_voyage_3_large",
                "similarity": "cosine",
                "type": "vector"
            },
            {
                "path": "ACL1",
                "type": "filter"
            },
            {
                "path": "ACL2",
                "type": "filter"
            },
            {
                "path": "ACL3",
                "type": "filter"
            }
        ]
    }

    print(f"\nCreating Atlas Vector Search index: {vector_index_name}")
    print(f"Definition: {json.dumps(vector_index_definition, indent=2)}")

    # Check if index already exists
    try:
        existing_indexes = list(collection.list_search_indexes())
        for idx in existing_indexes:
            if idx.get('name') == vector_index_name and idx.get('type') == 'vectorSearch':
                print(f"⚠️  Vector search index '{vector_index_name}' already exists and is {idx.get('status', 'unknown')}")
                return True
    except Exception as e:
        print(f"Warning: Could not check existing indexes: {e}")

    try:
        # Try different methods for creating vector search index
        try:
            # Method 1: Using SearchIndexModel (newer PyMongo versions)
            from pymongo.operations import SearchIndexModel
            vector_index_model = SearchIndexModel(
                definition=vector_index_definition,
                name=vector_index_name,
                type="vectorSearch"
            )
            result = collection.create_search_indexes([vector_index_model])
            print(f"✅ Vector search index creation initiated. Result: {result}")
            return True
        except ImportError:
            # Method 2: Direct method call (older versions)
            result = collection.create_search_index(
                model={
                    "definition": vector_index_definition,
                    "name": vector_index_name,
                    "type": "vectorSearch"
                }
            )
            print(f"✅ Vector search index creation initiated. Index name: {result}")
            return True
    except Exception as e:
        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
            print(f"⚠️  Vector search index '{vector_index_name}' already exists")
            return True
        else:
            print(f"❌ Error creating vector search index: {e}")
            return False

def list_existing_indexes(collection):
    """List all existing search indexes"""
    print("\n📋 Listing existing search indexes...")
    
    try:
        indexes = list(collection.list_search_indexes())
        if indexes:
            for idx in indexes:
                print(f"  - Name: {idx.get('name', 'N/A')}")
                print(f"    Type: {idx.get('type', 'search')}")
                print(f"    Status: {idx.get('status', 'N/A')}")
                print(f"    Queryable: {idx.get('queryable', 'N/A')}")
                print()
        else:
            print("  No search indexes found")
    except Exception as e:
        print(f"❌ Error listing indexes: {e}")

def wait_for_indexes(collection, timeout_minutes=10):
    """Wait for indexes to become queryable"""
    print(f"\n⏳ Waiting for indexes to become queryable (timeout: {timeout_minutes} minutes)...")
    
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    
    while time.time() - start_time < timeout_seconds:
        try:
            indexes = list(collection.list_search_indexes())
            all_ready = True
            
            for idx in indexes:
                if idx.get('name') in ['search_ACLs', 'vector_ACLs']:
                    if not idx.get('queryable', False):
                        all_ready = False
                        print(f"  Index '{idx.get('name')}' status: {idx.get('status', 'N/A')}")
            
            if all_ready:
                print("✅ All indexes are now queryable!")
                return True
            
            print("  Still waiting for indexes to be ready...")
            time.sleep(30)  # Wait 30 seconds before checking again
            
        except Exception as e:
            print(f"❌ Error checking index status: {e}")
            break
    
    print(f"⚠️  Timeout reached. Some indexes may still be building.")
    return False

def main():
    """Main function to create all indexes"""
    print("🚀 MongoDB Atlas Search Index Creator")
    print("=" * 50)
    
    # Connect to MongoDB Atlas
    try:
        client = MongoClient(ATLAS_CONNECTION_STRING)
        db = client["sample_mflix"]
        collection = db["embedded_movies"]
        
        print("✅ Connected to MongoDB Atlas")
        print(f"Database: {db.name}")
        print(f"Collection: {collection.name}")
        print(f"Document count: {collection.count_documents({})}")
        
    except Exception as e:
        print(f"❌ Failed to connect to MongoDB Atlas: {e}")
        return
    
    # List existing indexes first
    list_existing_indexes(collection)
    
    # Create indexes
    search_success = create_search_index(collection)
    vector_success = create_vector_index(collection)
    
    if search_success and vector_success:
        print("\n🎉 Index creation commands completed successfully!")
        print("\nNote: Index creation is asynchronous. It may take several minutes")
        print("for the indexes to become queryable, especially for large collections.")
        
        # Wait for indexes to become ready
        wait_for_indexes(collection)
        
        # List indexes again to show final status
        print("\n📋 Final index status:")
        list_existing_indexes(collection)
        
    else:
        print("\n❌ Some indexes failed to create. Check the errors above.")
    
    # Close connection
    client.close()
    print("\n🔌 MongoDB connection closed.")

def show_manual_instructions():
    """Show manual instructions for creating indexes via Atlas UI"""
    print("\n" + "="*60)
    print("📖 MANUAL INDEX CREATION INSTRUCTIONS")
    print("="*60)
    print("\nIf programmatic index creation fails, you can create them manually:")
    print("\n1. Go to MongoDB Atlas Dashboard")
    print("2. Navigate to your cluster")
    print("3. Click on 'Search' tab")
    print("4. Click 'Create Search Index'")
    
    print("\n🔍 ATLAS SEARCH INDEX:")
    print("   Name: search_ACLs")
    print("   Definition:")
    search_def = {
        "mappings": {
            "dynamic": True,
            "fields": {
                "ACL1": {"type": "number"},
                "ACL2": {"type": "number"},
                "ACL3": {"type": "number"}
            }
        }
    }
    print(json.dumps(search_def, indent=4))
    
    print("\n🎯 VECTOR SEARCH INDEX:")
    print("   Name: vector_ACLs")
    print("   Definition:")
    vector_def = {
        "fields": [
            {
                "numDimensions": 1536,
                "path": "plot_embedding",
                "similarity": "cosine",
                "type": "vector"
            },
            {
                "numDimensions": 2048,
                "path": "plot_embedding_voyage_3_large",
                "similarity": "cosine",
                "type": "vector"
            },
            {
                "path": "ACL1",
                "type": "filter"
            },
            {
                "path": "ACL2",
                "type": "filter"
            },
            {
                "path": "ACL3",
                "type": "filter"
            }
        ]
    }
    print(json.dumps(vector_def, indent=4))

if __name__ == "__main__":
    import sys
    
    if "--manual" in sys.argv:
        show_manual_instructions()
    else:
        try:
            main()
        except Exception as e:
            print(f"\n❌ Script failed: {e}")
            print("\nTrying manual instructions...")
            show_manual_instructions()
