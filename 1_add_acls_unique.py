import random
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from pymongo.errors import BulkWriteError
import time

# Load environment variables from .env file
load_dotenv()

# Load credentials from environment variables
ATLAS_CONNECTION_STRING = os.getenv("ATLAS_CONNECTION_STRING")

# Validate that required environment variables are set
if not ATLAS_CONNECTION_STRING:
    raise ValueError("ATLAS_CONNECTION_STRING environment variable is not set")

def get_random_int(min_val, max_val):
    """Generate a random integer between min_val and max_val (inclusive)"""
    return random.randint(min_val, max_val)

def generate_acl_array():
    """Generate an array of random length (0 to 500) with unique random ACLs (1 to 32767)"""
    max_length = 500
    length = get_random_int(0, max_length)
    acl_set = set()  # Use set to ensure uniqueness
    
    while len(acl_set) < length:
        acl_set.add(get_random_int(1, 32767))
    
    return sorted(list(acl_set))  # Convert set to list and sort numerically

def main():
    # Connect to MongoDB Atlas
    client = MongoClient(ATLAS_CONNECTION_STRING)
    db = client["sample_mflix"]
    collection = db["embedded_movies"]
    
    print("Connecting to MongoDB Atlas...")
    total_docs = collection.count_documents({})
    print(f"Total documents in collection: {total_docs}")
    
    if total_docs == 0:
        print("No documents found in collection!")
        return
    
    # Configuration
    BATCH_SIZE = 1000  # Process 1000 documents per batch
    operations = 0
    batch_operations = []
    
    print(f"Starting to add unique ACL arrays to all documents...")
    print(f"Processing in batches of {BATCH_SIZE} documents")
    
    start_time = time.time()
    
    try:
        # Iterate through all documents in the collection
        cursor = collection.find({}, {"_id": 1})  # Only fetch _id to minimize memory usage
        
        for doc in cursor:
            # Prepare update operation
            update_operation = {
                "updateOne": {
                    "filter": {"_id": doc["_id"]},
                    "update": {
                        "$set": {
                            "ACL1": generate_acl_array(),
                            "ACL2": generate_acl_array(),
                            "ACL3": generate_acl_array()
                        }
                    }
                }
            }
            
            batch_operations.append(update_operation)
            operations += 1
            
            # Execute batch when reaching BATCH_SIZE
            if operations % BATCH_SIZE == 0:
                try:
                    result = collection.bulk_write(batch_operations, ordered=False)
                    print(f"Processed {operations} documents - Modified: {result.modified_count}")
                    
                    # Clear batch operations for next batch
                    batch_operations = []
                    
                    # Show progress
                    elapsed_time = time.time() - start_time
                    docs_per_second = operations / elapsed_time
                    estimated_total_time = total_docs / docs_per_second
                    remaining_time = estimated_total_time - elapsed_time
                    
                    print(f"  Progress: {operations}/{total_docs} ({operations/total_docs*100:.1f}%)")
                    print(f"  Speed: {docs_per_second:.1f} docs/sec")
                    print(f"  Estimated time remaining: {remaining_time/60:.1f} minutes")
                    
                except BulkWriteError as e:
                    print(f"Bulk write error at operation {operations}: {e}")
                    # Continue with next batch
                    batch_operations = []
        
        # Execute any remaining operations
        if batch_operations:
            try:
                result = collection.bulk_write(batch_operations, ordered=False)
                print(f"Final batch processed - Modified: {result.modified_count}")
            except BulkWriteError as e:
                print(f"Bulk write error in final batch: {e}")
        
        elapsed_time = time.time() - start_time
        print(f"\nCompleted! Processed {operations} documents in {elapsed_time/60:.2f} minutes")
        print("Unique ACL arrays added to all documents")
        
        # Verify the update by checking a few random documents
        print("\nVerifying updates...")
        sample_docs = list(collection.aggregate([{"$sample": {"size": 3}}]))
        
        for i, doc in enumerate(sample_docs, 1):
            print(f"Sample document {i}:")
            print(f"  ID: {doc['_id']}")
            print(f"  ACL1 length: {len(doc.get('ACL1', []))}")
            print(f"  ACL2 length: {len(doc.get('ACL2', []))}")
            print(f"  ACL3 length: {len(doc.get('ACL3', []))}")
            if doc.get('ACL1'):
                print(f"  ACL1 sample: {doc['ACL1'][:5]}{'...' if len(doc['ACL1']) > 5 else ''}")
            print()
        
    except Exception as e:
        print(f"Error during processing: {e}")
        
    finally:
        # Close MongoDB connection
        client.close()
        print("MongoDB connection closed.")

def dry_run():
    """Perform a dry run to test connection and show what would be done"""
    # Connect to MongoDB Atlas
    client = MongoClient(ATLAS_CONNECTION_STRING)
    db = client["sample_mflix"]
    collection = db["embedded_movies"]

    print("🔍 DRY RUN MODE - Testing connection and showing sample data...")
    print("Connecting to MongoDB Atlas...")
    total_docs = collection.count_documents({})
    print(f"Total documents in collection: {total_docs}")

    if total_docs == 0:
        print("No documents found in collection!")
        return

    # Show sample ACL arrays that would be generated
    print("\nSample ACL arrays that would be generated:")
    for i in range(3):
        acl1 = generate_acl_array()
        acl2 = generate_acl_array()
        acl3 = generate_acl_array()
        print(f"  Sample {i+1}:")
        print(f"    ACL1 (length {len(acl1)}): {acl1[:10]}{'...' if len(acl1) > 10 else ''}")
        print(f"    ACL2 (length {len(acl2)}): {acl2[:10]}{'...' if len(acl2) > 10 else ''}")
        print(f"    ACL3 (length {len(acl3)}): {acl3[:10]}{'...' if len(acl3) > 10 else ''}")
        print()

    # Check if documents already have ACL fields
    docs_with_acls = collection.count_documents({"ACL1": {"$exists": True}})
    print(f"Documents that already have ACL1 field: {docs_with_acls}")

    client.close()
    print("Dry run completed. Use 'python3 add_acls_unique.py --run' to execute the actual update.")

if __name__ == "__main__":
    import sys

    if "--run" in sys.argv:
        # Confirm before running (this modifies all documents!)
        print("⚠️  WARNING: This script will modify ALL documents in the embedded_movies collection!")
        print("   It will add/update ACL1, ACL2, and ACL3 fields with random unique arrays.")
        print("   This operation cannot be easily undone.")

        confirm = input("\nDo you want to continue? (yes/no): ").lower().strip()

        if confirm in ['yes', 'y']:
            main()
        else:
            print("Operation cancelled.")
    else:
        # Default to dry run
        dry_run()
