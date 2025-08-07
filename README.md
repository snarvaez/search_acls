# ACLs on Atlas Search and Vector Search

This project demonstrates different search approaches using MongoDB Atlas with ACL (Access Control List) filtering

NOTE: Requires MongoDB Atlas 8.1 or above (with support for `$rankFusion`) and `sample_mflix` sample dataset loaded.

1. **Add Unique ACLs** (`1_add_acls_unique.py`) - Utility to populate all documents with unique random ACL arrays
2. **Create Indexes** (`2_create_indexes.py`) - Creates required Atlas Search and Vector Search indexes
3. **Atlas Search** (`3_atlas_search.py`) - Full-text search with fuzzy matching
4. **Vector Search** (`4_vector_search.py`) - Semantic similarity search using OpenAI embeddings
5. **Rank Fusion** (`5_rank_fusion.py`) - Combines both vector and text search using MongoDB 8.1's `$rankFusion`

## Setup

### 1. Install Dependencies

```bash
pip3 install openai pymongo python-dotenv
```

### 2. Environment Configuration

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` and add your actual credentials:
   ```bash
   # OpenAI API Configuration
   OPENAI_API_KEY=your_actual_openai_api_key_here
   
   # MongoDB Atlas Configuration
   ATLAS_CONNECTION_STRING=your_actual_mongodb_connection_string_here
   ```

### 3. Get Required Credentials

#### OpenAI API Key
1. Go to [OpenAI Platform](https://platform.openai.com/account/api-keys)
2. Create a new API key
3. Copy the key to your `.env` file

#### MongoDB Atlas Connection String
1. Log into your MongoDB Atlas dashboard
2. Go to your cluster and click "Connect"
3. Choose "Connect your application"
4. Copy the connection string and replace `<password>` with your actual password
5. Add the connection string to your `.env` file

## Quick Start

1. **Setup Environment**: Copy `.env.example` to `.env` and add your credentials
2. **Add ACL Data**: `python3 1_add_acls_unique.py --run` (first time only)
3. **Create Indexes**: `python3 2_create_indexes.py` (first time only)
4. **Run Searches**: Use scripts 3 onwards for different search approaches

## Usage

**⚠️ Important**: Run scripts in numerical order for first-time setup!

### 1. Add Unique ACLs (Data Setup - Run First)
```bash
# Dry run (safe - shows what would be done without modifying data)
python3 1_add_acls_unique.py

# Actual execution (modifies all documents)
python3 1_add_acls_unique.py --run
```
**⚠️ WARNING**: The `--run` flag modifies ALL documents in the collection! It adds unique random ACL arrays (ACL1, ACL2, ACL3) to every document. Use this to set up test data for ACL filtering experiments. The script includes safety prompts, progress tracking, and a dry-run mode by default.

### 2. Create Required Indexes (Setup)
```bash
# Automatic index creation
python3 2_create_indexes.py

# Show manual instructions for Atlas UI
python3 2_create_indexes.py --manual
```
Creates the required Atlas Search and Vector Search indexes with ACL filtering support. Run this after adding ACLs to documents.

### 3. Atlas Search
```bash
python3 3_atlas_search.py
```
Performs full-text search with fuzzy matching and ACL filtering.

### 4. Vector Search
```bash
python3 4_vector_search.py
```
Performs semantic similarity search using OpenAI embeddings with ACL filtering.

### 5. Rank Fusion (MongoDB 8.1+)
```bash
python3 5_rank_fusion.py
```
Combines both vector and text search using MongoDB's native `$rankFusion` operator.


## Indexes Created

The `2_create_indexes.py` script creates two essential indexes:

### Atlas Search Index (`search_ACLs`)
- **Purpose**: Enables full-text search with ACL filtering
- **Fields**: ACL1, ACL2, ACL3 (all as number type)
- **Features**: Dynamic mapping for flexible text search

### Vector Search Index (`vector_ACLs`)
- **Purpose**: Enables semantic similarity search with ACL filtering
- **Vector Fields**:
  - `plot_embedding` (1536 dimensions, cosine similarity)
  - `plot_embedding_voyage_3_large` (2048 dimensions, cosine similarity)
- **Filter Fields**: ACL1, ACL2, ACL3 for access control

## Features

- **ACL Filtering**: All searches respect Access Control List constraints
- **Environment Variables**: Secure credential management using `.env` files
- **Multiple Search Types**: Vector similarity, text search, and hybrid rank fusion
- **MongoDB 8.1 Support**: Uses latest `$rankFusion` capabilities
- **Automated Index Creation**: Programmatic setup of required search indexes

## Security

- The `.env` file is excluded from version control via `.gitignore`
- Never commit actual credentials to the repository
- Use the `.env.example` file as a template for required environment variables

## Requirements

- Python 3.7+
- MongoDB Atlas cluster (M10+ recommended for search indexes)
- OpenAI API key for embedding generation
- MongoDB 8.1+ for rank fusion functionality

## File Structure

```
1_add_acls_unique.py          # Step 1: Add ACL data to documents
2_create_indexes.py           # Step 2: Create search indexes
3_atlas_search.py             # Atlas Search with ACL filtering
4_vector_search.py            # Vector Search with ACL filtering
5_rank_fusion.py              # Hybrid search (MongoDB 8.1+)
```
