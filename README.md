# HMA_Main - S3 Data Ingestion System

Production-ready data ingestion system for uploading files to Amazon S3 with structured path conventions.

## Features

- **Dual Architecture**: Monolithic CLI and microservices modes
- **Smart Path Convention**: Automatic S3 key generation based on file type
- **Concurrent Processing**: Configurable parallel uploads
- **Robust Error Handling**: Retry logic and comprehensive logging
- **Flexible Filtering**: Include/exclude files by extension
- **Dry Run Mode**: Preview operations without uploading
- **Production Ready**: Centralized configuration, logging, and exception handling
- **Local Duplicate Detection**: Finds duplicate files within your local directories
- **S3 Duplicate Check**: Checks if files already exist in S3 before uploading
- **Smart Comparison**: Uses file size and MD5 hash for accurate duplicate detection
- **Caching**: Maintains a cache of file hashes for faster subsequent scans
- **Detailed Reporting**: Generates reports showing duplicate files and their locations


### Usage

#### Check for Duplicates Without Uploading
```bash
# Check for local duplicates only
hma-ingest --mode check-duplicates --input ./data

# Check local files against S3
hma-ingest --mode check-duplicates --input ./data --check-s3

# Check specific scope against S3
hma-ingest --mode check-duplicates --input ./data --scope mba --check-s3


## Quick Start (< 2 minutes)

### 1. Install Dependencies
```bash
# Clone repository
git clone <repo-url> HMA_Main
cd HMA_Main

# Install uv if not already installed
pip install uv

# Install package and dependencies
uv add boto3 python-dotenv fastapi uvicorn pydantic pydantic-settings
uv pip install -e .