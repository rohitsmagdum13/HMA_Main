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















+-----------------------------------------------------------------------------------+
|                                 HMA Ingestion System                              |
+-----------------------------------------------------------------------------------+
|                                   Core Layer                                      |
|  +----------------------+    +-----------------------+    +---------------------+  |
|  |  settings.py         |    | logging_config.py     |    | exceptions.py       |  |
|  |  (Pydantic config)   |    |  (logger factory)     |    | (error hierarchy)   |  |
|  +----------+-----------+    +-----------+-----------+    +----------+----------+  |
|             |                            |                           |             |
+-------------v----------------------------v---------------------------v-------------+
|                                   Services Layer                                 |
|  +----------------------+    +-----------------------+    +---------------------+  |
|  | file_utils.py        |    | s3_client.py          |    | duplicate_detector.py|  |
|  | (discover/build keys)|    | (session, upload,     |    | (local/S3 duplicates)|  |
|  | detect scope, filters|    |  list, head, hashing) |    |  + cache/report      |  |
|  +----------+-----------+    +-----------+-----------+    +----------+----------+  |
|             |                            |                           |             |
+-------------v----------------------------v---------------------------v-------------+
|                                Orchestration Layer                                |
|  +----------------------+    +-----------------------+    +---------------------+  |
|  | queue.py             |    | producer.py           |    | worker.py            |  |
|  | (Job dataclass +     |    | (discover files +     |    | (N workers: consume  |  |
|  |  thread-safe queue)  |    |  enqueue to queue)    |    |  jobs -> S3 upload)  |  |
|  +----------+-----------+    +-----------+-----------+    +----------+----------+  |
|             ^                            |                           |             |
|             |                            | (puts Job)                | (gets Job)  |
+-------------+----------------------------+---------------------------+-------------+
|                                  Ingestion Entrypoints                            |
|  +----------------------+    +-----------------------+    +---------------------+  |
|  | api.py (FastAPI)     |    | cli.py (Monolith)     |    | streamlit_app.py     |
|  | POST /jobs -> enqueue|    | Uploader: discover +  |    | UI for discovery,    |
|  | GET /health          |    | dup-check + upload    |    | dup-scan, uploads    |
|  +----+-----------------+    +-----------+-----------+    +----------+----------+  |
|       | (JobQueue.put)               | (direct S3 via services)        |           |
+-------v------------------------------v----------------------------------v----------+
|                                   AWS Integrations                                 |
|  +----------------------+                      +---------------------------------+ |
|  | Amazon S3            |<---------------------|  s3_client.upload_file          | |
|  | (mba/, policy/ etc.) |                      |  check exists/list/hash, SSE    | |
|  +----------------------+                      +---------------------------------+ |
|                                                                                     |
|  (Planned/Optional)                                                                |
|  +----------------------+                                                          |
|  | AWS RDS (MySQL)      |  <- Future ETL / loaders not shown in current codebase   |
|  +----------------------+                                                          |
+-----------------------------------------------------------------------------------+




What each “microservice” does
1) queue.py — the shared job mailbox

Think of this as a small post office where “upload jobs” are dropped off and picked up:

Defines a Job (what file to upload, which bucket, and the S3 key).

Provides a thread-safe JobQueue with methods to put() jobs in, get() jobs out, mark a task as done, count failures, and fetch live stats (queued/processed/failed).

Used by everyone else: Producer puts jobs in; Workers take jobs out; the API uses it to enqueue jobs from HTTP requests.

2) producer.py — the job creator (file scanner → job enqueuer)

This service scans a local folder, figures out which files to process (optionally filtering by extensions), and turns each file into a Job with the right bucket and S3 key, then drops those jobs into the queue. In short: “discover, describe, and enqueue.”

Validates the scope (mba or policy) and pulls the correct bucket/prefix from settings.

Uses helper functions to discover files and build S3 keys.

For every discovered file, it creates a Job and calls job_queue.put(job).

3) worker.py — the uploader (job consumer → S3)

Workers are the delivery people. They sit in a loop, pull jobs from the queue, and upload each file to S3:

Builds an AWS session, then starts N worker threads (configurable concurrency).

Each worker calls upload_file(...) for the job’s bucket/key and records success/failure.

If drain_once is set, a worker exits when the queue is empty (useful for batch runs).

Updates queue stats so you can see how many succeeded/failed. In short: “take, upload, mark done.”

4) api.py — the HTTP front door

A small FastAPI app that lets other tools or people submit jobs over HTTP:

POST /jobs: validate path + scope, compute the S3 key, build a Job, and enqueue it.

GET /health: returns service health and queue statistics (queued/processed/failed).

Perfect when you want to trigger uploads remotely or integrate with another system.

How they work together (two common flows)
A) API-driven flow (push via HTTP)

A client sends POST /jobs with path and scope.

api.py validates the file, computes the S3 key, creates a Job, and put()s it on the JobQueue.

worker.py workers are running; each get()s a Job, calls S3 upload_file, then task_done() and updates failure counts if needed.

GET /health shows the live queue stats.

B) Batch flow (scan folder → enqueue → drain)

Run the Producer with your folder and scope. It discovers files, builds S3 keys, and enqueues a Job per file.

Start the Workers (e.g., 4 threads). They drain the queue, uploading files to S3 until empty, and then exit if drain_once=True.

Why it’s split this way (benefits)

Decoupling: Producer (find files) and Worker (upload files) can scale independently.

Resilience & visibility: The queue tracks processed/failed counts and current backlog.

Multiple entrypoints: Use the CLI/Producer for batches, Workers for throughput, API for remote triggers.

Related helpers these services rely on (FYI)

Settings & config: Buckets/prefixes, regions, etc. pulled from Pydantic settings.

Logging & error types: Uniform logs and domain exceptions across all services.

S3 utilities: Sessions, existence checks, and robust upload_file with retries/backoff.

File utilities: Discovery, scope detection, and S3 key construction used by Producer/API.

Monolithic CLI option: There’s also a single-process “Uploader” if you want to do discover+upload without the queue API; handy for quick runs.





1. queue.py – The Message Queue

Provides a thread-safe in-memory queue (JobQueue) that stores jobs.

A Job = file to upload (with path, scope, S3 bucket, and key).

Keeps stats: queued, processed, failed.

Acts like a “to-do list” for files.

Analogy: Imagine a bakery’s order slip box – customers (producers or API) drop slips (jobs), and bakers (workers) pick them up.

2. producer.py – The Producer

Scans directories for files (e.g., /data/mba/).

Creates a Job for each file (adds bucket, scope, and S3 key info).

Pushes these Jobs into the job_queue.

Analogy: Producer = the cashier at the bakery who writes down orders and puts them in the order box.

3. worker.py – The Worker

Pulls jobs out of job_queue.

Uploads the files to AWS S3 (upload_file from s3_client.py).

Marks jobs as processed or failed.

Runs in multiple threads for parallel uploads.

Analogy: Worker = the baker who takes slips from the box and bakes the cakes (uploads files).

4. api.py – The API Gateway

Exposes REST endpoints (/jobs, /stats, /health).

Lets external clients submit jobs via HTTP (instead of CLI).

On POST /jobs, validates file, creates Job, pushes into job_queue.

On GET /stats, returns queue statistics.

Analogy: API = the bakery’s online ordering system – you place orders via app instead of visiting the cashier.

📊 Flow Example (Step by Step)

Producer CLI:
You run hma-producer --scope mba --input ./data.
→ Producer scans folder, finds 5 PDF files.
→ Creates 5 jobs and enqueues them.

Worker Service:
You run hma-worker --concurrency 2.
→ 2 worker threads start.
→ Each worker takes a job, uploads file to S3.
→ Logs success/failure and updates stats.

API Call:
A user calls POST /jobs with { "path": "./data/mba/report.pdf", "scope": "mba" }.
→ API validates the file exists.
→ Creates a Job and pushes to job_queue.
→ Workers eventually pick it up and upload.

🔄 Flowchart (ASCII “squared” format)
+------------------+
|  Producer (CLI)  |
|  - scan files    |
|  - enqueue jobs  |
+---------+--------+
          |
          v
+---------+--------+        +------------------+
|   Job Queue      | <----> | API (FastAPI)    |
| - stores jobs    |        | - POST /jobs     |
| - stats, thread  |        | - GET /stats     |
+---------+--------+        +------------------+
          |
          v
+---------+--------+
|   Worker(s)      |
| - pull jobs      |
| - upload to S3   |
| - update stats   |
+------------------+


👉 In short:

queue.py = shared task manager

producer.py = job creator (CLI)

worker.py = job executor (file uploader)

api.py = job receiver (HTTP gateway)




API-driven flow (HTTP → Queue → Workers → S3)
+----------------------------------------------------------------------------------+
|                                   Client / Caller                                |
|  curl / Postman / App                                                            |
|     │ POST /jobs {path, scope}                                                   |
+-----┴-----------------------------------------------------------------------------+
      │
      v
+-----+----------------------------+
| api.py                          |
| create_app()                    |
|  └─ defines routes:             |
|     • GET /health               |
|     • GET /stats                |
|     • POST /jobs → create_job() |
+-----+----------------------------+
      │  create_job(request)
      │  - Validate file path & type
      │  - bucket = settings.get_bucket(scope)
      │  - prefix = settings.get_prefix(scope)
      │  - key = build_s3_key(scope, file, prefix)
      │  - job = Job(path, scope, key, bucket)
      │  - job_queue.put(job)
      v
+-----+----------------------------+
| queue.py                         |
| JobQueue (global: job_queue)    |
|  • put(job)                     |
|  • get(timeout)                 |
|  • task_done() / mark_failed()  |
|  • stats()                      |
+-----+----------------------------+
      │         (workers running)
      v
+-----+----------------------------+       +----------------------------------------+
| worker.py                        |  uses | s3_client.py                           |
| run_workers(..)                 -+-----> | upload_file(session,bucket,path,key..) |
|  └─ starts N threads             |       |  - duplicate checks (HEAD)             |
|     Worker.run(drain_once)       |       |  - retries & backoff                   |
|        ├─ job = job_queue.get()  |       |  - SSE, metadata, logging              |
|        ├─ process_job(job) ------+       +----------------------------------------+
|        ├─ job_queue.task_done()  |
|        └─ if !success: job_queue.mark_failed()
+----------------------------------+


create_job() validates, builds the S3 key, wraps it in a Job, and enqueues it.

JobQueue provides put/get/task_done/stats, used by API and workers.

Each worker thread loops on job_queue.get(), then calls process_job() → s3_client.upload_file() and finally task_done().

Producer→Worker flow (batch enqueue → workers drain)
+----------------------------------------------------------------------------------+
| CLI: producer.py main()                                                          |
|  - parse args (input, scope, include/exclude)                                    |
|  - enqueue_files(input_dir, scope, include, exclude)                             |
+-----+-----------------------------------------------------------------------------+
      │
      v
+-----+-----------------------------+
| producer.py                       |
| enqueue_files(...)                |
|  - bucket = settings.get_bucket() |
|  - prefix = settings.get_prefix() |
|  - files = discover_files(...)    |  ← file_utils.py
|  - for f in files:                |
|      key = build_s3_key(...)      |  ← file_utils.py
|      job_queue.put(Job(...))      |  ← queue.py
+-----+-----------------------------+
      │
      │ (jobs are now queued; run workers separately)
      v
+-----+----------------------------+       +----------------------------------------+
| worker.py                        |  uses | s3_client.py                           |
| run_workers(..) → Worker.run() --+-----> | upload_file(...)                       |
|  - job_queue.get()               |       |  - HEAD check / duplicate heuristic    |
|  - process_job(job)              |       |  - retry + backoff + SSE               |
|  - job_queue.task_done()         |       |                                        |
|  - mark_failed() if needed       |       +----------------------------------------+
+----------------------------------+


enqueue_files() gets bucket/prefix, uses discover_files() + build_s3_key(), then job_queue.put() per file.

Workers are exactly the same loop as in the API flow.

Monolithic CLI flow (single process discover → parallel upload)
+----------------------------------------------------------------------------------+
| cli.py main()                                                                    |
|  args.mode == "monolith" → run_monolith(args)                                    |
+-----+-----------------------------------------------------------------------------+
      │
      v
+-----+------------------------------+
| cli.py                             |
| run_monolith(args)                 |
|  - include/exclude = parse_ext..   |  ← file_utils.py
|  - files = discover_files(...)     |  ← file_utils.py
|  - auto_detect? scope detection    |  ← detect_scope_from_path()
|  - uploader = Uploader(...)        |
|  - stats = uploader.upload_batch(..)
+-----+------------------------------+
      │
      │  Uploader.upload_batch(...)
      │   - optional duplicate scan (local)
      │   - ThreadPoolExecutor: submit upload_single(...) for each file
      v
+-----+------------------------------+       +--------------------------------------+
| cli.py (Uploader)                  |  uses | s3_client.py                         |
| upload_single(file, input_dir)     +-----> | upload_file(session,bucket,path,key) |
|  - bucket/prefix via settings      |       |  - duplicate checks / overwrite      |
|  - key = build_s3_key(...)         |       |  - metadata + logging                |
|  - (optional) duplicate checks     |       +--------------------------------------+
|  - dry-run? or actual upload       |
+------------------------------------+


run_monolith() does discovery and then parallel uploads; no queue or workers are involved here.

Uploader.upload_single() selects scope (auto or fixed), builds key, and calls upload_file().













repo-root/
├─ .env
├─ pyproject.toml
├─ requirements.txt
├─ README.md
├─ logs/
│  ├─ app.log
│  └─ file_cache.json
├─ data/
│  └─ mba/
│     ├─ csv/
│     │  ├─ benefit_accumulator.csv
│     │  ├─ deductibles_oop.csv
│     │  ├─ MemberData.csv
│     │  └─ plan_details.csv
│     └─ pdf/
│        └─ benefit_coverage.pdf
├─ src/
│  └─ hma_main/
│     ├─ __init__.py
│     ├─ core/
│     │  ├─ __init__.py
│     │  ├─ settings.py              # .env loader (pydantic)
│     │  ├─ logging_config.py        # structured logging setup
│     │  └─ exceptions.py            # custom exception hierarchy
│     ├─ database/
│     │  ├─ __init__.py
│     │  ├─ schema.sql               # DDL for four staging/base tables
│     │  ├─ connection.py            # MySQLConnection class (pooling)
│     │  ├─ etl_pipeline.py          # ETLOrchestrator (class-based)
│     │  └─ dao.py                   # Data access helpers (UPSERTs)
│     ├─ services/
│     │  ├─ __init__.py
│     │  ├─ s3_client.py             # S3Client wrapper
│     │  ├─ file_utils.py            # CSV parsing, dtype coercion, validators
│     │  ├─ transformers.py          # Row-level transforms, schema mapping
│     │  ├─ mba_csv_loader.py        # High-level loader using ETLOrchestrator
│     │  └─ duplicate_detector.py    # Optional duplicate checks
│     ├─ microservices/              # Optional RT ingestion
│     │  ├─ __init__.py
│     │  ├─ api.py                   # POST /ingest?key=s3://... (FastAPI)
│     │  ├─ queue.py                 # In-memory/Redis/SQS adapter
│     │  ├─ producer.py              # Publishes jobs to queue
│     │  └─ worker.py                # Worker pulls → runs ETLOrchestrator
│     ├─ lambdas/                    # Optional S3-event Lambda entrypoint
│     │  └─ s3_csv_ingest_handler.py # Calls same ETLOrchestrator
│     ├─ cli/
│     │  ├─ __init__.py
│     │  ├─ etl_cli.py               # hma-etl load-all / load-one
│     │  └─ db_cli.py                # hma-db init (apply schema)
│     └─ utils/
│        ├─ __init__.py
│        └─ timeit.py                # timing decorator
├─ scripts/
│  ├─ init.py
│  ├─ check_duplicates.py
│  ├─ monitor_etl.py
│  └─ run_server.sh
└─ main.py                           # Optional: quick entry for local tests












+-----------------------------+
|        S3 (mba/csv/)        |
| benefit_accumulator.csv ... |
+--------------+--------------+
               |
               |   (Option A) Monolith CLI: hma-etl load-all
               |   (Option B) Microservices: POST /ingest → queue → worker
               |   (Option C) S3 Event: Lambda → ETLOrchestrator
               v
+-----------------------------+
|      ETLOrchestrator        |
|  - discovers CSV keys       |
|  - downloads/streams CSV    |
|  - validates schema         |
|  - transforms rows          |
|  - dedup/merge strategies   |
+--------------+--------------+
               |
               v
+-----------------------------+
|        DAO / UPSERTS        |
| batched inserts to RDS      |
| (idempotent keys, logging)  |
+--------------+--------------+
               |
               v
+-----------------------------+
|      RDS MySQL Tables       |
|  members, plans,            |
|  deductibles_oop,           |
|  benefit_accumulator        |
+--------------+--------------+
               |
               v
+-----------------------------+
|        Observability        |
| logs/app.log, metrics,timer |
| duplicate reports           |
+-----------------------------+














Lambda Function: hma-mba-csv-to-rds

Subnets: subnet-0457db3ac01d9dbc5 (us-east-1d), subnet-06e7e6bb7682dd102 (us-east-1a)
VPC: vpc-00b895c68db75483a
Route Table: rtb-070c322d638e7b58d (Main Route Table)

S3 Route: pl-63a5400a → vpce-0f868ebc0798e217c
Coverage: ALL subnets in VPC (including your Lambda subnets)
S3 Gateway VPC Endpoint: vpce-0f868ebc0798e217c

Type: Gateway (free)
Status: Available
Policy: Restricted to hma-mba-bucket/mba/csv/*
🔄 Traffic Flow:
Lambda (hma-mba-csv-to-rds)
    ↓
Private Subnets (172.31.x.x)
    ↓
Main Route Table (rtb-070c322d638e7b58d)
    ↓ [pl-63a5400a → vpce-0f868ebc0798e217c]
S3 Gateway VPC Endpoint
    ↓ [Policy: Only hma-mba-bucket/mba/csv/*]
Amazon S3