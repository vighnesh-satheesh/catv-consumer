# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is **portal-catv-consumer-service**, a Django-based microservice for Sentinel Protocol's CATV (Crypto Asset Tracking & Visualization) system. It processes blockchain transaction tracking jobs from a database queue, fetches graph data from external APIs (Bitquery, Tracer), and produces flow/path visualization results. It supports 40+ blockchain networks (ETH, BTC, TRON, SOL, L2 EVMs, etc.).

## Development Setup

```bash
# System dependencies
# Mac: brew install libmagic
# Ubuntu: sudo apt install libmagic

# Create and activate virtualenv
virtualenv venv && source venv/bin/activate

# Initialize git submodule (indicator-lib)
git submodule init && git submodule update

# Install dependencies
pip install -r requirements/development.txt
cd library/indicator-lib/src/py && python setup.py install && cd -

# Environment variables required
export CATV_CONSUMER_API_ENV=development
# Option A: point to a JSON config file (path.json for staging)
export CATV_CONSUMER_ENV_PATH=path.json
# Option B: use GCP Secret Manager
export GCP_PROJECT_ID=<project-id>
export GCP_SECRET_NAME=<secret-name>
# Option C: use an env file (see sample.env.example)
export CATV_CONSUMER_API_ENV_PATH=/path/to/env/file.env
```

### Running Services

```bash
# Django dev server
python manage.py runserver

# CATV consumer (main job processor — long-running polling loop)
python manage.py catv_consume

# Celery worker
python -m celery --app=portal_api.celery_app:app worker -c 2 -l info
```

### Docker

```bash
cd docker
docker-compose up    # Builds and runs on port 8300 -> 8000
```

## Architecture

### Startup Flow
1. `manage.py` calls `portal_api.AppInit()` — a singleton
2. `AppInit` validates `CATV_CONSUMER_API_ENV` (must be `development` or `production`)
3. `startup_util.py` loads config from one of: local JSON file (`CATV_CONSUMER_ENV_PATH`), GCP Secret Manager (`GCP_PROJECT_ID`/`GCP_SECRET_NAME`), or env file (`CATV_CONSUMER_API_ENV_PATH`)
4. Django settings module is selected via `portal_api.settings.{development|production}`

### Core Processing Pipeline
- **`api/management/commands/catv_consume.py`** — Long-running management command that polls `CatvNeoJobQueue` and `CatvNeoCSVJobQueue` tables for pending jobs in a loop
- **`api/consumers/catvmessages.py`** — `process_catv_messages()` deserializes jobs using token-type + search-type specific serializers (e.g., `CATVETHSerializer`, `CATVBTCPathSerializer`), then delegates to tracking. Serializer selection is a nested map: `{token_type: {search_type: SerializerClass}}`
- **`api/catvutils/tracking_results.py`** — `TrackingResults` orchestrates external API calls and graph generation. Uses `multiprocessing.pool.ThreadPool` for parallel source/distribution fetches. Supports pre-fetched tracer data for KYT-sourced jobs
- **`api/catvutils/graphtools.py`** — Graph node/edge generation from raw API responses (separate functions for coinpath vs standard formats)
- **`api/tasks.py`** — Celery tasks for async history recording (`catv_history_task`, `catv_path_history_task`)

### Token Type Classification
Serializers and API interfaces classify chains into two families:
- **BTC-like** (BTC, LTC, BCH, DOGE, ZEC, DASH) — use `CATVBTCSerializer` / `CATVBTCPathSerializer`
- **ETH-like** (ETH, TRON, BSC, SOL, ARB, OP, BASE, and all other L2 EVMs) — use `CATVETHSerializer` / `CATVETHPathSerializer`

New chain support typically requires: adding to `CatvTokens` enum in `models.py`, adding serializer mapping in `catvmessages.py`, and adding chain mappings in `constants.py`.

### External Integrations
- **Bitquery/GraphQL** — Blockchain graph data via `bitquery_interface.py` and `graphql_interface.py`
- **Tracer API** — Transaction tracing via `tracer_interface.py`
- **RabbitMQ RPC** — `api/rpc/` for inter-service communication (indicator lookups, CARA reports, S3 file updates)
- **GCS** — File storage via `api/storages/gcs.py`
- **Redis** — Multiple instances for caching, Celery broker, token storage, and CATV data cache
- **PostgreSQL** — Primary DB with read-only replica (`DATABASE_URL` / `DATABASE_READONLY_URL`), routed via `portal_api/settings/DatabaseRouter.py`

### Key Models (`api/models.py`)
- `CatvTokens` enum — All supported blockchain networks
- `CatvSearchType` — FLOW vs PATH search types
- `CatvNeoJobQueue` / `CatvNeoCSVJobQueue` — Job queue tables (polled by consumer)
- `CatvResult` — Tracking results storage

### Settings
- Base config in `portal_api/settings/base.py`, extended by `development.py` and `production.py`
- Uses `django-environ` for env parsing
- Most API config lives in the `API_SETTINGS` dict in base settings
- `api/settings.py` exposes `api_settings` as a convenient accessor for `API_SETTINGS`
- `CATV_CONSUMER_API_ENV` env var selects the settings module

### PM2 Runtime (Production)
The container runs three processes via PM2:
1. `catv_consume` management command (the job consumer)
2. Celery worker
3. Local Redis server

## Key Conventions

- **Raw SQL** queries are centralized in `api/constants.py` (`Constants.QUERIES[...]`). Chain-to-network mappings also live here (`NETWORK_CHAIN_MAPPING_FOR_QUERY`, etc.)
- **Serializers** in `api/serializers.py` are mapped by token type and search type
- **`api/catvutils/`** contains all blockchain-specific logic and external API interfaces
- **No project-level test suite** — the repo has no `tests/` directory or test runner configuration. The only test-like files are manual scripts in `api/catvutils/` (`test_tracer.py`, `test_metrics_update.py`)
- **Git submodule** — `library/indicator-lib` is a submodule; run `git submodule update` after pulling
- **SQL injection risk** — `Constants.QUERIES` uses Python string formatting (`.format()`), not parameterized queries. Be careful when modifying these
