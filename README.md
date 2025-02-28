# StreamNative Pulsar Service Test

## Overview
This repository contains a project that interacts with StreamNative Pulsar service. The application demonstrates a specific behavior related to topic subscription that varies based on environment configuration.

## Setup Instructions

### 1. Environment Setup
1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file in the root directory with the following variables:

   ```
   # Determines whether to import the GCP Secret Manager library
   # Set to "true", "1", or "yes" to import, any other value to skip
   IMPORT_GCP_SECRET_MANAGER=false

   PULSAR_HOST=pulsar+ssl://pc-2c213b69.euw1-turtle.streamnative.g.snio.cloud:6651
   PULSAR_AUDIENCE=urn:sn:pulsar:o-w0y8l:staging
   PULSAR_ISSUER_URL=https://auth.streamnative.cloud/
   PULSAR_OAUTH_KEY_PATH=./pulsar-oauth-key.json
   ```

4. Place the `pulsar-oauth-key.json` file in the root of the repo


## Running the Application

### Local Execution
Run the application using the main script:
```bash
python src/streamnative/main.py
```

**Note:** The topic is hardcoded in `src/streamnative/pulsar_service/handler.py`.

### Observed Behavior
- When running locally without importing the GCP library (`IMPORT_GCP_SECRET_MANAGER` not set to true/1/yes), you'll observe a consumer attached to the topic `flowie/analytics/simonetest-partition-0`.
- Even if the GCP library is imported, the consumer will still be attached to this topic.

## Docker Deployment

### Building the Docker Image
Use the provided Makefile to build the Docker image:
```bash
make build
```

### Running with Docker Compose
The repository includes a docker-compose file to start the container:
```bash
docker compose up
```

The docker-compose configuration injects:
- The above mentioned oauth key file
- The `.env` file with environment variables

### Docker Behavior Issue
When running in Docker:
- If `IMPORT_GCP_SECRET_MANAGER` is **NOT** set (or set to false), the consumer attaches to the correct topic: `flowie/analytics/simonetest-partition-0`
- If the container is restarted with `IMPORT_GCP_SECRET_MANAGER` enabled (set to true/1/yes), the consumer attaches to the incorrect topic: `flowie/analytics/simonetest-partition-` (missing the trailing "0")

This inconsistency between local and Docker environments, and the dependency on the GCP library import, is the main issue being demonstrated in this repository.
