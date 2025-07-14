# Queue System with GPU Load Balancing

A distributed queue system for processing transcription tasks with intelligent GPU load balancing and task management.

## Features

- **Multi-Queue Support**: Process tasks from multiple Redis queues
- **GPU Load Balancing**: Automatically distributes transcription tasks across available GPUs
- **Task State Management**: Track task progress with MySQL database
- **Callback System**: Receive notifications when tasks complete
- **Graceful Shutdown**: Handles termination signals properly
- **Multiple Task Types**: Supports email, reports, transcription, and cleanup tasks

## Architecture

The system consists of two main components:

### Producer (`producer/producer.py`)
- Pushes tasks to Redis queues
- Supports AWS Lambda deployment
- Configurable queue names and task types

### Worker (`worker/worker.py`)
- Processes tasks from Redis queues
- Manages GPU slot allocation (4 slots per GPU)
- Handles callbacks and database updates
- Runs FastAPI server for status callbacks

## Prerequisites

- Python 3.8+
- Redis server
- MySQL server
- Access to GPU transcription endpoints

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd queue-system-py
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up your environment variables:
```bash
cp .example.env .env
# Edit .env with your configuration
```

4. Create the MySQL database and table:
```sql
-- Create database
CREATE DATABASE queue_system;

-- Create tasks table
CREATE TABLE tasks (
    task_id VARCHAR(255) PRIMARY KEY,
    queue VARCHAR(255),
    state ENUM('pending', 'processing', 'finished', 'failed'),
    type VARCHAR(255),
    gpu_id VARCHAR(255),
    slot_index INT
);

-- Add indexes for performance
CREATE INDEX idx_tasks_state ON tasks(state);
CREATE INDEX idx_tasks_gpu_id ON tasks(gpu_id);
CREATE INDEX idx_tasks_type ON tasks(type);
```

## Configuration

### Environment Variables

Copy `.example.env` to `.env` and configure:

```env
# Redis connection
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=

# MySQL database connection
DB_HOST=localhost
DB_PORT=3306
DB_NAME=queue_system
DB_USER=root
DB_PASSWORD=your_db_password

# Comma-separated list of queue names
TASK_QUEUES=task_queue_1,task_queue_2

# The callback URL that GPU endpoints will use
CALLBACK_URL=http://localhost:8000/callback

# Bearer token for GPU endpoint authentication
BEARER_TOKEN=your_bearer_token_here
```

## Usage

### Running the Worker

```bash
cd worker
python worker.py
```

The worker will:
- Connect to Redis and MySQL
- Start processing tasks from configured queues
- Run FastAPI callback server on port 8000
- Handle graceful shutdown on SIGTERM/SIGINT

### Running the Producer

```bash
cd producer
python producer.py
```

Or deploy as AWS Lambda function.

### Task Types

#### 1. Email Tasks
```json
{
  "type": "send_email",
  "data": {
    "user_id": 123,
    "email": "user@example.com",
    "subject": "Test Email"
  }
}
```

#### 2. Report Tasks
```json
{
  "type": "generate_report",
  "data": {
    "user_id": 456,
    "report_type": "monthly"
  }
}
```

#### 3. Transcription Tasks
```json
{
  "type": "start_transcribe",
  "data": {
    "s3_bucket": "esales-et-callrecordings",
    "s3_key": "recordings/2025/07/01/audio.wav",
    "s3_save_bucket": "esales-et-transcribes",
    "s3_save_key": "transcribed/audio.json",
    "pii_entities": "email,phone_number,address",
    "call_session": "1751353080.711222"
  }
}
```

#### 4. Cleanup Tasks
```json
{
  "type": "cleanup",
  "data": {
    "user_id": 789
  }
}
```

## GPU Load Balancing

The system automatically:
- Monitors GPU usage across 4 configured endpoints (g0-g3)
- Assigns tasks to the least-used GPU
- Manages 4 slots per GPU for concurrent processing
- Tracks task states in MySQL database

### GPU Endpoints

Currently configured endpoints:
- `g0`: https://ai.uk.customer360.co/g0/ai/get_detailed_transcript_from_s3?sort_by=start
- `g1`: https://ai.uk.customer360.co/g1/ai/get_detailed_transcript_from_s3?sort_by=start
- `g2`: https://ai.uk.customer360.co/g2/ai/get_detailed_transcript_from_s3?sort_by=start
- `g3`: https://ai.uk.customer360.co/g3/ai/get_detailed_transcript_from_s3?sort_by=start

## API Endpoints

### Callback Endpoint
`POST /callback`

Receives task completion notifications:
```json
{
  "task_id": "uuid-string",
  "status": "finished" // or "failed"
}
```

## Monitoring

### Task States
- `pending`: Task created but not yet processing
- `processing`: Task assigned to GPU and in progress
- `finished`: Task completed successfully
- `failed`: Task failed or encountered error

### Database Queries

Check task status:
```sql
SELECT * FROM tasks WHERE task_id = 'your-task-id';
```

Monitor GPU usage:
```sql
SELECT gpu_id, COUNT(*) as active_tasks 
FROM tasks 
WHERE state = 'processing' 
GROUP BY gpu_id;
```

## Deployment

### AWS Lambda (Producer)
The producer can be deployed as an AWS Lambda function:

1. Package the `producer/` directory
2. Set environment variables in Lambda console
3. Configure triggers (API Gateway, SQS, etc.)

### Docker (Worker)
Create a Dockerfile for the worker:

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY worker/ ./worker/
WORKDIR /app/worker

CMD ["python", "worker.py"]
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

[Add your license information here]

## Support

For issues and questions, please open an issue in the GitHub repository.