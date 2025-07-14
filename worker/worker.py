import json
import os
import time
import signal
import sys
import redis
import logging
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import threading
import uuid
import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

# SQLAlchemy setup
Base = declarative_base()

class Task(Base):
    __tablename__ = 'tasks'
    task_id = Column(String(255), primary_key=True)
    queue = Column(String(100))
    state = Column(String(20))
    type = Column(String(50))
    gpu_id = Column(String(10))  # e.g., 'g0', 'g1', etc.
    slot_index = Column(Integer)  # 0-3

# Database configuration from environment variables
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = os.environ.get('DB_PORT', '3306')
DB_NAME = os.environ.get('DB_NAME', 'esh_funbags')
DB_USER = os.environ.get('DB_USER', 'root')
DB_PASSWORD = os.environ.get('DB_PASSWORD', '')

# MySQL connection string
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

logging.debug(f"Database connection URL: {DATABASE_URL}")
logging.debug(f"Database config - Host: {DB_HOST}, Port: {DB_PORT}, Database: {DB_NAME}, User: {DB_USER}")

try:
    engine = create_engine(DATABASE_URL, echo=True)  # Enable SQL logging
    logging.debug("Database engine created successfully")
    
    # Test database connection
    with engine.connect() as connection:
        logging.debug("Database connection test successful")
    
    Base.metadata.create_all(engine)
    logging.debug("Database tables created/verified successfully")
    
except Exception as e:
    logging.error(f"Database connection failed: {e}")
    raise
SessionLocal = sessionmaker(bind=engine)

db_session = SessionLocal()

# FastAPI app for callback
app = FastAPI()

@app.post('/callback')
async def callback(request: Request):
    data = await request.json()
    task_id = data.get('task_id')
    status = data.get('status')  # 'finished' or 'failed'
    if not task_id or status not in ('finished', 'failed'):
        return JSONResponse(status_code=400, content={'error': 'Invalid payload'})
    session = SessionLocal()
    task = session.query(Task).filter(Task.task_id == task_id).first()
    if not task:
        logging.warning(f"Callback: Task {task_id} not found in database")
        session.close()
        return JSONResponse(status_code=404, content={'error': 'Task not found'})
    
    logging.debug(f"Callback: Found task {task_id} in database: {task.__dict__}")
    task.state = status
    session.commit()
    logging.debug(f"Callback: Task {task_id} updated to {status}")
    session.close()
    logging.info(f"Task {task_id} updated to {status} via callback.")
    return {'message': 'Task state updated'}

# Graceful shutdown flag
done = False
def handle_sigterm(signum, frame):
    global done
    logging.info('Received shutdown signal. Exiting...')
    done = True

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

def handle_send_email(data):
    logging.info(f"Handling send_email with data: {data}")
    # TODO: Implement actual email sending logic
    time.sleep(2)
    logging.info("Email sent.")

def handle_generate_report(data):
    logging.info(f"Handling generate_report with data: {data}")
    # TODO: Implement actual report generation logic
    time.sleep(3)
    logging.info("Report generated.")

def handle_start_transcribe(data):
    logging.info(f"Handling start_transcribe with data: {data}")
    
    # Configure internal parameters
    gpu_endpoints = {
        'g0': 'https://ai.uk.customer360.co/g0/ai/get_detailed_transcript_from_s3?sort_by=start',
        'g1': 'https://ai.uk.customer360.co/g1/ai/get_detailed_transcript_from_s3?sort_by=start',
        'g2': 'https://ai.uk.customer360.co/g2/ai/get_detailed_transcript_from_s3?sort_by=start',
        'g3': 'https://ai.uk.customer360.co/g3/ai/get_detailed_transcript_from_s3?sort_by=start',
    }
    callback_url = os.environ.get('CALLBACK_URL', 'http://localhost:8000/callback')
    max_slots_per_gpu = 4
    bearer_token = os.environ.get('BEARER_TOKEN')
    headers = {'Authorization': f'Bearer {bearer_token}'} if bearer_token else {}
    
    # Assign a unique task_id if not present
    task_id = data.get('task_id') or str(uuid.uuid4())
    data['task_id'] = task_id
    task_type = 'start_transcribe'
    queue_name = 'transcribe_queue'

    session = SessionLocal()
    logging.debug(f"Database session created for task_id: {task_id}")
    
    gpu_slot_counts = {}
    for gpu in gpu_endpoints.keys():
        count = session.query(Task).filter(Task.gpu_id == gpu, Task.state == 'processing').count()
        gpu_slot_counts[gpu] = count
        logging.debug(f"GPU {gpu} has {count} active slots")
    # Find GPUs with available slots
    available_gpus = [gpu for gpu, count in gpu_slot_counts.items() if count < max_slots_per_gpu]
    if not available_gpus:
        logging.info('All GPUs are full. Waiting for a slot to free up...')
        session.close()
        time.sleep(2)
        return
    # Least-used GPU
    least_used_gpu = min(available_gpus, key=lambda g: gpu_slot_counts[g])
    # Find first available slot index for this GPU
    used_slots = [t.slot_index for t in session.query(Task).filter(Task.gpu_id == least_used_gpu, Task.state == 'processing').all() if t.slot_index is not None]
    slot_index = next((i for i in range(max_slots_per_gpu) if i not in used_slots), None)
    if slot_index is None:
        logging.info(f'No free slot on {least_used_gpu}, this should not happen. Skipping task.')
        session.close()
        time.sleep(2)
        return
    # Store task in DB as 'processing' with GPU and slot
    db_task = Task(task_id=task_id, queue=queue_name, state='processing', type=task_type, gpu_id=least_used_gpu, slot_index=slot_index)
    logging.debug(f"Created task object: {db_task.__dict__}")
    
    session.merge(db_task)
    logging.debug(f"Task merged into session")
    
    session.commit()
    logging.debug(f"Task committed to database")
    
    session.close()
    logging.info(f"Assigned transcribe task {task_id} to {least_used_gpu} slot {slot_index}")
    
    # Send HTTPS request to remote server asynchronously
    payload = {
        's3_bucket': data.get('s3_bucket'),
        's3_key': data.get('s3_key'),
        's3_save_bucket': data.get('s3_save_bucket'),
        's3_save_key': data.get('s3_save_key'),
        'pii_entities': data.get('pii_entities'),
        'call_session': data.get('call_session')
    }
    logging.info(f"For manual callback testing, use: curl -X POST {callback_url} -H 'Content-Type: application/json' -d '{{\"task_id\": \"{task_id}\", \"status\": \"finished\"}}'")
    
    # Send request in background thread without waiting for response
    def send_transcribe_request():
        try:
            response = requests.post(gpu_endpoints[least_used_gpu], json=payload, timeout=30, headers=headers)
            response.raise_for_status()
            logging.info(f"Successfully sent transcribe task {task_id} to {least_used_gpu} endpoint. Status: {response.status_code}")
        except Exception as e:
            logging.error(f"Failed to send transcribe task {task_id} to {least_used_gpu} endpoint: {e}")
            # Update task state to 'failed' in DB if request fails
            session = SessionLocal()
            logging.debug(f"Updating task {task_id} to failed state due to request failure")
            db_task = session.query(Task).filter(Task.task_id == task_id).first()
            if db_task:
                logging.debug(f"Found task in DB: {db_task.__dict__}")
                db_task.state = 'failed'
                session.commit()
                logging.debug(f"Task {task_id} updated to failed state")
            else:
                logging.warning(f"Task {task_id} not found in database for failure update")
            session.close()
    
    # Start the request in a background thread
    thread = threading.Thread(target=send_transcribe_request)
    thread.daemon = True
    thread.start()
    
    logging.info(f"Transcribe task {task_id} marked as processing and request sent to {least_used_gpu} in background")

def process_task(task):
    logging.info(f"Processing task: {task}")
    task_type = task.get('type')
    data = task.get('data', {})
    if task_type == 'send_email':
        handle_send_email(data)
    elif task_type == 'generate_report':
        handle_generate_report(data)
    elif task_type == 'start_transcribe':
        handle_start_transcribe(data)
    elif task_type == 'cleanup':
        time.sleep(1)
    else:
        logging.warning(f"Unknown task type: {task_type}. Skipping.")
        time.sleep(1)
    logging.info(f"Finished task: {task}")

def main():
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = int(os.environ.get('REDIS_PORT', 6379))
    redis_password = os.environ.get('REDIS_PASSWORD')
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)

    # Multiple queues support
    queues = os.environ.get('TASK_QUEUES', 'task_queue_1,task_queue_2').split(',')

    logging.info(f'Worker started. Waiting for tasks on queues: {queues}')
    global done
    while not done:
        try:
            # BLPOP blocks for up to 5 seconds, then loop to check for shutdown
            task_data = redis_client.blpop(queues, timeout=5)
            if task_data:
                queue_name, task_json = task_data
                task = json.loads(task_json.decode('utf-8'))
                # Assign a unique task_id if not present
                task_id = task.get('task_id') or str(uuid.uuid4())
                task['task_id'] = task_id
                task_type = task.get('type', 'unknown')

                process_task(task)
        except Exception as e:
            logging.error(f"Error processing task: {e}")
            time.sleep(1)
    logging.info('Worker stopped.')

def run_fastapi():
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == '__main__':
    # Run FastAPI in a separate thread
    api_thread = threading.Thread(target=run_fastapi, daemon=True)
    api_thread.start()
    main() 