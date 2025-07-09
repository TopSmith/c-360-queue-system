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
from sqlalchemy import create_engine, Column, String, Integer, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import threading
import uuid
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# SQLAlchemy setup
Base = declarative_base()

class Task(Base):
    __tablename__ = 'tasks'
    task_id = Column(String, primary_key=True)
    queue = Column(String)
    state = Column(Enum('pending', 'processing', 'finished', 'failed', name='task_state'))
    type = Column(String)
    gpu_id = Column(String)  # e.g., 'g0', 'g1', etc.
    slot_index = Column(Integer)  # 0-3

engine = create_engine('sqlite:///tasks.db')
Base.metadata.create_all(engine)
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
        session.close()
        return JSONResponse(status_code=404, content={'error': 'Task not found'})
    task.state = status
    session.commit()
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

def process_task(task):
    logging.info(f"Processing task: {task}")
    if task.get('type') == 'send_email':
        time.sleep(2)
    elif task.get('type') == 'generate_report':
        time.sleep(3)
    elif task.get('type') == 'cleanup':
        time.sleep(1)
    else:
        time.sleep(1)
    logging.info(f"Finished task: {task}")

def main():
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = int(os.environ.get('REDIS_PORT', 6379))
    redis_password = os.environ.get('REDIS_PASSWORD')
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)

    # Multiple queues support
    queues = os.environ.get('TASK_QUEUES', 'task_queue_1,task_queue_2').split(',')
    callback_url = os.environ.get('CALLBACK_URL', 'http://localhost:8000/callback')
    # GPU endpoints
    gpu_endpoints = {
        'g0': 'https://ai.uk.customer360.co/g0/ai/get_detailed_transcript_from_s3?sort_by=start',
        'g1': 'https://ai.uk.customer360.co/g1/ai/get_detailed_transcript_from_s3?sort_by=start',
        'g2': 'https://ai.uk.customer360.co/g2/ai/get_detailed_transcript_from_s3?sort_by=start',
        'g3': 'https://ai.uk.customer360.co/g3/ai/get_detailed_transcript_from_s3?sort_by=start',
    }
    max_slots_per_gpu = 4

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

                # Find least-used GPU and available slot
                session = SessionLocal()
                gpu_slot_counts = {}
                for gpu in gpu_endpoints.keys():
                    count = session.query(Task).filter(Task.gpu_id == gpu, Task.state == 'processing').count()
                    gpu_slot_counts[gpu] = count
                # Find GPUs with available slots
                available_gpus = [gpu for gpu, count in gpu_slot_counts.items() if count < max_slots_per_gpu]
                if not available_gpus:
                    logging.info('All GPUs are full. Waiting for a slot to free up...')
                    session.close()
                    time.sleep(2)
                    continue
                # Least-used GPU
                least_used_gpu = min(available_gpus, key=lambda g: gpu_slot_counts[g])
                # Find first available slot index for this GPU
                used_slots = [t.slot_index for t in session.query(Task).filter(Task.gpu_id == least_used_gpu, Task.state == 'processing').all() if t.slot_index is not None]
                slot_index = next((i for i in range(max_slots_per_gpu) if i not in used_slots), None)
                if slot_index is None:
                    logging.info(f'No free slot on {least_used_gpu}, this should not happen. Skipping task.')
                    session.close()
                    time.sleep(2)
                    continue
                # Store task in DB as 'processing' with GPU and slot
                db_task = Task(task_id=task_id, queue=queue_name.decode('utf-8'), state='processing', type=task_type, gpu_id=least_used_gpu, slot_index=slot_index)
                session.merge(db_task)
                session.commit()
                session.close()
                logging.info(f"Dequeued task {task_id} from {queue_name.decode('utf-8')}: {task}, assigned to {least_used_gpu} slot {slot_index}")
                # Send HTTPS request to remote server with callback URL
                payload = {
                    'task_id': task_id,
                    'task': task,
                    'callback_url': callback_url
                }
                logging.info(f"For manual callback testing, use: curl -X POST {callback_url} -H 'Content-Type: application/json' -d '{{\"task_id\": \"{task_id}\", \"status\": \"finished\"}}'")
                try:
                    response = requests.post(gpu_endpoints[least_used_gpu], json=payload, timeout=10)
                    response.raise_for_status()
                    logging.info(f"Sent task {task_id} to {least_used_gpu} endpoint. Status: {response.status_code}")
                except Exception as e:
                    logging.error(f"Failed to send task {task_id} to {least_used_gpu} endpoint: {e}")
                    # Optionally, update task state to 'failed' in DB
                    session = SessionLocal()
                    db_task = session.query(Task).filter(Task.task_id == task_id).first()
                    if db_task:
                        db_task.state = 'failed'
                        session.commit()
                    session.close()
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