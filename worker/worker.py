import json
import os
import time
import signal
import sys
import redis
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

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

    logging.info('Worker started. Waiting for tasks...')
    global done
    while not done:
        try:
            # BLPOP blocks for up to 5 seconds, then loop to check for shutdown
            task_data = redis_client.blpop('task_queue', timeout=5)
            if task_data:
                _, task_json = task_data
                task = json.loads(task_json.decode('utf-8'))
                logging.info(f"Received task: {task}")
                process_task(task)
        except Exception as e:
            logging.error(f"Error processing task: {e}")
            time.sleep(1)
    logging.info('Worker stopped.')

if __name__ == '__main__':
    main() 