import json
import os
import redis
import logging
import socket
import uuid

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def test_connection(ip, port, timeout=3):
    try:
        sock = socket.create_connection((ip, port), timeout)
        sock.close()
        print(f"Successfully connected to {ip}:{port}")
        return True
    except Exception as e:
        print(f"Failed to connect to {ip}:{port} - {e}")
        return False

# Example usage in your lambda_handler:
internal_ip = '172.31.10.177'  # Replace with your EC2's private IP
port = 6379  # Replace with your service port (e.g., Redis)
test_connection(internal_ip, port)

def lambda_handler(event, context):
    """
    AWS Lambda handler for producing tasks to the Redis queue.
    Expects 'tasks' in the event, or uses sample tasks if not provided.
    Allows specifying 'queue_name' in the event or via environment variable.
    """
    logger.debug('Lambda handler started')
    logger.debug(f'Received event: {event}')
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = int(os.environ.get('REDIS_PORT', 6379))
    redis_password = os.environ.get('REDIS_PASSWORD')
    logger.debug(f'Redis connection params - host: {redis_host}, port: {redis_port}, password: {"set" if redis_password else "not set"}')
    try:
        redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
        logger.debug('Connected to Redis')
    except Exception as e:
        logger.error(f'Failed to connect to Redis: {e}')
        return {
            'statusCode': 500,
            'body': f'Failed to connect to Redis: {e}'
        }

    sample_tasks = event.get('tasks') if event and 'tasks' in event else [
        {'type': 'send_email', 'user_id': 123},
        {'type': 'generate_report', 'user_id': 456},
        {'type': 'cleanup', 'user_id': 789},
    ]
    logger.debug(f'Tasks to queue: {sample_tasks}')

    # Determine queue name
    queue_name = (event.get('queue_name') if event and 'queue_name' in event else os.environ.get('QUEUE_NAME', 'task_queue_1'))

    results = []
    for idx, task in enumerate(sample_tasks):
        try:
            logger.debug(f'Processing task {idx + 1}/{len(sample_tasks)}: {task}')
            # Add a unique task_id if not present
            if 'task_id' not in task:
                task['task_id'] = str(uuid.uuid4())
            task_json = json.dumps(task)
            redis_client.rpush(queue_name, task_json)
            logger.debug(f'Successfully queued task: {task}')
            results.append(f"Queued task: {task}")
        except Exception as e:
            logger.error(f'Failed to queue task {task}: {e}')
            results.append(f"Failed to queue task {task}: {e}")

    logger.debug(f'All tasks processed. Results: {results}')
    return {
        'statusCode': 200,
        'body': results
    } 