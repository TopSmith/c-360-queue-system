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
    Expects event structure: {"type": "task_type", "data": {...}}
    Or for multiple tasks: {"tasks": [{"type": "task_type", "data": {...}}, ...]}
    """
    logger.debug('Lambda handler started')
    logger.debug(f'Received event: {event}')
    
    # Connect to Redis
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

    # Process event structure
    tasks_to_process = []
    
    if event and 'tasks' in event:
        # Multiple tasks provided
        tasks_to_process = event['tasks']
        logger.debug(f'Processing {len(tasks_to_process)} tasks from event')
    elif event and 'type' in event and 'data' in event:
        # Single task provided
        tasks_to_process = [{'type': event['type'], 'data': event['data']}]
        logger.debug(f'Processing single task from event: {event["type"]}')
    else:
        # No valid event structure, use sample task
        logger.debug('No valid event structure, using sample task')
        tasks_to_process = [
            {
                'type': 'start_transcribe',
                'data': {
                    's3_bucket': 'esales-et-callrecordings',
                    's3_key': 'recordings/2025/07/01/stereo_in-02034320234-07927553718-20250701-065800-1751353080.711222.wav',
                    's3_save_bucket': 'esales-et-transcribes',
                    's3_save_key': 'internalTranscribed/redacted-internaltanscribedstereo_in-02034320234-07927553718-20250701-065800-1751353080.711222.wav.json',
                    'pii_entities': 'email,postcode,ni_number,sort_code,person,location,first_name,last_name,phone_number,address,company_name,country_name,city_name,state_name,password,national_id,sort_code,date_of_birth',
                    'call_session': '1751353080.711222'
                }
            }
        ]

    # Determine queue name
    queue_name = (event.get('queue_name') if event and 'queue_name' in event else os.environ.get('QUEUE_NAME', 'task_queue_1'))
    logger.debug(f'Using queue: {queue_name}')

    results = []
    for idx, task in enumerate(tasks_to_process):
        try:
            logger.debug(f'Processing task {idx + 1}/{len(tasks_to_process)}: {task}')
            
            # Validate task structure
            if not isinstance(task, dict) or 'type' not in task or 'data' not in task:
                error_msg = f'Invalid task structure. Expected {{"type": "...", "data": {{...}}}}, got: {task}'
                logger.error(error_msg)
                results.append(f"Failed to queue task: {error_msg}")
                continue
            
            # Add a unique task_id if not present in data
            if 'task_id' not in task['data']:
                task['data']['task_id'] = str(uuid.uuid4())
            
            task_json = json.dumps(task)
            redis_client.rpush(queue_name, task_json)
            logger.debug(f'Successfully queued task: {task}')
            results.append(f"Queued task type '{task['type']}' with ID: {task['data'].get('task_id', 'unknown')}")
            
        except Exception as e:
            logger.error(f'Failed to queue task {task}: {e}')
            results.append(f"Failed to queue task {task}: {e}")

    logger.debug(f'All tasks processed. Results: {results}')
    return {
        'statusCode': 200,
        'body': results
    } 