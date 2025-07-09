import json
import os
import redis

def lambda_handler(event, context):
    """
    AWS Lambda handler for producing tasks to the Redis queue.
    Expects 'tasks' in the event, or uses sample tasks if not provided.
    """
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = int(os.environ.get('REDIS_PORT', 6379))
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)

    sample_tasks = event.get('tasks') if event and 'tasks' in event else [
        {'type': 'send_email', 'user_id': 123},
        {'type': 'generate_report', 'user_id': 456},
        {'type': 'cleanup', 'user_id': 789},
    ]

    results = []
    for task in sample_tasks:
        task_json = json.dumps(task)
        redis_client.rpush('task_queue', task_json)
        results.append(f"Queued task: {task}")

    return {
        'statusCode': 200,
        'body': results
    } 