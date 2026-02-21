import os
import json
import time
from datetime import datetime
from google.cloud import pubsub_v1
from google.api_core import retry

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
TOPIC_NAME = os.environ.get("GCP_PUBSUB_TOPIC")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

def publish_message(data_str):
    data_bytes = data_str.encode("utf-8")
    future = publisher.publish(topic_path, data_bytes, retry=retry.Retry(deadline=60))
    try:
        message_id = future.result()
        print(f"Published message ID: {message_id}")
    except Exception as e:
        print(f"Failed to publish message: {e}")

def main():
    for i in range(10):
        valid_log = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "service_name": "auth-service",
            "log_level": "INFO",
            "message": f"User 'john.doe_{i}' logged in successfully",
            "request_id": f"req-valid-{i}"
        }
        publish_message(json.dumps(valid_log))
        time.sleep(0.5)

    malformed_logs = [
        "invalid non-JSON string",
        '{"timestamp": "2023-10-27", "service_name": "auth-service"',
        "just another bad payload"
    ]
    for bad_log in malformed_logs:
        publish_message(bad_log)
        time.sleep(0.5)

if __name__ == "__main__":
    main()