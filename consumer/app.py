import os
import json
import sys
from google.cloud import pubsub_v1

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
SUBSCRIPTION_NAME = os.environ.get("GCP_PUBSUB_SUBSCRIPTION")

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

def process_message(message_data):
    try:
        parsed_data = json.loads(message_data)
        print(f"Processed valid log: {json.dumps(parsed_data, indent=2)}")
        return True
    except json.JSONDecodeError:
        print(f"ERROR: Malformed JSON received: {message_data.decode('utf-8')}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"ERROR processing message: {e} - Data: {message_data.decode('utf-8')}", file=sys.stderr)
        return False

def callback(message):
    print(f"Received raw message: {message.data}")
    process_message(message.data)
    message.ack()

def run_subscriber():
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")
    with subscriber:
        try:
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
            streaming_pull_future.result()

if __name__ == "__main__":
    run_subscriber()