
import json
import time

from confluent_kafka import Producer
from birdweather_client import get_detections


# Configure our Kafka producer.
# Kafka runs inside Docker, but this Python script runs on Windows.
producer = Producer({
    # Address and port where our Kafka broker is accessible.
    "bootstrap.servers": "127.0.0.1:9092",

    # Force IPv4 because our Docker port is bound to 127.0.0.1.
    # This avoids the IPv6 localhost connection issue.
    "broker.address.family": "v4"
})


# Store detection IDs that Kafka has successfully received.
# A set allows fast membership checks and prevents duplicate entries.
#
# NOTE: This is in-memory storage. It resets when Python exits.
seen_ids = set()


def delivery_report(err, msg):
    """
    Callback executed when Kafka reports the delivery result
    of a previously queued message.
    """

    # Kafka returns the message key as bytes.
    # Convert it back into a Python string.
    detection_id = msg.key().decode("utf-8")

    if err is not None:
        # Kafka could not deliver the message successfully.
        print(f"Delivery failed for {detection_id}: {err}")
    else:
        # Only remember the ID after Kafka confirms delivery.
        seen_ids.add(detection_id)

        # Partition and offset identify where Kafka stored the record.
        print(
            f"Delivered {detection_id} to partition "
            f"{msg.partition()}, offset {msg.offset()}"
        )


# Run three polling cycles for our initial testing.
# Later, we'll replace this with a continuously running collector.
for poll_number in range(3):
    print(f"\nPolling cycle {poll_number + 1}")

    # Reset pagination at the beginning of each polling cycle.
    # cursor=None starts a new query from the first page.
    cursor = None
    page_number = 1
    max_pages = 3

    # Retrieve at most three pages per polling cycle.
    while page_number <= max_pages:

        # Request up to ten detections starting at the current cursor.
        result = get_detections(10, cursor)

        print(f"Fetching page {page_number}")

        # Each node represents one BirdWeather detection.
        for detection in result["nodes"]:
            detection_id = detection["id"]

            # Skip detections successfully delivered during this run.
            if detection_id in seen_ids:
                print(f"Skipping duplicate: {detection_id}")
                continue

            # Queue the detection for asynchronous delivery to Kafka.
            producer.produce(
                topic="wildlife-detections",

                # Use the BirdWeather detection ID as the Kafka key.
                key=detection_id,

                # Convert the Python dictionary into JSON text.
                value=json.dumps(detection),

                # Kafka will call this function with the delivery result.
                on_delivery=delivery_report
            )

            # Queued does not necessarily mean successfully delivered.
            print(f"Queued detection: {detection_id}")

        # Pagination metadata tells us whether another page exists.
        page_info = result["pageInfo"]

        if not page_info["hasNextPage"]:
            break

        # Continue from the end of the current page.
        cursor = page_info["endCursor"]
        page_number += 1

    # Wait for queued messages and process their delivery callbacks.
    # This happens once per polling cycle, not after every detection.
    remaining = producer.flush(10)

    # Detect messages still pending after the timeout.
    # Delivery failures themselves are reported by the callback.
    if remaining:
        raise RuntimeError(
            f"{remaining} Kafka messages remain undelivered"
        )

    # Wait ten seconds between cycles, but not after the final cycle.
    if poll_number < 2:
        time.sleep(10)
