
import requests


# BirdWeather GraphQL API endpoint.
API_URL = "https://app.birdweather.com/graphql"


# GraphQL query for retrieving wildlife acoustic detections.
#
# $count controls how many records we request.
# $cursor tells BirdWeather where pagination should continue.
QUERY = """
query GetDetections($count: Int, $cursor: String) {
  detections(first: $count, after: $cursor) {
    nodes {
      id
      timestamp
      confidence
      probability
      speciesId
      species {
        commonName
        scientificName
      }
      station {
        id
        name
      }
      coords {
        lat
        lon
      }
    }
    pageInfo {
      endCursor
      hasNextPage
    }
  }
}
"""


def get_detections(count, cursor=None):
    """
    Retrieve a page of BirdWeather detections.

    count: Maximum number of detections requested.
    cursor: Pagination position, or None for the first page.

    Returns the detections object containing nodes and pageInfo.
    """

    # Send a GraphQL query through an HTTP POST request.
    response = requests.post(
        API_URL,
        json={
            "query": QUERY,
            "variables": {
                "count": count,
                "cursor": cursor
            }
        },
        timeout=15
    )

    # Raise an exception for unsuccessful HTTP responses.
    response.raise_for_status()

    # Convert the JSON response into Python dictionaries.
    data = response.json()

    # GraphQL can return errors even when the HTTP request succeeds.
    if "errors" in data:
        raise ValueError(f"GraphQL errors: {data['errors']}")

    # Return both the detection records and pagination metadata.
    return data["data"]["detections"]


# Only execute this demonstration when this file runs directly.
# Importing get_detections from kafka_producer.py will not run it.
if __name__ == "__main__":

    # Track unique detection IDs during this demonstration.
    seen_ids = set()

    # Start pagination at the first page.
    cursor = None
    page_number = 1
    max_pages = 3

    while page_number <= max_pages:

        # Request five detections from the current page.
        result = get_detections(5, cursor)

        print(f"\n--- Page {page_number} ---")

        # Process each detection returned by BirdWeather.
        for detection in result["nodes"]:
            detection_id = detection["id"]

            # Only display a detection as new the first time we see it.
            if detection_id not in seen_ids:
                seen_ids.add(detection_id)

                print(
                    f'New: {detection_id} - '
                    f'{detection["species"]["commonName"]}'
                )
            else:
                print(f"Duplicate: {detection_id}")

        # Read pagination information returned by GraphQL.
        page_info = result["pageInfo"]

        # Stop if the API has no additional pages.
        if not page_info["hasNextPage"]:
            break

        # Advance the cursor to retrieve the next page.
        cursor = page_info["endCursor"]
        page_number += 1
