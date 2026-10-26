import requests
import json

API_URL = "https://app.birdweather.com/graphql"

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

    response.raise_for_status()

    data = response.json()

    if "errors" in data:
        raise ValueError(f"GraphQL errors: {data['errors']}")

    return data["data"]["detections"]


seen_ids = set()

cursor = None
page_number = 1
max_pages = 3

while page_number <= max_pages:
    result = get_detections(5, cursor)

    print(f"\n--- Page {page_number} ---")


    for detection in result["nodes"]:
        detection_id = detection["id"]

        if detection_id not in seen_ids:
            seen_ids.add(detection_id)
            print(f'New: {detection_id} - {detection["species"]["commonName"]}')
        else:
            print(f"Duplicate: {detection_id}")


    page_info = result["pageInfo"]

    if not page_info["hasNextPage"]:
        break

    cursor = page_info["endCursor"]
    page_number += 1

