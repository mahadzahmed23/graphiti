import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from logging import INFO

from dotenv import load_dotenv

from graphiti_core import Graphiti
from graphiti_core.nodes import EpisodeType
from graphiti_core.search.search_config_recipes import NODE_HYBRID_SEARCH_RRF
import requests
import json
import os
# Endpoint URL
url = "https://api.cci.aihr.com/search/document"
api_key = os.getenv("x-api-key")
# Headers with x-apikey
headers = {
    "Content-Type": "application/json",
    "x-api-key": api_key  # Replace with your actual API key
}

# Configure logging
logging.basicConfig(
    level=INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

load_dotenv()

# Neo4j connection parameters
neo4j_uri = os.environ.get('NEO4J_URI')
neo4j_user = os.environ.get('NEO4J_USER')
neo4j_password = os.environ.get('NEO4J_PASSWORD')

if not neo4j_uri or not neo4j_user or not neo4j_password:
    raise ValueError('NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD must be set')

async def main():
    # Main function implementation will go here
    # pass
    # Initialize Graphiti with Neo4j connection
    

    graphiti = Graphiti(neo4j_uri, neo4j_user, neo4j_password)

    try:
        # Initialize the graph database with graphiti's indices. This only needs to be done once.
        await graphiti.build_indices_and_constraints()
        
        # Additional code will go here
      
        payload = {
                "SearchType": 0.0,
                "Query": "",
                "Filter": f"document_id = '99bee373-04d4-40a6-aa45-b039f3ddcbe6'",
                "Offset": 0,
                "Limit": 99
        }

        try:
            # Send POST request
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()

            
            # Parse response JSON
            data = response.json().get("data", [])


        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        
        except Exception as err:
            print(f"Error occurred: {err}")
        
        # x = json.dumps(data,indent=4)
        # data_list = json.loads(x)
        # all_content = ""

        # # Iterate over each dictionary in the list
        # for item in data_list:
        #     # Ensure the item is a dictionary before accessing fields
        #     if isinstance(item, dict):
        #         content = item.get("page_content", "")
        #         all_content += content + "\n\n"  # Add a separator between content blocks

        # print(all_content)
        x = json.dumps(data, indent=4)
        data_list = json.loads(x)

        # Initialize variables
        all_content = ""
        consolidated_data = {}

        # Iterate over each dictionary in the list
        for item in data_list:
            # Ensure the item is a dictionary before accessing fields
            if isinstance(item, dict):
                # If the consolidated data is still empty, populate the common fields
                if not consolidated_data:
                    consolidated_data = {
                        "title": item.get("title", "Untitled"),
                        "url": item.get("url", ""),
                        "asset_type": item.get("asset_type", ""),
                        "category": item.get("category", ""),
                        "document_id": item.get("document_id", ""),
                        "external_id": item.get("external_id"),
                        "external_source": item.get("external_source"),
                        "external_url": item.get("external_url", ""),
                        "hr_domain": item.get("hr_domain", []),
                        "license_type": item.get("license_type"),
                        "id": item.get("id"),
                        "sku": item.get("sku", ""),
                        "duration": item.get("duration", ""),
                        "thumbnail_url": item.get("thumbnail_url"),
                        "published_date": item.get("published_date")
                    }

                # Concatenate page content
                all_content += item.get("page_content", "") + "\n\n"

        # Add the concatenated content to the consolidated data
        # consolidated_data["page_content"] = all_content.strip()
        consolidated_data["page_content"] = all_content

        # Convert to JSON
        final_json = json.dumps(consolidated_data, indent=4)
        # print(final_json)
    #     for chunk in data:
    #         episode_body = json.dumps(chunk, indent=4)

        # Add JSON Episode
        await graphiti.add_episode(
            name=consolidated_data.get("title", "Untitled Episode"),
            episode_body=final_json,  # Now passing as a JSON string
            source=EpisodeType.json,
            source_description=consolidated_data.get("asset_type", "Unknown Type"),
            reference_time=datetime.now(timezone.utc),
        )

        print(f"Added episode: {consolidated_data.get('title', 'Untitled Episode')}")

    finally:
        # Close the connection
        await graphiti.close()
        print('\nConnection closed')
    # finally:
    #     print('done')

if __name__ == '__main__':
    asyncio.run(main())
