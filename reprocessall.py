#!/usr/bin/env python3
"""Reset documents with specific errors back to queued status"""

import os
from azure.cosmos import CosmosClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Azure configuration
COSMOS_CONNECTION_STRING = os.environ.get("COSMOS_DB_CONNECTION_STRING")
DATABASE_NAME = "cangodb"
DOCUMENTS_CONTAINER = "document_metadata"

def main():
    # Connect to Cosmos DB
    cosmos_client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
    database = cosmos_client.get_database_client(DATABASE_NAME)
    container = database.get_container_client(DOCUMENTS_CONTAINER)
    
    # Specific document IDs to reset (from your diagnostics output)
    doc_ids = [
        "605e5736-1a53-418f-ac57-653fb2bfba78",
        "fc73272a-adcf-4bbf-a1d8-4b27d6736c56",
        "5f4f5a34-98bc-4912-a97d-727fb18c4931",
        "ed0d2614-3ebc-47e3-9260-1ecbb7b0c427",
        "da9f0d89-33dc-4aac-9cf7-a78d53460d01",
        "501f593a-6960-44c2-a1b4-cf1afcb3cdc0",
        "34f1f716-a11a-4e90-859b-08a19d91e324",
        "8ca299d5-c264-40a2-a09e-ce788f21f769",
        "8ea52b40-8d10-4d8d-8858-aed106df05ac",
        "886d8d64-946e-40b2-8cd6-9e702f7c0654",
        "2166844e-de61-4ad2-89eb-23e3d60e486e",
        "cfb1404a-9fd0-42c5-89de-48ab6283d632",
        "cfcd410e-9b45-431c-bf71-b9c93139ac44"
    ]
    
    reset_count = 0
    for doc_id in doc_ids:
        # Get the document
        query = f"SELECT * FROM c WHERE c.id = '{doc_id}'"
        items = list(container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        
        if items:
            document = items[0]
            # Reset the document status to queued
            document["status"] = "queued"
            document["errorMessage"] = None  # Clear error message
            container.replace_item(document["id"], document)
            print(f"Reset document {doc_id}")
            reset_count += 1
    
    print(f"Successfully reset {reset_count} documents to 'queued' status")

if __name__ == "__main__":
    main()