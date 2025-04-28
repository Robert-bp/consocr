#!/usr/bin/env python3
import os
import csv
from azure.cosmos import CosmosClient
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------
load_dotenv()
COSMOS_CONNECTION_STRING = os.environ["COSMOS_DB_CONNECTION_STRING"]
DATABASE_NAME = "cangodb"
CELLS_CONTAINER = "table_cells"

MISSISSAUGA_DOC_IDS = {
    "b2d1fc19-9bec-4a27-886c-c9a04a73ed0d",
    "acc5c6d7-b915-4019-9079-7be50bfb466f",
    "349e6399-b5b2-4e48-a0bd-4e1fd9701c01",
}

cosmos_client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
container = cosmos_client.get_database_client(DATABASE_NAME).get_container_client(CELLS_CONTAINER)

# -----------------------------------------------------------------------------
# EXPORT CELLS TO CSV
# -----------------------------------------------------------------------------
with open("mississauga_cells_from_cosmos.csv", "w", newline="", encoding="utf-8") as fout:
    writer = csv.writer(fout)
    writer.writerow([
        "document_id",
        "page_number",
        "table_id",
        "row",
        "column",
        "content",
        "confidence"
    ])

    for doc_id in MISSISSAUGA_DOC_IDS:
        query = f"SELECT * FROM c WHERE c.documentId = '{doc_id}'"
        for item in container.query_items(query=query, enable_cross_partition_query=True):
            writer.writerow([
                item.get("documentId", ""),
                item.get("pageNumber", ""),
                item.get("tableId", ""),
                item.get("row", ""),
                item.get("column", ""),
                item.get("content", "").replace("\n", " ").strip(),
                item.get("confidence", "")
            ])

print("✅ Done — exported mississauga_cells_from_cosmos.csv")
