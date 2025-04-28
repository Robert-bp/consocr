#!/usr/bin/env python3
import os, time, logging
from azure.cosmos import CosmosClient

# ── CONFIG ────────────────────────────────────────────────────────────────
COSMOS_CONNECTION_STRING = os.getenv("COSMOS_DB_CONNECTION_STRING")
DATABASE_NAME           = "cangodb"
DOCS_CONTAINER          = "document_metadata"
CELLS_CONTAINER         = "table_cells"

# ── LOGGING ───────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger()

def main():
    client       = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
    docs_cont    = client.get_database_client(DATABASE_NAME).get_container_client(DOCS_CONTAINER)
    cells_cont   = client.get_database_client(DATABASE_NAME).get_container_client(CELLS_CONTAINER)

    # 1️⃣ Find docs missing a reviewer
    docs_query = """
    SELECT c.id
    FROM c
    WHERE (NOT IS_DEFINED(c.reviewerId) OR c.reviewerId = null)
    """
    docs = list(docs_cont.query_items(query=docs_query, enable_cross_partition_query=True))
    log.info(f"Found {len(docs)} documents with no reviewerId")

    patched = 0
    for d in docs:
        doc_id = d["id"]

        # 2️⃣ Pull distinct assignedTo from table_cells
        cells_query = f"""
        SELECT DISTINCT c.assignedTo
        FROM c
        WHERE c.documentId = '{doc_id}' AND IS_DEFINED(c.assignedTo)
        """
        reviewers = [r["assignedTo"] for r in cells_cont.query_items(
            query=cells_query, enable_cross_partition_query=True
        )]

        if not reviewers:
            log.warning(f"No cell assignments found for {doc_id}, skipping")
            continue

        new_reviewer = reviewers[0]
        log.info(f"→ Setting {doc_id}.reviewerId = '{new_reviewer}'")

        # 3️⃣ Read full document, update reviewerId & lastUpdatedAt, replace
        doc = docs_cont.read_item(item=doc_id, partition_key=doc_id)
        doc["reviewerId"]    = new_reviewer
        doc["lastUpdatedAt"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        docs_cont.replace_item(item=doc_id, body=doc)
        patched += 1

    log.info(f"Patched {patched} documents")

if __name__ == "__main__":
    main()
