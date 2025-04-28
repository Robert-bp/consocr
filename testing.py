#!/usr/bin/env python3
import os, json, csv
from azure.storage.blob import BlobServiceClient

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------
CONN_STR = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
CONTAINER = "processed-results"
PREFIX = "results/"
svc = BlobServiceClient.from_connection_string(CONN_STR)
client = svc.get_container_client(CONTAINER)

# -----------------------------------------------------------------------------
# OPEN CSV FOR WRITING
# -----------------------------------------------------------------------------
with open("raw_cells_fixed.csv", "w", newline="", encoding="utf-8") as fout:
    writer = csv.writer(fout)
    writer.writerow([
        "document_id",
        "blob_name",
        "page_number",
        "table_index",
        "row_index",
        "column_index",
        "content",
        "confidence"
    ])

    # -----------------------------------------------------------------------------
    # LIST & PARSE EACH *_raw.json
    # -----------------------------------------------------------------------------
    for blob in client.list_blobs(name_starts_with=PREFIX):
        if not blob.name.endswith("_raw.json"):
            continue

        parts = blob.name.split("/")  # ["results", "<doc_id>", "page_X_raw.json"]
        if len(parts) < 2:
            continue
        doc_id = parts[1]
        blob_nm = os.path.basename(blob.name)

        # Download JSON content
        raw_json = client.get_blob_client(blob.name).download_blob().content_as_text()
        data = json.loads(raw_json)

        tables = data.get("tables", [])
        if not tables:
            continue  # Skip files with no tables

        for ti, table in enumerate(tables, start=1):
            cells = table.get("cells", [])
            for cell in cells:
                # Safe get confidence from spans if they exist
                spans = cell.get("spans", [])
                if spans and isinstance(spans, list) and len(spans) > 0:
                    confidence = spans[0].get("confidence", "")
                else:
                    confidence = ""

                writer.writerow([
                    doc_id,
                    blob_nm,
                    cell.get("bounding_regions", [{}])[0].get("page_number", ""),
                    ti,
                    cell.get("row_index", ""),
                    cell.get("column_index", ""),
                    cell.get("content", "").replace("\n", " ").strip(),
                    confidence
                ])

print("✅ Done — fixed raw_cells_fixed.csv generated with real row/column numbers and confidence.")
