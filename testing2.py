#!/usr/bin/env python3
import pandas as pd
import os
from azure.storage.blob import BlobServiceClient

# Load the extracted cells CSV (your version that has document_id, blob_name, page_number, table_index, row_index, column_index, content)
input_csv = "raw_cells_fixed.csv"  # <- Change to your actual filename if different
output_csv = "matched_pairs.csv"

# Connect to your blob storage to retrieve riding info
CONN_STR = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
UPLOAD_CONTAINER = "document-uploads"
svc = BlobServiceClient.from_connection_string(CONN_STR)
client = svc.get_container_client(UPLOAD_CONTAINER)

# Build a map: doc_id -> riding
docid_to_riding = {}

for blob in client.list_blobs(include=["metadata"]):
    metadata = blob.metadata or {}
    doc_id = metadata.get("doc_id")
    riding = metadata.get("riding", "")

    if doc_id:
        docid_to_riding[doc_id] = riding

print(f"✅ Loaded {len(docid_to_riding)} document_id → riding mappings")

# Read the extracted cells
cells_df = pd.read_csv(input_csv)

# Print full table grouped by document_id, blob_name, page_number, table_index
for (document_id, blob_name, page_number, table_index), group_df in cells_df.groupby([
    "document_id", "blob_name", "page_number", "table_index"]):
    print(f"\n📄 Document: {document_id}, Blob: {blob_name}, Page: {page_number}, Table: {table_index}")
    print(group_df.sort_values(by=["row_index", "column_index"]))

# Prepare an empty list to hold the matched pairs
pairs = []

# Group by document, blob, page, table to handle separately per table
for (document_id, blob_name, page_number, table_index), group_df in cells_df.groupby([
    "document_id", "blob_name", "page_number", "table_index"]):

    # Pivot the table: rows by row_index, columns by column_index
    pivot_df = group_df.pivot_table(
        index="row_index", columns="column_index", values="content", aggfunc="first"
    )

    # Skip header rows (assume row_index 0 is headers)
    for row_idx in sorted(pivot_df.index):
        if row_idx == 0:
            continue  # Skip header row

        pd_value = pivot_df.at[row_idx, 0] if 0 in pivot_df.columns else None
        seq_value = pivot_df.at[row_idx, 1] if 1 in pivot_df.columns else None

        if not (str(pd_value).strip() or str(seq_value).strip()):
            continue  # skip completely empty row

        pairs.append({
            "document_id": document_id,
            "blob_name": blob_name,
            "page_number": page_number,
            "table_index": table_index,
            "row_index": row_idx,
            "pd_value": pd_value,
            "seq_value": seq_value,
            "riding": docid_to_riding.get(document_id, "")
        })

# Save the matched pairs to a new CSV
pairs_df = pd.DataFrame(pairs)
pairs_df.to_csv(output_csv, index=False)

print(f"✅ Done! {len(pairs)} pairs saved to {output_csv}")