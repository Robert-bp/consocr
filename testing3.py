#!/usr/bin/env python3
import os
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
# KNOWN DOCUMENT IDS
# -----------------------------------------------------------------------------
known_document_ids = set([
    "02c783f8-97ce-4d65-95ea-04d9a2ff4e1d",
    "2166844e-de61-4ad2-89eb-23e3d60e486e",
    "34f1f716-a11a-4e90-859b-08a19d91e324",
    "4c8dcd2c-4c56-4a4d-aa18-5e4b18984028",
    "501f593a-6960-44c2-a1b4-cf1afcb3cdc0",
    "563d4203-d626-4626-9f14-83dba55738ef",
    "5a189a13-12ff-48f5-bc7e-0aa04281c5c9",
    "5f4f5a34-98bc-4912-a97d-727fb18c4931",
    "605e5736-1a53-418f-ac57-653fb2bfba78",
    "7aefce22-a7b1-4665-8aac-9d6c328f71f0",
    "886d8d64-946e-40b2-8cd6-9e702f7c0654",
    "8ca299d5-c264-40a2-a09e-ce788f21f769",
    "8ea52b40-8d10-4d8d-8858-aed106df05ac",
    "985b90db-6616-4d3c-aed7-6c306991625e",
    "b228632c-ca70-4a72-83f4-2e5534eac545",
    "c04f39cb-5f63-4929-b1e5-1074fe8f0572",
    "cfb1404a-9fd0-42c5-89de-48ab6283d632",
    "cfcd410e-9b45-431c-bf71-b9c93139ac44",
    "da9f0d89-33dc-4aac-9cf7-a78d53460d01",
    "ed0d2614-3ebc-47e3-9260-1ecbb7b0c427",
    "f3f466fc-d15c-4e8d-8f68-72a23f31cf83",
    "fc73272a-adcf-4bbf-a1d8-4b27d6736c56",
])

# -----------------------------------------------------------------------------
# FIND DOCUMENT IDS IN BLOB STORAGE
# -----------------------------------------------------------------------------
found_document_ids = set()

for blob in client.list_blobs(name_starts_with=PREFIX):
    parts = blob.name.split("/")
    if len(parts) >= 2:
        doc_id = parts[1]
        found_document_ids.add(doc_id)

# -----------------------------------------------------------------------------
# ANALYSIS
# -----------------------------------------------------------------------------
matched = known_document_ids.intersection(found_document_ids)
missing_in_blob = known_document_ids.difference(found_document_ids)
unknown_in_blob = found_document_ids.difference(known_document_ids)

print("✅ Document IDs found in both known list and blobs:")
for doc_id in sorted(matched):
    print(f"  - {doc_id}")

print("\n❌ Known Document IDs missing from blobs:")
for doc_id in sorted(missing_in_blob):
    print(f"  - {doc_id}")

print("\n⚠️ Unknown Document IDs found in blobs but not in known list:")
for doc_id in sorted(unknown_in_blob):
    print(f"  - {doc_id}")
