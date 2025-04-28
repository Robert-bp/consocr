from azure.storage.blob import BlobServiceClient
import os


# Connect to your blob storage
CONN_STR = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
UPLOAD_CONTAINER = "document-uploads"  # <-- not processed-results! The original uploads!
PREFIX = "default_user/saint-john/"     # <-- or whatever your prefix is
svc = BlobServiceClient.from_connection_string(CONN_STR)
client = svc.get_container_client(UPLOAD_CONTAINER)

# Build a map: doc_id -> riding
docid_to_riding = {}

for blob in client.list_blobs(name_starts_with=PREFIX):
    props = client.get_blob_client(blob.name).get_blob_properties()
    metadata = props.metadata

    doc_id = metadata.get("doc_id")
    riding = metadata.get("riding", "")

    if doc_id:
        docid_to_riding[doc_id] = riding

print(f"✅ Loaded {len(docid_to_riding)} document_id → riding mappings")
