import os
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
COSMOS_CONNECTION_STRING = os.getenv("COSMOS_DB_CONNECTION_STRING")

# Configuration
CONTAINER_NAME = "document-uploads"  # or whatever your container is (adjust if needed)
DATABASE_NAME = "cangodb"
DOCUMENTS_CONTAINER = "document_metadata"

# Your list of filenames
filenames = [
    "1745868601_674fa946-6a78-47d2-a883-aae390b89a1d_Elections Canada.pdf",
    "1745868604_3944343e-3f80-4563-b949-047518a414fc_Elections Canada.pdf",
    "1745868607_5b48fa47-435c-49e5-80cb-65ffcafddca7_Elections Canada.pdf",
    "1745868610_396a312a-83ea-4a9d-ad0c-63429bdac165_Elections Canada.pdf",
    "1745868612_f5e4f2ee-1b43-4422-ae17-e98f9a5ab81d_Elections Canada.pdf",
    "1745868672_b1502ec2-ffe5-42d9-9dac-ace6fe84fc2e_f444.pdf",
    "1745868719_fa1e90a0-7c9f-4edc-bd3a-a199fb159c73_Sequence Number Sheet - Election Day-12.pdf",
    "1745868759_938bf2ca-63b6-4d30-9f94-7601e3cdb582_f55566333.pdf",
    "1745868767_e9429160-7a80-4990-a0c8-79fdc1c66b88_f55566.pdf",
    "1745868931_1dbf263f-565f-4c26-ad70-6605caa7f283_f444.pdf"
]

# Initialize Azure clients
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
blob_container_client = blob_service_client.get_container_client(CONTAINER_NAME)

cosmos_client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
cosmos_container = cosmos_client.get_database_client(DATABASE_NAME).get_container_client(DOCUMENTS_CONTAINER)

# Script
for filename in filenames:
    source_blob = filename
    target_blob = f"moncton/{filename}"

    # 1. Copy blob
    source_blob_client = blob_container_client.get_blob_client(source_blob)
    target_blob_client = blob_container_client.get_blob_client(target_blob)

    # Get the URL of the source blob
    source_blob_url = source_blob_client.url
    print(f"Copying {source_blob} to {target_blob}...")

    # Start copy
    copy_props = target_blob_client.start_copy_from_url(source_blob_url)
    print(f"Copy started: {copy_props['copy_status']}")

    # (Optional) Delete original blob if you want
    # source_blob_client.delete_blob()

    # 2. Update riding field in CosmosDB
    # Document ID is usually the base without extension
    document_id = filename.split('_')[1].split('-')[0]  # or adjust how your IDs are generated

    query = f"SELECT * FROM c WHERE c.originalFilename = '{filename}'"
    items = list(cosmos_container.query_items(query=query, enable_cross_partition_query=True))

    if items:
        doc = items[0]
        doc["riding"] = "Moncton"
        doc["blobName"] = target_blob  # Update the blobName to the new location if necessary
        cosmos_container.replace_item(doc["id"], doc)
        print(f"Updated document {doc['id']} riding to Moncton")
    else:
        print(f"No document found for {filename} in Cosmos DB!")

print("Done moving and updating.")
