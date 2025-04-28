import streamlit as st
import os
import io
import time
import pandas as pd
import zipfile
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient
from dotenv import load_dotenv
from PIL import Image

# Load environment variables
load_dotenv()

# Azure configuration
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
COSMOS_CONNECTION_STRING = os.getenv("COSMOS_DB_CONNECTION_STRING")
DATABASE_NAME = "cangodb"
DOCUMENTS_CONTAINER = "document_metadata"
TABLES_CONTAINER = "extracted_tables"
PAIRS_CONTAINER = "pd_seq_pairs"
CELLS_CONTAINER = "table_cells"
RESULTS_CONTAINER_NAME = "processed-results"

# Azure clients
def get_blob_service_client():
    return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

def get_cosmos_container(container_name):
    cosmos_client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
    database = cosmos_client.get_database_client(DATABASE_NAME)
    return database.get_container_client(container_name)

# Data fetching

def list_documents(status_filter="reviewed", limit=100):
    container = get_cosmos_container(DOCUMENTS_CONTAINER)
    query = f"SELECT * FROM c WHERE c.status = '{status_filter}' ORDER BY c.createdAt ASC OFFSET 0 LIMIT {limit}"
    return list(container.query_items(query=query, enable_cross_partition_query=True))

def get_document_tables(doc_id):
    container = get_cosmos_container(TABLES_CONTAINER)
    query = f"SELECT * FROM c WHERE c.documentId = '{doc_id}'"
    return list(container.query_items(query=query, enable_cross_partition_query=True))

def get_document_pairs(doc_id):
    container = get_cosmos_container(PAIRS_CONTAINER)
    query = f"SELECT * FROM c WHERE c.documentId = '{doc_id}'"
    return list(container.query_items(query=query, enable_cross_partition_query=True))

def get_document_cells(doc_id):
    container = get_cosmos_container(CELLS_CONTAINER)
    query = f"SELECT * FROM c WHERE c.documentId = '{doc_id}'"
    return list(container.query_items(query=query, enable_cross_partition_query=True))

def download_blob(blob_name):
    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client(RESULTS_CONTAINER_NAME)
    blob_client = container_client.get_blob_client(blob_name)
    return blob_client.download_blob().readall()

# Zip creation

def create_zip(documents, include_images=True):
    memory_zip = io.BytesIO()

    with zipfile.ZipFile(memory_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for doc in documents:
            doc_id = doc["id"]
            filename = doc.get("originalFilename", f"{doc_id}.pdf")
            folder = f"{doc_id}/"

            # Tables
            tables = get_document_tables(doc_id)
            if tables:
                df = pd.DataFrame(tables)
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                archive.writestr(folder + "tables.csv", csv_buffer.getvalue())

            # Pairs
            pairs = get_document_pairs(doc_id)
            if pairs:
                df = pd.DataFrame(pairs)
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                archive.writestr(folder + "pairs.csv", csv_buffer.getvalue())

            # Cells
            cells = get_document_cells(doc_id)
            if cells:
                df = pd.DataFrame(cells)
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                archive.writestr(folder + "cells.csv", csv_buffer.getvalue())

            # Images (optional)
            if include_images:
                for page_num in range(1, doc.get("pageCount", 1) + 1):
                    try:
                        blob_name = f"pages/{doc_id}/page_{page_num}.jpg"
                        image_bytes = download_blob(blob_name)
                        archive.writestr(folder + f"images/page_{page_num}.jpg", image_bytes)
                    except Exception as e:
                        pass  # skip missing images

    memory_zip.seek(0)
    return memory_zip

# Streamlit UI

st.title("Bulk Export Document Data")

status_filter = st.selectbox("Select Document Status to Export", ["ready_for_review", "in_review", "reviewed"], index=2)
limit = st.number_input("Max Documents to Export", min_value=1, max_value=500, value=10)
include_images = st.checkbox("Include Page Images", value=True)

if st.button("Export Now"):
    with st.spinner("Preparing export..."):
        documents = list_documents(status_filter=status_filter, limit=int(limit))

        if not documents:
            st.warning("No documents found for the selected criteria.")
        else:
            zip_buffer = create_zip(documents, include_images=include_images)

            st.success(f"Exported {len(documents)} documents!")
            st.download_button(
                label="Download Exported Data (ZIP)",
                data=zip_buffer,
                file_name="exported_documents.zip",
                mime="application/zip"
            )
