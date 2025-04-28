#!/usr/bin/env python3
"""
Process Missing Metadata Script

This script identifies blobs in Azure Blob Storage that don't have corresponding
metadata entries in Cosmos DB, creates metadata records for them, and processes them.
"""

import os
import io
import json
import uuid
import time
import traceback
import re
import argparse
from datetime import datetime
import logging
import sys

# Azure imports
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Azure configuration
AZURE_STORAGE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
COSMOS_CONNECTION_STRING = os.environ.get("COSMOS_DB_CONNECTION_STRING")
DOC_ENDPOINT = os.environ.get("DOCUMENT_INTELLIGENCE_ENDPOINT")
DOC_KEY = os.environ.get("DOCUMENT_INTELLIGENCE_KEY")

# Container names
BLOB_CONTAINER_NAME = "document-uploads"
RESULTS_CONTAINER_NAME = "processed-results"

# Database configuration
DATABASE_NAME = "cangodb"
DOCUMENTS_CONTAINER = "document_metadata"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("missing_metadata_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_blob_service_client():
    """Create a blob service client from the connection string"""
    return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

def get_cosmos_container(container_name):
    """Get Cosmos DB container"""
    cosmos_client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
    database = cosmos_client.get_database_client(DATABASE_NAME)
    return database.get_container_client(container_name)

def get_all_blob_names():
    """Get all blob names from the blob storage container"""
    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
    blobs = container_client.list_blobs()
    return [blob.name for blob in blobs]

def get_all_document_blob_names():
    """Get all blob names that already have metadata entries"""
    container = get_cosmos_container(DOCUMENTS_CONTAINER)
    query = "SELECT c.blobName FROM c"
    return [item['blobName'] for item in container.query_items(
        query=query,
        enable_cross_partition_query=True
    )]

def get_blobs_without_metadata(limit=10):
    """Get blobs that don't have corresponding metadata entries"""
    all_blobs = get_all_blob_names()
    existing_metadata_blobs = get_all_document_blob_names()
    
    logger.info(f"Found {len(all_blobs)} total blobs in storage")
    logger.info(f"Found {len(existing_metadata_blobs)} blobs with metadata entries")
    
    # Find blobs that don't have metadata entries
    missing_metadata_blobs = [blob for blob in all_blobs if blob not in existing_metadata_blobs]
    
    logger.info(f"Found {len(missing_metadata_blobs)} blobs without metadata entries")
    
    # Return limited number of results
    return missing_metadata_blobs[:limit]

def detect_content_type(blob_name):
    """Detect content type based on file extension"""
    lower_name = blob_name.lower()
    if lower_name.endswith('.pdf'):
        return "application/pdf"
    elif lower_name.endswith('.jpg') or lower_name.endswith('.jpeg'):
        return "image/jpeg"
    elif lower_name.endswith('.png'):
        return "image/png"
    elif lower_name.endswith('.tiff') or lower_name.endswith('.tif'):
        return "image/tiff"
    else:
        return "application/octet-stream"

def extract_riding(blob_name):
    """Extract riding from blob name"""
    parts = blob_name.split('/')
    if len(parts) > 1:
        return parts[0]
    return "unknown"

def create_metadata_for_blob(blob_name):
    """Create metadata entry for a blob"""
    # Generate a new document ID
    doc_id = str(uuid.uuid4())
    
    # Extract filename from blob path
    filename = blob_name.split('/')[-1]
    
    # Extract riding from blob path
    riding = extract_riding(blob_name)
    
    # Determine content type
    content_type = detect_content_type(blob_name)
    
    # Create document metadata
    doc_info = {
        "id": doc_id,
        "documentId": doc_id,
        "blobName": blob_name,
        "originalFilename": filename,
        "riding": riding,
        "contentType": content_type,
        "userId": "default_user",
        "status": "queued",
        "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "lastUpdatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "reviewerId": None,
        "reviewStartedAt": None,
        "reviewCompletedAt": None,
        "rawResultsPath": None,
        "cleanedResultsPath": None,
        "errorMessage": None,
        "pageCount": 0,
        "tableCount": 0
    }
    
    # Insert into Cosmos DB
    container = get_cosmos_container(DOCUMENTS_CONTAINER)
    container.create_item(doc_info)
    
    logger.info(f"Created metadata entry for blob: {blob_name}")
    
    return doc_info

def main(args):
    """Main function to process blobs without metadata"""
    try:
        # Get blobs without metadata
        missing_blobs = get_blobs_without_metadata(limit=args.limit)
        
        if not missing_blobs:
            logger.info("No blobs found without metadata")
            return
        
        logger.info(f"Processing {len(missing_blobs)} blobs without metadata")
        
        # Process each blob
        for blob_name in missing_blobs:
            logger.info(f"Processing blob: {blob_name}")
            
            try:
                # Create metadata for blob
                doc_info = create_metadata_for_blob(blob_name)
                
                # Log the created metadata info
                logger.info(f"Created metadata with ID: {doc_info['id']} for blob: {blob_name}")
                
                # Document is now queued for processing
                logger.info(f"Document {doc_info['id']} queued for processing")
                
            except Exception as e:
                logger.error(f"Error processing blob {blob_name}: {str(e)}")
                logger.error(traceback.format_exc())
            
            # Sleep between blobs
            if args.delay > 0:
                time.sleep(args.delay)
                
        logger.info("Completed processing blobs without metadata")
        
    except Exception as e:
        logger.error(f"Error in main processing: {str(e)}")
        logger.error(traceback.format_exc())
        return 1
    
    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process blobs without metadata entries."
    )
    parser.add_argument("-n", "--limit", type=int, default=10,
                        help="Maximum number of blobs to process")
    parser.add_argument("--delay", type=int, default=1,
                        help="Delay (s) between blobs")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't actually create metadata, just show what would be done")
    
    args = parser.parse_args()
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No metadata will be created")
    
    sys.exit(main(args))