#!/usr/bin/env python3
"""
Document Processing Script

This script processes documents uploaded to Azure Blob Storage:
1. Checks for documents with 'uploaded' status in Cosmos DB
2. Downloads and processes documents with Azure Form Recognizer
3. Splits PDFs into individual pages
4. Extracts tables and analyzes content
5. Updates document status and stores results
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
import pandas as pd
from pdf2image import convert_from_bytes
import logging
import argparse

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
TABLES_CONTAINER = "extracted_tables" 
PAIRS_CONTAINER = "pd_seq_pairs"
# Add this to your database configuration
CELLS_CONTAINER = "table_cells"

# Max PDF pages to process
MAX_PDF_PAGES = 20

REVIEWERS = os.getenv("REVIEWER_POOL", "").split(",")
REVIEWERS = [r.strip() for r in REVIEWERS if r.strip()]
POOL_LEN  = len(REVIEWERS) or 1


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("document_processing.log"),
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

def get_document_client():
    """Creates an Azure Document Intelligence client"""
    return DocumentAnalysisClient(
        endpoint=DOC_ENDPOINT,
        credential=AzureKeyCredential(DOC_KEY)
    )

def update_document_status(doc_id, status, **kwargs):
    """Update document status and additional fields in Cosmos DB"""
    container = get_cosmos_container(DOCUMENTS_CONTAINER)
    
    # Get the current document
    query = f"SELECT * FROM c WHERE c.id = '{doc_id}'"
    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    
    if items:
        # Update the existing document
        document = items[0]
        document["status"] = status
        document["lastUpdatedAt"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        
        # Update additional fields
        for key, value in kwargs.items():
            document[key] = value
        
        container.replace_item(document["id"], document)
        return True
    
    return False

def get_documents_to_process(limit=10):
    """Get list of documents that need processing"""
    container = get_cosmos_container(DOCUMENTS_CONTAINER)
    
    # Query for documents with 'uploaded' status
    query = "SELECT * FROM c WHERE c.status = 'queued' ORDER BY c.createdAt ASC"
    
    if limit:
        query += f" OFFSET 0 LIMIT {limit}"
    
    return list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

def download_blob(blob_name):
    """Download a blob from Azure Blob Storage"""
    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
    blob_client = container_client.get_blob_client(blob_name)
    
    return blob_client.download_blob().readall()

def save_blob(container_name, blob_name, data, content_type=None):
    """Save data to Azure Blob Storage"""
    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client(container_name)
    
    # Create container if it doesn't exist
    try:
        container_client.create_container()
    except:
        # Container already exists
        pass
    
    blob_client = container_client.get_blob_client(blob_name)
    
    # Upload blob
    blob_client.upload_blob(data, overwrite=True)
    
    return blob_name

def process_pdf_pages(pdf_bytes, document_client, doc_id):
    """Process a PDF file page by page"""
    # Convert PDF to images
    pages = convert_from_bytes(pdf_bytes, dpi=300)
    
    # Limit to MAX_PDF_PAGES
    pages = pages[:MAX_PDF_PAGES]
    
    results = []
    
    for i, page in enumerate(pages):
        logger.info(f"Processing page {i+1} of {len(pages)} for document {doc_id}")
        
        # Convert page to bytes
        img_byte_arr = io.BytesIO()
        page.save(img_byte_arr, format='JPEG')
        img_bytes = img_byte_arr.getvalue()
        
        # Create blob name for this page
        page_blob_name = f"pages/{doc_id}/page_{i+1}.jpg"
        
        # Save page image to blob storage
        save_blob(RESULTS_CONTAINER_NAME, page_blob_name, img_bytes)
        
        # Analyze with Document Intelligence
        poller = document_client.begin_analyze_document("prebuilt-document", img_bytes)
        result = poller.result()
        
        if result:
            # Add to results
            results.append({
                'page_number': i+1,
                'analysis': result,
                'page_blob_name': page_blob_name
            })
            
            # Save raw results to blob storage
            result_blob_name = f"results/{doc_id}/page_{i+1}_raw.json"
            save_blob(
                RESULTS_CONTAINER_NAME, 
                result_blob_name, 
                json.dumps(result.to_dict())
            )
    
    # Update document with page count
    update_document_status(doc_id, "processed", pageCount=len(pages))
    
    return results

# ----------------------------------------------------------------------
#  FULL REPLACEMENT – paste over the old process_table_cells definition
# ----------------------------------------------------------------------
from PIL import Image
import io, uuid, time

CONF_THRESHOLD = 0.90        # < 0.90 → needs_review

def process_table_cells(doc_id, table_df, page_number, table_idx):
    """
    • Writes/updates one row per cell in the `table_cells` container.
    • If confidence < CONF_THRESHOLD, the cell is flagged "needs_review" and
      a cropped JPEG of that bounding-box is saved to Blob Storage.
    • Returns (total_cells, auto_approved_cells)
    """
    cells_container = get_cosmos_container(CELLS_CONTAINER)
    blob_service    = get_blob_service_client()
    container_client = blob_service.get_container_client(RESULTS_CONTAINER_NAME)

    # Load the already-saved full-page JPEG once
    try:
        page_blob_name = f"pages/{doc_id}/page_{page_number}.jpg"
        page_img_bytes = download_blob(page_blob_name)
        page_img = Image.open(io.BytesIO(page_img_bytes))
    except Exception:
        page_img = None                                  # silently skip crops

    total_cells = 0
    auto_approved_cells = 0
    pending_counter = 0             # counts cells that need review


    for _, row in table_df.iterrows():
        confidence = float(row.get("Confidence", 0.0))
        needs_review = confidence < CONF_THRESHOLD
        crop_blob_name = None

        # ------------------------------------------------------------------
        #  ❶  Crop and upload low-confidence cells
        # ------------------------------------------------------------------
        if needs_review and page_img is not None and "BoundingBox" in row:
            # row["BoundingBox"] expected as [x0, y0, x1, y1, …] 0-1 floats
            bb = row["BoundingBox"]
            xs, ys = bb[::2], bb[1::2]
            left   = int(min(xs) * page_img.width)  - 2
            right  = int(max(xs) * page_img.width)  + 2
            top    = int(min(ys) * page_img.height) - 2
            bottom = int(max(ys) * page_img.height) + 2
            crop   = page_img.crop((left, top, right, bottom))

            crop_blob_name = (
                f"pages/{doc_id}/crops/"
                f"page_{page_number}_{int(row['Row'])}_{int(row['Column'])}.jpg"
            )

            buf = io.BytesIO()
            crop.save(buf, format="JPEG", quality=80)
            buf.seek(0)
            container_client.upload_blob(crop_blob_name, buf, overwrite=True)

        # ------------------------------------------------------------------
        #  ❷  Persist the cell record
        # ------------------------------------------------------------------
        status = "needs_review" if needs_review else "approved"

                # pick a reviewer *only* for cells that need review
        if needs_review:
            assignee = REVIEWERS[pending_counter % POOL_LEN] if REVIEWERS else None
            pending_counter += 1
        else:
            assignee = None            # auto-approved cells have no assignee



        cell_item = {
            "id": f"{doc_id}_page{page_number}_tbl{table_idx}_row{row['Row']}_col{row['Column']}",
            "assignedTo": assignee, 
            "documentId": doc_id,
            "pageNumber": int(page_number),
            "tableId": int(table_idx),
            "row": int(row["Row"]),
            "column": int(row["Column"]),
            "content": row["Content"],
            "originalContent": row["Content"],
            "cleanedContent": row["CleanedContent"],
            "confidence": confidence,
            "status": status,
            "blobName": crop_blob_name,         # ★ new field (None when approved)
            "reviewerId": None,
            "reviewedAt": None,
            "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

        cells_container.upsert_item(cell_item)

        total_cells += 1
        if not needs_review:
            auto_approved_cells += 1

    # ----------------------------------------------------------------------
    #  ❸  Update document-level status & counts
    # ----------------------------------------------------------------------
    document_status = (
        "ready_for_export" if auto_approved_cells == total_cells else "ready_for_review"
    )
    # Choose a reviewer from the pool
    assigned_reviewer = None
    if REVIEWERS:
        # Assign document to a reviewer (round-robin)
        doc_count = get_document_count()  # implement this to count total docs
        assigned_reviewer = REVIEWERS[doc_count % POOL_LEN]

    # Determine document status based on cell approval
    document_status = "ready_for_export" if auto_approved_cells == total_cells else "ready_for_review"

    # Update document status
    update_document_status(
        doc_id,
        document_status,  # Use the variable we set above
        reviewerId=assigned_reviewer,  # Assign at document level
        totalCells=total_cells,
        pendingCells=(total_cells - auto_approved_cells),
    )
    
    # Get all assigned reviewers for the document
    cells_container = get_cosmos_container(CELLS_CONTAINER)
    assigned_reviewers_query = f"""
        SELECT DISTINCT c.assignedTo 
        FROM c 
        WHERE c.documentId = '{doc_id}' AND c.assignedTo != null
    """
    reviewers = [item['assignedTo'] for item in cells_container.query_items(
        query=assigned_reviewers_query,
        enable_cross_partition_query=True
    )]
    
    update_document_status(
        doc_id,
        document_status,
        totalCells=total_cells,
        approvedCells=auto_approved_cells,
        reviewedCells=auto_approved_cells,
        pendingCells=(total_cells - auto_approved_cells),
        assignedReviewers=reviewers  # Add this field to track which reviewers are assigned
    )       

    return total_cells, auto_approved_cells
# ----------------------------------------------------------------------
def get_document_count():
    """Get total count of documents for round-robin assignment"""
    container = get_cosmos_container(DOCUMENTS_CONTAINER)
    query = "SELECT VALUE COUNT(1) FROM c"
    count = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))[0]
    return count
def analyze_document(doc_id, blob_name, content_type):
    """Process document with Azure Document Intelligence"""
    document_client = get_document_client()
    
    # Download the document from blob storage
    file_bytes = download_blob(blob_name)
    
    if content_type == 'application/pdf':
        # Process PDF page by page
        return process_pdf_pages(file_bytes, document_client, doc_id)
    else:
        # Process single image
        poller = document_client.begin_analyze_document("prebuilt-document", file_bytes)
        result = poller.result()
        
        if result:
            # Save raw results to blob storage
            result_blob_name = f"results/{doc_id}/image_raw.json"
            save_blob(
                RESULTS_CONTAINER_NAME, 
                result_blob_name, 
                json.dumps(result.to_dict())
            )
            
            # Update document status
            update_document_status(doc_id, "processed", pageCount=1)
            
            return [{
                'page_number': 1,
                'analysis': result,
                'page_blob_name': None
            }]
    
    return None

def extract_tables_from_result(result):
    """Return one DataFrame row per table-cell, incl. bounding box."""
    rows = []
    for t_idx, table in enumerate(result.tables):
        for cell in table.cells:
            bb = None
            if cell.bounding_regions and cell.bounding_regions[0].polygon:
                # polygon → flat list [x0,y0,x1,y1 …]  (normalised 0-1 coords)
                poly = cell.bounding_regions[0].polygon
                bb = [p.x for p in poly] + [p.y for p in poly]

            rows.append(
                dict(
                    Table       = t_idx + 1,
                    Row         = cell.row_index,
                    Column      = cell.column_index,
                    Content     = cell.content,
                    RowSpan     = cell.row_span,
                    ColumnSpan  = cell.column_span,
                    Confidence  = 1.0,              # will be overwritten below
                    CleanedContent = re.sub(r"[^0-9]", "", str(cell.content)),
                    BoundingBox = bb,
                )
            )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # --- word-level confidence ------------------------------------------------
    for pg in result.pages:
        for w in pg.words:
            mask = df["Content"].str.contains(re.escape(w.content), case=False, na=False)
            df.loc[mask, "Confidence"] = (
                df.loc[mask, "Confidence"] * 0 + w.confidence
            )

    return df

def save_table_to_cosmos(doc_id, table_df, page_number):
    """Save table data to Cosmos DB"""
    if table_df.empty:
        return
    
    tables_container = get_cosmos_container(TABLES_CONTAINER)
    
    for _, row in table_df.iterrows():
        table_item = row.to_dict()
        
        # Convert any non-serializable objects
        for key, value in table_item.items():
            if pd.isna(value):
                table_item[key] = None
            elif isinstance(value, (pd.Timestamp, pd._libs.tslibs.timestamps.Timestamp)):
                table_item[key] = value.isoformat()
            elif isinstance(value, (int, float)):
                # Ensure numbers are serializable
                table_item[key] = float(value) if isinstance(value, float) else int(value)
        
        # Add document and page info
        table_item["id"] = f"{doc_id}_page{page_number}_table{row['Table']}_row{row['Row']}_col{row['Column']}"
        table_item["documentId"] = doc_id
        table_item["pageNumber"] = page_number
        
        # Save to Cosmos DB
        tables_container.upsert_item(table_item)

def find_pd_seq_pairs(table_df):
    """Find PD and Seq pairs in table data"""
    if table_df.empty:
        return pd.DataFrame()
    
    # Try to identify header row (usually 0)
    headers_df = table_df[table_df["Row"] == 0]
    
    pd_col = None
    seq_col = None
    
    for _, header in headers_df.iterrows():
        header_text = str(header["Content"]).lower() if isinstance(header["Content"], str) else ""
        if "pd no" in header_text or "pd" in header_text:
            pd_col = header["Column"]
        elif "seq" in header_text:
            seq_col = header["Column"]
    
    # If both columns were found, create pairs
    if pd_col is not None and seq_col is not None:
        # Get data rows (exclude header)
        data_rows = table_df[table_df["Row"] > 0]
        
        pairs = []
        for row_idx in data_rows["Row"].unique():
            pd_value = ""
            seq_value = ""
            
            # Get PD value
            pd_cells = data_rows[(data_rows["Row"] == row_idx) & (data_rows["Column"] == pd_col)]
            if not pd_cells.empty:
                pd_value = pd_cells["Content"].iloc[0]
            
            # Get Seq value
            seq_cells = data_rows[(data_rows["Row"] == row_idx) & (data_rows["Column"] == seq_col)]
            if not seq_cells.empty:
                seq_value = seq_cells["Content"].iloc[0]
            
            # Only add if at least one value exists
            if pd_value or seq_value:
                pairs.append({
                    "FULL_PD": pd_value,
                    "SEQUENCE_NUMBER": seq_value,
                    "FULL_PD_cleaned": re.sub(r'[^0-9]', '', str(pd_value)),
                    "SEQUENCE_NUMBER_cleaned": re.sub(r'[^0-9]', '', str(seq_value)),
                    "ED_CODE": 35023  # Hardcoded from your example
                })
        
        if pairs:
            return pd.DataFrame(pairs)
    
    # If no pairs found, return empty DataFrame
    return pd.DataFrame()

def save_pairs_to_cosmos(doc_id, pairs_df, page_number, table_idx):
    """Save PD/Seq pairs to Cosmos DB"""
    if pairs_df.empty:
        return
    
    pairs_container = get_cosmos_container(PAIRS_CONTAINER)
    
    for idx, row in pairs_df.iterrows():
        pair_item = row.to_dict()
        
        # Convert any non-serializable objects
        for key, value in pair_item.items():
            if pd.isna(value):
                pair_item[key] = None
            elif isinstance(value, (pd.Timestamp, pd._libs.tslibs.timestamps.Timestamp)):
                pair_item[key] = value.isoformat()
        
        # Add document and page info
        pair_item["id"] = f"{doc_id}_page{page_number}_table{table_idx}_pair{idx}"
        pair_item["documentId"] = doc_id
        pair_item["pageNumber"] = page_number
        pair_item["tableIndex"] = table_idx
        
        # Save to Cosmos DB
        pairs_container.upsert_item(pair_item)

def process_document(doc_info):
    """Process a single document"""
    doc_id = doc_info["id"]
    blob_name = doc_info["blobName"]
    content_type = doc_info["contentType"]
    
    logger.info(f"Processing document {doc_id} from {blob_name}")
    
    try:
        # Update status to processing
        update_document_status(doc_id, "processing")
        
        # Process the document
        all_results = analyze_document(doc_id, blob_name, content_type)
        
        if not all_results:
            raise Exception("Failed to analyze document")
        
        # Count total tables
        total_tables = sum(len(page_result['analysis'].tables) for page_result in all_results)
        
        # Update document with table count
        update_document_status(doc_id, "processing", tableCount=total_tables)
        
        # Process each page result
        for page_result in all_results:
            page_num = page_result['page_number']
            result = page_result['analysis']
            
            # Process tables if present
            if result.tables:
                # Extract all tables
                all_tables_df = extract_tables_from_result(result)
                
                # Save to Cosmos DB
                save_table_to_cosmos(doc_id, all_tables_df, page_num)
                
                # For each table on this page
                for table_idx in all_tables_df['Table'].unique():
                    # Get just this table's data
                    table_df = all_tables_df[all_tables_df['Table'] == table_idx].copy()
                    
                    # Process cell-level confidence and status
                    total_cells, auto_approved = process_table_cells(doc_id, table_df, page_num, int(table_idx))
                    logger.info(f"Table {table_idx}: {auto_approved}/{total_cells} cells auto-approved")
                    
                    # Generate and save pairs
                    paired_df = find_pd_seq_pairs(table_df)
                    if not paired_df.empty:
                        save_pairs_to_cosmos(doc_id, paired_df, page_num, int(table_idx))
                        
                        # Save pairs to blob storage as CSV
                        csv_blob_name = f"results/{doc_id}/page_{page_num}_table_{int(table_idx)}_pairs.csv"
                        save_blob(
                            RESULTS_CONTAINER_NAME, 
                            csv_blob_name, 
                            paired_df.to_csv(index=False)
                        )
        
        # Update document status to complete
        update_document_status(doc_id, "ready_for_review")
        logger.info(f"Successfully processed document {doc_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing document {doc_id}: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Update document status to error
        update_document_status(doc_id, "error", errorMessage=str(e))
        
        return False

def main(args):
    """Main function to process documents"""
    # Get documents that need processing
    documents = get_documents_to_process(limit=args.limit)
    
    if not documents:
        logger.info("No documents found for processing")
        return
    
    logger.info(f"Found {len(documents)} documents to process")
    
    # Process each document
    for doc in documents:
        process_document(doc)
        
        # Sleep between documents if requested
        if args.delay > 0:
            time.sleep(args.delay)

# --- replace the whole “if __name__ == '__main__'” block --------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process queued documents from Cosmos."
    )
    parser.add_argument("-n", "--batch-size", type=int, default=10,
                        help="How many queued docs to process per run")
    parser.add_argument("--delay", type=int, default=1,
                        help="Delay (s) between docs")
    parser.add_argument("--doc-id", type=str,
                        help="Process *only* the given document")
    args = parser.parse_args()

    if args.doc_id:
        # single-doc path (unchanged)
        container = get_cosmos_container(DOCUMENTS_CONTAINER)
        query = f"SELECT * FROM c WHERE c.id = '{args.doc_id}'"
        doc = next(container.query_items(query=query, enable_cross_partition_query=True), None)
        if doc:
            process_document(doc)
        else:
            logger.error(f"Document {args.doc_id} not found")
    else:
        # batch path
        docs = get_documents_to_process(limit=args.batch_size)
        if not docs:
            logger.info("No documents found for processing")
        else:
            logger.info(f"Found {len(docs)} documents to process")
            for d in docs:
                process_document(d)
                if args.delay:
                    time.sleep(args.delay)
