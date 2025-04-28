# function_app.py
import logging
import json
import os
import datetime
import azure.functions as func
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from azure.cosmos import CosmosClient
import re
import pandas as pd
import datetime  # Add this at the top with other imports
from azure.core.credentials import AzureKeyCredential  # Add this import

# And update this line:




# Configuration
DOC_ENDPOINT = os.environ["DOCUMENT_INTELLIGENCE_ENDPOINT"]
DOC_KEY = os.environ["DOCUMENT_INTELLIGENCE_KEY"]
COSMOS_CONNECTION_STRING = os.environ["COSMOS_DB_CONNECTION_STRING"]

DATABASE_NAME = "cango_documents"
TABLES_CONTAINER = "extracted_tables"
PAIRS_CONTAINER = "pd_seq_pairs"
DOCUMENTS_CONTAINER = "document_metadata"

def main(myblob: func.InputStream):
    """
    Azure Function triggered when a new blob is uploaded to the storage container.
    """
    logging.info(f"Python blob trigger function processed blob: {myblob.name}")
    
    # Get the blob content
    blob_content = myblob.read()
    
    # Extract metadata from blob name
    file_name = myblob.name.split('/')[-1]
    file_extension = os.path.splitext(file_name)[1].lower()
    
    # Connect to Document Intelligence
    document_client = DocumentAnalysisClient(
        endpoint=DOC_ENDPOINT,
        credential=AzureKeyCredential(DOC_KEY)  # Wrap DOC_KEY in AzureKeyCredential
    )
    
    # Analyze document
    try:
        poller = document_client.begin_analyze_document("prebuilt-document", blob_content)
        result = poller.result()
        
        # Connect to Cosmos DB
        cosmos_client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
        database = cosmos_client.get_database_client(DATABASE_NAME)

        
        # Store document metadata
        doc_container = database.get_container_client(DOCUMENTS_CONTAINER)
        document_metadata = {
            "id": file_name.replace(".", "_"),
            "fileName": file_name,
            "fileSize": len(blob_content),
            "pageCount": len(result.pages),
            "tableCount": len(result.tables),
            "processedDate": datetime.datetime.now().isoformat(),
            "status": "processed"
        }
        doc_container.upsert_item(document_metadata)
        
        # Process the tables
        tables_container = database.get_container_client(TABLES_CONTAINER)
        pairs_container = database.get_container_client(PAIRS_CONTAINER)
        
        for table_idx, table in enumerate(result.tables):
            # Create table data
            table_data = []
            for cell in table.cells:
                # Calculate confidence
                confidence = get_word_confidence(result.pages, cell.content)
                
                # Create cell data
                cell_data = {
                    "Row": cell.row_index,
                    "Column": cell.column_index,
                    "Content": cell.content,
                    "Confidence": confidence if confidence is not None else 0.0,
                    "CleanedContent": digits_only(cell.content)
                }
                table_data.append(cell_data)
            
            # Convert to DataFrame for easier processing
            df = pd.DataFrame(table_data)
            df["Table"] = table_idx + 1
            
            # Save table data to Cosmos DB
            for _, row in df.iterrows():
                table_item = row.to_dict()
                table_item["id"] = f"{file_name.replace('.', '_')}_table{table_idx + 1}_row{row['Row']}_col{row['Column']}"
                table_item["documentId"] = file_name.replace(".", "_")
                tables_container.upsert_item(table_item)
            
            # Create pairs (PD no. and Seq.)
            try:
                # Identify header row (usually 0)
                headers_df = df[df["Row"] == 0]
                
                # Look for "PD no." and "Seq." columns
                pd_col = None
                seq_col = None
                
                for _, header in headers_df.iterrows():
                    header_text = header["Content"].lower() if isinstance(header["Content"], str) else ""
                    if "pd no" in header_text:
                        pd_col = header["Column"]
                    elif "seq" in header_text:
                        seq_col = header["Column"]
                
                # If we found both columns, create pairs
                if pd_col is not None and seq_col is not None:
                    # Get data rows (exclude header)
                    data_rows = df[df["Row"] > 0]
                    
                    # Create pairs
                    for row_idx in data_rows["Row"].unique():
                        # Get PD and Seq values
                        pd_value = data_rows[(data_rows["Row"] == row_idx) & (data_rows["Column"] == pd_col)]["Content"].iloc[0] if not data_rows[(data_rows["Row"] == row_idx) & (data_rows["Column"] == pd_col)].empty else ""
                        seq_value = data_rows[(data_rows["Row"] == row_idx) & (data_rows["Column"] == seq_col)]["Content"].iloc[0] if not data_rows[(data_rows["Row"] == row_idx) & (data_rows["Column"] == seq_col)].empty else ""
                        
                        # Only create pair if at least one value exists
                        if pd_value or seq_value:
                            pair_item = {
                                "id": f"{file_name.replace('.', '_')}_table{table_idx + 1}_row{row_idx}_pair",
                                "documentId": file_name.replace(".", "_"),
                                "tableIndex": table_idx + 1,
                                "rowIndex": int(row_idx),
                                "ED_CODE": 35023,
                                "FULL_PD": pd_value,
                                "SEQUENCE_NUMBER": seq_value,
                                "FULL_PD_cleaned": digits_only(pd_value),
                                "SEQUENCE_NUMBER_cleaned": digits_only(seq_value)
                            }
                            pairs_container.upsert_item(pair_item)
            except Exception as e:
                logging.error(f"Error processing pairs for table {table_idx + 1}: {str(e)}")
        
        logging.info(f"Successfully processed document: {file_name}")
        return "Success"
    
    except Exception as e:
        logging.error(f"Error processing document: {str(e)}")
        
        # Update document status to "failed"
        try:
            doc_container = database.get_container_client(DOCUMENTS_CONTAINER)
            doc_container.upsert_item({
                "id": file_name.replace(".", "_"),
                "fileName": file_name,
                "status": "failed",
                "error": str(e),
                "processedDate": datetime.datetime.now().isoformat()
            })
        except Exception as inner_e:
            logging.error(f"Error updating document status: {str(inner_e)}")
        
        return "Error"

def get_word_confidence(pages, content):
    """Calculate confidence for a text span based on word confidences"""
    if not content:
        return None
    
    content_lower = content.lower()
    matching_words = []
    
    for page in pages:
        for word in page.words:
            if word.content.lower() in content_lower:
                matching_words.append(word)
    
    if matching_words:
        return sum(word.confidence for word in matching_words) / len(matching_words)
    
    return None

def digits_only(text):
    """Extract only digits from text"""
    if text is None:
        return ""
    return re.sub(r'[^0-9]', '', str(text))