import streamlit as st
import os
import json
import time
import io
import pandas as pd
import numpy as np
from PIL import Image
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

#teehe


# Load environment variables
load_dotenv()
REVIEWER_POOL = [r.strip() for r in os.getenv("REVIEWER_POOL","").split(",") if r.strip()]

# ── pull creds from st.secrets when running on Streamlit Cloud ─────────
if "azure" in st.secrets:
    os.environ["AZURE_STORAGE_CONNECTION_STRING"] = st.secrets["azure"]["AZURE_STORAGE_CONNECTION_STRING"]
    os.environ["COSMOS_DB_CONNECTION_STRING"]     = st.secrets["azure"]["COSMOS_DB_CONNECTION_STRING"]
if "document_intelligence" in st.secrets:
    os.environ["DOCUMENT_INTELLIGENCE_ENDPOINT"]  = st.secrets["document_intelligence"]["DOCUMENT_INTELLIGENCE_ENDPOINT"]
    os.environ["DOCUMENT_INTELLIGENCE_KEY"]       = st.secrets["document_intelligence"]["DOCUMENT_INTELLIGENCE_KEY"]
if "openai" in st.secrets:
    os.environ["OPENAI_API_KEY"]                  = st.secrets["openai"]["OPENAI_API_KEY"]

# ── NOW turn env-vars into real Python constants ───────────────────────
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
COSMOS_CONNECTION_STRING        = os.getenv("COSMOS_DB_CONNECTION_STRING")
DOC_ENDPOINT                    = os.getenv("DOCUMENT_INTELLIGENCE_ENDPOINT")
DOC_KEY                         = os.getenv("DOCUMENT_INTELLIGENCE_KEY")


RESULTS_CONTAINER_NAME = "processed-results"

# Database configuration
DATABASE_NAME = "cangodb"
DOCUMENTS_CONTAINER = "document_metadata"
TABLES_CONTAINER = "extracted_tables"
PAIRS_CONTAINER = "pd_seq_pairs"
CELLS_CONTAINER = "table_cells"

# Configure Streamlit page
st.set_page_config(
    page_title="Document Review", 
    page_icon="📄", 
    layout="wide"
)

def get_cosmos_container(container_name):
    """Get Cosmos DB container"""
    cosmos_client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
    database = cosmos_client.get_database_client(DATABASE_NAME)
    return database.get_container_client(container_name)

def get_blob_service_client():
    """Create a blob service client from the connection string"""
    return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

def get_documents_for_review(reviewer_id=None, limit=100):
    """Get list of documents that need review"""
    container = get_cosmos_container(DOCUMENTS_CONTAINER)
    
    # Query for documents with 'ready_for_review' status
    query = "SELECT * FROM c WHERE c.status = 'ready_for_review'"
    
    # ❌ REMOVE reviewer filtering
    # (We don't add anything based on reviewer_id)

    query += " ORDER BY c.createdAt ASC"
    
    if limit:
        query += f" OFFSET 0 LIMIT {limit}"
    
    return list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

def get_reviewed_documents(reviewer_id=None, limit=100):
    """Get list of documents that have been reviewed"""
    container = get_cosmos_container(DOCUMENTS_CONTAINER)
    
    # Query for documents with 'reviewed' status
    query = "SELECT * FROM c WHERE c.status = 'reviewed'"
    
    # ❌ REMOVE reviewer filtering
    # (We don't add anything based on reviewer_id)

    query += " ORDER BY c.reviewCompletedAt DESC"
    
    if limit:
        query += f" OFFSET 0 LIMIT {limit}"
    
    return list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))


def get_document_tables(doc_id, page_number=None):
    """Get tables for a specific document"""
    container = get_cosmos_container(TABLES_CONTAINER)
    
    # Query for tables in this document
    query = f"SELECT * FROM c WHERE c.documentId = '{doc_id}'"
    
    # Filter by page if specified
    if page_number:
        query += f" AND c.pageNumber = {page_number}"
    
    return list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

def get_document_pairs(doc_id, page_number=None):
    """Get PD/Seq pairs for a specific document"""
    container = get_cosmos_container(PAIRS_CONTAINER)
    
    # Query for pairs in this document
    query = f"SELECT * FROM c WHERE c.documentId = '{doc_id}'"
    
    # Filter by page if specified
    if page_number:
        query += f" AND c.pageNumber = {page_number}"
    
    return list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

# ────────────────────────────────────────────────────────────────────────────────
#  FULL FUNCTION  –  replace the old display_cell_review in review_app.py
# ────────────────────────────────────────────────────────────────────────────────
def display_cell_review(doc_id, page_number, table_id):
    """
    Show every cell that still needs review for this table.
    Includes the crop thumbnail (if saved) and approve / reject UI.
    """
    reviewer = st.session_state.get("reviewer_id")   # may be None on first load

    # 1️⃣  fetch cells assigned to the reviewer (or un-assigned)
    query = f"""
        SELECT * FROM c
        WHERE  c.documentId = '{doc_id}'
        AND  c.pageNumber = {page_number}
        AND  c.tableId    = {table_id}
        AND  c.status     = 'needs_review'
        """
    cells = list(
        get_cosmos_container(CELLS_CONTAINER).query_items(
            query=query, enable_cross_partition_query=True
        )
    )

    if not cells:
        st.success("All cells in this table have been approved!")
        return

    st.write(f"**{len(cells)} cells need review in this table**")

    # 2️⃣  one mini-form per cell
    for cell in cells:
        with st.container():
            col_img, col_meta, col_edit, col_btn = st.columns([1, 2, 3, 1])

            # thumbnail
            with col_img:
                if cell.get("blobName"):
                    try:
                        bytes_ = download_blob(RESULTS_CONTAINER_NAME, cell["blobName"])
                        st.image(Image.open(io.BytesIO(bytes_)), width=120)
                    except Exception:
                        st.warning("image not found", icon="⚠️")

            # meta info
            with col_meta:
                st.write(f"Row **{cell['row']}**, Col **{cell['column']}**")
                st.write(f"Confidence {cell['confidence']:.2f}")

            # editable text
            with col_edit:
                new_content = st.text_input(
                    "Content",
                    value = cell["content"],
                    key   = f"cell_{cell['id']}",
                )

            # action buttons
            with col_btn:
                if st.button("Approve", key=f"approve_{cell['id']}"):
                    update_cell_status(cell["id"], "approved", new_content)
                    st.rerun()
                if st.button("Reject", key=f"reject_{cell['id']}"):
                    update_cell_status(cell["id"], "rejected", new_content)
                    st.rerun()


def download_blob(container_name, blob_name):
    """Download a blob from Azure Blob Storage"""
    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    
    return blob_client.download_blob().readall()

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

def update_table_cell(table_id, content, cleaned_content=None):
    """Update the content of a table cell in Cosmos DB"""
    container = get_cosmos_container(TABLES_CONTAINER)
    
    # Get the current table cell
    query = f"SELECT * FROM c WHERE c.id = '{table_id}'"
    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    
    if items:
        # Update the existing cell
        cell = items[0]
        cell["Content"] = content
        
        # Update cleaned content if provided, otherwise generate it
        if cleaned_content is not None:
            cell["CleanedContent"] = cleaned_content
        else:
            import re
            cell["CleanedContent"] = re.sub(r'[^0-9]', '', str(content))
        
        container.replace_item(cell["id"], cell)
        return True
    
    return False

def update_cell_status(cell_id, status, content=None):
    """Update status and optionally content of a cell"""
    container = get_cosmos_container(CELLS_CONTAINER)
    
    # Get the current cell
    query = f"SELECT * FROM c WHERE c.id = '{cell_id}'"
    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    
    if items:
        cell = items[0]
        cell["status"] = status
        
        if content is not None:
            cell["content"] = content
            # Update cleaned content
            import re
            cell["cleanedContent"] = re.sub(r'[^0-9]', '', str(content))
        
        cell["reviewedAt"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        cell["reviewerId"] = st.session_state.get("reviewer_id", "default_reviewer")
        
        container.replace_item(cell["id"], cell)
        return True
    
    return False

def update_pair(pair_id, pd_value=None, seq_value=None):
    """Update PD/Seq pair in Cosmos DB"""
    container = get_cosmos_container(PAIRS_CONTAINER)
    
    # Get the current pair
    query = f"SELECT * FROM c WHERE c.id = '{pair_id}'"
    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    
    if items:
        # Update the existing pair
        pair = items[0]
        
        if pd_value is not None:
            pair["FULL_PD"] = pd_value
            import re
            pair["FULL_PD_cleaned"] = re.sub(r'[^0-9]', '', str(pd_value))
        
        if seq_value is not None:
            pair["SEQUENCE_NUMBER"] = seq_value
            import re
            pair["SEQUENCE_NUMBER_cleaned"] = re.sub(r'[^0-9]', '', str(seq_value))
        
        container.replace_item(pair["id"], pair)
        return True
    
    return False

def claim_document_for_review(doc_id, reviewer_id):
    """Claim a document for review by a specific reviewer"""
    return update_document_status(
        doc_id, 
        "in_review",
        reviewerId=reviewer_id,
        reviewStartedAt=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    )

def mark_document_as_reviewed(doc_id):
    """Mark a document as fully reviewed"""
    return update_document_status(
        doc_id, 
        "reviewed",
        reviewCompletedAt=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    )

def check_document_completion(doc_id):
    """Check if all cells in a document have been reviewed"""
    cells_container = get_cosmos_container(CELLS_CONTAINER)
    
    # Count cells that still need review
    query = f"SELECT VALUE COUNT(1) FROM c WHERE c.documentId = '{doc_id}' AND c.status = 'needs_review'"
    pending_count = list(cells_container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))[0]
    
    return pending_count == 0

def display_document_image(doc_id, page_blob_name):
    """Display the document image for review"""
    if not page_blob_name:
        st.warning("No image available for this document.")
        return
    
    try:
        # Download the image from blob storage
        image_bytes = download_blob(RESULTS_CONTAINER_NAME, page_blob_name)
        
        # Display the image
        image = Image.open(io.BytesIO(image_bytes))
        st.image(image, caption=f"Document {doc_id} - Page {page_blob_name.split('/')[-1]}")
    except Exception as e:
        st.error(f"Error displaying image: {str(e)}")

def tables_to_dataframe(tables_data):
    """Convert table data from Cosmos DB to a DataFrame"""
    if not tables_data:
        return {}
    
    # Group by table
    tables_by_id = {}
    for item in tables_data:
        table_id = item.get("Table")
        if table_id not in tables_by_id:
            tables_by_id[table_id] = []
        tables_by_id[table_id].append(item)
    
    # Convert each table to a DataFrame
    dataframes = {}
    for table_id, cells in tables_by_id.items():
        df_data = []
        for cell in cells:
            df_data.append({
                "Row": cell.get("Row"),
                "Column": cell.get("Column"),
                "Content": cell.get("Content"),
                "Confidence": cell.get("Confidence"),
                "CleanedContent": cell.get("CleanedContent"),
                "id": cell.get("id")
            })
        
        # Create DataFrame and pivot to make it look like a table
        df = pd.DataFrame(df_data)
        pivot_df = df.pivot(index="Row", columns="Column", values=["Content", "Confidence", "id"])
        
        dataframes[table_id] = {
            "df": df,
            "pivot": pivot_df
        }
    
    return dataframes

def pairs_to_dataframe(pairs_data):
    """Convert pairs data from Cosmos DB to a DataFrame"""
    if not pairs_data:
        return pd.DataFrame()
    
    # Extract relevant fields
    df_data = []
    for pair in pairs_data:
        df_data.append({
            "id": pair.get("id"),
            "FULL_PD": pair.get("FULL_PD"),
            "SEQUENCE_NUMBER": pair.get("SEQUENCE_NUMBER"),
            "FULL_PD_cleaned": pair.get("FULL_PD_cleaned"),
            "SEQUENCE_NUMBER_cleaned": pair.get("SEQUENCE_NUMBER_cleaned"),
            "ED_CODE": pair.get("ED_CODE"),
            "Table": pair.get("tableIndex"),
            "Page": pair.get("pageNumber")
        })
    
    return pd.DataFrame(df_data)

def visualize_table_with_confidence(pivot_df, confidence_threshold=0.9):
    if pivot_df.empty:
        return pd.DataFrame()

    content_df = pivot_df["Content"]
    confidence_df = pivot_df["Confidence"]

    # Build a mask DataFrame where True = low confidence
    mask = confidence_df < confidence_threshold

    # Function to highlight cells
    def highlight_cells(x):
        return [
            'background-color: #ffcccc' if mask.at[x.name, col] else ''
            for col in x.index
        ]

    styled_df = content_df.style.apply(highlight_cells, axis=1)
    return styled_df



def document_review_ui():
    """Streamlit UI for document review"""
    st.title("Document Review Interface")
    
    # Sidebar for user ID and document selection
    # ── Sidebar ───────────────────────────────────────────────────────────
   # ── Sidebar ───────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Reviewer Information")

        # make sure we always have a reviewer id
        if "reviewer_id" not in st.session_state:
            st.session_state.reviewer_id = (
                REVIEWER_POOL[0] if REVIEWER_POOL else "default_reviewer"
            )

        # 1️⃣ choose from the pool
        pool_choice = st.selectbox(
            "Reviewer ID (choose)",
            options = REVIEWER_POOL if REVIEWER_POOL else ["default_reviewer"],
            index   = (
                REVIEWER_POOL.index(st.session_state.reviewer_id)
                if st.session_state.reviewer_id in REVIEWER_POOL else 0
            ),
        )

        # 2️⃣ …or type another one
        manual_input = st.text_input(
            "…or type another ID",
            value = st.session_state.reviewer_id,
        )

        # whichever field changed last wins
        if manual_input.strip() != st.session_state.reviewer_id:
            st.session_state.reviewer_id = manual_input.strip()
        elif pool_choice != st.session_state.reviewer_id:
            st.session_state.reviewer_id = pool_choice

        st.markdown(f"**Current reviewer:** `{st.session_state.reviewer_id}`")

        # ── Document selection ────────────────────────────────────────────
        st.header("Document Selection")

        # manual load
        manual_doc = st.text_input("Enter Document ID")
        if manual_doc and st.button("Load"):
            st.session_state.selected_doc_id = manual_doc.strip()
            st.session_state.page = "review"
            st.rerun()

        # list queued docs
        st.subheader("Documents Ready for Review")
        docs = get_documents_for_review(st.session_state.reviewer_id)
        if not docs:
            st.info("No documents available for review.")
        else:
            for d in docs:
                doc_id   = d["id"]
                filename = d.get("originalFilename", "Unknown")
                if st.button(f"{filename} ({doc_id[:8]}…)", key=f"doc_{doc_id}"):
                    if claim_document_for_review(doc_id, st.session_state.reviewer_id):
                        st.session_state.selected_doc_id = doc_id
                        st.session_state.page            = "review"
                        st.rerun()

    
    # Main content area
    if "page" not in st.session_state:
        st.session_state.page = "home"
    
    if st.session_state.page == "home":
        st.write("Select a document from the sidebar to begin review.")
    
    elif st.session_state.page == "review":
        # Get the selected document
        container = get_cosmos_container(DOCUMENTS_CONTAINER)
        query = f"SELECT * FROM c WHERE c.id = '{st.session_state.selected_doc_id}'"
        documents = list(container.query_items(query=query, enable_cross_partition_query=True))
        
        if not documents:
            st.error("Document not found.")
            return
        
        document = documents[0]
        doc_id = document.get("id")
        filename = document.get("originalFilename", "Unknown")
        
        # Display document information
        st.header(f"Reviewing: {filename}")
        st.write(f"Document ID: {doc_id}")
        st.write(f"Status: {document.get('status', 'Unknown')}")
        st.write(f"Page Count: {document.get('pageCount', 0)}")
        st.write(f"Table Count: {document.get('tableCount', 0)}")
        
        # Get page number from session state or set to 1
        if "current_page" not in st.session_state:
            st.session_state.current_page = 1
        
        # Page navigation
        page_count = document.get("pageCount", 1)
        col1, col2, col3 = st.columns([1, 3, 1])
        
        with col1:
            if st.button("Previous Page", disabled=st.session_state.current_page <= 1):
                st.session_state.current_page -= 1
                st.rerun()
        
        with col2:
            st.write(f"Page {st.session_state.current_page} of {page_count}")
        
        with col3:
            if st.button("Next Page", disabled=st.session_state.current_page >= page_count):
                st.session_state.current_page += 1
                st.rerun()
        
        # Get the page image
        page_blob_name = f"pages/{doc_id}/page_{st.session_state.current_page}.jpg"
        display_document_image(doc_id, page_blob_name)
        
        # Get tables for this page
        tables_data = get_document_tables(doc_id, st.session_state.current_page)
        table_dataframes = tables_to_dataframe(tables_data)
        
        # Get pairs for this page
        pairs_data = get_document_pairs(doc_id, st.session_state.current_page)
        pairs_df = pairs_to_dataframe(pairs_data)
        
        # Display tables
        if table_dataframes:
            st.header("Tables")
            
            for table_id, table_data in table_dataframes.items():
                with st.expander(f"Table {table_id}", expanded=True):
                    # Display the table with confidence highlighting
                    styled_df = visualize_table_with_confidence(table_data["pivot"], 0.9)
                    st.dataframe(styled_df)
                    
                    # Add cells that need review
                    st.subheader("Cells needing review")
                    with st.container():
                        display_cell_review(doc_id, st.session_state.current_page, table_id)

                    
                    # Add editing capability
                    st.subheader("Edit Table Cells")
                    
                    # Select cell to edit
                    df = table_data["df"]
                    row = st.selectbox("Row", sorted(df["Row"].unique()), key=f"row_{table_id}")
                    column = st.selectbox("Column", sorted(df["Column"].unique()), key=f"col_{table_id}")
                    
                    # Get current cell value
                    cell_data = df[(df["Row"] == row) & (df["Column"] == column)]
                    if not cell_data.empty:
                        cell_id = cell_data["id"].iloc[0]
                        current_value = cell_data["Content"].iloc[0]
                        
                        # Edit cell
                        new_value = st.text_input("Cell Content", value=current_value, key=f"cell_{cell_id}")
                        
                        if st.button("Update Cell", key=f"update_{cell_id}"):
                            if update_table_cell(cell_id, new_value):
                                st.success("Cell updated successfully!")
                                st.rerun()
                            else:
                                st.error("Failed to update cell.")
        
        # Display pairs
        if not pairs_df.empty:
            st.header("PD and Sequence Pairs")
            st.dataframe(pairs_df)
            
            # Edit pairs
            st.subheader("Edit Pairs")
            
            pair_id = st.selectbox("Select Pair", pairs_df["id"])
            pair_row = pairs_df[pairs_df["id"] == pair_id]
            
            if not pair_row.empty:
                pd_value = pair_row["FULL_PD"].iloc[0]
                seq_value = pair_row["SEQUENCE_NUMBER"].iloc[0]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    new_pd = st.text_input("PD Number", value=pd_value)
                
                with col2:
                    new_seq = st.text_input("Sequence Number", value=seq_value)
                
                if st.button("Update Pair"):
                    if update_pair(pair_id, new_pd, new_seq):
                        st.success("Pair updated successfully!")
                        st.rerun()
                    else:
                        st.error("Failed to update pair.")
        
        # Document completion buttons
        st.header("Document Review")
        
        if st.button("Mark Page as Reviewed"):
            # Here you could add additional logic to mark a specific page
            st.success(f"Page {st.session_state.current_page} marked as reviewed!")
        
        if st.button("Complete Document Review"):
            is_complete = check_document_completion(doc_id)
            if is_complete:
                if mark_document_as_reviewed(doc_id):
                    st.success("Document review completed!")
                    st.session_state.page = "home"
                    st.rerun()
                else:
                    st.error("Failed to complete document review.")
            else:
                st.warning("Document has cells that still need review.")

# Main function
def main():
    # Check for required environment variables
    if not AZURE_STORAGE_CONNECTION_STRING:
        st.error("Azure Storage connection string not found. Please set the AZURE_STORAGE_CONNECTION_STRING environment variable.")
        return
    
    if not COSMOS_CONNECTION_STRING:
        st.error("Cosmos DB connection string not found. Please set the COSMOS_DB_CONNECTION_STRING environment variable.")
        return
    
    # Render the document review UI
    document_review_ui()

if __name__ == "__main__":
    main()