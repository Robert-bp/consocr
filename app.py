import streamlit as st
import os
import uuid
import time
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.cosmos import CosmosClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Azure configuration
AZURE_STORAGE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
COSMOS_CONNECTION_STRING = os.environ.get("COSMOS_DB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = "document-uploads"
DATABASE_NAME = "cangodb"
DOCUMENTS_CONTAINER = "document_metadata"
# Add this to your database configuration
CELLS_CONTAINER = "table_cells"

# Configure Streamlit page
st.set_page_config(page_title="Document Upload", page_icon="📄")

def get_blob_service_client():
    """Create a blob service client from the connection string"""
    return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

def get_cosmos_container():
    """Get Cosmos DB container for document metadata"""
    cosmos_client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
    database = cosmos_client.get_database_client(DATABASE_NAME)
    return database.get_container_client(DOCUMENTS_CONTAINER)

def upload_document(
    file_obj, file_name, content_type, user_id="default", riding_tag="unassigned"
):
    doc_id = str(uuid.uuid4())
    timestamp = int(time.time())
    riding_slug = riding_tag.lower().replace(" ", "-")
    # now include the riding in the path if you like:
    blob_name = f"{user_id}/{riding_slug}/{timestamp}_{doc_id}_{file_name}"

    
    # Get blob client
    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
    
    # Create container if it doesn't exist
    try:
        container_client.create_container()
    except:
        # Container already exists
        pass
    
    blob_client = container_client.get_blob_client(blob_name)
    
    # Set the content type
    content_settings = ContentSettings(content_type=content_type)
    
    # Upload file to blob storage
    blob_client.upload_blob(
            file_obj,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type),
            metadata={
                "doc_id": doc_id,
                "original_filename": file_name,
                "upload_timestamp": str(timestamp),
                "user_id": user_id,
                "riding": riding_tag,
            },
        )

    
    # Create document record in Cosmos DB
    create_document_record(
        doc_id,
        blob_name,
        file_name,
        content_type,
        user_id,
        riding_tag
    )
    return doc_id, blob_name
    
    return doc_id, blob_name

def create_document_record(
    doc_id, blob_name, original_filename, content_type, user_id, riding_tag
):
    container = get_cosmos_container()
    document = {
        "id": doc_id,
        "documentId": doc_id,
        "blobName": blob_name,
        "originalFilename": original_filename,
        "riding": riding_tag,        # now stored in Cosmos too
        "contentType": content_type,
        "userId": user_id,
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
    
    # Store in Cosmos DB
    container.upsert_item(document)
    return doc_id

def validate_file(uploaded_file):
    """Validate file type and size"""
    # Check file type
    file_type = uploaded_file.type
    valid_types = [
        'application/pdf',  # PDF
        'image/jpeg',       # JPEG
        'image/png',        # PNG
        'image/tiff',       # TIFF
        'image/bmp'         # BMP
    ]
    
    if file_type not in valid_types:
        return False, f"Invalid file type: {file_type}. Please upload PDF or image files."
    
    # Check file size (limit to 10MB)
    size_limit = 100 * 1024 * 1024  # 10MB in bytes
    if uploaded_file.size > size_limit:
        return False, f"File too large: {uploaded_file.size/1024/1024:.1f}MB. Maximum size is 10MB."
    
    return True, "File is valid"

def document_upload_ui():
    """Streamlit UI for multi-file upload"""
    st.title("Document Upload")

    st.write(
        """
        Upload one **or many** PDFs / images.  
        Each file is streamed to Azure Blob Storage and queued for OCR.
        """
    )

    # document_upload_ui()

    ridings = ["Fredericton", "Moncton", "Saint John", "Mississauga"]
    selected_riding = st.selectbox("Tag these uploads with a riding:", ridings)


    # 1️⃣ pick several files at once
    uploaded_files = st.file_uploader(
        "Choose PDF or image files",
        type=["pdf", "png", "jpg", "jpeg", "tiff", "bmp"],
        accept_multiple_files=True
    )

    if not uploaded_files:
        return  # nothing chosen yet

    # 2️⃣ get the current user *once* (replace with your auth later)
    user_id = st.session_state.get("user_id", "default_user")

    # 3️⃣ iterate through the selection
    for f in uploaded_files:
        st.markdown(f"##### 📄 {f.name}")

        # — validation —
        ok, msg = validate_file(f)
        if not ok:
            st.error(msg)
            continue

        # — upload with a progress spinner —
        with st.spinner("Uploading..."):
            try:
                doc_id, _ = upload_document(
                    f,          # pass the stream directly
                    f.name,
                    f.type,
                    user_id,
                    riding_tag=selected_riding
                )
            except Exception as e:
                st.error(f"Upload failed: {e}")
                continue

        # — success feedback —
        st.success(f"Uploaded ✔️  Document ID `{doc_id}`")
        st.code(doc_id)

        # stash for later pages
        st.session_state.setdefault("uploaded_docs", []).append(doc_id)


# Main function
def main():
    # Check for required environment variables
    if not AZURE_STORAGE_CONNECTION_STRING:
        st.error("Azure Storage connection string not found. Please set the AZURE_STORAGE_CONNECTION_STRING environment variable.")
        return
    
    if not COSMOS_CONNECTION_STRING:
        st.error("Cosmos DB connection string not found. Please set the COSMOS_DB_CONNECTION_STRING environment variable.")
        return
    
    # Render the document upload UI
    document_upload_ui()

if __name__ == "__main__":
    main()