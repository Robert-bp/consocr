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

def upload_document(file_obj, file_name, content_type, user_id="default"):
    """
    Upload a document to Azure Blob Storage
    Returns: (document_id, blob_name)
    """
    # Generate a unique document ID
    doc_id = str(uuid.uuid4())
    
    # Create a unique blob name to avoid collisions
    timestamp = int(time.time())
    blob_name = f"{user_id}/{timestamp}_{doc_id}_{file_name}"
    
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
        content_settings=content_settings,
        metadata={
            "doc_id": doc_id,
            "original_filename": file_name,
            "upload_timestamp": str(timestamp),
            "user_id": user_id
        }
    )
    
    # Create document record in Cosmos DB
    create_document_record(doc_id, blob_name, file_name, content_type, user_id)
    
    return doc_id, blob_name

def create_document_record(doc_id, blob_name, original_filename, content_type, user_id):
    """Create a record in Cosmos DB with status 'uploaded'"""
    container = get_cosmos_container()
    
    # Create the document metadata record
    document = {
        "id": doc_id,
        "documentId": doc_id,
        "blobName": blob_name,
        "originalFilename": original_filename,
        "contentType": content_type,
        "userId": user_id,
        "status": "queued",  # Initial status after upload
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
    size_limit = 10 * 1024 * 1024  # 10MB in bytes
    if uploaded_file.size > size_limit:
        return False, f"File too large: {uploaded_file.size/1024/1024:.1f}MB. Maximum size is 10MB."
    
    return True, "File is valid"

def document_upload_ui():
    """Streamlit UI for document upload"""
    st.title("Document Upload")
    
    st.write("""
    Upload PDF documents or images for processing. 
    Files will be analyzed using Azure Document Intelligence.
    """)
    
    # File uploader widget
    uploaded_file = st.file_uploader("Choose a file", type=["pdf", "png", "jpg", "jpeg", "tiff", "bmp"])
    
    if uploaded_file is not None:
        # Display file info
        st.write(f"Filename: {uploaded_file.name}")
        st.write(f"File size: {uploaded_file.size/1024:.1f} KB")
        st.write(f"File type: {uploaded_file.type}")
        
        # Add a process button
        if st.button("Upload Document"):
            # Validate the file
            is_valid, message = validate_file(uploaded_file)
            
            if not is_valid:
                st.error(message)
            else:
                # Show progress
                with st.spinner("Uploading document..."):
                    try:
                        # Get current user ID (you would implement your own user management)
                        user_id = st.session_state.get("user_id", "default_user")
                        
                        # Upload the file
                        doc_id, blob_name = upload_document(
                            uploaded_file.getvalue(),
                            uploaded_file.name,
                            uploaded_file.type,
                            user_id
                        )
                        
                        # Success message
                        st.success(f"Document uploaded successfully! Document ID: {doc_id}")
                        st.info("Your document has been uploaded and is ready for processing.")
                        
                        # Store doc_id in session state for later reference
                        if "uploaded_docs" not in st.session_state:
                            st.session_state.uploaded_docs = []
                        st.session_state.uploaded_docs.append(doc_id)
                        
                        # Display the document ID in a format that's easy to copy
                        st.code(doc_id)
                        
                    except Exception as e:
                        st.error(f"Error uploading document: {str(e)}")
                        import traceback
                        st.code(traceback.format_exc())

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