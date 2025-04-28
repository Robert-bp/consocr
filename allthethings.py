from azure.cosmos import CosmosClient
import pandas as pd

# 1) Connect to your Cosmos account
COSMOS_CONNECTION_STRING = "<your-cosmos-connection-string>"
client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
db = client.get_database_client("cangodb")

# 2) Function to export any container to a DataFrame + CSV
def export_container(container_name, csv_name):
    container = db.get_container_client(container_name)
    items = list(container.query_items(
        query="SELECT * FROM c",
        enable_cross_partition_query=True
    ))
    if not items:
        print(f"No items found in {container_name}")
        return pd.DataFrame()
    df = pd.json_normalize(items)
    df.to_csv(csv_name, index=False)
    print(f"Exported {len(df)} rows from {container_name} → {csv_name}")
    return df

# 3) Export your tables, pairs, cells, and metadata
tables_df     = export_container("extracted_tables",    "all_tables.csv")
pairs_df      = export_container("pd_seq_pairs",        "all_pd_seq_pairs.csv")
cells_df      = export_container("table_cells",         "all_cells.csv")
metadata_df   = export_container("document_metadata",   "all_documents.csv")

# 4) (Optional) Join them together on documentId
merged = tables_df.merge(metadata_df[["id","originalFilename","riding","status"]],
                         left_on="documentId", right_on="id",
                         how="left", suffixes=("","_doc"))
merged.to_csv("tables_with_doc_meta.csv", index=False)
print("Wrote tables_with_doc_meta.csv with", len(merged), "rows")
