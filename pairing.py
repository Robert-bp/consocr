#!/usr/bin/env python3
from dotenv import load_dotenv
import os
import pandas as pd
from azure.cosmos import CosmosClient

def dump(db, name):
    cont = db.get_container_client(name)
    df = pd.json_normalize(list(cont.query_items(
        query="SELECT * FROM c", enable_cross_partition_query=True
    )))
    print(f"{name} columns:", df.columns.tolist())
    return df

def main():
    load_dotenv()
    conn_str = os.environ["COSMOS_DB_CONNECTION_STRING"]
    client = CosmosClient.from_connection_string(conn_str)
    db = client.get_database_client("cangodb")

    df_docs   = dump(db, "document_metadata")
    df_cells  = dump(db, "table_cells")
    df_tables = dump(db, "extracted_tables")
    df_pairs  = dump(db, "pd_seq_pairs")

    # build lookup with both fields
    df_meta = (
        df_docs[["id","riding","blobName"]]
          .rename(columns={"id":"documentId"})
    )
    print("meta lookup sample:\n", df_meta.head(), sep="")

    # merge into the detail tables
    df_cells  = df_cells.merge(df_meta, on="documentId", how="left")
    df_tables = df_tables.merge(df_meta, on="documentId", how="left")
    df_pairs  = df_pairs.merge(df_meta, on="documentId", how="left")

    # write out two CSVs for you to join externally
    df_meta.to_csv("meta_lookup.csv", index=False)
    df_cells[[
        "documentId","pageNumber","tableId","row","column","content",
        "riding","blobName"
    ]].to_csv("cells_with_meta.csv", index=False)

    print("→ Wrote meta_lookup.csv and cells_with_meta.csv")

if __name__ == "__main__":
    main()
