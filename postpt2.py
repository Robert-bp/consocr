#!/usr/bin/env python3
import pandas as pd

def main():
    # 1) load your table_cells export
    df = pd.read_csv("cells.csv", dtype={"confidence": float})
    
    # 2) build a grouping key: drop the trailing "_colX"
    df["base_key"] = df["id"].str.rsplit("_col", n=1).str[0]
    
    # 3) pull out the column index for sorting
    df["col_index"] = (
        df["id"]
          .str.rsplit("_col", n=1)
          .str[1]
          .astype(int)
    )
    
    # 4) for each group, sort by col_index and pair up
    pairs = []
    for key, group in df.groupby("base_key", sort=False):
        g = group.sort_values("col_index").reset_index(drop=True)
        # must have an even number of cols to pair
        n = (len(g) // 2) * 2
        for i in range(0, n, 2):
            left = g.loc[i]
            right = g.loc[i+1]
            pairs.append({
                "documentId":     left["documentId"],
                "pageNumber":     left["pageNumber"],
                "tableId":        left["tableId"],
                "row":            left["row"],
                "left_content":   left["content"],
                "right_content":  right["content"],
                "left_confidence":  left["confidence"],
                "right_confidence": right["confidence"],
            })
    
    df_pairs = pd.DataFrame(pairs)
    
    # 5) drop any pair where either confidence < 0.90
    df_pairs = df_pairs[
        (df_pairs.left_confidence  >= 0.90) &
        (df_pairs.right_confidence >= 0.90)
    ].reset_index(drop=True)
    
    # 6) save
    df_pairs.to_csv("column_pairs.csv", index=False)
    print(f"→ Wrote {len(df_pairs)} confident column-pairs to column_pairs.csv")

if __name__ == "__main__":
    main()
