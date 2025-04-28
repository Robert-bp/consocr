import pandas as pd

# Set options to avoid truncation
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # Use full width of the terminal
pd.set_option('display.max_colwidth', None)  # Don't truncate column contents

# Load your exports
docs = pd.read_csv("docs.csv", usecols=["documentId", "tableCount", "errorMessage"])
cells = pd.read_csv("cells.csv", usecols=["documentId"])

# How many cells per document?
cell_counts = cells.groupby("documentId").size().rename("cellCount").reset_index()

# Merge with metadata
summary = docs.merge(cell_counts, on="documentId", how="left").fillna({"cellCount": 0})

# Docs that thought they had tables but ended up with zero cells
problematic = summary[(summary.tableCount > 0) & (summary.cellCount == 0)]

print(f"Total documents in metadata: {len(docs)}")
print(f"Documents with ≥1 cell extracted: {len(summary[summary.cellCount > 0])}")
print(f"Documents with tables but NO cells: {len(problematic)}")
print("\nThese documents had tableCount>0 but no cells extracted:\n")
print(problematic.to_string(index=False))

# Optionally save the problematic list to a file for reference
problematic.to_csv("problematic_docs.csv", index=False)