# document_processor.py
import pandas as pd
import re

def extract_tables_from_result(analysis_result, with_confidence=True):
    """Extract tables from Azure Document Intelligence result"""
    all_tables = []
    
    for i, table in enumerate(analysis_result.tables):
        # Create DataFrame from table
        table_data = []
        for cell in table.cells:
            row_idx = cell.row_index
            col_idx = cell.column_index
            content = cell.content
            
            cell_info = {
                'Row': row_idx,
                'Column': col_idx,
                'Content': content,
            }
            
            # Add confidence if requested
            if with_confidence:
                # Find cell confidence from words
                confidence = None
                for page in analysis_result.pages:
                    confidence = get_word_confidence(page.words, content)
                    if confidence is not None:
                        break
                
                cell_info['Confidence'] = confidence
            
            table_data.append(cell_info)
        
        # Create DataFrame
        df = pd.DataFrame(table_data)
        df['Table'] = i + 1
        
        all_tables.append(df)
    
    # Combine all tables if there are any
    if all_tables:
        return pd.concat(all_tables, ignore_index=True)
    else:
        return pd.DataFrame()

def get_word_confidence(words, content):
    """Find the confidence of words in a cell content"""
    if not content or not words:
        return None
    
    # Simple average of word confidences that match the content
    content_lower = content.lower()
    matching_words = [word for word in words if word.content.lower() in content_lower]
    
    if matching_words:
        return sum(word.confidence for word in matching_words) / len(matching_words)
    return None

def create_paired_csv(table_df, filename_base):
    """
    Create a CSV with paired columns (for PD and sequence numbers)
    """
    # Get only data rows (skip header row 0)
    data_df = table_df[table_df['Row'] > 0].copy()
    
    # Convert to wide format for pairing
    try:
        wide_df = data_df.pivot(index='Row', columns='Column', values='Content')
    except:
        # If pivot fails, return empty DataFrame
        return pd.DataFrame()
    
    # Create pairs from columns
    pairs = []
    columns = sorted(wide_df.columns.tolist())
    
    # Group columns into pairs (0,1), (2,3), etc.
    for i in range(0, len(columns), 2):
        if i+1 < len(columns):  # Make sure we have a pair
            col1 = columns[i]
            col2 = columns[i+1]
            
            for row in wide_df.index:
                val1 = wide_df.at[row, col1] if row in wide_df.index and col1 in wide_df.columns and pd.notna(wide_df.at[row, col1]) else ""
                val2 = wide_df.at[row, col2] if row in wide_df.index and col2 in wide_df.columns and pd.notna(wide_df.at[row, col2]) else ""
                
                # Only include if at least one value exists
                if val1 or val2:
                    pairs.append({
                        "ED_CODE": 35023,
                        "FULL_PD": val1,
                        "SEQUENCE_NUMBER": val2,
                        "ROW": int(row)
                    })
    
    # Create DataFrame from pairs
    pairs_df = pd.DataFrame(pairs)
    
    # Save to CSV
    import os
    os.makedirs("results", exist_ok=True)
    output_path = os.path.join("results", f"{filename_base}_paired.csv")
    
    if not pairs_df.empty:
        pairs_df.to_csv(output_path, index=False)
    
    return pairs_df

def digits_only(text):
    """Extract only digits from text"""
    if pd.isna(text) or text is None:
        return ""
    return re.sub(r'[^0-9]', '', str(text))

def clean_table_data(table_df):
    """Clean the table data for better analysis"""
    # Create a copy to avoid modifying the original
    df = table_df.copy()
    
    # Add a CleanedContent column with only digits
    df['CleanedContent'] = df['Content'].apply(digits_only)
    
    return df