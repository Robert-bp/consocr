# visualization.py
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def visualize_table_with_confidence(table_df, confidence_threshold=0.9):
    """Create a visual representation of a table with confidence highlighting"""
    # Apply styling based on confidence
    def style_by_confidence(val):
        if pd.isna(val) or val == 'N/A':
            return 'background-color: lightgray'
        
        try:
            confidence = float(val)
            if confidence < confidence_threshold:
                return 'background-color: #ffcccc'  # Light red for low confidence
            elif confidence >= 0.98:
                return 'background-color: #ccffcc'  # Light green for high confidence
            else:
                return 'background-color: #ffffcc'  # Light yellow for medium confidence
        except (ValueError, TypeError):
            return 'background-color: lightgray'
    
    # Apply styling to the dataframe
    styled_df = table_df.style.applymap(style_by_confidence, subset=['Confidence'])
    
    return styled_df

def create_confidence_heatmap(table_df):
    """Create a heatmap visualization of confidence scores in a table"""
    if 'Confidence' not in table_df.columns:
        return None
    
    # Create a pivot table with row/column indices and confidence values
    pivot_df = table_df.pivot(index='Row', columns='Column', values='Confidence')
    
    # Create a figure and axis
    fig, ax = plt.figure(figsize=(10, 6)), plt.axes()
    
    # Create the heatmap
    sns.heatmap(pivot_df, annot=True, cmap='RdYlGn', ax=ax, 
                vmin=0.0, vmax=1.0, linewidths=0.5)
    
    ax.set_title('Confidence Scores by Cell Position')
    
    return fig