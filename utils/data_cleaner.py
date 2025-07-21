# utils/data_cleaner.py
"""
Data cleaning utilities for handling special characters, CLOB data, and export formatting.
"""
import pandas as pd
import numpy as np
import re
from typing import List, Dict, Any, Optional
from utils.logger import logger

class DataCleaner:
    """Utility class for cleaning and preprocessing data for export."""
    
    def __init__(self):
        """Initialize data cleaner."""
        self.max_excel_cell_length = 32767  # Excel cell character limit
        self.max_csv_field_length = 1000000  # Practical CSV field limit
    
    def clean_data_for_export(self, df: pd.DataFrame, export_format: str = 'excel') -> pd.DataFrame:
        """
        Clean DataFrame for CSV/Excel export handling special characters and CLOB data.
        
        Args:
            df: Input DataFrame
            export_format: Target export format ('csv' or 'excel')
            
        Returns:
            Cleaned DataFrame
        """
        logger.info(f"Cleaning data for {export_format} export - {len(df)} rows")
        
        cleaned_df = df.copy()
        
        # Clean column names
        cleaned_df.columns = self._clean_column_names(cleaned_df.columns.tolist())
        
        # Process each column
        for column in cleaned_df.columns:
            logger.debug(f"Cleaning column: {column}")
            cleaned_df[column] = cleaned_df[column].apply(
                lambda x: self._clean_cell_value(x, export_format)
            )
        
        # Handle NaN values
        cleaned_df = self._handle_nan_values(cleaned_df)
        
        logger.info(f"Data cleaning completed")
        return cleaned_df
    
    def handle_clob_data(self, df: pd.DataFrame, clob_columns: List[str] = None) -> pd.DataFrame:
        """
        Handle CLOB data types for Excel export.
        
        Args:
            df: Input DataFrame
            clob_columns: List of column names containing CLOB data
            
        Returns:
            DataFrame with processed CLOB data
        """
        if not clob_columns:
            # Auto-detect potential CLOB columns
            clob_columns = self._detect_clob_columns(df)
        
        logger.info(f"Processing CLOB columns: {clob_columns}")
        
        processed_df = df.copy()
        
        for column in clob_columns:
            if column in processed_df.columns:
                logger.debug(f"Processing CLOB column: {column}")
                processed_df[column] = processed_df[column].apply(
                    self._truncate_clob_content
                )
        
        return processed_df
    
    def _clean_column_names(self, columns: List[str]) -> List[str]:
        """Clean and normalize column names."""
        cleaned_columns = []
        
        for col in columns:
            # Convert to string and strip whitespace
            clean_col = str(col).strip()
            
            # Replace special characters with underscores
            clean_col = re.sub(r'[^\w\s-]', '_', clean_col)
            
            # Replace spaces with underscores
            clean_col = re.sub(r'\s+', '_', clean_col)
            
            # Remove multiple consecutive underscores
            clean_col = re.sub(r'_+', '_', clean_col)
            
            # Remove leading/trailing underscores
            clean_col = clean_col.strip('_')
            
            # Ensure column name is not empty
            if not clean_col:
                clean_col = f"column_{len(cleaned_columns)}"
            
            cleaned_columns.append(clean_col.lower())
        
        return cleaned_columns
    
    def _clean_cell_value(self, value: Any, export_format: str) -> Any:
        """Clean individual cell value."""
        if pd.isna(value) or value is None:
            return value
        
        if isinstance(value, str):
            # Remove null characters
            cleaned_value = value.replace('\x00', '')
            
            # Handle line breaks based on export format
            if export_format == 'csv':
                cleaned_value = cleaned_value.replace('\n', ' ').replace('\r', ' ')
                cleaned_value = re.sub(r'\s+', ' ', cleaned_value)
            else:
                cleaned_value = cleaned_value.replace('\r\n', '\n').replace('\r', '\n')
            
            # Remove or escape problematic characters for CSV
            if export_format == 'csv':
                cleaned_value = cleaned_value.replace('"', '""')
            
            # Truncate if too long
            max_length = (self.max_excel_cell_length if export_format == 'excel' 
                         else self.max_csv_field_length)
            
            if len(cleaned_value) > max_length:
                cleaned_value = cleaned_value[:max_length-3] + '...'
                logger.debug(f"Truncated long text value to {max_length} characters")
            
            return cleaned_value.strip()
        
        return value
    
    def _handle_nan_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle NaN values in DataFrame."""
        for column in df.columns:
            if df[column].dtype == 'object':
                df[column] = df[column].fillna('')
        return df
    
    def _detect_clob_columns(self, df: pd.DataFrame, min_length: int = 4000) -> List[str]:
        """Auto-detect columns that might contain CLOB data."""
        clob_columns = []
        
        for column in df.columns:
            if df[column].dtype == 'object':
                string_lengths = df[column].dropna().astype(str).str.len()
                if len(string_lengths) > 0 and string_lengths.mean() > min_length:
                    clob_columns.append(column)
                    logger.debug(f"Detected potential CLOB column: {column}")
        
        return clob_columns
    
    def _truncate_clob_content(self, content: Any) -> str:
        """Truncate CLOB content for export."""
        if pd.isna(content) or content is None:
            return ''
        
        content_str = str(content)
        
        if len(content_str) > self.max_excel_cell_length:
            truncated = content_str[:self.max_excel_cell_length-50] + '\n[Content truncated...]'
            logger.debug(f"Truncated CLOB content from {len(content_str)} characters")
            return truncated
        
        return content_str
    
    def remove_duplicates_with_logging(self, df: pd.DataFrame, primary_key: str) -> tuple:
        """Remove duplicates and return both clean data and duplicates DataFrame."""
        logger.info(f"Checking for duplicates based on primary key: {primary_key}")
        
        duplicate_mask = df.duplicated(subset=[primary_key], keep='first')
        duplicates_df = df[duplicate_mask].copy()
        clean_df = df[~duplicate_mask].copy()
        
        logger.info(f"Found {len(duplicates_df)} duplicate records")
        logger.info(f"Clean dataset contains {len(clean_df)} records")
        
        return clean_df, duplicates_df

# Global data cleaner instance
data_cleaner = DataCleaner()


