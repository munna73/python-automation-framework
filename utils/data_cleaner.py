# utils/data_cleaner.py
"""
Data cleaning utilities for handling special characters, CLOB data, and export formatting.
"""
import pandas as pd
import numpy as np
import re
from typing import List, Dict, Any, Optional, Tuple, Union
from datetime import datetime

from utils.logger import logger, db_logger


class DataCleaner:
    """Utility class for cleaning and preprocessing data for export."""
    
    def __init__(self):
        """Initialize data cleaner."""
        self.max_excel_cell_length = 32767  # Excel cell character limit
        self.max_csv_field_length = 1000000  # Practical CSV field limit
        self.max_sheet_name_length = 31  # Excel sheet name limit
        
        # Define problematic characters for different formats
        self.problematic_chars = {
            'excel': ['\x00', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08'],
            'csv': ['\x00', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08'],
            'general': ['\x00', '\ufffd']  # NULL and replacement characters
        }
        
        # Common data type patterns
        self.data_patterns = {
            'phone': re.compile(r'^\+?[\d\s\-\(\)]+$'),
            'email': re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            'url': re.compile(r'^https?://[^\s]+$'),
            'numeric_string': re.compile(r'^\d+\.?\d*$')
        }
    
    def clean_data_for_export(self, df: pd.DataFrame, export_format: str = 'excel') -> pd.DataFrame:
        """
        Clean DataFrame for CSV/Excel export handling special characters and CLOB data.
        
        Args:
            df: Input DataFrame
            export_format: Target export format ('csv', 'excel', or 'json')
            
        Returns:
            Cleaned DataFrame
        """
        if df is None or df.empty:
            logger.warning("DataFrame is empty or None, returning empty DataFrame")
            return pd.DataFrame()
        
        try:
            logger.info(f"Cleaning data for {export_format} export - {len(df)} rows, {len(df.columns)} columns")
            
            cleaned_df = df.copy()
            
            # Clean column names
            cleaned_df.columns = self._clean_column_names(cleaned_df.columns.tolist())
            
            # Remove completely empty columns
            cleaned_df = self._remove_empty_columns(cleaned_df)
            
            # Process each column
            for column in cleaned_df.columns:
                logger.debug(f"Cleaning column: {column}")
                cleaned_df[column] = cleaned_df[column].apply(
                    lambda x: self._clean_cell_value(x, export_format)
                )
            
            # Handle NaN values
            cleaned_df = self._handle_nan_values(cleaned_df, export_format)
            
            # Format specific cleaning
            if export_format.lower() == 'excel':
                cleaned_df = self._excel_specific_cleaning(cleaned_df)
            elif export_format.lower() == 'csv':
                cleaned_df = self._csv_specific_cleaning(cleaned_df)
            
            logger.info(f"Data cleaning completed - {len(cleaned_df)} rows, {len(cleaned_df.columns)} columns")
            return cleaned_df
            
        except Exception as e:
            logger.error(f"Error during data cleaning: {e}")
            # Return original DataFrame on error
            return df.copy()
    
    def handle_clob_data(self, df: pd.DataFrame, clob_columns: List[str] = None) -> pd.DataFrame:
        """
        Handle CLOB data types for Excel export.
        
        Args:
            df: Input DataFrame
            clob_columns: List of column names containing CLOB data
            
        Returns:
            DataFrame with processed CLOB data
        """
        if df is None or df.empty:
            return df
        
        try:
            if not clob_columns:
                # Auto-detect potential CLOB columns
                clob_columns = self._detect_clob_columns(df)
            
            if not clob_columns:
                logger.debug("No CLOB columns detected")
                return df
            
            logger.info(f"Processing CLOB columns: {clob_columns}")
            
            processed_df = df.copy()
            
            for column in clob_columns:
                if column in processed_df.columns:
                    logger.debug(f"Processing CLOB column: {column}")
                    processed_df[column] = processed_df[column].apply(
                        self._truncate_clob_content
                    )
                else:
                    logger.warning(f"CLOB column '{column}' not found in DataFrame")
            
            return processed_df
            
        except Exception as e:
            logger.error(f"Error handling CLOB data: {e}")
            return df
    
    def remove_duplicates_with_logging(self, df: pd.DataFrame, primary_key: Union[str, List[str]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Remove duplicates and return both clean data and duplicates DataFrame.
        
        Args:
            df: Input DataFrame
            primary_key: Column name(s) to use for duplicate detection
            
        Returns:
            Tuple of (clean_df, duplicates_df)
        """
        if df is None or df.empty:
            logger.warning("DataFrame is empty or None")
            return pd.DataFrame(), pd.DataFrame()
        
        try:
            # Ensure primary_key is a list
            if isinstance(primary_key, str):
                key_columns = [primary_key]
            else:
                key_columns = primary_key
            
            # Check if all key columns exist
            missing_columns = [col for col in key_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Primary key columns not found: {missing_columns}")
                return df.copy(), pd.DataFrame()
            
            logger.info(f"Checking for duplicates based on primary key(s): {key_columns}")
            
            # Identify duplicates
            duplicate_mask = df.duplicated(subset=key_columns, keep='first')
            duplicates_df = df[duplicate_mask].copy()
            clean_df = df[~duplicate_mask].copy()
            
            logger.info(f"Found {len(duplicates_df)} duplicate records")
            logger.info(f"Clean dataset contains {len(clean_df)} records")
            
            return clean_df, duplicates_df
            
        except Exception as e:
            logger.error(f"Error removing duplicates: {e}")
            return df.copy(), pd.DataFrame()
    
    def standardize_data_types(self, df: pd.DataFrame, type_mapping: Dict[str, str] = None) -> pd.DataFrame:
        """
        Standardize data types in DataFrame.
        
        Args:
            df: Input DataFrame
            type_mapping: Optional mapping of column names to desired types
            
        Returns:
            DataFrame with standardized types
        """
        if df is None or df.empty:
            return df
        
        try:
            standardized_df = df.copy()
            
            # Apply custom type mapping if provided
            if type_mapping:
                for column, data_type in type_mapping.items():
                    if column in standardized_df.columns:
                        try:
                            if data_type.lower() == 'datetime':
                                standardized_df[column] = pd.to_datetime(standardized_df[column], errors='coerce')
                            elif data_type.lower() in ['int', 'integer']:
                                standardized_df[column] = pd.to_numeric(standardized_df[column], errors='coerce').astype('Int64')
                            elif data_type.lower() in ['float', 'decimal']:
                                standardized_df[column] = pd.to_numeric(standardized_df[column], errors='coerce')
                            elif data_type.lower() == 'string':
                                standardized_df[column] = standardized_df[column].astype(str)
                            elif data_type.lower() == 'bool':
                                standardized_df[column] = standardized_df[column].astype(bool)
                        except Exception as e:
                            logger.warning(f"Failed to convert column '{column}' to {data_type}: {e}")
            
            # Auto-detect and standardize common patterns
            for column in standardized_df.columns:
                if standardized_df[column].dtype == 'object':
                    standardized_df[column] = self._auto_standardize_column(standardized_df[column])
            
            logger.info("Data type standardization completed")
            return standardized_df
            
        except Exception as e:
            logger.error(f"Error standardizing data types: {e}")
            return df
    
    def validate_data_integrity(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate data integrity and return report.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dictionary with validation results
        """
        if df is None or df.empty:
            return {'status': 'empty', 'issues': ['DataFrame is empty or None']}
        
        try:
            validation_report = {
                'status': 'valid',
                'row_count': len(df),
                'column_count': len(df.columns),
                'issues': [],
                'warnings': [],
                'column_info': {}
            }
            
            for column in df.columns:
                col_info = {
                    'data_type': str(df[column].dtype),
                    'null_count': df[column].isnull().sum(),
                    'null_percentage': (df[column].isnull().sum() / len(df)) * 100,
                    'unique_count': df[column].nunique(),
                    'duplicate_count': len(df) - df[column].nunique()
                }
                
                # Check for high null percentage
                if col_info['null_percentage'] > 50:
                    validation_report['warnings'].append(
                        f"Column '{column}' has {col_info['null_percentage']:.1f}% null values"
                    )
                
                # Check for completely null columns
                if col_info['null_percentage'] == 100:
                    validation_report['issues'].append(f"Column '{column}' is completely null")
                
                # Check for single-value columns (excluding nulls)
                if col_info['unique_count'] == 1 and col_info['null_count'] < len(df):
                    validation_report['warnings'].append(f"Column '{column}' has only one unique value")
                
                validation_report['column_info'][column] = col_info
            
            # Overall status
            if validation_report['issues']:
                validation_report['status'] = 'issues_found'
            elif validation_report['warnings']:
                validation_report['status'] = 'warnings_found'
            
            logger.info(f"Data validation completed: {validation_report['status']}")
            return validation_report
            
        except Exception as e:
            logger.error(f"Error during data validation: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def _clean_column_names(self, columns: List[str]) -> List[str]:
        """Clean and normalize column names."""
        cleaned_columns = []
        
        for i, col in enumerate(columns):
            try:
                # Convert to string and strip whitespace
                clean_col = str(col).strip()
                
                # Replace special characters with underscores
                clean_col = re.sub(r'[^\w\s\-]', '_', clean_col)
                
                # Replace spaces and dashes with underscores
                clean_col = re.sub(r'[\s\-]+', '_', clean_col)
                
                # Remove multiple consecutive underscores
                clean_col = re.sub(r'_+', '_', clean_col)
                
                # Remove leading/trailing underscores
                clean_col = clean_col.strip('_')
                
                # Ensure column name is not empty
                if not clean_col:
                    clean_col = f"column_{i}"
                
                # Ensure uniqueness
                original_name = clean_col.lower()
                counter = 1
                while original_name in [c.lower() for c in cleaned_columns]:
                    original_name = f"{clean_col.lower()}_{counter}"
                    counter += 1
                
                cleaned_columns.append(original_name)
                
            except Exception as e:
                logger.warning(f"Error cleaning column name '{col}': {e}")
                cleaned_columns.append(f"column_{i}")
        
        return cleaned_columns
    
    def _clean_cell_value(self, value: Any, export_format: str) -> Any:
        """Clean individual cell value."""
        if pd.isna(value) or value is None:
            return value
        
        try:
            if isinstance(value, str):
                cleaned_value = value
                
                # Remove problematic characters
                problematic_chars = self.problematic_chars.get(export_format, self.problematic_chars['general'])
                for char in problematic_chars:
                    cleaned_value = cleaned_value.replace(char, '')
                
                # Handle line breaks based on export format
                if export_format == 'csv':
                    cleaned_value = cleaned_value.replace('\n', ' ').replace('\r', ' ')
                    cleaned_value = re.sub(r'\s+', ' ', cleaned_value)
                else:
                    # For Excel, preserve line breaks but clean them
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
            
            # Handle datetime objects
            elif isinstance(value, (datetime, pd.Timestamp)):
                return value.strftime('%Y-%m-%d %H:%M:%S') if export_format == 'csv' else value
            
            # Handle numpy types
            elif isinstance(value, (np.integer, np.floating)):
                return value.item()
            
            return value
            
        except Exception as e:
            logger.debug(f"Error cleaning cell value: {e}")
            return str(value) if value is not None else value
    
    def _handle_nan_values(self, df: pd.DataFrame, export_format: str) -> pd.DataFrame:
        """Handle NaN values in DataFrame based on export format."""
        try:
            cleaned_df = df.copy()
            
            for column in cleaned_df.columns:
                if cleaned_df[column].dtype == 'object':
                    # For text columns, replace NaN with empty string
                    cleaned_df[column] = cleaned_df[column].fillna('')
                elif export_format == 'csv':
                    # For CSV, replace numeric NaN with empty string for better readability
                    cleaned_df[column] = cleaned_df[column].fillna('')
                # For Excel, keep NaN as is for numeric columns
            
            return cleaned_df
            
        except Exception as e:
            logger.error(f"Error handling NaN values: {e}")
            return df
    
    def _remove_empty_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove completely empty columns."""
        try:
            initial_columns = len(df.columns)
            df_cleaned = df.dropna(axis=1, how='all')
            
            removed_count = initial_columns - len(df_cleaned.columns)
            if removed_count > 0:
                logger.info(f"Removed {removed_count} completely empty columns")
            
            return df_cleaned
            
        except Exception as e:
            logger.error(f"Error removing empty columns: {e}")
            return df
    
    def _excel_specific_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply Excel-specific cleaning rules."""
        try:
            cleaned_df = df.copy()
            
            # Handle Excel formula injection prevention
            for column in cleaned_df.select_dtypes(include=['object']).columns:
                cleaned_df[column] = cleaned_df[column].apply(
                    lambda x: f"'{x}" if isinstance(x, str) and x.startswith(('=', '+', '-', '@')) else x
                )
            
            return cleaned_df
            
        except Exception as e:
            logger.error(f"Error in Excel-specific cleaning: {e}")
            return df
    
    def _csv_specific_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply CSV-specific cleaning rules."""
        try:
            cleaned_df = df.copy()
            
            # Ensure no leading/trailing whitespace in string columns
            for column in cleaned_df.select_dtypes(include=['object']).columns:
                cleaned_df[column] = cleaned_df[column].apply(
                    lambda x: x.strip() if isinstance(x, str) else x
                )
            
            return cleaned_df
            
        except Exception as e:
            logger.error(f"Error in CSV-specific cleaning: {e}")
            return df
    
    def _detect_clob_columns(self, df: pd.DataFrame, min_length: int = 4000) -> List[str]:
        """Auto-detect columns that might contain CLOB data."""
        clob_columns = []
        
        try:
            for column in df.columns:
                if df[column].dtype == 'object':
                    # Get non-null string lengths
                    non_null_values = df[column].dropna()
                    if len(non_null_values) > 0:
                        string_lengths = non_null_values.astype(str).str.len()
                        
                        # Check if average length or max length exceeds threshold
                        if (string_lengths.mean() > min_length or 
                            string_lengths.max() > self.max_excel_cell_length):
                            clob_columns.append(column)
                            logger.debug(f"Detected potential CLOB column: {column} "
                                       f"(avg: {string_lengths.mean():.0f}, max: {string_lengths.max()})")
        
        except Exception as e:
            logger.error(f"Error detecting CLOB columns: {e}")
        
        return clob_columns
    
    def _truncate_clob_content(self, content: Any) -> str:
        """Truncate CLOB content for export."""
        if pd.isna(content) or content is None:
            return ''
        
        try:
            content_str = str(content)
            
            if len(content_str) > self.max_excel_cell_length:
                # Truncate and add indicator
                truncated = content_str[:self.max_excel_cell_length-50] + '\n[Content truncated...]'
                logger.debug(f"Truncated CLOB content from {len(content_str)} characters")
                return truncated
            
            return content_str
            
        except Exception as e:
            logger.debug(f"Error truncating CLOB content: {e}")
            return str(content) if content is not None else ''
    
    def _auto_standardize_column(self, series: pd.Series) -> pd.Series:
        """Auto-standardize column based on detected patterns."""
        try:
            # Sample non-null values
            sample_values = series.dropna().astype(str).head(100)
            
            if len(sample_values) == 0:
                return series
            
            # Check for numeric strings that should be numbers
            if all(self.data_patterns['numeric_string'].match(val) for val in sample_values):
                try:
                    return pd.to_numeric(series, errors='coerce')
                except:
                    pass
            
            # Check for phone numbers
            if all(self.data_patterns['phone'].match(val) for val in sample_values):
                # Keep as string but standardize format
                return series.astype(str).str.replace(r'[^\d+]', '', regex=True)
            
            # Check for emails
            if all(self.data_patterns['email'].match(val) for val in sample_values):
                # Keep as string but convert to lowercase
                return series.astype(str).str.lower()
            
            return series
            
        except Exception as e:
            logger.debug(f"Error in auto-standardization: {e}")
            return series


# Global data cleaner instance
data_cleaner = DataCleaner()