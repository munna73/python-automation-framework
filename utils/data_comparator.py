"""
Data comparison utilities for comparing DataFrames and detecting differences.
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional, Union
from pathlib import Path
import os

from utils.logger import logger, db_logger

# Import optional dependencies with fallbacks
try:
    from utils.data_cleaner import data_cleaner
except ImportError:
    data_cleaner = None
    logger.warning("data_cleaner module not found, using basic cleaning")

try:
    from utils.export_utils import ExportUtils
except ImportError:
    ExportUtils = None
    logger.warning("export_utils module not found, using basic export")


class DataComparator:
    """Utility for comparing DataFrames and detecting differences."""
    
    def __init__(self):
        """Initialize data comparator."""
        if ExportUtils:
            self.export_utils = ExportUtils()
        else:
            self.export_utils = None
        self.comparison_results = {}
    
    def compare_datasets(self,
                        source_df: pd.DataFrame,
                        target_df: pd.DataFrame,
                        key_columns: Optional[Union[str, List[str]]] = None,
                        exclude_columns: Optional[List[str]] = None,
                        comparison_name: str = "dataset_comparison",
                        tolerance: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
        """
        Compare two datasets (alias for compare_dataframes for backward compatibility).
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            key_columns: Primary key column(s) for comparison
            exclude_columns: Columns to exclude from comparison
            comparison_name: Name for this comparison
            tolerance: Numeric tolerance for float columns
            
        Returns:
            Comparison results dictionary
        """
        db_logger.info(f"Starting dataset comparison: {comparison_name}")
        
        # Call the main comparison method
        return self.compare_dataframes(
            source_df=source_df,
            target_df=target_df,
            key_columns=key_columns,
            exclude_columns=exclude_columns,
            comparison_name=comparison_name,
            tolerance=tolerance
        )

    def compare_dataframes(self,
                          source_df: pd.DataFrame,
                          target_df: pd.DataFrame,
                          key_columns: Optional[Union[str, List[str]]] = None,
                          exclude_columns: Optional[List[str]] = None,
                          comparison_name: str = "comparison",
                          tolerance: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
        """
        Compare two DataFrames and identify differences.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            key_columns: Primary key column(s) for comparison
            exclude_columns: Columns to exclude from comparison
            comparison_name: Name for this comparison
            tolerance: Numeric tolerance for float columns
            
        Returns:
            Comparison results dictionary
        """
        try:
            db_logger.info(f"Starting DataFrame comparison: {comparison_name}")
            db_logger.info(f"Source records: {len(source_df)}, Target records: {len(target_df)}")
            
            # Handle key columns
            if isinstance(key_columns, str):
                key_columns = [key_columns]
            elif key_columns is None:
                # Try to auto-detect key columns
                key_columns = self._auto_detect_key_columns(source_df, target_df)
            
            # Clean and normalize data
            source_clean, source_duplicates = self._clean_dataframe(source_df, key_columns)
            target_clean, target_duplicates = self._clean_dataframe(target_df, key_columns)
            
            # Handle exclude columns
            exclude_columns = exclude_columns or []
            
            # Find common columns (excluding excluded ones)
            common_columns_set = set(source_clean.columns) & set(target_clean.columns)
            common_columns = [str(col) for col in common_columns_set if col not in exclude_columns]
            
            db_logger.info(f"Common columns for comparison: {len(common_columns)}")
            
            # Perform comparison
            differences = self._find_differences(source_clean, target_clean, key_columns, common_columns, tolerance)
            missing_in_target = self._find_missing_records(source_clean, target_clean, key_columns)
            missing_in_source = self._find_missing_records(target_clean, source_clean, key_columns)
            
            # Compile results
            results = {
                'comparison_name': comparison_name,
                'timestamp': datetime.now(),
                'summary': {
                    'source_rows': len(source_df),
                    'target_rows': len(target_df),
                    'source_clean_count': len(source_clean),
                    'target_clean_count': len(target_clean),
                    'source_duplicates_count': len(source_duplicates),
                    'target_duplicates_count': len(target_duplicates),
                    'differences_count': len(differences),
                    'missing_in_target_count': len(missing_in_target),
                    'missing_in_source_count': len(missing_in_source),
                    'total_differences': len(differences) + len(missing_in_target) + len(missing_in_source),
                    'match_percentage': self._calculate_match_percentage(
                        len(source_clean), len(target_clean), len(differences), 
                        len(missing_in_target), len(missing_in_source)
                    ),
                    'source_only': len(missing_in_target),
                    'target_only': len(missing_in_source),
                    'modified': len(differences)
                },
                'metadata': {
                    'key_columns': key_columns,
                    'common_columns': common_columns,
                    'exclude_columns': exclude_columns,
                    'tolerance': tolerance
                },
                'differences': {
                    'field_differences': differences,
                    'missing_in_target': missing_in_target,
                    'missing_in_source': missing_in_source,
                    'source_duplicates': source_duplicates,
                    'target_duplicates': target_duplicates
                }
            }
            
            self.comparison_results[comparison_name] = results
            
            total_diff = results['summary']['total_differences']
            match_pct = results['summary']['match_percentage']
            
            db_logger.info(f"Comparison completed: {total_diff} total differences found")
            db_logger.info(f"Match percentage: {match_pct:.2f}%")
            
            return results
            
        except Exception as e:
            db_logger.error(f"DataFrame comparison failed: {e}")
            raise
    
    def compare_dataframes_with_tolerance(self,
                                        source_df: pd.DataFrame,
                                        target_df: pd.DataFrame,
                                        key_columns: Optional[Union[str, List[str]]] = None,
                                        numeric_tolerance: float = 0.01,
                                        exclude_columns: Optional[List[str]] = None,
                                        comparison_name: str = "tolerance_comparison") -> Dict[str, Any]:
        """
        Compare DataFrames with numeric tolerance (compatibility method for step definitions).
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            key_columns: Primary key column(s) for comparison
            numeric_tolerance: Tolerance for numeric comparisons
            exclude_columns: Columns to exclude from comparison
            comparison_name: Name for this comparison
            
        Returns:
            Comparison results dictionary
        """
        # Convert numeric tolerance to tolerance dict for all numeric columns
        tolerance = {}
        
        # Apply tolerance to all numeric columns in both dataframes
        numeric_cols = []
        for col in source_df.select_dtypes(include=[np.number]).columns:
            if col not in (exclude_columns or []):
                numeric_cols.append(col)
                tolerance[col] = numeric_tolerance
        
        for col in target_df.select_dtypes(include=[np.number]).columns:
            if col not in (exclude_columns or []) and col not in tolerance:
                tolerance[col] = numeric_tolerance
        
        db_logger.info(f"Applying numeric tolerance {numeric_tolerance} to columns: {list(tolerance.keys())}")
        
        return self.compare_dataframes(
            source_df=source_df,
            target_df=target_df,
            key_columns=key_columns,
            exclude_columns=exclude_columns,
            comparison_name=comparison_name,
            tolerance=tolerance
        )
    
    def _auto_detect_key_columns(self, source_df: pd.DataFrame, target_df: pd.DataFrame) -> List[str]:
        """Auto-detect key columns based on common patterns."""
        potential_keys = []
        
        # Look for columns with 'id' in name
        for col in source_df.columns:
            if 'id' in col.lower() and col in target_df.columns:
                potential_keys.append(col)
        
        # If no ID columns, look for unique columns
        if not potential_keys:
            for col in source_df.columns:
                if col in target_df.columns:
                    if (source_df[col].nunique() == len(source_df) and 
                        target_df[col].nunique() == len(target_df)):
                        potential_keys.append(col)
                        break
        
        # Default to first column if nothing found
        if not potential_keys and len(source_df.columns) > 0:
            first_col = source_df.columns[0]
            if first_col in target_df.columns:
                potential_keys = [first_col]
        
        db_logger.info(f"Auto-detected key columns: {potential_keys}")
        return potential_keys # type: ignore
    
    def _clean_dataframe(self, df: pd.DataFrame, key_columns: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Clean DataFrame and return cleaned data and duplicates."""
        try:
            if data_cleaner and key_columns:
                # Use data_cleaner if available
                primary_key = key_columns[0]  # Use first key column for duplicate detection
                clean_df, duplicates_df = data_cleaner.remove_duplicates_with_logging(df, primary_key)
                # Ensure duplicates_df is a DataFrame
                if not isinstance(duplicates_df, pd.DataFrame):
                    duplicates_df = pd.DataFrame()
            else:
                # Basic cleaning
                clean_df = df.copy()
                duplicates_df = pd.DataFrame()
                
                # Remove duplicates based on key columns if available
                if key_columns:
                    duplicates_mask = clean_df.duplicated(subset=key_columns, keep=False)
                    duplicates_df = clean_df[duplicates_mask].copy()
                    clean_df = clean_df.drop_duplicates(subset=key_columns, keep='first')
            
            # Normalize column names
            clean_df.columns = [col.lower().strip() for col in clean_df.columns]
            if not duplicates_df.empty:
                duplicates_df.columns = [col.lower().strip() for col in duplicates_df.columns]
            
            return clean_df, duplicates_df # type: ignore
            
        except Exception as e:
            db_logger.warning(f"Error during DataFrame cleaning: {e}, using original data")
            return df.copy(), pd.DataFrame()
    
    def _find_differences(self,
                         source_df: pd.DataFrame,
                         target_df: pd.DataFrame,
                         key_columns: List[str],
                         common_columns: List[str],
                         tolerance: Optional[Dict[str, float]] = None) -> pd.DataFrame:
        """Find differences between DataFrames."""
        try:
            if not key_columns:
                db_logger.warning("No key columns provided, cannot perform detailed comparison")
                return pd.DataFrame()
            
            # Normalize key column names
            key_columns_norm = [col.lower().strip() for col in key_columns]
            
            # Merge on key columns
            merged = pd.merge(
                source_df[common_columns],
                target_df[common_columns],
                on=key_columns_norm,
                suffixes=('_source', '_target'),
                how='inner'
            )
            
            if merged.empty:
                db_logger.info("No matching records found for comparison")
                return pd.DataFrame()
            
            differences = []
            
            for _, row in merged.iterrows():
                # Get key values
                key_values = {}
                for key_col in key_columns_norm:
                    key_values[key_col] = row[key_col]
                
                row_differences = []
                
                for col in common_columns:
                    if col in key_columns_norm:
                        continue
                    
                    source_col = f"{col}_source"
                    target_col = f"{col}_target"
                    
                    source_val = row.get(source_col)
                    target_val = row.get(target_col)
                    
                    if self._values_differ(source_val, target_val, col, tolerance):
                        row_differences.append({
                            'column': col,
                            'source_value': source_val,
                            'target_value': target_val
                        })
                
                if row_differences:
                    diff_record = key_values.copy()
                    diff_record.update({
                        'differences': row_differences,
                        'difference_count': len(row_differences)
                    })
                    differences.append(diff_record)
            
            # Convert to DataFrame for easier handling
            if differences:
                diff_records = []
                for diff in differences:
                    for d in diff['differences']:
                        record = {}
                        # Add key columns
                        for key_col in key_columns_norm:
                            record[key_col] = diff[key_col]
                        # Add difference details
                        record.update({
                            'column': d['column'],
                            'source_value': d['source_value'],
                            'target_value': d['target_value']
                        })
                        diff_records.append(record)
                
                return pd.DataFrame(diff_records)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            db_logger.error(f"Error finding differences: {e}")
            return pd.DataFrame()
    
    def _find_missing_records(self,
                            df1: pd.DataFrame,
                            df2: pd.DataFrame,
                            key_columns: List[str]) -> pd.DataFrame:
        """Find records in df1 that are missing in df2."""
        try:
            if not key_columns:
                return pd.DataFrame()
            
            key_columns_norm = [col.lower().strip() for col in key_columns]
            
            # Create composite key for comparison
            if len(key_columns_norm) == 1:
                key_col = key_columns_norm[0]
                missing = df1[~df1[key_col].isin(df2[key_col])].copy()
            else:
                # For multiple key columns, create a composite key
                df1_keys = df1[key_columns_norm].apply(lambda x: '|'.join(x.astype(str)), axis=1)
                df2_keys = df2[key_columns_norm].apply(lambda x: '|'.join(x.astype(str)), axis=1)
                missing = df1[~df1_keys.isin(df2_keys)].copy()
            
            return missing # type: ignore
            
        except Exception as e:
            db_logger.error(f"Error finding missing records: {e}")
            return pd.DataFrame()
    
    def _values_differ(self,
                      val1: Any,
                      val2: Any,
                      column_name: str,
                      tolerance: Optional[Dict[str, float]] = None) -> bool:
        """Check if two values differ, considering tolerance for numeric values."""
        # Handle null values
        if pd.isna(val1) and pd.isna(val2):
            return False
        if pd.isna(val1) or pd.isna(val2):
            return True
        
        # Handle numeric values with tolerance
        if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
            if tolerance and column_name in tolerance:
                return abs(float(val1) - float(val2)) > tolerance[column_name]
            else:
                # Default small tolerance for floating point comparison
                return abs(float(val1) - float(val2)) > 1e-9
        
        # Handle string comparison (case insensitive)
        if isinstance(val1, str) and isinstance(val2, str):
            return val1.strip().lower() != val2.strip().lower()
        
        # Direct comparison for other types
        return val1 != val2
    
    def _calculate_match_percentage(self,
                                  source_count: int,
                                  target_count: int,
                                  differences_count: int,
                                  missing_in_target: int = 0,
                                  missing_in_source: int = 0) -> float:
        """Calculate match percentage."""
        if source_count == 0 and target_count == 0:
            return 100.0
        
        total_records = max(source_count, target_count)
        if total_records == 0:
            return 100.0
        
        total_differences = differences_count + missing_in_target + missing_in_source
        matches = total_records - total_differences
        
        return max(0.0, (matches / total_records) * 100.0)
    
    def export_comparison_results(self,
                                comparison_name: str,
                                export_format: str = 'excel',
                                output_dir: str = 'output/exports') -> str:
        """
        Export comparison results to file.
        
        Args:
            comparison_name: Name of the comparison to export
            export_format: Export format ('excel', 'csv', or 'json')
            output_dir: Output directory for files
            
        Returns:
            Path to the exported file(s)
        """
        try:
            if comparison_name not in self.comparison_results:
                raise ValueError(f"Comparison '{comparison_name}' not found")
            
            results = self.comparison_results[comparison_name]
            timestamp = results['timestamp'].strftime('%Y%m%d_%H%M%S')
            
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            
            if export_format.lower() == 'excel':
                filepath = os.path.join(output_dir, f"{comparison_name}_{timestamp}.xlsx")
                return self._export_to_excel(results, filepath)
            elif export_format.lower() == 'json':
                filepath = os.path.join(output_dir, f"{comparison_name}_{timestamp}.json")
                return self._export_to_json(results, filepath)
            else:
                base_filepath = os.path.join(output_dir, f"{comparison_name}_{timestamp}")
                return self._export_to_csv(results, base_filepath)
                
        except Exception as e:
            db_logger.error(f"Export failed for comparison {comparison_name}: {e}")
            raise
    
    def _export_to_excel(self, results: Dict[str, Any], filepath: str) -> str:
        """Export comparison results to Excel file."""
        try:
            if self.export_utils:
                # Use export_utils if available
                data_dict = self._prepare_export_data(results)
                success = self.export_utils.write_to_excel_with_sheets(data_dict, filepath)
                if success:
                    db_logger.info(f"Comparison results exported to Excel: {filepath}")
                    return filepath
                else:
                    raise Exception("Excel export failed")
            else:
                # Basic Excel export using pandas
                data_dict = self._prepare_export_data(results)
                
                with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                    for sheet_name, df in data_dict.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                db_logger.info(f"Comparison results exported to Excel: {filepath}")
                return filepath
                
        except Exception as e:
            db_logger.error(f"Excel export error: {e}")
            raise
    
    def _export_to_csv(self, results: Dict[str, Any], base_filepath: str) -> str:
        """Export comparison results to CSV files."""
        try:
            exported_files = []
            data_dict = self._prepare_export_data(results)
            
            for sheet_name, df in data_dict.items():
                if not df.empty:
                    csv_filepath = f"{base_filepath}_{sheet_name}.csv"
                    df.to_csv(csv_filepath, index=False)
                    exported_files.append(csv_filepath)
            
            db_logger.info(f"Comparison results exported to {len(exported_files)} CSV files")
            return ', '.join(exported_files)
            
        except Exception as e:
            db_logger.error(f"CSV export error: {e}")
            raise
    
    def _export_to_json(self, results: Dict[str, Any], filepath: str) -> str:
        """Export comparison results to JSON file."""
        try:
            import json
            
            # Convert DataFrames to dictionaries for JSON serialization
            export_data = {
                'summary': results['summary'],
                'metadata': results['metadata'],
                'timestamp': results['timestamp'].isoformat()
            }
            
            # Add difference data as records
            differences = results['differences']
            if not differences['field_differences'].empty:
                export_data['field_differences'] = differences['field_differences'].to_dict('records')
            
            if not differences['missing_in_target'].empty:
                export_data['missing_in_target'] = differences['missing_in_target'].to_dict('records')
            
            if not differences['missing_in_source'].empty:
                export_data['missing_in_source'] = differences['missing_in_source'].to_dict('records')
            
            with open(filepath, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            db_logger.info(f"Comparison results exported to JSON: {filepath}")
            return filepath
            
        except Exception as e:
            db_logger.error(f"JSON export error: {e}")
            raise
    
    def _prepare_export_data(self, results: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Prepare comparison results for export."""
        data_dict = {}
        
        # Summary sheet
        summary_data = []
        for key, value in results['summary'].items():
            summary_data.append({'Metric': key, 'Value': value})
        data_dict['Summary'] = pd.DataFrame(summary_data)
        
        # Differences sheets
        differences = results['differences']
        
        if not differences['field_differences'].empty:
            data_dict['Field_Differences'] = differences['field_differences']
        
        if not differences['missing_in_target'].empty:
            data_dict['Missing_in_Target'] = differences['missing_in_target']
        
        if not differences['missing_in_source'].empty:
            data_dict['Missing_in_Source'] = differences['missing_in_source']
        
        if not differences['source_duplicates'].empty:
            data_dict['Source_Duplicates'] = differences['source_duplicates']
        
        if not differences['target_duplicates'].empty:
            data_dict['Target_Duplicates'] = differences['target_duplicates']
        
        return data_dict
    
    def get_comparison_summary(self, comparison_name: str = None) -> Dict[str, Any]: # type: ignore
        """Get summary of comparison results."""
        if comparison_name:
            if comparison_name in self.comparison_results:
                return {
                    'comparison_name': comparison_name,
                    'summary': self.comparison_results[comparison_name]['summary']
                }
            else:
                return {'error': f"Comparison '{comparison_name}' not found"}
        else:
            # Return summary of all comparisons
            summary = {}
            for name, results in self.comparison_results.items():
                summary[name] = {
                    'match_percentage': results['summary']['match_percentage'],
                    'total_differences': results['summary']['total_differences'],
                    'timestamp': results['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                }
            return summary
    
    def clear_results(self):
        """Clear all stored comparison results."""
        self.comparison_results.clear()
        db_logger.info("Cleared all comparison results")


# Global data comparator instance
data_comparator = DataComparator()