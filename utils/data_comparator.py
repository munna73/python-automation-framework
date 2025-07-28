"""
Data comparison utilities for comparing DataFrames and detecting differences.
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path
from utils.logger import logger, db_logger
from utils.data_cleaner import data_cleaner
from utils.export_utils import ExportUtils

class DataComparator:
    """Utility for comparing DataFrames and identifying differences."""
    
    def __init__(self):
        """Initialize data comparator."""
        self.export_utils = ExportUtils()
        self.comparison_results = {}
    
    def compare_dataframes(self,
                          source_df: pd.DataFrame,
                          target_df: pd.DataFrame,
                          primary_key: str,
                          comparison_name: str = "comparison",
                          tolerance: Dict[str, float] = None) -> Dict[str, Any]:
        """
        Compare two DataFrames and identify differences.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame
            primary_key: Primary key column for comparison
            comparison_name: Name for this comparison
            tolerance: Numeric tolerance for float columns
            
        Returns:
            Comparison results dictionary
        """
        try:
            db_logger.info(f"Starting DataFrame comparison: {comparison_name}")
            db_logger.info(f"Source records: {len(source_df)}, Target records: {len(target_df)}")
            
            # Clean and normalize data
            source_clean, source_duplicates = data_cleaner.remove_duplicates_with_logging(source_df, primary_key)
            target_clean, target_duplicates = data_cleaner.remove_duplicates_with_logging(target_df, primary_key)
            
            # Normalize column names
            source_clean.columns = [col.lower().strip() for col in source_clean.columns]
            target_clean.columns = [col.lower().strip() for col in target_clean.columns]
            
            # Ensure primary key is lowercase
            primary_key_norm = primary_key.lower().strip()
            
            # Find common columns
            common_columns = list(set(source_clean.columns) & set(target_clean.columns))
            db_logger.info(f"Common columns for comparison: {len(common_columns)}")
            
            # Perform comparison
            differences = self._find_differences(source_clean, target_clean, primary_key_norm, common_columns, tolerance)
            missing_in_target = self._find_missing_records(source_clean, target_clean, primary_key_norm)
            missing_in_source = self._find_missing_records(target_clean, source_clean, primary_key_norm)
            
            # Compile results
            results = {
                'comparison_name': comparison_name,
                'timestamp': datetime.now(),
                'source_count': len(source_df),
                'target_count': len(target_df),
                'source_clean_count': len(source_clean),
                'target_clean_count': len(target_clean),
                'source_duplicates_count': len(source_duplicates),
                'target_duplicates_count': len(target_duplicates),
                'common_columns': common_columns,
                'differences_count': len(differences),
                'missing_in_target_count': len(missing_in_target),
                'missing_in_source_count': len(missing_in_source),
                'match_percentage': self._calculate_match_percentage(
                    len(source_clean), len(target_clean), len(differences)
                ),
                'differences': differences,
                'missing_in_target': missing_in_target,
                'missing_in_source': missing_in_source,
                'source_duplicates': source_duplicates,
                'target_duplicates': target_duplicates
            }
            
            self.comparison_results[comparison_name] = results
            
            db_logger.info(f"Comparison completed: {results['differences_count']} differences found")
            db_logger.info(f"Match percentage: {results['match_percentage']:.2f}%")
            
            return results
            
        except Exception as e:
            db_logger.error(f"DataFrame comparison failed: {e}")
            raise
    
    def _find_differences(self,
                         source_df: pd.DataFrame,
                         target_df: pd.DataFrame,
                         primary_key: str,
                         common_columns: List[str],
                         tolerance: Dict[str, float] = None) -> pd.DataFrame:
        """Find differences between DataFrames."""
        try:
            # Merge on primary key
            merged = pd.merge(
                source_df[common_columns],
                target_df[common_columns],
                on=primary_key,
                suffixes=('_source', '_target'),
                how='inner'
            )
            
            differences = []
            
            for _, row in merged.iterrows():
                pk_value = row[primary_key]
                row_differences = []
                
                for col in common_columns:
                    if col == primary_key:
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
                    differences.append({
                        primary_key: pk_value,
                        'differences': row_differences,
                        'difference_count': len(row_differences)
                    })
            
            # Convert to DataFrame for easier handling
            if differences:
                diff_records = []
                for diff in differences:
                    for d in diff['differences']:
                        diff_records.append({
                            primary_key: diff[primary_key],
                            'column': d['column'],
                            'source_value': d['source_value'],
                            'target_value': d['target_value']
                        })
                
                return pd.DataFrame(diff_records)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            db_logger.error(f"Error finding differences: {e}")
            raise
    
    def _find_missing_records(self,
                            df1: pd.DataFrame,
                            df2: pd.DataFrame,
                            primary_key: str) -> pd.DataFrame:
        """Find records in df1 that are missing in df2."""
        try:
            missing = df1[~df1[primary_key].isin(df2[primary_key])]
            return missing.copy()
        except Exception as e:
            db_logger.error(f"Error finding missing records: {e}")
            return pd.DataFrame()
    
    def _values_differ(self,
                      val1: Any,
                      val2: Any,
                      column_name: str,
                      tolerance: Dict[str, float] = None) -> bool:
        """Check if two values differ, considering tolerance for numeric values."""
        # Handle null values
        if pd.isna(val1) and pd.isna(val2):
            return False
        if pd.isna(val1) or pd.isna(val2):
            return True
        
        # Handle numeric values with tolerance
        if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
            if tolerance and column_name in tolerance:
                return abs(val1 - val2) > tolerance[column_name]
            else:
                # Default small tolerance for floating point comparison
                return abs(val1 - val2) > 1e-9
        
        # Handle string comparison (case insensitive)
        if isinstance(val1, str) and isinstance(val2, str):
            return val1.strip().lower() != val2.strip().lower()
        
        # Direct comparison for other types
        return val1 != val2
    
    def _calculate_match_percentage(self,
                                  source_count: int,
                                  target_count: int,
                                  differences_count: int) -> float:
        """Calculate match percentage."""
        if source_count == 0 and target_count == 0:
            return 100.0
        
        total_comparisons = min(source_count, target_count)
        if total_comparisons == 0:
            return 0.0
        
        matches = total_comparisons - differences_count
        return (matches / total_comparisons) * 100.0
    
    def export_comparison_results(self,
                                comparison_name: str,
                                export_format: str = 'excel') -> str:
        """
        Export comparison results to file.
        
        Args:
            comparison_name: Name of the comparison to export
            export_format: Export format ('excel' or 'csv')
            
        Returns:
            Path to the exported file
        """
        try:
            if comparison_name not in self.comparison_results:
                raise ValueError(f"Comparison '{comparison_name}' not found")
            
            results = self.comparison_results[comparison_name]
            timestamp = results['timestamp'].strftime('%Y%m%d_%H%M%S')
            
            if export_format.lower() == 'excel':
                filepath = Path(f"output/exports/{comparison_name}_{timestamp}.xlsx")
                return self._export_to_excel(results, filepath)
            else:
                filepath = Path(f"output/exports/{comparison_name}_{timestamp}")
                return self._export_to_csv(results, filepath)
                
        except Exception as e:
            db_logger.error(f"Export failed for comparison {comparison_name}: {e}")
            raise
    
    def _export_to_excel(self, results: Dict[str, Any], filepath: Path) -> str:
        """Export comparison results to Excel file."""
        try:
            # Prepare data for export
            data_dict = {}
            
            # Summary sheet
            summary_data = {
                'Metric': [
                    'Comparison Name', 'Timestamp', 'Source Records', 'Target Records',
                    'Source Clean Records', 'Target Clean Records',
                    'Source Duplicates', 'Target Duplicates',
                    'Common Columns', 'Differences Found',
                    'Missing in Target', 'Missing in Source', 'Match Percentage'
                ],
                'Value': [
                    results['comparison_name'], results['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                    results['source_count'], results['target_count'],
                    results['source_clean_count'], results['target_clean_count'],
                    results['source_duplicates_count'], results['target_duplicates_count'],
                    len(results['common_columns']), results['differences_count'],
                    results['missing_in_target_count'], results['missing_in_source_count'],
                    f"{results['match_percentage']:.2f}%"
                ]
            }
            
            data_dict['Summary'] = pd.DataFrame(summary_data)
            
            # Differences sheet
            if not results['differences'].empty:
                data_dict['Differences'] = data_cleaner.clean_data_for_export(results['differences'])
            
            # Missing records sheets
            if not results['missing_in_target'].empty:
                data_dict['Missing_in_Target'] = data_cleaner.clean_data_for_export(results['missing_in_target'])
            
            if not results['missing_in_source'].empty:
                data_dict['Missing_in_Source'] = data_cleaner.clean_data_for_export(results['missing_in_source'])
            
            # Duplicates sheets
            if not results['source_duplicates'].empty:
                data_dict['Source_Duplicates'] = data_cleaner.clean_data_for_export(results['source_duplicates'])
            
            if not results['target_duplicates'].empty:
                data_dict['Target_Duplicates'] = data_cleaner.clean_data_for_export(results['target_duplicates'])
            
            # Export to Excel
            success = self.export_utils.write_to_excel_with_sheets(data_dict, str(filepath))
            
            if success:
                db_logger.info(f"Comparison results exported to Excel: {filepath}")
                return str(filepath)
            else:
                raise Exception("Excel export failed")
                
        except Exception as e:
            db_logger.error(f"Excel export error: {e}")
            raise
    
    def _export_to_csv(self, results: Dict[str, Any], base_filepath: Path) -> str:
        """Export comparison results to CSV files."""
        try:
            exported_files = []
            
            # Export differences
            if not results['differences'].empty:
                diff_filepath = f"{base_filepath}_differences.csv"
                if self.export_utils.write_to_csv_safe(results['differences'], diff_filepath):
                    exported_files.append(diff_filepath)
            
            # Export missing records
            if not results['missing_in_target'].empty:
                missing_target_filepath = f"{base_filepath}_missing_in_target.csv"
                if self.export_utils.write_to_csv_safe(results['missing_in_target'], missing_target_filepath):
                    exported_files.append(missing_target_filepath)
            
            if not results['missing_in_source'].empty:
                missing_source_filepath = f"{base_filepath}_missing_in_source.csv"
                if self.export_utils.write_to_csv_safe(results['missing_in_source'], missing_source_filepath):
                    exported_files.append(missing_source_filepath)
            
            db_logger.info(f"Comparison results exported to {len(exported_files)} CSV files")
            return ', '.join(exported_files)
            
        except Exception as e:
            db_logger.error(f"CSV export error: {e}")
            raise
    
    def get_comparison_summary(self, comparison_name: str = None) -> Dict[str, Any]:
        """Get summary of comparison results."""
        if comparison_name:
            if comparison_name in self.comparison_results:
                return {
                    'comparison_name': comparison_name,
                    'summary': {
                        'match_percentage': self.comparison_results[comparison_name]['match_percentage'],
                        'differences_count': self.comparison_results[comparison_name]['differences_count'],
                        'missing_in_target': self.comparison_results[comparison_name]['missing_in_target_count'],
                        'missing_in_source': self.comparison_results[comparison_name]['missing_in_source_count']
                    }
                }
            else:
                return {'error': f"Comparison '{comparison_name}' not found"}
        else:
            # Return summary of all comparisons
            summary = {}
            for name, results in self.comparison_results.items():
                summary[name] = {
                    'match_percentage': results['match_percentage'],
                    'differences_count': results['differences_count'],
                    'timestamp': results['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                }
            return summary

# Global data comparator instance
data_comparator = DataComparator()